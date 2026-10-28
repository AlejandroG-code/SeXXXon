# Architecture & Reliability Specification

## 1. System Topology Overview

The asynchronous report generation system separates HTTP client request handling from background batch report compilation using RabbitMQ, PostgreSQL, FastAPI, and dedicated worker processes.

```
+-----------------------------------------------------------------------------------+
|                                  Client Tier                                      |
|  - Web Frontend Dashboard (SPA: Tailwind + Alpine.js)                             |
|  - REST API Consumers (curl, scripts, microservices)                              |
+------------------------------------------+----------------------------------------+
                                           |
                                           | HTTP Requests (REST / JSON)
                                           v
+-----------------------------------------------------------------------------------+
|                                FastAPI API Server                                 |
|  - Validates request payload & date boundaries                                    |
|  - Idempotency-Key validation & deduplication                                     |
|  - Records initial report state ('pending') in PostgreSQL                         |
|  - Publishes job to RabbitMQ with Publisher Confirms                              |
|  - Conditionally advances status to 'queued'                                      |
|  - Exposes report lifecycle status, CSV downloads, and reconciliation API         |
+---------------------+---------------------------------------+---------------------+
                      |                                       |
    Publisher Confirms| Persistent Messages                   | Read/Write
    mandatory=True    v                                       v
+---------------------+-------------------+   +---------------+---------------------+
|              RabbitMQ Broker            |   |         PostgreSQL Database         |
|  Exchanges:                             |   |  - sales: >10,000 synthetic rows    |
|   - reports.direct (Direct, Durable)    |   |  - reports: state machine, attempt  |
|   - reports.retry.direct (Direct)       |   |    counts, metrics, file paths,     |
|   - reports.dlx (Direct, Durable)       |   |    and error details                |
|  Queues:                                |   +---------------+---------------------+
|   - reports.queue (Durable, prefetch=1) |                   ^
|   - reports.retry.queue (TTL=5s, DLX)   |                   |
|   - reports.dlq (Dead Letter Queue)     |                   | SQL Queries &
+---------------------+-------------------+                   | Status Updates
                      |                                       |
                      | basic_consume(prefetch=1)             |
                      v                                       |
+---------------------+---------------------------------------+---------------------+
|                             Worker Pool (Consumers)                               |
|  - ReportWorker 1 (container: report_worker_1)                                    |
|  - ReportWorker 2 (container: report_worker_2)                                    |
|  - ReportWorker 3 (container: report_worker_3)                                    |
|                                                                                   |
|  Processing Steps:                                                                |
|   1. Parse message & atomically update state to 'processing' (attempts + 1)       |
|   2. Check deduplication: if already 'completed', ACK immediately and skip        |
|   3. Apply configurable processing delay (5s - 15s)                               |
|   4. Stream ordered sales records (sale_date ASC, id ASC) to temp CSV file        |
|   5. Atomically rename .tmp_<id>_<pid>.csv -> <id>.csv                            |
|   6. Update database record: status='completed', row_count, file_size             |
|   7. Send manual basic_ack to broker ONLY after DB commit                         |
+------------------------------------------+----------------------------------------+
                                           |
                                           | Atomic write & replace
                                           v
+-----------------------------------------------------------------------------------+
|                             Shared File Storage Volume                            |
|  - Local Mount / Docker Volume: /app/storage/reports/                             |
|  - Canonical artifacts: <report_id>.csv                                           |
+-----------------------------------------------------------------------------------+
```

---

## 2. RabbitMQ Exchange & Queue Architecture

```
                      +-------------------+
                      |   POST /reports   |
                      +---------+---------+
                                |
                 publish_job    | routing_key: reports.process
                                v
                     +----------+----------+
                     |    reports.direct   |
                     |  (Direct, Durable)  |
                     +----------+----------+
                                |
                                | binding: reports.process
                                v
                     +----------+----------+
                     |    reports.queue    | <------+
                     |  (Durable, Main)    |        |
                     +----------+----------+        |
                                |                   |
                 basic_consume  | prefetch=1        | Dead-Letter back
                                v                   | after TTL expiry
                     +----------+----------+        |
                     |     Worker Pool     |        |
                     +----+-----------+----+        |
                          |           |             |
        Transient Failure |           | Permanent   |
       (attempts < 3)     |           | or Max Retries
                          v           v             |
             +------------+---+   +---+------------+|
             |  reports.retry |   |   reports.dlx  ||
             |     .direct    |   | (Dead-Letter)  ||
             +--------+-------+   +---+------------+|
                      |               |             |
     routing_key:     |               |             |
     reports.retry    v               v             |
             +--------+-------+   +---+------------+|
             |  reports.retry |   |   reports.dlq  ||
             |     .queue     |   | (DLQ Inspection)|
             | (TTL = 5000ms) +---+-----------------+
             +----------------+
```

### Queue Declarations & Properties
1. **`reports.direct`**:
   - Type: `direct`, Durable: `true`
   - Purpose: Routes incoming report jobs to the primary queue.
2. **`reports.queue`**:
   - Type: Standard queue, Durable: `true`
   - Arguments:
     - `x-dead-letter-exchange`: `reports.dlx`
     - `x-dead-letter-routing-key`: `reports.failed`
   - Purpose: Holds pending report jobs awaiting worker pickup.
3. **`reports.retry.direct`**:
   - Type: `direct`, Durable: `true`
   - Purpose: Routes retrying messages into the delayed holding queue.
4. **`reports.retry.queue`**:
   - Type: Standard queue with TTL, Durable: `true`
   - Arguments:
     - `x-message-ttl`: `5000` (5 seconds delay)
     - `x-dead-letter-exchange`: `reports.direct`
     - `x-dead-letter-routing-key`: `reports.process`
   - Purpose: Non-blocking delayed backoff without busy looping. When the 5-second TTL expires, RabbitMQ dead-letters the message back to `reports.direct`, which re-delivers it to `reports.queue`.
5. **`reports.dlx` & `reports.dlq`**:
   - Type: `direct`, Durable: `true`
   - Purpose: Holds unrecoverable or permanently failed jobs for operator inspection and diagnostic triage.

---

## 3. Database Models & Schema

### PostgreSQL Schema DDL

```sql
-- Sales Dataset Table
CREATE TABLE sales (
    id BIGSERIAL PRIMARY KEY,
    sale_date DATE NOT NULL,
    customer_name VARCHAR(255) NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(10, 2) NOT NULL,
    total_amount NUMERIC(10, 2) NOT NULL
);

CREATE INDEX idx_sales_date_id ON sales (sale_date, id);

-- Report Lifecycle Table
CREATE TABLE reports (
    id UUID PRIMARY KEY,
    idempotency_key VARCHAR(255) UNIQUE,
    report_type VARCHAR(50) NOT NULL DEFAULT 'sales',
    parameters JSONB NOT NULL,
    status VARCHAR(30) NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    row_count INTEGER,
    file_path VARCHAR(500),
    file_size_bytes BIGINT,
    error_message TEXT,
    error_category VARCHAR(50),
    worker_id VARCHAR(100),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    queued_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);

CREATE INDEX idx_reports_idempotency ON reports (idempotency_key);
CREATE INDEX idx_reports_status_created ON reports (status, created_at);
```

---

## 4. Message Payload Schemas

### Standard Report Job Message
Published to `reports.direct` with routing key `reports.process`:
```json
{
  "report_id": "07c5a69a-6534-4b53-8321-7294863c0e11",
  "attempt": 1,
  "simulate_failure": null
}
```

### Retry Message
Published to `reports.retry.direct` with routing key `reports.retry`:
```json
{
  "report_id": "07c5a69a-6534-4b53-8321-7294863c0e11",
  "attempt": 2,
  "simulate_failure": "transient"
}
```

### Dead-Letter Queue (DLQ) Message
Published to `reports.dlx` with routing key `reports.failed`:
```json
{
  "report_id": "41c543a4-921a-4712-b258-4720912cb841",
  "attempt": 3,
  "error_message": "Max attempts (3) reached. Last error: Simulated permanent corrupt data format encountered",
  "error_category": "permanent"
}
```

---

## 5. Job Lifecycle State Machine

```
   [ Client Request ]
           |
           v
      +---------+
      | pending |  <-- Saved in PostgreSQL before broker publish
      +----+----+
           |
   [ Publish Confirmed ]
           v
      +---------+
      | queued  |  <-- Stored in RabbitMQ reports.queue
      +----+----+
           |
   [ Worker basic_consume ]
           v
    +------------+
    | processing |  <-- Worker claims job atomically, increments attempt
    +----+---+---+
         |   |
         |   +-- [ Transient error & attempt < 3 ]
         |              |
         |              v
         |        +------------+
         |        |  retrying  |  --> [ Published to reports.retry.queue (TTL 5s) ]
         |        +-----+------+          |
         |              +-----------------+ (TTL expired: dead-letter to reports.queue)
         |
         +-- [ Error & attempt >= 3 OR Permanent error ]
         |              |
         |              v
         |        +------------+
         |        |   failed   |  --> [ Published to reports.dlq for inspection ]
         |        +------------+
         |
    [ CSV file written + DB committed ]
         |
         v
    +-----------+
    | completed |  <-- File ready at /reports/{id}/download, basic_ack sent
    +-----------+
```
