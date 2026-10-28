# Asynchronous Report Generator with RabbitMQ

[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue.svg)](https://www.postgresql.org/)
[![RabbitMQ](https://img.shields.io/badge/RabbitMQ-3.13-orange.svg)](https://www.rabbitmq.com/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115-green.svg)](https://fastapi.tiangolo.com/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg)](https://www.docker.com/)

A production-grade, asynchronous sales report generation system built with **RabbitMQ**, **PostgreSQL**, **FastAPI**, background worker pools, and an interactive **single-page web dashboard**.

---

## 1. Architecture Diagram

```
                     +----------------------------------------+
                     |           Client / Frontend            |
                     |       (HTML5, Tailwind, Alpine.js)     |
                     +-------------------+--------------------+
                                         |
                       HTTP POST / GET   |   HTTP File Download
                                         v
                     +-------------------+--------------------+
                     |               API Server               |
                     |         (FastAPI / Uvicorn)            |
                     +----------+-------------------+---------+
                                |                   |
                 Publisher      |                   | Read / Write
                 Confirms       |                   | Report State
                 (mandatory=True)                   |
                                v                   v
                     +----------+---------+  +------+---------+
                     |      RabbitMQ      |  |   PostgreSQL   |
                     |  - reports.direct  |  |  - sales (15k) |
                     |  - reports.queue   |  |  - reports     |
                     |  - reports.retry   |  +------+---------+
                     |  - reports.dlq     |         ^
                     +----------+---------+         |
                                |                   | Query &
                                | basic_consume     | Status Update
                                v (prefetch=1)      |
                     +----------+-------------------+---------+
                     |           Worker Pool                  |
                     |  [Worker 1]   [Worker 2]   [Worker 3]  |
                     +-------------------+--------------------+
                                         |
                                         | Atomic write (.tmp -> .csv)
                                         v
                     +-------------------+--------------------+
                     |      Persistent File Storage           |
                     |         (/storage/reports)             |
                     +----------------------------------------+
```

---

## 2. Key Architecture & Reliability Guarantees

1. **Non-blocking Request Handling:** The API accepts report generation requests in under 30 milliseconds, issues a UUID `report_id`, and returns `HTTP 202 Accepted` without keeping the client connection waiting.
2. **Publisher Confirms & Durability:**
   - RabbitMQ queues (`reports.queue`, `reports.retry.queue`, `reports.dlq`) and exchanges are declared **durable**.
   - Messages are published with persistent delivery mode (`delivery_mode=2`) and verified via **Publisher Confirms** (`confirm_delivery()`).
   - If broker publication fails, the report record is retained as `pending` in PostgreSQL for subsequent reconciliation (`POST /reports/reconcile`).
3. **Manual Consumer Acknowledgments:**
   - Workers use manual acknowledgments (`basic_ack`).
   - Acknowledgments are dispatched **only after** the CSV file is generated, atomically renamed, and the database status is committed as `completed`.
4. **Fair Workload Distribution:** Workers configure `channel.basic_qos(prefetch_count=1)` to prevent round-robin monopolization and evenly distribute jobs across active workers.
5. **Bounded Retries with Backoff:**
   - Transient failures trigger up to 3 bounded processing attempts.
   - Retries use a dedicated delayed holding queue (`reports.retry.queue`) with a 5-second TTL that automatically dead-letters back to the main processing queue, avoiding tight busy-requeue loops.
6. **Dead-Letter Queue (DLQ):** Messages exceeding 3 attempts or encountering permanent unrecoverable errors are routed to `reports.dlq` for operator inspection.
7. **Idempotency & Deduplication:**
   - Supported via the `Idempotency-Key` HTTP header. Identical requests return the existing report; requests with modified parameters return `HTTP 409 Conflict`.
   - Workers verify state before re-running generation; duplicate queue messages are safely acknowledged without re-processing or corrupting existing files.
8. **Crash & Worker Disconnect Recovery:** If a worker dies while holding an unacknowledged job, RabbitMQ detects the TCP disconnection and immediately redelivers the message to an available worker.

---

## 3. Database Model & Message Schema

### Database Schema (PostgreSQL)
- **`sales` Table:** Stores 15,000 synthetic records with `id` (BigSerial PK), `sale_date` (Date, indexed), `customer_name`, `product_name`, `quantity`, `unit_price`, and `total_amount`.
- **`reports` Table:** Tracks lifecycle with `id` (UUID PK), `idempotency_key` (Unique), `report_type`, `parameters` (JSONB), `status` (`pending`, `queued`, `processing`, `retrying`, `completed`, `failed`), `attempts`, `row_count`, `file_path`, `file_size_bytes`, `worker_id`, `created_at`, `updated_at`, `started_at`, and `completed_at`.

### Message Payload Schema
```json
{
  "report_id": "07c5a69a-6534-4b53-8321-7294863c0e11",
  "attempt": 1,
  "simulate_failure": null
}
```

---

## 4. Prerequisites & Environment Setup

### System Requirements
- **Docker** (version 24.0 or higher) and **Docker Compose** (version 2.20 or higher)
- **Python** 3.11+ (optional, for local development outside Docker)

### Environment Configuration
Copy the template configuration file:
```bash
cp .env.example .env
```

Contents of `.env`:
```ini
DATABASE_URL=postgresql://postgres:postgres_password@localhost:5434/reports_db
RABBITMQ_URL=amqp://guest:guest@localhost:5672/
RABBITMQ_MANAGEMENT_URL=http://localhost:15672/
STORAGE_DIR=./storage/reports
WORKER_PROCESSING_DELAY_SECONDS=5
RETRY_DELAY_SECONDS=5
MAX_ATTEMPTS=3
LOG_LEVEL=INFO
```

---

## 5. Startup Commands

### Start All Services (API, PostgreSQL, RabbitMQ, 3 Workers)
```bash
docker compose up -d --build
```

### Verify Service Status
```bash
docker compose ps
```
The stack will be accessible at:
- **Web Frontend Dashboard:** [http://localhost:8000](http://localhost:8000)
- **Interactive Swagger API Docs:** [http://localhost:8000/docs](http://localhost:8000/docs)
- **RabbitMQ Management Dashboard:** [http://localhost:15672](http://localhost:15672) (Username: `guest`, Password: `guest`)

---

## 6. Seed Instructions (15,000 Records)

Populate the database with synthetic sales data including special CSV characters (quotes, commas):
```bash
docker compose exec api python -m scripts.seed_sales --count 15000
```
Or via local Python environment:
```bash
python -m scripts.seed_sales --count 15000
```

---

## 7. API Examples with Curl

### 1. Request a Sales Report (Asynchronous)
```bash
curl -X POST http://localhost:8000/reports \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: req-idem-001" \
  -d '{
    "report_type": "sales",
    "start_date": "2025-01-01",
    "end_date": "2025-03-31"
  }'
```
**Response (HTTP 202 Accepted):**
```json
{
  "report_id": "07c5a69a-6534-4b53-8321-7294863c0e11",
  "status": "queued",
  "status_url": "/reports/07c5a69a-6534-4b53-8321-7294863c0e11",
  "message": "Report generation request accepted."
}
```

### 2. Poll Report Status
```bash
curl http://localhost:8000/reports/07c5a69a-6534-4b53-8321-7294863c0e11
```
**Response (When Completed):**
```json
{
  "report_id": "07c5a69a-6534-4b53-8321-7294863c0e11",
  "status": "completed",
  "status_url": "/reports/07c5a69a-6534-4b53-8321-7294863c0e11",
  "download_url": "/reports/07c5a69a-6534-4b53-8321-7294863c0e11/download",
  "report_type": "sales",
  "parameters": {
    "report_type": "sales",
    "start_date": "2025-01-01",
    "end_date": "2025-03-31"
  },
  "attempts": 1,
  "row_count": 1883,
  "file_size_bytes": 166820,
  "created_at": "2026-10-28T01:31:11Z",
  "updated_at": "2026-10-28T01:31:16Z",
  "worker_id": "worker-1",
  "error": null,
  "error_category": null
}
```

### 3. Download Completed CSV File
```bash
curl -OJ http://localhost:8000/reports/07c5a69a-6534-4b53-8321-7294863c0e11/download
```

### 4. Reconcile Pending Jobs (After Broker Recovery)
```bash
curl -X POST http://localhost:8000/reports/reconcile
```

---

## 8. Test Execution Commands & Results

Execute the complete 12-scenario test suite:
```bash
python -m tests.test_scenarios
```

### Test Scenario Summary Table

| Scenario | Objective | Observed Result | Verdict |
| :--- | :--- | :--- | :---: |
| **1. Valid report request** | API responds < 3s, CSV contains sorted rows | Responded in 0.02s, CSV generated with 1,883 sorted rows | **PASS** |
| **2. Invalid date range** | Request rejected synchronously; no job published | 422 Unprocessable Entity returned; queue remained empty | **PASS** |
| **3. No matching sales** | Empty date range produces header-only CSV | Completed with 0 rows; valid header-only CSV generated | **PASS** |
| **4. 20 requests with 1 worker** | Sequential queue processing by single worker | Completed all 20 jobs sequentially in 102.0s | **PASS** |
| **5. 20 requests with 3 workers** | Work distributed evenly across multiple workers | Distributed across workers (7, 7, 6 jobs) in 37.1s | **PASS** |
| **6. Worker stopped mid-job** | Unacknowledged work recovered upon worker crash | Worker-1 killed; Worker-2 recovered and finished job | **PASS** |
| **7. RabbitMQ restarted** | Queued work survives broker reboot | Queued jobs survived restart with volume intact | **PASS** |
| **8. Simulated transient failure** | Bounded retry via TTL queue succeeds on attempt 2 | Retried through retry queue and succeeded on attempt 2 | **PASS** |
| **9. Repeated permanent failure** | Fails bounded attempts; routes message to DLQ | Marked failed, DLQ message sent, download returns 409 | **PASS** |
| **10. Duplicate request & message** | Idempotency key & queue deduplication verified | Reused existing job ID; duplicate queue message skipped | **PASS** |
| **11. Broker down during submit** | No data loss during broker outage; reconciles | Retained as 'pending'; reconciled and finished | **PASS** |
| **12. DB down during processing** | No false ACK during database outage | Worker did not falsely ACK; finished once DB resumed | **PASS** |

*For complete step-by-step logs, see [docs/TEST_REPORT.md](docs/TEST_REPORT.md).*

---

## 9. Performance Benchmark (1 vs. 3 Workers)

Measure 20 jobs with 1 worker vs 3 workers:
```bash
python -m scripts.benchmark --jobs 20 --label "1-Worker"
python -m scripts.benchmark --jobs 20 --label "3-Workers"
```

### Empirical Results Comparison

| Metric | 1 Worker | 3 Workers | Improvement |
| :--- | :--- | :--- | :--- |
| **Total Jobs Submitted** | 20 | 20 | - |
| **Completed Jobs** | 20 | 20 | 100% Success |
| **Total Elapsed Time** | **102.04 s** | **37.20 s** | **2.74x Speedup** |
| **Average Queue Wait Time** | **47.00 s** | **13.70 s** | **70.8% Latency Reduction** |
| **Average Processing Time** | **5.01 s** | **5.01 s** | Consistent work |
| **Per-Worker Load Distribution** | Worker 1: 20 jobs | Worker 1: 7, Worker 2: 7, Worker 3: 6 | Balanced |

*For in-depth analysis on Amdahl's Law and resource contention, see [docs/PERFORMANCE.md](docs/PERFORMANCE.md).*

---

## 10. Design Questions Answered

Comprehensive architectural answers addressing:
- Asynchronous processing trade-offs.
- Data boundaries between RabbitMQ and PostgreSQL.
- Publisher confirms vs consumer acknowledgments.
- Post-commit worker crash scenarios.
- At-least-once delivery and idempotency.
- FIFO delivery limitations with competing consumers.
- Corruption prevention via atomic state transitions and atomic file renaming.
- Backpressure and queue saturation dynamics.
- Remaining failure windows and multi-machine cloud evolution.

*Read the full analysis in [docs/DESIGN_QUESTIONS.md](docs/DESIGN_QUESTIONS.md).*

---

## 11. Demonstration Video Outline (5–8 Minutes)

For the video submission:
1. **Normal Processing (0:00 - 2:00):** Open web dashboard at `http://localhost:8000`. Submit a sales report for `2025-01-01` to `2025-03-31`. Observe transition `pending` -> `queued` -> `processing` -> `completed`. Download and inspect CSV.
2. **Parallel Workload (2:00 - 3:30):** Submit multiple jobs; inspect RabbitMQ Management UI (`http://localhost:15672`) showing `prefetch=1` and simultaneous processing across `worker-1`, `worker-2`, and `worker-3`.
3. **Worker Crash Recovery (3:30 - 5:00):** Submit a report. While `processing`, run `docker compose stop -t 0 worker1`. Show that worker-2 immediately recovers the unacknowledged job and finishes it.
4. **Failure Injection & DLQ (5:00 - 6:30):** Select "Simulate Transient Error" on frontend; watch automatic retry through the 5s delayed retry queue. Select "Simulate Permanent Error"; watch report fail and inspect the message in `reports.dlq`.
5. **Idempotency & Reconciliation (6:30 - 8:00):** Resubmit with same `Idempotency-Key` showing immediate 200/202 reuse. Stop RabbitMQ, submit a job, restart RabbitMQ, and click "Reconcile Pending".

---

## 12. Teardown & Clean Environment Instructions

### Stop All Containers
```bash
docker compose down
```

### Complete Teardown (Remove Data Volumes & Storage Files)
```bash
docker compose down -v
rm -rf storage/reports/*
```

---

## 13. Team Contribution Statement

- **Architecture & Broker Topology:** Designed RabbitMQ durable exchanges, retry TTL queues, DLX, and manual ACK mechanics.
- **Backend API & Database:** Implemented FastAPI endpoints, SQLAlchemy data models, idempotency enforcement, and atomic state transitions.
- **Worker & Failure Engine:** Implemented background worker consumer, prefetch configuration, crash recovery, and simulated failure injection hooks.
- **Frontend & Verification:** Built interactive Tailwind/Alpine.js dashboard, benchmark harness, and the 12-scenario automated test suite.

---

## 14. AI Assistance Disclosure

AI tools (Google Antigravity / Gemini) were utilized during the development of this project for:
- Drafting boilerplate configuration for RabbitMQ topologies (exchange, queue, and DLX arguments).
- Formatting structured JSON logging conforming to project specifications.
- Brainstorming edge-case failure scenarios in distributed systems.

**Verification & Validation:**
Every line of code and configuration was manually verified through live Docker execution and the automated 12-scenario test suite (`tests/test_scenarios.py`). All benchmark measurements were empirically collected and reproduced on a live cluster.
