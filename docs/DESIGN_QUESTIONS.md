# Design Questions & Architectural Analysis

This document provides rigorous, production-grade answers to the 10 core architectural and reliability design questions for the Asynchronous Report Generator.

---

### 1. Why is asynchronous processing appropriate here?

Report generation is an I/O-heavy, computationally intensive, and variable-duration workload. Generating a report involves querying tens of thousands of records, aggregating amounts, formatting and escaping text, and writing data to disk. 

If this were handled synchronously inside an HTTP request-response cycle:
- **HTTP Timeout Risks:** Standard client, reverse proxy (Nginx, Envoy), and gateway timeouts (typically 30–60 seconds) would terminate connections prematurely during peak loads.
- **Thread/Worker Starvation:** Synchronous web workers (e.g., Uvicorn/Gunicorn processes) would be tied up holding open sockets while waiting for CSV generation, blocking subsequent incoming requests and causing rapid Denial of Service (DoS).
- **Poor User Experience:** Users would stare at a frozen loading spinner without progress indication or the ability to navigate away and return later.

Asynchronous processing decouples request ingestion from execution. The API accepts the request in milliseconds (`HTTP 202 Accepted`), issues a tracking identifier, and buffers the task in RabbitMQ. Workers process the queue at a sustainable, controlled rate while users can poll status or download the file when ready.

---

### 2. What does RabbitMQ store, and what belongs in PostgreSQL?

| Storage Tier | Data Owned | Characteristics & Rationale |
| :--- | :--- | :--- |
| **RabbitMQ** | - Ephemeral task notifications<br>- Routing metadata<br>- Attempt counters<br>- Message pointers (`report_id`) | **Transient Work Queue:** Optimized for fast, in-memory message routing, competing consumer distribution, queue buffering, backpressure, and automatic re-delivery upon disconnect. RabbitMQ is not an archival database. Once a message is acknowledged, it is purged. |
| **PostgreSQL** | - Sales business dataset (15,000+ rows)<br>- Report lifecycle history & timestamps<br>- Job statuses (`pending`, `completed`, etc.)<br>- Idempotency keys<br>- File paths, row counts, & error details | **Durable System of Record:** Optimized for structured relational queries, ACID transactional consistency, multi-index searches, immutable audit logs, and long-term state persistence that survives queue completion. |

---

### 3. How do publisher confirms differ from consumer acknowledgments?

| Dimension | Publisher Confirms (`confirm_delivery`) | Consumer Acknowledgments (`basic_ack` / `basic_nack`) |
| :--- | :--- | :--- |
| **Direction** | **Producer -> Broker** | **Consumer -> Broker** |
| **Guarantee** | Verifies the broker has safely accepted, routed, and persisted the message to durable storage on disk before confirming back to the API. | Verifies that a consumer worker has completely finished processing the business task and safely committed results before the broker removes the message. |
| **Failure Handling** | If the broker rejects or drops the message (`nack` or unroutable), the producer knows publication failed and keeps the job in `pending` for reconciliation. | If the consumer dies or rejects the message (`basic_nack(requeue=True)` or TCP drop), RabbitMQ returns the message to the queue for another consumer. |

---

### 4. What happens if the worker finishes the file but crashes before acknowledging?

This is the classic **post-commit crash window**:
1. The worker successfully creates the CSV file and executes `db.commit()` in PostgreSQL, updating the status to `completed`.
2. Before the worker can invoke `channel.basic_ack()`, the worker process crashes (e.g., `SIGKILL`, power failure, OOM).
3. RabbitMQ detects the severed TCP connection and re-queues the message with `redelivered=True`.
4. A second worker picks up the message from the queue.
5. **Deduplication Safeguard:** The second worker immediately inspects PostgreSQL. It finds `report.status == 'completed'`.
6. Instead of re-querying the database or overwriting the canonical CSV file, the second worker logs a `duplicate_skipped` action and immediately invokes `channel.basic_ack()`.
7. The message is safely removed from the broker, preserving single logical execution and file integrity.

---

### 5. Can messages be processed more than once?

**Yes.** In distributed messaging systems with crash recovery and network unreliability, message delivery guarantees are **at-least-once**, not "exactly-once". 

Duplicates occur when:
- A worker crashes after doing work but before sending an acknowledgment (as described in Question 4).
- Network partitions cause TCP timeouts between worker and broker, causing the broker to requeue while the worker was still finalizing.
- Upstream clients retry HTTP requests.

Because at-least-once delivery is inherent to message brokers, **idempotency is mandatory**. Our implementation guarantees idempotency at both levels:
1. **HTTP Layer:** `Idempotency-Key` header with database uniqueness constraints.
2. **Worker Layer:** State-machine verification preventing duplicate execution of completed reports.

---

### 6. Does FIFO delivery guarantee completion order with several workers?

**No.** While RabbitMQ preserves FIFO (First-In, First-Out) delivery order when dispatching messages from a single queue to a single consumer, FIFO ordering does **not** guarantee completion order when multiple concurrent workers are active.

Reasons:
- **Variable Execution Duration:** A job requesting 2 years of sales data takes longer to query, serialize, and write than a job requesting 2 days of data.
- **Worker Scheduling Differences:** Operating system CPU scheduling, garbage collection, and database latency vary across worker processes.
- **Retries & Re-queuing:** If message 1 experiences a transient network glitch and enters the retry queue while message 2 proceeds normally, message 2 will complete long before message 1.

Systems using competing consumers must never rely on completion order matching arrival order.

---

### 7. How do you prevent two workers from corrupting the same report?

Concurrent corruption is prevented through two complementary mechanisms:

1. **Atomic Database State Transitions:**
   Workers claim jobs using an atomic SQL transition:
   ```sql
   UPDATE reports 
   SET status = 'processing', worker_id = :worker_id, attempts = attempts + 1, started_at = NOW() 
   WHERE id = :id AND status NOT IN ('completed', 'processing')
   RETURNING *;
   ```
   If two workers race to claim the same report, PostgreSQL's row-level locking ensures only one transaction updates the row and receives a result; the second worker receives 0 rows, realizes the job is already claimed or completed, and yields.

2. **Isolated Temporary Files & Atomic Rename:**
   Workers write report output to unique, PID- and worker-specific temporary files:
   ```python
   temp_filename = f".tmp_{report_id}_{self.worker_id}_{os.getpid()}.csv"
   ```
   Only when writing and flushing are 100% complete does the worker execute `os.replace(temp_path, canonical_path)`. On POSIX filesystems, `rename(2)` / `os.replace()` is an atomic operation that guarantees the file is never observed in a partial, corrupted, or half-written state.

---

### 8. What happens if requests arrive faster than workers can process them?

When the request arrival rate exceeds aggregate worker processing capacity:
1. **Queue Buffering & Backpressure:** Pending jobs accumulate safely in RabbitMQ's durable `reports.queue`.
2. **Worker Protection (`prefetch_count=1`):** Because workers consume with `prefetch_count=1`, individual workers are never overwhelmed or starved. Each worker processes exactly 1 job at a time.
3. **Queue Wait Times Grow:** The average queue waiting time increases proportionally to the queue depth (as demonstrated in our benchmark).
4. **Saturation Limits & Mitigation:**
   - **Horizontal Autoscaling:** Additional worker containers/processes can be spawned dynamically based on queue length (e.g., via Kubernetes KEDA or Docker Compose scale).
   - **RabbitMQ Memory Alarms:** If queue volume exceeds high watermarks, RabbitMQ blocks producers or leverages lazy queues (`queue_mode=lazy`) to page messages to disk.
   - **API Rate Limiting:** An API gateway can enforce HTTP 429 Too Many Requests to prevent client floods.

---

### 9. What failure windows remain in your implementation?

While the system is robust against worker crashes, broker restarts, and network glitches, the following edge-case failure windows exist in this baseline architecture:

1. **Shared Volume Failures:** A shared local volume (`/app/storage/reports`) is a Single Point of Failure (SPOF). If the host disk runs out of inodes/space or crashes, completed files cannot be written or served.
2. **Dual-Write Gap between DB and Broker:** In `POST /reports`, the report is saved as `pending` in PostgreSQL, then published to RabbitMQ. If the API process crashes *between* DB commit and broker publish, the job remains stuck in `pending` until the periodic `POST /reports/reconcile` command runs.
3. **Database Write Failure Post-File Generation:** If the worker writes the CSV file and the database dies immediately before updating `status='completed'`, the message will be requeued and regenerated when the DB recovers.
4. **Unbounded Storage Growth:** CSV files persist indefinitely unless an artifact retention policy or garbage collection worker purges files older than 30 days.

---

### 10. How would you evolve this design for multiple machines?

To evolve this single-host Docker Compose deployment into a distributed, multi-machine cloud infrastructure:

1. **Shared Storage -> Cloud Object Storage:**
   Replace the local volume mount with an S3-compatible object store (AWS S3, Google Cloud Storage, or MinIO). Workers upload completed CSVs using pre-signed URLs or IAM credentials.
2. **RabbitMQ Cluster:**
   Deploy a 3-node clustered RabbitMQ setup with **Quorum Queues** (Raft-based consensus) across multiple availability zones, ensuring zero data loss if a broker node suffers hardware failure.
3. **Transactional Outbox Pattern:**
   Eliminate the API publish failure window by writing the report record and an `outbox` event table within the *same* PostgreSQL ACID transaction. A Debezium CDC (Change Data Capture) connector or lightweight relay then reads the WAL and guarantees publication to RabbitMQ.
4. **Dynamic Worker Autoscaling (KEDA):**
   Deploy workers as Kubernetes Deployments with **KEDA (Kubernetes Event-driven Autoscaling)** scaling pods from 1 to N based on the `reports.queue` depth metric.
5. **Distributed Tracing & Centralized Logging:**
   Inject OpenTelemetry trace contexts (`traceparent` headers) into AMQP message properties so end-to-end distributed traces connect the HTTP request, broker queue latency, and worker execution in Grafana/Jaeger.
