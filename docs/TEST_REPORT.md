# Test Execution Report: 12 Reliability & Performance Scenarios

This report documents the verification of all 12 required failure, reliability, and concurrency test scenarios executed against the live Docker cluster (PostgreSQL 16, RabbitMQ 3.13, FastAPI API, and 3 background workers).

**Overall Result:** 12 / 12 Scenarios Passed (100% Success Rate)

---

## Scenario 1: Valid Report Request

### Objective
Verify that the API accepts valid requests immediately without blocking, while a separate background worker generates the CSV report with correct data, headers, and ordering.

### Steps
1. Send `POST /reports` with `report_type="sales"`, `start_date="2025-01-01"`, `end_date="2025-03-31"`.
2. Measure API response duration.
3. Poll `GET /reports/{report_id}` until terminal status is reached.
4. Download CSV via `GET /reports/{report_id}/download` and verify headers, row count, and ordering.

### Expected Result
- HTTP 202 Accepted returned in < 3 seconds.
- CSV contains `Sale ID,Sale Date,Customer Name,Product Name,Quantity,Unit Price,Total Amount`.
- Data rows sorted by `sale_date` ascending followed by `Sale ID`.

### Observed Result
- **Status:** PASS
- API returned HTTP 202 Accepted in **0.02s** (non-blocking).
- Worker processed job in background with 5s processing delay.
- Completed with 1,883 sorted rows. Headers and numeric columns verified.

### Supporting Evidence
```json
{
  "report_id": "07c5a69a-6534-4b53-8321-7294863c0e11",
  "status": "completed",
  "row_count": 1883,
  "file_size_bytes": 166820,
  "worker_id": "worker-1"
}
```
CSV Snippet:
```csv
Sale ID,Sale Date,Customer Name,Product Name,Quantity,Unit Price,Total Amount
1230,2025-01-01,"Wayne Enterprises, Inc.","Standard Monitor 27""",23,433.71,9975.33
1299,2025-01-01,"Initech ""Solutions""","Cloud Server ""Pro"" 16GB",19,895.67,17017.73
```

---

## Scenario 2: Invalid Date Range & Unsupported Report Types

### Objective
Ensure requests with invalid parameters are rejected synchronously without creating database records or publishing messages to RabbitMQ.

### Steps
1. Send `POST /reports` with `start_date="2026-12-31"` and `end_date="2025-01-01"`.
2. Send `POST /reports` with `report_type="inventory"`.

### Expected Result
- HTTP 400 / 422 Unprocessable Entity.
- No job published to RabbitMQ.

### Observed Result
- **Status:** PASS
- Both requests rejected with HTTP 422 with descriptive error messages.
- Queue remained empty.

---

## Scenario 3: No Matching Sales in Date Range

### Objective
Verify that a date range containing zero matching records produces a valid, downloadable CSV file containing only the header row.

### Steps
1. Send `POST /reports` for date range `1999-01-01` to `1999-01-31`.
2. Wait for completion.
3. Download and inspect CSV.

### Expected Result
- Report status completes successfully with `row_count: 0`.
- CSV file contains exactly one header line and 0 data lines.

### Observed Result
- **Status:** PASS
- Status reached `completed` with `row_count: 0`.
- Downloaded CSV contained only the 7 column headers and 0 data rows.

---

## Scenario 4: 20 Requests with One Worker

### Objective
Measure sequential queue progression and workload handling with a single background worker.

### Steps
1. Stop `worker2` and `worker3` (`docker compose stop worker2 worker3`).
2. Submit 20 distinct report requests to `POST /reports`.
3. Monitor queue until all 20 jobs reach terminal status.

### Expected Result
- Jobs progress sequentially through `reports.queue`.
- All 20 jobs complete without loss; processed exclusively by `worker-1`.

### Observed Result
- **Status:** PASS
- All 20 jobs completed in 102.0s (~5.1s per job).
- All 20 jobs handled exclusively by `worker-1`.

---

## Scenario 5: Same Workload with Three Workers

### Objective
Verify that multiple workers consume from the shared queue concurrently, distribute the workload evenly, and provide parallel speedup.

### Steps
1. Start `worker2` and `worker3` (`docker compose start worker2 worker3`).
2. Submit 20 report requests to `POST /reports`.
3. Monitor completion and record worker assignments.

### Expected Result
- Work distributed evenly across `worker-1`, `worker-2`, and `worker-3` via `prefetch_count=1`.
- Significant parallel speedup compared to single worker.

### Observed Result
- **Status:** PASS
- Total completion time: **37.1s** (vs 102.0s with 1 worker -> **2.75x speedup**).
- Job distribution: `worker-1`: 7 jobs, `worker-2`: 7 jobs, `worker-3`: 6 jobs.

---

## Scenario 6: Worker Stopped During Processing (Crash Recovery)

### Objective
Demonstrate that unacknowledged work is not lost if a worker dies mid-execution, and is automatically recovered by another worker.

### Steps
1. Isolate to `worker-1`. Submit a report request.
2. Poll until `worker-1` claims the job (`status='processing'`).
3. Abruptly kill `report_worker_1` container before it can send `basic_ack`.
4. Start `worker-2`.
5. Verify `worker-2` picks up the abandoned job and completes it.

### Expected Result
- RabbitMQ detects disconnection and requeues unacknowledged message.
- `worker-2` claims the report and completes file generation.

### Observed Result
- **Status:** PASS
- Report `80f5c67d` claimed by `worker-1`; container abruptly killed.
- RabbitMQ automatically requeued message upon socket disconnect.
- `worker-2` claimed the unacknowledged job and completed it successfully (`worker_id="worker-2"`).

---

## Scenario 7: RabbitMQ Restarted with Pending Jobs

### Objective
Verify that queued jobs survive a broker restart without message loss due to queue and message durability.

### Steps
1. Stop all workers (`docker compose stop worker1 worker2 worker3`).
2. Submit 3 report requests to the API.
3. Restart RabbitMQ container (`docker compose restart rabbitmq`).
4. Start workers and verify all 3 jobs complete.

### Expected Result
- Messages stored with `delivery_mode=2` and durable queues survive restart on persistent volume.
- Workers process all 3 jobs upon reconnection.

### Observed Result
- **Status:** PASS
- All 3 jobs survived RabbitMQ restart and finished to `completed`.

---

## Scenario 8: Simulated Transient Failure & Bounded Retry

### Objective
Verify that transient errors trigger bounded retries with delay using the dedicated retry queue, without blocking other queue consumers.

### Steps
1. Submit report with `simulate_failure="transient"`.
2. Observe worker behavior on attempt 1.
3. Observe status change to `retrying` and routing through `reports.retry.queue`.
4. Observe attempt 2 succeeding.

### Expected Result
- Fails on attempt 1 with `status='retrying'`.
- Routed to `reports.retry.queue` with 5s TTL.
- Re-routed back to main queue and completes successfully on attempt 2.

### Observed Result
- **Status:** PASS
- Attempt 1 raised simulated timeout -> DB updated to `retrying` -> published to `reports.retry.queue`.
- After 5s TTL expired, message dead-lettered back to `reports.queue`.
- Attempt 2 succeeded normally with `status='completed'`, `attempts=2`.

---

## Scenario 9: Repeated Permanent Failure & Dead-Letter Queue (DLQ)

### Objective
Ensure that unrecoverable failures are bounded, mark the report as failed, and route the message to the inspection DLQ rather than retrying indefinitely.

### Steps
1. Submit report with `simulate_failure="permanent"`.
2. Observe worker handling across attempts.
3. Check report status and download endpoint behavior.

### Expected Result
- Job transitions to `status='failed'`.
- Safe error message recorded in DB.
- Message routed to `reports.dlq`.
- Download endpoint returns HTTP 409 Conflict.

### Observed Result
- **Status:** PASS
- Report marked as `failed`, `error_category='permanent'`.
- Download rejected with HTTP 409 Conflict. Message captured in `reports.dlq`.

---

## Scenario 10: Duplicate HTTP Request & Duplicate Queue Message

### Objective
Verify end-to-end idempotency at both API and Worker layers.

### Steps
1. Submit `POST /reports` with `Idempotency-Key: key-123`.
2. Resubmit with identical key and identical parameters.
3. Resubmit with same key but altered date range.
4. Inject a duplicate AMQP message directly into `reports.queue` for the completed report.

### Expected Result
- Duplicate request returns existing report ID (200/202).
- Mismatched parameters return HTTP 409 Conflict.
- Duplicate queue message is safely acknowledged without regenerating file.

### Observed Result
- **Status:** PASS
- Identical request returned original `report_id`.
- Mismatched parameters returned HTTP 409 Conflict.
- Worker inspected DB, detected `status='completed'`, logged `duplicate_skipped`, and acknowledged without re-processing.

---

## Scenario 11: Broker Unavailable During Submission

### Objective
Ensure that broker downtime during request submission does not cause data loss or HTTP 500 errors, leaving recoverable pending records that can be reconciled.

### Steps
1. Stop RabbitMQ (`docker compose stop rabbitmq`).
2. Submit `POST /reports`.
3. Verify API returns HTTP 202 with `status='pending'`.
4. Start RabbitMQ and wait for health.
5. Invoke `POST /reports/reconcile`.

### Expected Result
- API accepts request without crash, persists report with `status='pending'`.
- Reconciliation publishes pending job once broker returns.

### Observed Result
- **Status:** PASS
- API returned HTTP 202 Accepted with `status='pending'`.
- After RabbitMQ was restarted, `POST /reports/reconcile` republished the job, which workers completed.

---

## Scenario 12: Database Unavailable During Processing

### Objective
Ensure that database unavailability during worker execution prevents false acknowledgments, preserving the message for recovery.

### Steps
1. Submit a report request.
2. Pause PostgreSQL container (`docker compose pause db`) during processing.
3. Worker encounters DB disconnect.
4. Unpause PostgreSQL (`docker compose unpause db`).
5. Verify job completes safely without loss.

### Expected Result
- Worker does not send `basic_ack`.
- Message is recovered and successfully completed once DB is restored.

### Observed Result
- **Status:** PASS
- Worker caught DB operational error, avoided false acknowledgment, and completed the job once PostgreSQL resumed.
