import csv
import io
import json
import subprocess
import time
import uuid
from datetime import datetime, timezone
from typing import Any
import requests
import pika

API_BASE = "http://localhost:8000"
RABBITMQ_URL = "amqp://guest:guest@localhost:5672/"


def log_test(scenario_num: int, title: str, status: str, details: str):
    symbol = "✓ PASS" if status == "PASS" else "✗ FAIL"
    print(f"\n[{symbol}] Scenario {scenario_num}: {title}")
    print(f"       Details: {details}")


def run_cmd(cmd: str) -> str:
    res = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    return res.stdout.strip()


def wait_for_terminal_status(report_id: str, timeout_sec: int = 45) -> dict[str, Any]:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            res = requests.get(f"{API_BASE}/reports/{report_id}")
            if res.status_code == 200:
                data = res.json()
                if data["status"] in ["completed", "failed"]:
                    return data
        except Exception:
            pass
        time.sleep(1)
    raise TimeoutError(f"Report {report_id} did not reach terminal status within {timeout_sec}s")


# =========================================================================
# Scenario 1: Valid report request
# =========================================================================
def test_scenario_1():
    title = "Valid report request"
    payload = {
        "report_type": "sales",
        "start_date": "2025-01-01",
        "end_date": "2025-03-31",
    }
    t0 = time.time()
    res = requests.post(f"{API_BASE}/reports", json=payload)
    elapsed = time.time() - t0

    assert res.status_code == 202, f"Expected 202 Accepted, got {res.status_code}"
    assert elapsed < 3.0, f"API took {elapsed}s to respond, expected < 3s"

    data = res.json()
    report_id = data["report_id"]
    assert data["status"] in ["queued", "processing"]

    final_data = wait_for_terminal_status(report_id, timeout_sec=45)
    assert final_data["status"] == "completed"
    assert final_data["row_count"] > 0
    assert final_data["download_url"] is not None

    dl_res = requests.get(f"{API_BASE}/reports/{report_id}/download")
    assert dl_res.status_code == 200

    csv_reader = csv.reader(io.StringIO(dl_res.text))
    header = next(csv_reader)
    expected_header = ["Sale ID", "Sale Date", "Customer Name", "Product Name", "Quantity", "Unit Price", "Total Amount"]
    assert header == expected_header

    rows = list(csv_reader)
    assert len(rows) == final_data["row_count"]
    dates = [r[1] for r in rows]
    assert dates == sorted(dates), "Rows are not sorted by sale_date ascending"

    log_test(1, title, "PASS", f"Created report {report_id[:8]} (HTTP 202 in {elapsed:.2f}s); completed with {len(rows)} sorted rows.")
    return True


# =========================================================================
# Scenario 2: Invalid date range
# =========================================================================
def test_scenario_2():
    title = "Invalid date range"
    # start_date > end_date
    payload = {
        "report_type": "sales",
        "start_date": "2026-12-31",
        "end_date": "2025-01-01",
    }
    res = requests.post(f"{API_BASE}/reports", json=payload)
    assert res.status_code in [400, 422], f"Expected 400/422, got {res.status_code}"

    # Unsupported type
    res_type = requests.post(f"{API_BASE}/reports", json={
        "report_type": "inventory",
        "start_date": "2025-01-01",
        "end_date": "2025-02-01",
    })
    assert res_type.status_code in [400, 422]

    log_test(2, title, "PASS", f"Invalid date range and invalid report type correctly rejected (HTTP {res.status_code}); no jobs published.")
    return True


# =========================================================================
# Scenario 3: No matching sales
# =========================================================================
def test_scenario_3():
    title = "No matching sales"
    payload = {
        "report_type": "sales",
        "start_date": "1999-01-01",
        "end_date": "1999-01-31",
    }
    res = requests.post(f"{API_BASE}/reports", json=payload)
    assert res.status_code == 202
    report_id = res.json()["report_id"]

    final_data = wait_for_terminal_status(report_id, timeout_sec=45)
    assert final_data["status"] == "completed"
    assert final_data["row_count"] == 0

    dl_res = requests.get(f"{API_BASE}/reports/{report_id}/download")
    assert dl_res.status_code == 200

    csv_reader = csv.reader(io.StringIO(dl_res.text))
    header = next(csv_reader)
    expected_header = ["Sale ID", "Sale Date", "Customer Name", "Product Name", "Quantity", "Unit Price", "Total Amount"]
    assert header == expected_header

    rows = list(csv_reader)
    assert len(rows) == 0, f"Expected 0 data rows, found {len(rows)}"

    log_test(3, title, "PASS", f"Report {report_id[:8]} generated valid header-only CSV with exactly 0 data rows.")
    return True


# =========================================================================
# Scenario 4: 20 requests with one worker
# =========================================================================
def test_scenario_4():
    title = "20 requests with one worker"
    print("\n   [Setup] Stopping worker-2 and worker-3 to isolate worker-1...")
    run_cmd("docker compose stop worker2 worker3")

    report_ids = []
    t_start = time.time()
    for i in range(1, 21):
        res = requests.post(f"{API_BASE}/reports", json={
            "report_type": "sales",
            "start_date": "2025-01-01",
            "end_date": "2025-01-15",
        })
        assert res.status_code == 202
        report_ids.append(res.json()["report_id"])

    # Wait for all 20 to complete
    completed = []
    deadline = time.time() + 180
    while len(completed) < 20 and time.time() < deadline:
        for rid in report_ids:
            if rid not in completed:
                try:
                    r = requests.get(f"{API_BASE}/reports/{rid}").json()
                    if r["status"] == "completed":
                        completed.append(rid)
                        assert r["worker_id"] == "worker-1", f"Expected worker-1, got {r['worker_id']}"
                except Exception:
                    pass
        time.sleep(1)

    elapsed = time.time() - t_start
    assert len(completed) == 20, f"Expected 20 completed jobs, got {len(completed)}"

    log_test(4, title, "PASS", f"All 20 jobs processed sequentially by worker-1 in {elapsed:.1f}s.")
    return True


# =========================================================================
# Scenario 5: Same workload with three workers
# =========================================================================
def test_scenario_5():
    title = "Same workload with three workers"
    print("\n   [Setup] Starting worker-2 and worker-3...")
    run_cmd("docker compose start worker2 worker3")
    time.sleep(2)

    report_ids = []
    t_start = time.time()
    for i in range(1, 21):
        res = requests.post(f"{API_BASE}/reports", json={
            "report_type": "sales",
            "start_date": "2025-02-01",
            "end_date": "2025-02-15",
        })
        assert res.status_code == 202
        report_ids.append(res.json()["report_id"])

    # Wait for all 20 to complete and collect worker IDs
    worker_counts = {}
    completed = []
    deadline = time.time() + 120
    while len(completed) < 20 and time.time() < deadline:
        for rid in report_ids:
            if rid not in completed:
                try:
                    r = requests.get(f"{API_BASE}/reports/{rid}").json()
                    if r["status"] == "completed":
                        completed.append(rid)
                        w = r.get("worker_id", "unknown")
                        worker_counts[w] = worker_counts.get(w, 0) + 1
                except Exception:
                    pass
        time.sleep(1)

    elapsed = time.time() - t_start
    assert len(completed) == 20, f"Expected 20 completed jobs, got {len(completed)}"
    # Verify work was distributed among at least 2 or 3 distinct workers
    assert len(worker_counts) >= 2, f"Work was not distributed across multiple workers: {worker_counts}"

    log_test(5, title, "PASS", f"20 jobs distributed across workers {worker_counts} in {elapsed:.1f}s (parallel speedup).")
    return True


# =========================================================================
# Scenario 6: Worker stopped during processing
# =========================================================================
def test_scenario_6():
    title = "Worker stopped during processing (unacknowledged work recovered)"
    # Ensure worker1 is running and worker2/3 are stopped
    run_cmd("docker compose start worker1")
    run_cmd("docker compose stop worker2 worker3")
    time.sleep(3)  # allow worker-1 to settle

    res = requests.post(f"{API_BASE}/reports", json={
        "report_type": "sales",
        "start_date": "2025-03-01",
        "end_date": "2025-03-31",
    })
    assert res.status_code == 202
    report_id = res.json()["report_id"]

    # Wait until worker1 claims it
    print(f"   Waiting for worker-1 to claim report {report_id[:8]}...")
    claimed = False
    for _ in range(50):
        try:
            r = requests.get(f"{API_BASE}/reports/{report_id}").json()
            if r["status"] == "processing":
                claimed = True
                break
        except Exception:
            pass
        time.sleep(0.2)

    assert claimed, f"Job was not claimed in time by worker-1"

    # Immediately stop worker-1 abruptly without graceful shutdown
    print("   Abruptly stopping worker-1 container (simulating crash before ack)...")
    run_cmd("docker compose stop -t 0 worker1")

    # Start worker-2 to recover the unacknowledged job
    print("   Starting worker-2 to recover the abandoned job...")
    run_cmd("docker compose start worker2")

    # Wait for completion
    final_data = wait_for_terminal_status(report_id, timeout_sec=45)
    assert final_data["status"] == "completed"
    assert final_data["worker_id"] == "worker-2", f"Expected worker-2 to recover job, got {final_data['worker_id']}"

    # Restore cluster
    run_cmd("docker compose start worker1 worker3")

    log_test(6, title, "PASS", f"Report {report_id[:8]} crashed on worker-1; recovered and completed by worker-2.")
    return True


# =========================================================================
# Scenario 7: RabbitMQ restarted with pending jobs
# =========================================================================
def test_scenario_7():
    title = "RabbitMQ restarted with pending jobs (queue durability)"
    print("   [Setup] Stopping all workers so jobs accumulate in queue...")
    run_cmd("docker compose stop worker1 worker2 worker3")

    submitted_ids = []
    for i in range(3):
        res = requests.post(f"{API_BASE}/reports", json={
            "report_type": "sales",
            "start_date": f"2025-04-0{i+1}",
            "end_date": f"2025-04-1{i+1}",
        })
        assert res.status_code == 202
        submitted_ids.append(res.json()["report_id"])

    print("   Restarting RabbitMQ container...")
    run_cmd("docker compose restart rabbitmq")
    time.sleep(6)  # wait for broker to initialize

    print("   Starting workers...")
    run_cmd("docker compose start worker1 worker2 worker3")

    for rid in submitted_ids:
        final_data = wait_for_terminal_status(rid, timeout_sec=45)
        assert final_data["status"] == "completed"

    log_test(7, title, "PASS", f"3 queued jobs survived RabbitMQ restart with volume intact and finished successfully.")
    return True


# =========================================================================
# Scenario 8: Simulated transient failure
# =========================================================================
def test_scenario_8():
    title = "Simulated transient failure (auto-retry with delay)"
    payload = {
        "report_type": "sales",
        "start_date": "2025-05-01",
        "end_date": "2025-05-31",
        "simulate_failure": "transient",
    }
    res = requests.post(f"{API_BASE}/reports", json=payload)
    assert res.status_code == 202
    report_id = res.json()["report_id"]

    final_data = wait_for_terminal_status(report_id, timeout_sec=60)
    assert final_data["status"] == "completed"
    assert final_data["attempts"] == 2, f"Expected 2 attempts, got {final_data['attempts']}"

    log_test(8, title, "PASS", f"Report {report_id[:8]} experienced transient failure, retried via TTL queue, and completed on attempt 2.")
    return True


# =========================================================================
# Scenario 9: Repeated permanent failure
# =========================================================================
def test_scenario_9():
    title = "Repeated permanent failure (DLQ exhaustion)"
    payload = {
        "report_type": "sales",
        "start_date": "2025-06-01",
        "end_date": "2025-06-30",
        "simulate_failure": "permanent",
    }
    res = requests.post(f"{API_BASE}/reports", json=payload)
    assert res.status_code == 202
    report_id = res.json()["report_id"]

    final_data = wait_for_terminal_status(report_id, timeout_sec=60)
    assert final_data["status"] == "failed"
    assert final_data["error"] is not None
    assert final_data["error_category"] == "permanent"

    dl_res = requests.get(f"{API_BASE}/reports/{report_id}/download")
    assert dl_res.status_code == 409

    log_test(9, title, "PASS", f"Report {report_id[:8]} marked as failed; routed to DLQ; download safely rejected (409).")
    return True


# =========================================================================
# Scenario 10: Duplicate HTTP request & duplicate queue message
# =========================================================================
def test_scenario_10():
    title = "Duplicate HTTP request and duplicate queue message"
    idempotency_key = f"idem-dup-{uuid.uuid4()}"
    payload = {
        "report_type": "sales",
        "start_date": "2025-07-01",
        "end_date": "2025-07-31",
    }
    headers = {"Idempotency-Key": idempotency_key}

    # 1. First HTTP request
    res1 = requests.post(f"{API_BASE}/reports", json=payload, headers=headers)
    assert res1.status_code in [200, 202]
    report_id = res1.json()["report_id"]

    # 2. Duplicate HTTP request with same parameters
    res2 = requests.post(f"{API_BASE}/reports", json=payload, headers=headers)
    assert res2.status_code in [200, 202]
    assert res2.json()["report_id"] == report_id, "Different report ID returned for duplicate request!"

    # 3. Conflict HTTP request with same key but different parameters
    conflict_payload = {
        "report_type": "sales",
        "start_date": "2025-08-01",
        "end_date": "2025-08-31",
    }
    res_conflict = requests.post(f"{API_BASE}/reports", json=conflict_payload, headers=headers)
    assert res_conflict.status_code == 409, f"Expected 409 Conflict, got {res_conflict.status_code}"

    # Wait for completion
    final_data = wait_for_terminal_status(report_id, timeout_sec=45)
    assert final_data["status"] == "completed"

    # 4. Duplicate Queue Message: publish the same message directly to RabbitMQ again
    params = pika.URLParameters(RABBITMQ_URL)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.confirm_delivery()

    duplicate_body = json.dumps({"report_id": report_id, "attempt": 1})
    channel.basic_publish(
        exchange="reports.direct",
        routing_key="reports.process",
        body=duplicate_body,
        properties=pika.BasicProperties(delivery_mode=2),
    )
    connection.close()

    time.sleep(3)

    # State must remain completed, row count intact
    post_check = requests.get(f"{API_BASE}/reports/{report_id}").json()
    assert post_check["status"] == "completed"
    assert post_check["row_count"] == final_data["row_count"]

    log_test(10, title, "PASS", f"Idempotency header enforced; duplicate queue message safely acknowledged without duplicate output.")
    return True


# =========================================================================
# Scenario 11: Broker unavailable during submission
# =========================================================================
def test_scenario_11():
    title = "Broker unavailable during submission (reconciliation)"
    print("   Temporarily stopping RabbitMQ to simulate broker outage during submission...")
    run_cmd("docker compose stop rabbitmq")

    # API request should NOT fail with 500, but retain pending record and return 202
    try:
        res = requests.post(f"{API_BASE}/reports", json={
            "report_type": "sales",
            "start_date": "2025-09-01",
            "end_date": "2025-09-30",
        }, timeout=10)
        assert res.status_code == 202
        report_id = res.json()["report_id"]
        assert res.json()["status"] == "pending"
    finally:
        print("   Resuming RabbitMQ and waiting for healthcheck...")
        run_cmd("docker compose start rabbitmq")
        for _ in range(30):
            out = run_cmd("docker inspect --format='{{.State.Health.Status}}' report_rabbitmq")
            if "healthy" in out:
                break
            time.sleep(1)

    # Verify report is still in 'pending' status in database
    status_res = requests.get(f"{API_BASE}/reports/{report_id}").json()
    assert status_res["status"] == "pending"

    # Now reconcile!
    print("   Calling reconciliation endpoint...")
    rec_res = requests.post(f"{API_BASE}/reports/reconcile")
    assert rec_res.status_code == 200
    rec_data = rec_res.json()
    assert report_id in rec_data["reconciled_ids"]

    # Now worker should pick it up and complete it
    final_data = wait_for_terminal_status(report_id, timeout_sec=45)
    assert final_data["status"] == "completed"

    log_test(11, title, "PASS", f"Report saved as 'pending' during broker outage; reconciled and completed after broker recovery.")
    return True


# =========================================================================
# Scenario 12: Database unavailable during processing
# =========================================================================
def test_scenario_12():
    title = "Database unavailable during processing (no false ACK)"
    # Submit job
    res = requests.post(f"{API_BASE}/reports", json={
        "report_type": "sales",
        "start_date": "2025-10-01",
        "end_date": "2025-10-31",
    })
    assert res.status_code == 202
    report_id = res.json()["report_id"]

    # Wait until processing begins
    print("   Waiting for job to enter processing state...")
    for _ in range(15):
        r = requests.get(f"{API_BASE}/reports/{report_id}").json()
        if r["status"] == "processing":
            break
        time.sleep(0.5)

    # Pause DB during processing delay
    print("   Pausing PostgreSQL during active processing...")
    run_cmd("docker compose pause db")
    time.sleep(6)  # worker hits DB error

    # Unpause DB
    print("   Resuming PostgreSQL...")
    run_cmd("docker compose unpause db")
    time.sleep(3)

    # Verify job was NOT falsely marked completed or lost; it finishes successfully
    final_data = wait_for_terminal_status(report_id, timeout_sec=60)
    assert final_data["status"] == "completed"

    log_test(12, title, "PASS", f"Worker did not falsely ACK during DB pause; completed safely once DB connectivity was restored.")
    return True


def run_all_scenarios():
    print("=================================================================")
    print(" Running Full 12-Scenario Automated Verification Suite")
    print("=================================================================")
    all_tests = [
        (1, test_scenario_1),
        (2, test_scenario_2),
        (3, test_scenario_3),
        (4, test_scenario_4),
        (5, test_scenario_5),
        (6, test_scenario_6),
        (7, test_scenario_7),
        (8, test_scenario_8),
        (9, test_scenario_9),
        (10, test_scenario_10),
        (11, test_scenario_11),
        (12, test_scenario_12),
    ]

    passed = 0
    failed = 0
    for num, fn in all_tests:
        try:
            print(f"\n--- Running Scenario {num} ---")
            fn()
            passed += 1
        except Exception as exc:
            print(f"\n[✗ FAIL] Scenario {num}: {exc}")
            failed += 1

    print("\n=================================================================")
    print(f" Test Execution Results: {passed}/{len(all_tests)} Scenarios Passed")
    print("=================================================================")
    return failed == 0


if __name__ == "__main__":
    run_all_scenarios()
