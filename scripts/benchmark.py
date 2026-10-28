import argparse
import json
import time
from datetime import datetime, timezone
from typing import Any
import requests
from tabulate import tabulate


def run_benchmark(api_url: str = "http://localhost:8000", job_count: int = 20, label: str = "Benchmark") -> dict[str, Any]:
    print(f"\n=======================================================")
    print(f" Starting Benchmark: {label} ({job_count} jobs)")
    print(f" Target API: {api_url}")
    print(f"=======================================================\n")

    start_benchmark_time = time.time()
    submitted_jobs = []

    # Submit jobs
    print(f"Submitting {job_count} asynchronous report requests...")
    for i in range(1, job_count + 1):
        # Vary date ranges across jobs to test different subsets of sales data
        start_month = (i % 12) + 1
        payload = {
            "report_type": "sales",
            "start_date": f"2025-{start_month:02d}-01",
            "end_date": f"2025-{start_month:02d}-28",
        }
        headers = {
            "Content-Type": "application/json",
            "Idempotency-Key": f"bench-{label}-{i}-{int(time.time())}"
        }

        t_submit = time.time()
        res = requests.post(f"{api_url}/reports", json=payload, headers=headers)
        if res.status_code != 202 and res.status_code != 200:
            print(f"Error submitting job {i}: {res.status_code} - {res.text}")
            continue

        data = res.json()
        report_id = data["report_id"]
        submitted_jobs.append({
            "index": i,
            "report_id": report_id,
            "submitted_at": t_submit,
        })
        time.sleep(0.05)  # slight stagger

    print(f"Submitted {len(submitted_jobs)} jobs. Monitoring until all complete...")

    completed_jobs = {}
    pending_ids = set(j["report_id"] for j in submitted_jobs)

    while pending_ids:
        for report_id in list(pending_ids):
            try:
                res = requests.get(f"{api_url}/reports/{report_id}")
                if res.status_code == 200:
                    data = res.json()
                    status = data["status"]
                    if status in ["completed", "failed"]:
                        completed_jobs[report_id] = data
                        pending_ids.remove(report_id)
                        print(f"  [Job {len(completed_jobs)}/{len(submitted_jobs)}] Report {report_id[:8]} -> {status} (worker: {data.get('worker_id')})")
            except Exception as e:
                pass
        time.sleep(1)

    total_duration = time.time() - start_benchmark_time

    # Calculate metrics
    queue_wait_times = []
    processing_times = []
    completed_count = 0
    failed_count = 0

    for j in submitted_jobs:
        rep = completed_jobs.get(j["report_id"], {})
        if rep.get("status") == "completed":
            completed_count += 1
        else:
            failed_count += 1

        created_str = rep.get("created_at")
        started_str = rep.get("started_at")
        completed_str = rep.get("completed_at")

        if created_str and started_str:
            t0 = datetime.fromisoformat(created_str.replace("Z", "+00:00"))
            t1 = datetime.fromisoformat(started_str.replace("Z", "+00:00"))
            wait_sec = max(0.0, (t1 - t0).total_seconds())
            queue_wait_times.append(wait_sec)

        if started_str and completed_str:
            t1 = datetime.fromisoformat(started_str.replace("Z", "+00:00"))
            t2 = datetime.fromisoformat(completed_str.replace("Z", "+00:00"))
            proc_sec = max(0.0, (t2 - t1).total_seconds())
            processing_times.append(proc_sec)

    avg_wait = sum(queue_wait_times) / len(queue_wait_times) if queue_wait_times else 0.0
    avg_proc = sum(processing_times) / len(processing_times) if processing_times else 0.0

    result = {
        "label": label,
        "job_count": job_count,
        "total_completion_time_sec": round(total_duration, 2),
        "avg_queue_wait_time_sec": round(avg_wait, 2),
        "avg_processing_time_sec": round(avg_proc, 2),
        "completed_count": completed_count,
        "failed_count": failed_count,
    }

    print("\nBenchmark Summary:")
    table = [
        ["Configuration / Label", result["label"]],
        ["Total Jobs Submitted", result["job_count"]],
        ["Completed Jobs", result["completed_count"]],
        ["Failed Jobs", result["failed_count"]],
        ["Total Elapsed Time (s)", f"{result['total_completion_time_sec']}s"],
        ["Avg Queue Wait Time (s)", f"{result['avg_queue_wait_time_sec']}s"],
        ["Avg Processing Time (s)", f"{result['avg_processing_time_sec']}s"],
    ]
    print(tabulate(table, headers=["Metric", "Value"], tablefmt="grid"))
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run 20-job report benchmark.")
    parser.add_argument("--api-url", default="http://localhost:8000", help="API base URL")
    parser.add_argument("--jobs", type=int, default=20, help="Number of jobs to submit")
    parser.add_argument("--label", default="Run", help="Label for this benchmark run")
    parser.add_argument("--output", default="benchmark_result.json", help="Output file for metrics")
    args = parser.parse_args()

    metrics = run_benchmark(api_url=args.api_url, job_count=args.jobs, label=args.label)
    with open(args.output, "w") as f:
        json.dump(metrics, f, indent=2)
    print(f"\nSaved metrics to {args.output}")
