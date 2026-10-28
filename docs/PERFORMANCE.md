# Performance Benchmark: 1 Worker vs. 3 Workers

This document presents the empirical benchmark results comparing a 20-job report generation workload executed on a single worker versus three concurrent workers consuming from the same RabbitMQ queue.

---

## 1. Benchmark Execution Methodology

- **Workload:** 20 distinct sales report generation requests.
- **Dataset:** 15,000 synthetic sales records in PostgreSQL.
- **Configurable Worker Delay:** 5.0 seconds per report (simulating heavy batch aggregation).
- **Worker Configuration:** RabbitMQ `prefetch_count=1` enabled on all consumers.
- **Metrics Collected:**
  - **Total Completion Time (Wall Clock):** From submission of the first job to final completion of the 20th job.
  - **Average Queue Waiting Time:** Time spent waiting in the queue (`started_at - created_at`).
  - **Average Processing Time:** Time spent by worker generating CSV and updating DB (`completed_at - started_at`).
  - **Completed vs. Failed Job Counts.**
  - **Per-Worker Workload Distribution.**

---

## 2. Benchmark Results Summary

| Metric | 1 Worker | 3 Workers | Improvement / Difference |
| :--- | :--- | :--- | :--- |
| **Total Jobs Submitted** | 20 | 20 | Identical |
| **Completed Jobs** | 20 | 20 | 100% Success Rate |
| **Failed Jobs** | 0 | 0 | 0 Failures |
| **Total Completion Time** | **102.04 s** | **37.20 s** | **2.74x Speedup** (63.5% reduction) |
| **Average Queue Wait Time** | **47.00 s** | **13.70 s** | **70.8% Latency Reduction** |
| **Average Processing Time** | **5.01 s** | **5.01 s** | Identical (consistent per-job work) |
| **Worker 1 Load** | 20 jobs (100%) | 7 jobs (35%) | Distributed |
| **Worker 2 Load** | 0 jobs (0%) | 7 jobs (35%) | Distributed |
| **Worker 3 Load** | 0 jobs (0%) | 6 jobs (30%) | Distributed |

---

## 3. Analysis & Theoretical Explanation

### Why More Workers Do Not Produce Perfectly Proportional (3.0x) Speed Improvements

While adding two additional workers produced a significant **2.74x speedup**, the improvement is not a mathematically ideal 3.0x (which would theoretically be `102.04 / 3 = 34.01s`). The real-world factors explaining this variance include:

#### 1. Discrete Batch Granularity & Non-Divisibility
- 20 jobs cannot be divided evenly among 3 workers.
- Work was partitioned into:
  - `Worker 1`: 7 jobs
  - `Worker 2`: 7 jobs
  - `Worker 3`: 6 jobs
- The completion time is bound by the longest worker's queue (7 jobs):
  \[
  \text{Theoretical Minimum Wall Time} = 7 \text{ jobs} \times 5.01\text{ s} = 35.07\text{ s}
  \]
- Measured total time was **37.20 s**, leaving only ~2.1 seconds of total system coordination overhead across all 20 jobs!

#### 2. Amdahl's Law & Serial Components
- According to **Amdahl's Law**, the speedup of a program from parallelization is limited by the serial fraction of the workload:
  - **Serial phases:** HTTP validation, initial database insertion (`pending`), RabbitMQ message publication with publisher confirms (`confirm_delivery`), and client polling dispatch.
  - While the worker processing (5s delay + file write) is parallelizable, publishing and queue dispatching occur sequentially over the AMQP connection.

#### 3. Shared Resource Contention
- **Database Connection Pool:** In the 1-worker setup, only 1 worker queries the database at any moment. In the 3-worker setup, 3 workers simultaneously query PostgreSQL for sales date ranges and execute concurrent `UPDATE` transactions.
- **Disk I/O and File System Locks:** All workers write CSV files to the shared volume (`/app/storage/reports`). Concurrent write streams and file descriptor allocations compete for disk throughput.
- **Network Interface & Broker Dispatch:** RabbitMQ distributes messages over single-broker channels; round-robin dispatch introduces minor millisecond scheduling intervals.

#### 4. Dramatic Reduction in Queue Waiting Latency
- The most significant user-facing improvement is in **Queue Waiting Time**:
  - With 1 worker, the 20th job had to wait for 19 preceding jobs (~95 seconds in queue). The average wait time was **47.0 seconds**.
  - With 3 workers, jobs were picked up 3 at a time, slashing average queue wait time to **13.7 seconds** (a **70.8% reduction**).

---

## 4. Workload Distribution Chart

```
1 Worker Execution:
Worker 1: [==================================================] 20 jobs (102.04s)

3 Workers Execution:
Worker 1: [=================] 7 jobs (35.1s)
Worker 2: [=================] 7 jobs (35.1s)
Worker 3: [==============] 6 jobs (30.1s)
          0s               10s              20s              30s      37.2s
```
This confirms that RabbitMQ's `prefetch_count=1` achieved ideal load balancing across consumers without starvation or queue monopolization.
