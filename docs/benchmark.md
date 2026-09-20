# Spark scalability benchmark

Workload cố định: Delta read -> limit/sample -> filter -> groupBy -> aggregate -> join -> Parquet write. Mỗi configuration nên chạy ít nhất ba lần và báo cáo median runtime.

## Matrix

```text
sizes: 100K, 500K, 1M, 2M
workers: 1, 2
runs: 1, 2, 3
modes: full, incremental
```

## Chạy job

Đặt environment tương ứng rồi chạy `spark/jobs/benchmark_scalability.py` bằng cùng `spark-submit --packages` như DAG. Worker dùng một service có thể scale, ví dụ `docker compose up -d --scale spark-worker=1` hoặc `--scale spark-worker=2`. Không thay đổi cores/memory giữa các lượt so sánh.

Ví dụ biến:

```bash
BENCHMARK_SIZE=500000
BENCHMARK_WORKER_COUNT=2
BENCHMARK_RUN_NUMBER=1
BENCHMARK_MODE=full
```

Kết quả Delta `benchmarks/results` gồm requested size, processed records, worker count, run number, mode, runtime seconds và records/second. Median nên được tính theo size/worker/mode sau khi loại warm-up nếu có.

Full mode xử lý toàn sample. Incremental mode dùng deterministic quarter partition theo transaction hash để so sánh workload cập nhật.
