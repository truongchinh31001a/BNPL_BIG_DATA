# Spark scalability benchmark

Workload cố định: Delta read -> deterministic expansion -> filter -> groupBy -> aggregate -> join -> Parquet write. Mỗi configuration chạy ba lần chính thức và báo cáo median runtime; một warm-up riêng cho mỗi cặp worker/mode không được đưa vào thống kê.

## Matrix

```text
sizes: 100K, 500K, 1M, 2M
workers: 1, 2
runs: 1, 2, 3
modes: full, incremental
```

## Chạy toàn bộ ma trận

Từ PowerShell tại root repository:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_benchmark_matrix.ps1
```

Script dừng producer/streaming để giữ tài nguyên ổn định, scale lần lượt 1 và 2 Spark workers, chạy warm-up rồi chạy đủ 48 lượt chính thức.

## Chạy một job

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

## Xuất evidence

`spark/jobs/export_benchmark_results.py` tính median từ ba runs và ghi CSV tại `benchmarks/summary_csv`. Sau khi đưa CSV về repository, tạo biểu đồ bằng:

```powershell
python scripts/render_benchmark_charts.py docs/evidence/p1/benchmark_summary.csv docs/evidence/p1
```

Kết quả đã kiểm chứng nằm tại:

- `docs/evidence/p1/benchmark_summary.csv`
- `docs/evidence/p1/benchmark_runtime.svg`
- `docs/evidence/p1/benchmark_throughput.svg`
- `docs/p1_evidence.md`

Spark workers bật automatic app cleanup với interval 60 giây và TTL 300 giây. Nếu Docker bị dừng cứng giữa benchmark, kiểm tra `docker system df -v`; tạo lại riêng worker sẽ giải phóng phần tạm mà không xóa named volume MinIO/PostgreSQL.
