# Benchmarks

Hai job đo hai câu hỏi riêng: Spark scalability và full reload so với Delta MERGE thật.

## Spark scalability

Workload cố định: Delta read -> limit/sample -> filter -> groupBy -> aggregate -> join -> Parquet write. Mỗi configuration nên chạy ít nhất ba lần và báo cáo median runtime.

## Matrix

```text
sizes: 100K, 500K, 1M, 2M
workers: 1, 2
runs: 1, 2, 3
```

## Chạy job

Đặt environment tương ứng rồi chạy `spark/jobs/benchmark_scalability.py` bằng cùng `spark-submit --packages` như DAG. Worker dùng một service có thể scale, ví dụ `docker compose up -d --scale spark-worker=1` hoặc `--scale spark-worker=2`. Không thay đổi cores/memory giữa các lượt so sánh.

Ví dụ biến:

```bash
BENCHMARK_SIZE=500000
BENCHMARK_WORKER_COUNT=2
BENCHMARK_RUN_NUMBER=1
```

Kết quả Delta `benchmarks/results` gồm requested size, processed records, worker count, run number, runtime seconds và records/second. Median nên được tính theo size/worker sau khi loại warm-up nếu có. Job này không đo cập nhật tăng dần.

## Full reload vs Incremental Delta MERGE

`spark/jobs/benchmark_incremental.py` lấy mẫu Silver theo `transaction_id` một cách xác định, tách baseline và new batch. Mặc định new batch chiếm 25%; đổi bằng `BENCHMARK_INCREMENTAL_FRACTION` (giá trị trong khoảng 0–1). Job ghi số dòng thực tế của mỗi phần, kể cả khi Silver ít hơn `BENCHMARK_SIZE`.

- Full reload: transform toàn bộ mẫu bằng shared `add_bnpl_features()`, ghi lại toàn Delta target và đếm kết quả trong thời gian đo.
- Incremental: transform chỉ new batch, `MERGE` vào Delta target có sẵn theo `transaction_id`, rồi đếm kết quả trong thời gian đo.
- Baseline Delta target được khởi tạo ngoài thời gian đo incremental, vì nó đại diện cho bảng production đã tồn tại trước khi batch mới tới.
- Sau khi dừng timer, job retry đúng new batch và so sánh số dòng cùng tập transaction IDs. Retry không cộng vào runtime chính.

Biến môi trường:

```text
BENCHMARK_SIZE=100000|500000|1000000|2000000
BENCHMARK_WORKER_COUNT=1|2
BENCHMARK_RUN_NUMBER=1..N
BENCHMARK_INCREMENTAL_FRACTION=0.25
```

Kết quả riêng ở Delta `benchmarks/incremental_results`; khóa idempotent là `(dataset_size, worker_count, run_number, processing_mode)`. Các cột gồm `baseline_records`, `incremental_records`, `processed_records`, `runtime_seconds`, `records_per_second`, `final_row_count`, `idempotency_verified` và `created_at`. Không trộn kết quả này với scalability benchmark.
