# Benchmarks

Hai job đo hai câu hỏi riêng: Spark scalability và full reload so với Delta MERGE thật.

## Spark scalability

Workload cố định: Delta read -> deterministic expansion -> filter -> groupBy -> aggregate -> join -> Parquet write. Mỗi configuration chạy ba lần chính thức và báo cáo median runtime; một warm-up riêng cho mỗi cặp worker/suite không được đưa vào thống kê.

## Matrix

```text
sizes: 100K, 500K, 1M, 2M
workers: 1, 2
runs: 1, 2, 3
```

## Chạy toàn bộ ma trận

Từ terminal tại root repository:

```bash
python scripts/run_benchmark_matrix.py
```

Script dừng producer/streaming để giữ tài nguyên ổn định, scale lần lượt 1 và 2 Spark workers, rồi chạy hai suite `scalability` và `incremental`. Mỗi suite có warm-up riêng và 24 lượt chính thức, tổng cộng 48 lượt chính thức. Có thể chỉ chạy một suite bằng `--benchmark scalability` hoặc `--benchmark incremental`.

## Chạy một job

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

## Xuất evidence

`spark/jobs/export_benchmark_results.py` tính median từ ba runs và ghi CSV tại `benchmarks/summary_csv`. Sau khi đưa CSV về repository, tạo biểu đồ bằng:

```bash
python scripts/render_benchmark_charts.py docs/evidence/p1/benchmark_summary.csv docs/evidence/p1
```

Exporter gộp hai Delta result sets và thêm cột `benchmark_suite` để không trộn workload scalability với full reload/MERGE. Kết quả HDFS mới sau khi chạy đầy đủ sẽ nằm tại:

- `docs/evidence/p1/benchmark_summary.csv`
- `docs/evidence/p1/benchmark_runtime.svg`
- `docs/evidence/p1/benchmark_throughput.svg`
- `docs/evidence/p1/benchmark_incremental_runtime.svg`
- `docs/evidence/p1/benchmark_incremental_throughput.svg`
- `docs/p1_evidence.md`

Ba artifacts có hậu tố `_legacy_pre_hdfs` chỉ là bằng chứng lịch sử từ implementation benchmark cũ; không dùng chúng làm kết quả chính thức của kiến trúc HDFS.

Spark workers bật automatic app cleanup với interval 60 giây và TTL 300 giây. Nếu Docker bị dừng cứng giữa benchmark, kiểm tra `docker system df -v`; tạo lại riêng worker sẽ giải phóng phần tạm mà không xóa named volume HDFS/PostgreSQL.
