# P3 Big Data Hardening Evidence

Ngày kiểm chứng: 22/09/2026. Môi trường là Docker Desktop local với một Spark master, hai Spark workers, một Redpanda broker và một MinIO node.

## Batch ingestion và partition pruning

- Historical ingestion được đổi sang hai pha: Hugging Face -> staged Parquet trên MinIO -> distributed Spark read -> Bronze.
- Validation đọc trực tiếp `source_slug=<source>/batch_key=<batch>` thay vì recursive scan toàn Bronze.
- Smoke run `p3_ingest_smoke_20260922` stage hai file, tạo 100 Bronze rows và validation trả về `total=100`, `rejected=0`.

## Kafka và distributed inference

- Topic `bnpl.transactions.raw` đã được tăng từ 1 lên 6 partitions.
- Sau khi producer mới chạy, cả partitions 0-5 đều có messages và PostgreSQL nhận prediction từ đủ 6 partitions.
- Inference sử dụng `foreachPartition` và batched PostgreSQL upsert; không còn `scored.collect()` trong micro-batch path.
- Sau hai lượt compact/restart từ checkpoint: 26,948 prediction rows tương ứng đúng 13,474 transactions x 2 horizons, không có duplicate theo unique key.
- `ml.vw_horizon_consistency`: 13,474 paired transactions, 0 probability violations và 0 class violations. Trước P3 có 828 probability violations và 1 class violation.

## Event time và latency

- Event timestamp, Kafka partition và offset được giữ trong `ml.predictions`.
- `ml.vw_streaming_latency` báo latency toàn lịch sử, bao gồm replay backlog.
- `ml.vw_streaming_latency_recent` báo p50/p95/p99 cho event 15 phút gần nhất để tách steady-state khỏi backfill.
- Lần kiểm chứng đầu tiên có 1,157 predictions cho mỗi horizon trong recent window. Số latency còn bao gồm backlog tạo trong lúc Spark workers restart, nên không dùng lượt này làm SLA production.

## Small-file compaction

- Streaming Bronze v2 partition theo `event_date/event_hour`.
- Job `13_lake_maintenance.py` chạy offline và compact thành công.
- Lượt kiểm chứng cuối giảm Streaming Bronze từ 15 xuống 6 Parquet files cho 14,029 rows và rejected từ 26 xuống 6 files cho 661 rows; P1 từng ghi nhận 448 files cho khoảng 9,594 events ở layout cũ.
- Maintenance report được lưu tại `gold/monitoring/file_maintenance`.
- Lượt resume đầu tiên phát hiện file-sink metadata cũ không tương thích với path swap. Bronze writer đã được chuyển sang `foreachBatch`; maintenance hiện chặn mọi legacy path còn `_spark_metadata` trước khi compact.

## Data drift

Job `12_monitor_data_drift.py` so sánh 110,000 historical records với 10,666 valid streaming records cũ và ghi PSI vào Delta/PostgreSQL.

| Feature | PSI | Level |
|---|---:|---|
| loan_size_category | 2.488 | SIGNIFICANT |
| customer_state | 1.162 | SIGNIFICANT |
| credit_score_band | 0.440 | SIGNIFICANT |
| interest_rate_bucket | 0.424 | SIGNIFICANT |
| merchant_category | 0.334 | SIGNIFICANT |
| tenor_bucket | 0.222 | MODERATE |
| provider | 0.114 | MODERATE |
| first_time_customer_bucket | 0.038 | STABLE |

Kết quả xác nhận generator uniform cũ tạo training-serving skew. Producer P3 đã chuyển sang weighted/lognormal/Gaussian distributions và đủ 37 states; PSI cần được chạy lại sau khi traffic mới đủ lớn để đo mức cải thiện.

## Phase benchmark smoke test

Run `size=1,000`, `workers=2`, `mode=incremental`, `run_number=99` kiểm chứng schema phase metrics:

| Phase | Seconds | Records |
|---|---:|---:|
| source_read | 19.076 | 110,000 |
| sample_preparation | 0.686 | 236 |
| transform_shuffle | 0.723 | 163 |
| output_write | 0.964 | 163 |
| total | 21.448 | 236 |

Lượt smoke này cho thấy cold source read chiếm phần lớn runtime. Đây là kiểm chứng chức năng của phase instrumentation, không thay thế ma trận benchmark P1 chính thức.

## Tests

- Docker Spark test suite cuối: `12 passed in 27.14s` khi streaming stack đang chạy.
- Hai test mới xác nhận PSI bằng 0 cho phân phối giống nhau và gắn `SIGNIFICANT` cho phân phối dịch chuyển mạnh.

## Giới hạn còn lại

- Chưa chạy full source 2 triệu unique rows với staged ingestion.
- Chưa có physical multi-node benchmark; hai workers vẫn dùng chung một host.
- Chưa có test tự động cho event đến trễ ngoài watermark và throughput matrix theo producer rate.
- Auxiliary personal-loan dataset vẫn chỉ được giữ ở Bronze vì chưa có business objective rõ cho Silver/Gold.
