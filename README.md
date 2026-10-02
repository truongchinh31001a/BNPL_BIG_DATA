# BNPL Big Data Platform

Nền tảng Big Data end-to-end cho phân tích và dự báo rủi ro BNPL tại Nigeria. Trọng tâm của project là ingestion, data quality, xử lý phân tán, incremental/idempotent pipeline và serving; Machine Learning là một downstream use case.

## Kiến trúc

```text
BATCH                                           STREAMING
Hugging Face historical data                   Fake BNPL producer
        |                                               |
        v                                               v
Bronze historical (HDFS Parquet)              Kafka: bnpl.transactions.raw
        |                                               |
Schema validation + Data Quality Gate                   v
        | PASS                         Spark Structured Streaming
        +------> Silver (Delta MERGE)                    |
        | FAIL                                          v
        +------> Rejected/Quarantine            Bronze streaming (Parquet)
                    |                                   |
                    v                                   v
          Shared feature transforms          Validation + saved 30D/90D models
                    |                                   |
           +--------+--------+                          v
           v                 v                    PostgreSQL predictions
    Gold Analytics     Gold ML Features                   |
           |                 |                            v
      Star Schema      Spark MLlib models              Power BI
           |
       PostgreSQL -> Power BI
```

Kafka là transport layer, không phải Bronze. Airflow chỉ orchestrate batch pipeline; các Structured Streaming jobs chạy lâu dài dưới Docker Compose.

```text
Spark executors -> hdfs://namenode:8020/bnpl-data
                         |
             +-----------+-----------+
             v           v           v
        datanode-1   datanode-2   datanode-3
        volume #1    volume #2    volume #3
             \________ replication = 3 ________/
```

## Công nghệ

- Python, PySpark 3.5.1, Spark SQL, Spark MLlib và Structured Streaming
- Apache Kafka 3.9.1 (KRaft), Airflow 2.9.2
- HDFS 3.3.6: 1 NameNode + 3 DataNodes, replication factor 3; Bronze Parquet; Silver và Gold Delta Lake 3.2
- PostgreSQL 16 làm serving/data warehouse cho Power BI
- Prometheus và Grafana cho quan sát Spark
- Docker Compose với 1 Spark master và service worker có thể scale bằng `SPARK_WORKER_REPLICAS`

Delta Lake 3.2 được chọn vì tương thích với Spark 3.5; global automatic schema evolution bị tắt.

## Cấu trúc chính

```text
airflow/dags/bnpl_batch_pipeline.py       Batch orchestration
kafka/producer/fake_bnpl_producer.py      Label-free event generator
kafka/schemas/bnpl_event_schema.json      Streaming contract
spark/bnpl_common/                        Config, schemas, validation, features, Delta
spark/jobs/01_ingest.py ... 10_load_postgres.py
spark/jobs/streaming_kafka_to_bronze.py
spark/jobs/streaming_predict.py
sql/                                      Serving, DQ and batch-registry schemas
monitoring/                               Prometheus rules và Grafana provisioning
.github/workflows/tests.yml               Docker-based CI tests
tests/                                    Deterministic Spark transformation tests
docs/                                     Architecture and runbooks
```

## Yêu cầu

- Docker Desktop và Docker Compose v2
- Cấp tối thiểu 10 GB RAM cho batch stack; khuyến nghị 12 GB RAM khi chạy full stack hoặc job sinh 10 triệu dòng
- Các port batch: `5432`, `8020`, `9864`, `9865`, `9866`, `9870`, `8080`, `8088`
- Các port optional: `19092`, `9090`, `3000`

## Khởi động

```bash
cp .env.example .env
docker compose up -d --build
docker compose ps
docker compose exec -T namenode hdfs dfsadmin -report
```

Để nâng cụm cũ lên v2 và chờ HDFS sao chép dữ liệu hiện có sang đủ ba DataNodes mà không stream log giữa chừng:

```bash
python scripts/migrate_hdfs_v2.py
```

Trên PowerShell dùng `Copy-Item .env.example .env` thay cho `cp`.

Lệnh mặc định chỉ bật batch stack. Apache Kafka, fake producer và streaming consumer nằm trong profile `streaming`, nên không chiếm tài nguyên khi chỉ chạy Airflow batch.

Các endpoint:

- Airflow: http://localhost:8088 (`airflow` / giá trị trong `.env`)
- Spark Master: http://localhost:8080
- HDFS NameNode UI: http://localhost:9870
- HDFS DataNode UI: http://localhost:9864, http://localhost:9865, http://localhost:9866
- PostgreSQL: `localhost:5432`, database `bnpl_dw`
- Kafka external bootstrap khi bật profile streaming: `localhost:19092`
- Prometheus khi bật monitoring: http://localhost:9090
- Grafana khi bật monitoring: http://localhost:3000

`INGEST_MAX_ROWS=100000` trong `.env.example` phù hợp demo. Đặt `0` để ingest toàn bộ. Giới hạn được áp dụng riêng cho từng source.

## Sinh 10 triệu dòng bằng PySpark

Job `spark/jobs/12_generate_10m_bronze.py` dùng hoàn toàn Spark SQL expressions, không tạo dữ liệu bằng Python loop hoặc Hadoop MapReduce. Job ghi raw Parquet vào `bronze/generated_streaming_transactions/dataset_version=v2_10m`, partition theo `event_date/event_hour`:

```bash
python scripts/run_10m_generation.py
```

Kiểm tra sau khi job kết thúc:

```bash
docker compose exec -T namenode hdfs dfs -du -h /bnpl-data/bronze/generated_streaming_transactions
docker compose exec -T namenode hdfs fsck /bnpl-data/bronze/generated_streaming_transactions
```

## Batch pipeline

Trigger DAG `bnpl_batch_pipeline` trong Airflow. Có thể truyền config:

```json
{
  "batch_id": "batch_20260917_001",
  "model_version": "v1",
  "ingest_max_rows": 100000
}
```

Thứ tự:

```text
ingest_data -> validate_bronze -> bronze_to_silver -> data_quality_metrics
             -> feature_engineering
                 |-> build_analytics --------------------|
                 |-> build_ml_features -> train -> eval -|-> load_postgres
```

- Historical sources được đọc bằng Hugging Face streaming API nhưng đi thẳng vào Bronze batch, không qua Kafka.
- Dataset BNPL chính đi qua Silver/Gold. Dataset personal-loan bổ sung được giữ raw tại Bronze cho audit và pipeline riêng.
- Chạy lại cùng `batch_id` không nhân đôi Silver/Gold vì Bronze partition được ghi lại và Delta MERGE dùng `transaction_id`.
- Mỗi Spark job có thể chạy độc lập trước khi nối vào Airflow.

## Streaming

Khởi động streaming path khi cần:

```bash
docker compose --profile streaming up -d --build
```

Profile này chạy:

- `fake-bnpl-producer`: tạo event không có `default_30d/default_90d`.
- `spark-streaming-job`: đọc Kafka và lưu raw event + Kafka metadata vào Bronze Parquet.
- `spark-streaming-prediction` (profile `inference`): đọc file Bronze mới, chạy quality gate và shared features, rồi ghi dự báo vào PostgreSQL; không đọc Kafka trực tiếp.

Sau khi batch pipeline đã tạo model registry, bật inference:

```bash
docker compose --profile streaming --profile inference up -d
```

Prediction 30D và 90D được upsert vào `ml.predictions`. Event lỗi được giữ tại `rejected/streaming_transactions`.

Luồng đầy đủ: Fake BNPL Producer → Apache Kafka → Spark Structured Streaming → Bronze Parquet → Validation / Quality Gate → Shared Feature Engineering → Saved 30D + 90D Models → PostgreSQL Predictions → Power BI.

## Monitoring

Khởi động monitoring stack độc lập với batch mặc định:

```bash
docker compose --profile monitoring up -d
```

Prometheus scrape Spark master/applications và toàn bộ Spark workers qua Docker DNS. Grafana tự provision datasource cùng dashboard `BNPL Platform Overview`. Tài khoản demo mặc định là `admin` / `admin`; thay các giá trị `GRAFANA_ADMIN_*` trong `.env` khi dùng ngoài máy cá nhân.

Chi tiết endpoint, alert và lệnh kiểm tra nằm trong [monitoring runbook](docs/monitoring.md).

## HDFS layout

Mọi lake path nằm dưới `hdfs://namenode:8020/bnpl-data/`:

```text
bronze/historical_transactions/           Parquet
bronze/streaming_transactions_v2/         Parquet, event_date/event_hour
bronze/generated_streaming_transactions/  Parquet 10M, event_date/event_hour
rejected/transactions/                    Parquet
rejected/streaming_transactions/          Parquet
silver/transactions/                      Delta
gold/shared/enriched_transactions/        Delta
gold/analytics/*                          Delta
gold/ml/ml_bnpl_features/                 Delta
gold/ml/model_metrics/                    Delta
models/default_30d/* và models/default_90d/*
```

## PostgreSQL và Power BI

Power BI kết nối PostgreSQL và ưu tiên các object:

- `analytics.vw_bnpl_overview`
- `analytics.vw_bnpl_transactions`
- `ml.vw_prediction_monitoring`
- `ml.model_metrics`
- `data_quality.data_quality_metrics`
- `pipeline.pipeline_batches`

Không lưu raw Big Data trong PostgreSQL.

## Tests

Nếu host có Java 17 và `JAVA_HOME`:

```bash
python -m pytest -q tests
```

Hoặc chạy bằng Spark Docker image:

```bash
docker build -t bnpl-spark:3.5.1 ./spark
docker run --rm \
  -e PYTHONPATH=/workspace/spark:/opt/bitnami/spark/python:/opt/bitnami/spark/python/lib/py4j-0.10.9.7-src.zip \
  -v "$PWD:/workspace" -w /workspace \
  bnpl-spark:3.5.1 python -m pytest -q tests
```

Trên PowerShell thay dấu nối dòng `\` bằng backtick (`` ` ``); giữ nguyên giá trị `PYTHONPATH` ở trên.

GitHub Actions chạy cùng Docker test command trên mọi push và pull request bằng workflow `.github/workflows/tests.yml`.

## Benchmark

Scalability benchmark `spark/jobs/benchmark_scalability.py` đo workload đọc/lọc/group/join/ghi theo kích thước và số worker:

```text
BENCHMARK_SIZE=100000|500000|1000000|2000000
BENCHMARK_WORKER_COUNT=1|2
BENCHMARK_RUN_NUMBER=1..N
```

Kết quả nằm ở Delta `benchmarks/results`. Job riêng `spark/jobs/benchmark_incremental.py` so sánh full reload với cập nhật Delta MERGE theo `transaction_id`, lưu vào `benchmarks/incremental_results`; baseline được chuẩn bị ngoài thời gian đo. Xem [benchmark runbook](docs/benchmark.md).

Chạy toàn bộ ma trận chuẩn bằng Python:

```bash
python scripts/run_benchmark_matrix.py
```

Kết quả pre-HDFS cũ được giữ dưới tên `*_legacy_pre_hdfs.*` để tham khảo. Sau khi chạy lại ma trận, exporter tạo CSV và bốn biểu đồ HDFS mới; xem [P1 evidence](docs/p1_evidence.md).

## Troubleshooting

- Airflow không thấy DAG: kiểm tra `docker compose logs airflow-scheduler`.
- Spark báo thiếu Delta/Kafka class: luôn dùng `--packages` như trong DAG/Compose.
- PostgreSQL dùng volume cũ: service `postgres-schema-init` tự chạy migration idempotent khi startup.
- Streaming inference restart liên tục: chạy batch DAG để tạo `gold/ml/model_registry` trước.
- Không test được Spark trên Windows: cài Java 17 và đặt `JAVA_HOME`, hoặc chạy test trong image.

Chi tiết: [architecture](docs/architecture.md), [data model](docs/data_model.md), [batch pipeline](docs/pipeline.md), [streaming](docs/streaming.md), [benchmark](docs/benchmark.md).
