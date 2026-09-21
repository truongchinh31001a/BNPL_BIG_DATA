# BNPL Big Data Platform

Nền tảng Big Data end-to-end cho phân tích và dự báo rủi ro BNPL tại Nigeria. Trọng tâm của project là ingestion, data quality, xử lý phân tán, incremental/idempotent pipeline và serving; Machine Learning là một downstream use case.

## Kiến trúc

```text
BATCH                                           STREAMING
Hugging Face historical data                   Fake BNPL producer
        |                                               |
        v                                               v
Bronze historical (MinIO Parquet)             Kafka: bnpl.transactions.raw
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

## Công nghệ

- Python, PySpark 3.5.1, Spark SQL, Spark MLlib và Structured Streaming
- Redpanda (Kafka-compatible), Airflow 2.9.2
- MinIO: Bronze Parquet; Silver và Gold Delta Lake 3.2
- PostgreSQL 16 làm serving/data warehouse cho Power BI
- Redpanda Console, Prometheus và Grafana cho quan sát vận hành
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
- Khuyến nghị tối thiểu 8 GB RAM dành cho Docker
- Các port batch: `5432`, `8080`, `8088`, `9001`, `9002`
- Các port optional: `19092`, `8089`, `9090`, `3000`

## Khởi động

```bash
cp .env.example .env
docker compose up -d --build
docker compose ps
```

Trên PowerShell dùng `Copy-Item .env.example .env` thay cho `cp`.

Lệnh mặc định chỉ bật batch stack. Redpanda, fake producer và streaming consumer nằm trong profile `streaming`, nên không chiếm tài nguyên khi chỉ chạy Airflow batch.

Các endpoint:

- Airflow: http://localhost:8088 (`airflow` / giá trị trong `.env`)
- Spark Master: http://localhost:8080
- MinIO API: http://localhost:9002; Console: http://localhost:9001
- PostgreSQL: `localhost:5432`, database `bnpl_dw`
- Kafka external bootstrap khi bật profile streaming: `localhost:19092`
- Kafka UI khi bật streaming/monitoring: http://localhost:8089
- Prometheus khi bật monitoring: http://localhost:9090
- Grafana khi bật monitoring: http://localhost:3000

`INGEST_MAX_ROWS=100000` trong `.env.example` phù hợp demo. Đặt `0` để ingest toàn bộ. Giới hạn được áp dụng riêng cho từng source.

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

Sau khi batch pipeline đã tạo model registry, bật inference:

```bash
docker compose --profile streaming --profile inference up -d
```

Prediction 30D và 90D được upsert vào `ml.predictions`. Event lỗi được giữ tại `rejected/streaming_transactions`.

Redpanda Console cũng được bật tại http://localhost:8089 để xem topic, partitions, messages và consumer groups.

## Monitoring

Khởi động monitoring stack độc lập với batch mặc định:

```bash
docker compose --profile monitoring up -d
```

Prometheus scrape Redpanda public metrics, Spark master/applications và toàn bộ Spark workers qua Docker DNS. Grafana tự provision datasource cùng dashboard `BNPL Platform Overview`. Tài khoản demo mặc định là `admin` / `admin`; thay các giá trị `GRAFANA_ADMIN_*` trong `.env` khi dùng ngoài máy cá nhân.

Chi tiết endpoint, alert và lệnh kiểm tra nằm trong [monitoring runbook](docs/monitoring.md).

## MinIO layout

```text
bronze/historical_transactions/           Parquet
bronze/streaming_transactions/            Parquet
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

Job `spark/jobs/benchmark_scalability.py` hỗ trợ các biến:

```text
BENCHMARK_SIZE=100000|500000|1000000|2000000
BENCHMARK_WORKER_COUNT=1|2
BENCHMARK_RUN_NUMBER=1..N
BENCHMARK_MODE=full|incremental
```

Kết quả được ghi idempotently vào Delta `benchmarks/results`. Xem [benchmark runbook](docs/benchmark.md).

Chạy toàn bộ ma trận chuẩn trên PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run_benchmark_matrix.ps1
```

Kết quả chạy thật, CSV và biểu đồ nằm trong [P1 evidence](docs/p1_evidence.md).

## Troubleshooting

- Airflow không thấy DAG: kiểm tra `docker compose logs airflow-scheduler`.
- Spark báo thiếu Delta/Kafka class: luôn dùng `--packages` như trong DAG/Compose.
- PostgreSQL dùng volume cũ: service `postgres-schema-init` tự chạy migration idempotent khi startup.
- Streaming inference restart liên tục: chạy batch DAG để tạo `gold/ml/model_registry` trước.
- Không test được Spark trên Windows: cài Java 17 và đặt `JAVA_HOME`, hoặc chạy test trong image.

Chi tiết: [architecture](docs/architecture.md), [data model](docs/data_model.md), [batch pipeline](docs/pipeline.md), [streaming](docs/streaming.md), [benchmark](docs/benchmark.md).
