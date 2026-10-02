# Streaming path

## Event lifecycle

```text
Fake BNPL Producer -> Apache Kafka -> Spark Structured Streaming
  -> Bronze Parquet -> Validation / Quality Gate
  -> Shared Feature Engineering -> Saved 30D + 90D Models
  -> PostgreSQL Predictions -> Power BI
```

`streaming_kafka_to_bronze.py` là consumer Kafka duy nhất. Nó ghi raw JSON và Kafka metadata vào `bronze/streaming_transactions` (Parquet + checkpoint `_checkpoints/kafka_to_bronze`). Giữ nguyên checkpoint của Parquet sink để các batch tiếp theo không xung đột với metadata log của Bronze đã tồn tại. `streaming_predict.py` đọc file Bronze với `BRONZE_STREAMING_SCHEMA` và checkpoint mới `_checkpoints/bronze_to_prediction_v1`, không đọc Kafka trực tiếp.

Events mô phỏng đúng historical BNPL schema nhưng không chứa `default_30d/default_90d`. `FAKE_INVALID_RATE` tạo một tỷ lệ nhỏ events sai để demo quarantine.

Streaming services không chạy trong batch stack mặc định. Bật chúng theo nhu cầu:

```bash
docker compose --profile streaming up -d --build
```

## Inference

`streaming_predict.py` dùng một checkpoint file-stream riêng, chạy quality gate trong mỗi micro-batch, dùng cùng `add_bnpl_features`, load model được chọn từ Gold model registry, và upsert cả prediction 30D/90D vào PostgreSQL. Job chờ Bronze path trong thời gian giới hạn; nếu chưa có thì Compose restart, không chuyển sang đọc Kafka.

Inference service dùng Compose profile vì model registry chỉ tồn tại sau batch training:

```bash
docker compose --profile streaming --profile inference up -d
```

## Reliability

- Kafka offsets được quản lý bằng checkpoint trên HDFS.
- Raw invalid events vẫn tồn tại ở Bronze; invalid inference events được append vào quarantine.
- Prediction unique key gồm transaction, model, version và horizon nên retry micro-batch không nhân đôi.
- Apache Kafka KRaft có named volume `kafka_data`; Bronze mới là raw source of truth lâu dài. Bootstrap nội bộ là `kafka:9092`, bên ngoài là `localhost:19092`.
