# Streaming path

## Event lifecycle

```text
fake_bnpl_producer.py
  -> bnpl.transactions.raw
  -> streaming_kafka_to_bronze.py
  -> bronze/streaming_transactions (Parquet + checkpoint)
```

Events mô phỏng đúng historical BNPL schema nhưng không chứa `default_30d/default_90d`. `FAKE_INVALID_RATE` tạo một tỷ lệ nhỏ events sai để demo quarantine.

Streaming services không chạy trong batch stack mặc định. Bật chúng theo nhu cầu:

```bash
docker compose --profile streaming up -d --build
```

## Inference

`streaming_predict.py` dùng một Kafka checkpoint riêng, chạy quality gate trong mỗi micro-batch, dùng cùng `add_bnpl_features`, load model được chọn từ Gold model registry, và upsert cả prediction 30D/90D vào PostgreSQL.

Inference service dùng Compose profile vì model registry chỉ tồn tại sau batch training:

```bash
docker compose --profile streaming --profile inference up -d
```

## Reliability

- Kafka offsets được quản lý bằng checkpoint trên MinIO.
- Raw invalid events vẫn tồn tại ở Bronze; invalid inference events được append vào quarantine.
- Prediction unique key gồm transaction, model, version và horizon nên retry micro-batch không nhân đôi.
- Kafka/Redpanda có named volume; Kafka không được coi là permanent lake storage.
