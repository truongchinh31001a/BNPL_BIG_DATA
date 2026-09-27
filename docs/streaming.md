# Streaming path

## Event lifecycle

```text
fake_bnpl_producer.py
  -> bnpl.transactions.raw
  -> streaming_kafka_to_bronze.py
  -> bronze/streaming_transactions_v2 (Parquet partitioned by event_date/event_hour)
```

Events mô phỏng đúng historical BNPL schema nhưng không chứa `default_30d/default_90d`. Principal, credit score, provider, category, tenor và first-time ratio dùng phân phối gần historical hơn; generator hỗ trợ đủ 37 bang. `FAKE_INVALID_RATE` tạo một tỷ lệ nhỏ events sai để demo quarantine.

Streaming services không chạy trong batch stack mặc định. Bật chúng theo nhu cầu:

```bash
docker compose --profile streaming up -d --build
```

## Inference

`streaming_predict.py` dùng một Kafka checkpoint riêng, chạy quality gate trong mỗi micro-batch, dùng cùng `add_bnpl_features`, load model được chọn từ Gold model registry, và upsert cả prediction 30D/90D vào PostgreSQL. PostgreSQL write chạy bằng `foreachPartition` với batch upsert; dữ liệu prediction không còn được `collect()` về Spark driver.

Inference service dùng Compose profile vì model registry chỉ tồn tại sau batch training:

```bash
docker compose --profile streaming --profile inference up -d
```

## Reliability

- Topic mặc định có 6 partitions và Spark giới hạn `KAFKA_MAX_OFFSETS_PER_TRIGGER` để kiểm soát backlog.
- Event-time watermark mặc định là 10 phút; `dropDuplicatesWithinWatermark` loại transaction trùng mà không giữ state vô hạn.
- Kafka offsets được quản lý bằng checkpoint trên MinIO.
- Bronze dùng `foreachBatch` để ghi Parquet partitioned; checkpoint không phụ thuộc vào file-sink `_spark_metadata`, vì vậy có thể compact offline rồi resume an toàn.
- Raw invalid events vẫn tồn tại ở Bronze; invalid inference events được append vào quarantine.
- Prediction unique key gồm transaction, model, version và horizon nên retry micro-batch không nhân đôi.
- Xác suất 90D được nâng tối thiểu bằng xác suất 30D để giữ `probability_30d <= probability_90d`.
- Kafka/Redpanda có named volume; Kafka không được coi là permanent lake storage.

Raw Bronze và inference là hai consumer độc lập. Đây là chủ ý thiết kế: Bronze giữ nguyên event để replay/audit, trong khi inference ưu tiên latency và tái sử dụng validation/features. Nếu cần Streaming Silver/Gold đầy đủ, có thể thêm consumer downstream từ Bronze mà không thay đổi producer.

## Drift và maintenance

Chạy `spark/jobs/12_monitor_data_drift.py` sau khi đã có streaming traffic. Job tính PSI giữa historical Gold và streaming cho credit band, loan size, merchant category, provider, state, first-time customer, tenor và interest-rate bucket; chi tiết lưu ở `gold/monitoring/data_drift`, PSI tổng được upsert vào Data Quality metrics.

Khi đã dừng streaming writers, chạy `spark/jobs/13_lake_maintenance.py` để compact Parquet theo giờ, compact Delta và `VACUUM` với retention mặc định 168 giờ. Job từ chối compact legacy file-stream sink còn `_spark_metadata`; retention thấp hơn 168 giờ bị chặn trừ khi đặt rõ `ALLOW_UNSAFE_VACUUM=true`.
