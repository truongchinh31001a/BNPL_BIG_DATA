# Tổng hợp Big Data và quy trình xử lý dữ liệu

## 1. Mục đích tài liệu

Tài liệu này tổng hợp phần trọng tâm Big Data của BNPL Big Data Platform dựa trên báo cáo `Final Bigdata_Group2.docx`, mã nguồn hiện tại và các bằng chứng thực thi P0-P3. Phạm vi gồm dữ liệu đầu vào, batch processing, streaming processing, Medallion Architecture, Data Quality, incremental processing, Machine Learning downstream, benchmark và vận hành dữ liệu. Phần triển khai cloud không nằm trong tài liệu này.

Kết luận chính: điểm mạnh của project không nằm ở số lượng công nghệ hay riêng mô hình Machine Learning. Giá trị cốt lõi là một data platform end-to-end có hai đường xử lý độc lập, giữ được raw data để audit, áp dụng quality gate trước trusted data, xử lý incremental có idempotency, tái sử dụng cùng feature contract cho batch và streaming, đồng thời có bằng chứng chạy thật cho từng lớp.

## 2. Bài toán dữ liệu

Project xử lý giao dịch Buy Now, Pay Later tại Nigeria với hai đầu ra:

- Analytics: số lượng giao dịch, tổng giá trị BNPL, khoản vay trung bình, default rate, phân tích provider, merchant, địa lý và credit risk.
- Machine Learning: dự đoán xác suất và nhãn default trong 30 ngày và 90 ngày.

Một record nghiệp vụ tương ứng một giao dịch BNPL và được định danh bởi `transaction_id`. Dataset lịch sử chứa nhãn `default_30d` và `default_90d`; event streaming không chứa hai nhãn này vì kết quả default chưa tồn tại tại thời điểm giao dịch phát sinh.

Nguồn chính là dataset synthetic `electricsheepafrica/africa-synth-banking-bnpl-nigeria` trên Hugging Face, có khoảng 2 triệu record. Các kết quả không được diễn giải thành kết luận thực nghiệm cho toàn bộ thị trường Nigeria.

## 3. Vì sao đây là bài toán Big Data

Project không chứng minh Big Data chỉ bằng row count. Các đặc tính cần trình bày cùng nhau gồm:

| Khía cạnh | Cách project thể hiện |
| --- | --- |
| Volume | Nguồn khoảng 2 triệu record; batch đã kiểm chứng 110.000 record và benchmark tới 2 triệu record. |
| Velocity | Fake producer phát event liên tục vào Kafka; Spark Structured Streaming xử lý theo micro-batch. |
| Variety | Dữ liệu lịch sử, JSON event, Kafka metadata, DQ metrics, model metrics, prediction và monitoring data. |
| Veracity | Explicit schema, quality gate, quarantine, controlled schema evolution và drift monitoring. |
| Value | Gold Analytics, Star Schema, Power BI và dự đoán default 30D/90D. |

Quy mô demo local chưa phải quy mô production. Giá trị học thuật nằm ở kiến trúc, tính đúng đắn, khả năng replay, xử lý phân tán logic và bằng chứng benchmark có kiểm soát.

## 4. Kiến trúc dữ liệu tổng thể

Hệ thống có hai data path độc lập nhưng dùng chung các contract dữ liệu:

```text
Historical source
  -> staged Parquet
  -> Bronze historical Parquet
  -> Quality Gate
  -> Silver Delta
  -> Gold Analytics / Gold ML
  -> PostgreSQL / Power BI / Model training

Fake producer
  -> Redpanda Kafka, 6 partitions
  -> Spark Structured Streaming
      -> Bronze streaming Parquet
      -> Quality Gate + shared features + saved models
      -> PostgreSQL predictions 30D / 90D
```

Airflow chỉ điều phối batch workflow. Streaming jobs là long-running services chạy bằng Docker Compose. Kafka là transport layer, không phải Bronze; MinIO mới là persistent data lake.

## 5. Vai trò của từng thành phần

| Thành phần | Vai trò chính |
| --- | --- |
| Spark | Distributed processing, transformation, aggregation, ML training và streaming inference. |
| MinIO | Object storage cho Bronze, Silver, Gold, checkpoint, model và benchmark artifacts. |
| Delta Lake | ACID table, schema enforcement và `MERGE` cho Silver/Gold. |
| Redpanda | Kafka-compatible event transport cho giao dịch mới. |
| Airflow | Dependency, retry, logging và scheduling cho batch DAG. |
| PostgreSQL | Serving layer cho BI, predictions, DQ metrics và batch registry. |
| Power BI | Trình bày analytics, risk, geography, streaming prediction và platform monitoring. |
| Prometheus/Grafana | Quan sát Redpanda, Spark và trạng thái platform. |

## 6. Medallion Architecture

### 6.1 Bronze

Bronze giữ dữ liệu gần nguyên bản để audit và reprocess.

- Historical Bronze dùng Parquet và metadata `_ingested_at`, `_source`, `_batch_id`, `_pipeline_run_id`, `_ingestion_type`.
- Streaming Bronze giữ raw JSON cùng `_kafka_topic`, `_kafka_partition`, `_kafka_offset`, `_kafka_timestamp` và `_schema_parse_ok`.
- Historical và streaming được tách path để tránh trộn responsibility.
- Historical ingestion dùng hai pha: tải source thành staged Parquet rồi để Spark đọc phân tán vào Bronze.

### 6.2 Silver

Silver là canonical trusted transaction table:

- Parse và cast kiểu dữ liệu bằng explicit schema.
- Chuẩn hóa date, string và category.
- Kiểm tra range và required fields.
- Deduplicate theo `transaction_id`.
- Ghi Delta `MERGE` thay vì blind append hoặc overwrite toàn bảng.
- Không tự động đưa extra column vào Silver; schema change phải được cập nhật có chủ đích.

### 6.3 Gold

Gold tách thành hai nhánh:

- Gold Analytics: transaction grain, deterministic dimension keys và Star Schema.
- Gold ML: flat feature table dùng chung cho Spark MLlib.

PostgreSQL chỉ nhận dữ liệu phục vụ query và dashboard. Raw Big Data không được chuyển vào PostgreSQL.

## 7. Batch pipeline

Batch DAG gồm mười bước chính:

1. `01_ingest.py`: source snapshot sang Bronze Parquet.
2. `02_validate_bronze.py`: explicit parse, schema comparison, quality gate và quarantine.
3. `03_bronze_to_silver.py`: Delta `MERGE` theo `transaction_id`.
4. `04_data_quality.py`: ghi metrics vào Delta và PostgreSQL.
5. `05_feature_engineering.py`: reusable feature transformation.
6. `06_build_star_schema.py`: dimensions và transaction fact.
7. `07_build_ml_features.py`: Gold ML feature contract.
8. `08_train_models.py`: train LR, RF và GBT cho 30D/90D.
9. `09_evaluate_models.py`: đánh giá và chọn model.
10. `10_load_postgres.py`: staging và upsert vào serving schemas.

Lần kiểm chứng P0 xử lý 100.000 BNPL transactions end-to-end. Lần P1 incremental tăng trạng thái cuối lên 110.000 transaction duy nhất.

## 8. Schema Validation và Data Quality

Quality Gate nằm giữa Bronze và Silver. Các rule chính gồm:

- `transaction_id` bắt buộc và không trùng trong batch.
- Date phải parse được.
- Categorical identifiers không được rỗng.
- `principal_ngn > 0`.
- `interest_rate_monthly >= 0`.
- Tenor và số installment phải dương.
- Credit score nằm trong khoảng 300 đến 850.
- Boolean và target chỉ nhận giá trị hợp lệ; target NULL được phép cho streaming.

Record FAIL không bị drop âm thầm. Nó được ghi vào quarantine với `_validation_errors`, `_rejected_at`, `_pipeline_run_id` và `_batch_id`.

Batch `quality_demo` có 4 record: 2 pass và 2 reject. Các lỗi gồm duplicate, invalid date, principal âm và credit score ngoài miền. Metrics đã ghi nhận `duplicate_count=1`, `rejected_record_count=2` và `valid_record_rate=0.5`.

Quality Gate và Quality Monitoring có vai trò khác nhau:

- Gate quyết định record PASS hoặc FAIL.
- Monitoring đo row count, null, duplicate, invalid, rejected và valid rate theo mỗi run.

## 9. Incremental processing và idempotency

Pipeline đảm bảo retry cùng input không làm thay đổi trạng thái business cuối:

- Bronze partition của cùng source/batch được ghi lại có kiểm soát.
- Silver và Gold dùng Delta `MERGE` theo business key.
- PostgreSQL dùng staging và `ON CONFLICT` upsert.
- Batch registry ghi trạng thái Bronze, Silver và Gold theo batch.
- Prediction có unique key gồm transaction, model, version và horizon.

Bằng chứng:

| Tình huống | Fact rows | Distinct transaction | Kết quả |
| --- | ---: | ---: | --- |
| Batch đầu | 100.000 | 100.000 | Thành công |
| Retry cùng batch | 100.000 | 100.000 | Không duplicate |
| Batch incremental | 110.000 | 110.000 | Thêm đúng 10.000 key |

Kết quả 110.000 thay vì 210.000 chứng minh pipeline cập nhật key cũ và insert key mới, không append mù.

## 10. Streaming processing

Streaming path hiện tại:

```text
fake_bnpl_producer.py
  -> bnpl.transactions.raw, 6 partitions
  -> Spark Structured Streaming
      -> raw Bronze consumer
      -> inference consumer
```

Hai consumer độc lập là quyết định có chủ ý:

- Bronze consumer giữ raw event và Kafka metadata cho replay/audit.
- Inference consumer ưu tiên latency, chạy quality gate, dùng shared features và load saved models.

Các cơ chế reliability:

- Checkpoint Kafka offsets trên MinIO.
- Event-time watermark mặc định 10 phút.
- `dropDuplicatesWithinWatermark` giới hạn duplicate state.
- `KAFKA_MAX_OFFSETS_PER_TRIGGER` kiểm soát backlog.
- `foreachBatch` ghi Bronze partition theo ngày/giờ.
- `foreachPartition` và batched upsert ghi prediction, không `collect()` dữ liệu về driver.
- Probability 90D được giữ không thấp hơn 30D.

P3 đã kiểm chứng prediction từ đủ 6 partitions qua hai executors. Sau compact/restart, 13.474 transaction tạo đúng 26.948 prediction rows cho hai horizons và không có duplicate.

## 11. Feature engineering và Machine Learning

Feature contract nằm trong `spark/bnpl_common/features.py` và được dùng chung cho batch training và streaming inference. Điều này giảm training-serving skew.

Ba model được huấn luyện riêng cho 30D và 90D:

- Logistic Regression làm baseline.
- Random Forest cho nonlinear ensemble.
- Gradient Boosted Trees cho boosting.

GBT được chọn cho cả hai horizons theo Recall rồi ROC-AUC. Tuy nhiên, accuracy cao chịu ảnh hưởng của class imbalance:

| Horizon | Recall default | False negative | ROC-AUC |
| --- | ---: | ---: | ---: |
| 30D | 0,258639 | 708/955 | 0,950321 |
| 90D | 0,316464 | 1.013/1.482 | 0,949777 |

Do đó không nên chỉ trình bày accuracy. Hạn chế quan trọng là model còn bỏ sót nhiều trường hợp default; hướng tiếp theo là class weighting, threshold tuning hoặc resampling.

## 12. Analytics và serving

Gold Analytics dùng Star Schema:

- `fact_bnpl_transaction` ở grain giao dịch.
- `dim_customer`, `dim_date`, `dim_merchant`, `dim_provider`, `dim_location`.
- Dimension key dùng deterministic `xxhash64` để ổn định giữa các incremental batches.

PostgreSQL có bốn nhóm schema:

- `analytics.*`: fact, dimensions và Power BI views.
- `ml.*`: predictions, model metrics và monitoring views.
- `data_quality.*`: DQ metrics.
- `pipeline.*`: batch registry.

P0 đã load 100.000 fact rows; P1 incremental đạt 110.000 fact rows. Power BI project có 5 trang và semantic model đọc PostgreSQL ở Import mode.

## 13. Benchmark và cách diễn giải

Benchmark dùng workload cố định:

```text
Delta read -> deterministic expansion -> filter -> groupBy
-> aggregate -> join -> Parquet write
```

Ma trận chính gồm 4 kích thước, 1/2 workers, full/incremental và 3 lần chạy, tổng cộng 48 lượt chính thức. Kết quả dùng median.

Hai workers chậm hơn một worker trên phần lớn cấu hình vì cùng chia sẻ Docker host, MinIO node, disk và network bridge. Đây là bằng chứng về logical distributed execution và overhead, không phải physical multi-node scale-out.

P3 đã tách runtime thành `source_read`, `sample_preparation`, `transform_shuffle`, `output_write` và `total`. Smoke run cho thấy cold source read chiếm phần lớn thời gian, giúp xác định bottleneck thay vì chỉ nhìn tổng runtime.

## 14. P3 Big Data hardening

Các nâng cấp P3 trực tiếp xử lý vấn đề data engineering:

| Vấn đề | Nâng cấp | Bằng chứng |
| --- | --- | --- |
| Driver bottleneck khi ingest | Staged Parquet rồi Spark distributed read | Smoke 100 rows, validation 100 pass |
| Kafka parallelism thấp | Tăng topic lên 6 partitions | Đủ message và prediction từ partitions 0-5 |
| Driver collect khi inference | `foreachPartition` + batched upsert | 26.948 rows, không duplicate |
| Small files | Offline compaction và Delta maintenance | Bronze 15 xuống 6 files; rejected 26 xuống 6 |
| Training-serving skew | PSI drift monitoring | 8 features có PSI và mức cảnh báo |
| Benchmark khó giải thích | Phase-level metrics | Tách được source read và compute/write |

PSI ban đầu cho thấy generator cũ lệch mạnh so với historical data, đặc biệt ở loan size, state, credit score và interest rate. Generator P3 đã chuyển sang weighted, lognormal và Gaussian distributions; cần chạy lại drift sau khi tích lũy đủ traffic mới.

## 15. Observability và kiểm thử

- Redpanda Console hiển thị topic, partition, message và consumer group.
- Prometheus scrape Redpanda, Spark master, Spark applications và hai Spark workers.
- Grafana được provision sẵn dashboard `BNPL Platform Overview`.
- Alert hiện có: service down và broker unavailable.
- CI kiểm tra Compose config, build đúng Spark image và chạy tests.
- Bộ kiểm thử cuối P3: `12 passed`.

## 16. Giới hạn phải nói rõ

- Dataset là synthetic; không suy rộng kết quả model ra thị trường thực.
- Hai Spark workers đang chạy trên cùng một Docker host; chưa phải cluster vật lý nhiều máy.
- Chưa chạy full source 2 triệu unique rows qua toàn bộ staged ingestion mới.
- Chưa có throughput matrix cho nhiều producer rates, partitions và executor counts.
- Chưa có test tự động cho event đến trễ ngoài watermark.
- Personal-loan auxiliary dataset mới được giữ ở Bronze vì chưa có business objective rõ cho downstream.
- Model có class imbalance và Recall default còn thấp.

## 17. Điểm nhấn khi bảo vệ

Nên trình bày theo chuỗi lập luận sau:

1. Dữ liệu lịch sử và dữ liệu live có vai trò khác nhau nên phải tách batch và streaming path.
2. Bronze giữ raw data; Quality Gate bảo vệ Silver; Gold chia riêng cho analytics và ML.
3. Delta `MERGE`, PostgreSQL upsert và batch registry giúp retry không tạo duplicate.
4. Kafka 6 partitions và Spark Structured Streaming tạo parallel event processing; checkpoint, watermark và dedup bảo vệ tính đúng đắn.
5. Feature contract được dùng chung cho training và inference.
6. Benchmark được diễn giải trung thực: local shared-host có overhead và không đại diện physical scale-out.
7. P3 không chỉ thêm tính năng mà xử lý bottleneck thật: driver ingest, collect, small files, drift và phase timing.

Câu mô tả ngắn:

> Project xây dựng một nền tảng Big Data BNPL end-to-end, trong đó batch processing tạo trusted analytics và model, còn Kafka cùng Spark Structured Streaming xử lý giao dịch mới để dự đoán rủi ro 30 ngày và 90 ngày gần thời gian thực.

## 18. Kịch bản demo phần Big Data

1. Mở Airflow và cho thấy batch DAG đã chạy thành công.
2. Trình bày MinIO layout từ Bronze sang Silver và Gold.
3. Chạy hoặc mở bằng chứng `quality_demo`: 2 pass, 2 quarantine.
4. Query 110.000 fact rows và 110.000 transaction duy nhất.
5. Mở model metrics, nhấn mạnh Recall và False Negative.
6. Khởi động streaming/inference profiles.
7. Mở Redpanda Console để thấy 6 partitions và event mới.
8. Query prediction 30D/90D và latency views trong PostgreSQL.
9. Mở Grafana/Prometheus và Power BI.
10. Kết thúc bằng benchmark và giới hạn shared-host.

## 19. Nguồn đối chiếu trong repository

- Kiến trúc: `docs/architecture.md`.
- Data model: `docs/data_model.md`.
- Batch pipeline: `docs/pipeline.md`.
- Streaming: `docs/streaming.md`.
- Benchmark: `docs/benchmark.md` và `docs/p1_evidence.md`.
- Observability: `docs/monitoring.md` và `docs/p2_evidence.md`.
- Big Data hardening: `docs/p3_evidence.md`.
- Trạng thái công việc: `checklist.md`.
- Mã nguồn xử lý dùng chung: `spark/bnpl_common/`.
- Spark jobs: `spark/jobs/`.

