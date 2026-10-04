# BNPL Big Data Platform v2 — Dữ liệu và pipeline

Ngày tổng hợp: **03/10/2026**. Tài liệu mô tả implementation trong repository và các kết quả đã kiểm chứng trước đó; không phải một lần chạy pipeline mới. Số lượng streaming thay đổi liên tục nên được ghi kèm mốc đo.

## 1. Bức tranh tổng thể

```text
Hugging Face historical datasets
  └─ Airflow → Spark batch
       └─ HDFS Bronze Parquet
            ├─ Personal Loans: giữ raw-only
            └─ BNPL → Validation/Quality Gate
                       ├─ FAIL → Quarantine
                       └─ PASS → Silver Delta MERGE
                                  └─ Gold Shared Features
                                       ├─ Gold Analytics Star Schema → PostgreSQL → BI
                                       └─ Gold ML Features → Train/Evaluate → Models/Registry

PySpark generator 10M → HDFS Bronze riêng để kiểm chứng quy mô

Fake Producer / Streamlit form → Kafka KRaft → Spark Structured Streaming
  └─ HDFS Streaming Bronze → Quality Gate → Shared Features → Saved 30D/90D models
       └─ PostgreSQL ml.predictions → Streamlit / Power BI
```

HDFS là nơi lưu lake, checkpoint và model artifacts. Spark 3.5.1 xử lý batch, streaming và ML. PostgreSQL phục vụ dữ liệu đã chuẩn bị cho truy vấn nghiệp vụ; raw data nằm trên HDFS.

## 2. Nguồn dữ liệu gốc

Trong tài liệu này, **data gốc** nghĩa là nguồn upstream lấy từ Hugging Face. Cả hai nguồn đều là dữ liệu synthetic mô phỏng bối cảnh Nigeria, không phải hồ sơ khách hàng ngân hàng thực tế. Số lượng đầy đủ của nguồn khác số lượng đã ingest vào project.

| Nguồn | Quy mô nguồn | Project đã ingest | Vai trò |
|---|---:|---:|---|
| `electricsheepafrica/africa-synth-banking-bnpl-nigeria` | Khoảng 2 triệu dòng | 100.000 | Nguồn chính cho Silver, Gold và ML |
| `electricsheepafrica/nigerian-banking-personal-loans` | Khoảng 2 triệu dòng | 100.000 | Bronze raw-only, nguồn phụ để mở rộng |

### 2.1. BNPL transactions

Nguồn: [Hugging Face BNPL dataset](https://huggingface.co/datasets/electricsheepafrica/africa-synth-banking-bnpl-nigeria).

Schema nghiệp vụ đã dùng trong project gồm 16 trường:

| Trường | Ý nghĩa |
|---|---|
| `transaction_id` | Business key của giao dịch BNPL |
| `purchase_date` | Ngày mua hàng |
| `customer_id` | Mã khách hàng |
| `merchant_name`, `merchant_category` | Merchant và nhóm hàng hóa |
| `customer_state` | Bang/khu vực khách hàng |
| `principal_ngn` | Giá trị khoản vay, đơn vị Nigerian naira (NGN) |
| `interest_rate_monthly` | Lãi suất tháng dạng tỷ lệ thập phân; 0,025 tương ứng 2,5% |
| `tenor_days`, `num_installments` | Kỳ hạn theo ngày và số kỳ trả |
| `provider` | Nhà cung cấp BNPL |
| `credit_score` | Điểm tín dụng |
| `first_time_customer` | Khách hàng giao dịch lần đầu hay không |
| `first_payment_due` | Ngày đến hạn thanh toán đầu tiên |
| `default_30d`, `default_90d` | Hai nhãn default cho huấn luyện/evaluation |

Dataset viewer đã đối chiếu cho thấy thời gian giao dịch 2022–2024, principal khoảng ₦5.000–₦500.000, credit score 300–850, tenor 14–90 ngày, 37 vùng và 5 provider. Đây là đặc điểm nguồn, không phải các ràng buộc đã áp dụng toàn bộ trong Quality Gate.

### 2.2. Personal loans

Nguồn: [Hugging Face Personal Loans dataset](https://huggingface.co/datasets/electricsheepafrica/nigerian-banking-personal-loans).

Schema nguồn gồm 20 trường:

```text
loan_id, customer_id, application_date, disbursement_date, loan_status,
principal_ngn, interest_rate_annual, tenor_months, monthly_payment_ngn,
salary_ngn, debt_to_income_ratio, credit_score, employment_type,
loan_purpose, state, salary_detected, dpd_current, dpd_max_90d,
default_90d, default_180d
```

Nguồn mô phỏng application trong giai đoạn 2021–2024. Schema khác BNPL: dùng `loan_id`, lãi suất năm, tenor tháng và nhãn 90D/180D. Vì chưa có mapping/pipeline riêng, project giữ nguồn này ở Bronze để audit và reprocess; không gộp với BNPL hoặc dùng nó để train model hiện tại.

### 2.3. Cách ingest

Job [01_ingest.py](../spark/jobs/01_ingest.py) gọi `load_dataset(..., split="train", streaming=True)`. Đây là cách đọc hữu hạn nguồn lịch sử theo iterator, không phải Spark Structured Streaming.

- `INGEST_MAX_ROWS=100000`: giới hạn riêng cho mỗi nguồn; đặt 0 để đọc toàn bộ nguồn.
- `INGEST_BATCH_SIZE=50000`: gom mỗi chunk 50.000 record ở driver trước khi tạo Spark DataFrame.
- Raw record được serialize thành JSON trong `value`, bổ sung provenance rồi ghi HDFS Parquet.
- Cùng source/batch, chunk đầu ghi overwrite và các chunk sau append.

Đường dẫn:

```text
bronze/historical_transactions/source_slug=<source>/batch_key=<batch_id>/
```

## 3. Hai công cụ sinh dữ liệu

### 3.1. PySpark generator 10M

Job [12_generate_10m_bronze.py](../spark/jobs/12_generate_10m_bronze.py), runner [run_10m_generation.py](../scripts/run_10m_generation.py).

Job dùng `spark.range(0, 10_000_000, 1, 96)` rồi Spark SQL expressions để tạo record từ ID. Các công thức modulo làm dữ liệu xác định và có thể tái lập; không sao chép record thật và không mô phỏng đầy đủ phân bố tín dụng của nguồn gốc.

| Thành phần | Cách tạo |
|---|---|
| Transaction | ID duy nhất `TX_V2_%010d` |
| Customer | `id % 2_000_000`: tối đa 2 triệu ID |
| Merchant | `id % 50_000`: 50.000 tên |
| Category / state / provider | Xoay vòng 8 nhóm / 6 bang / 5 provider |
| Principal | `5000 + (id * 7919 % 495000)` |
| Credit score | `300 + id % 551`, phạm vi 300–850 |
| Tenor / installments | 30–360 ngày / 1–12 kỳ |
| First-time | `id % 4 == 0`, khoảng 25% |
| Timestamp | Xoay vòng 30 ngày từ epoch 1767225600 |
| Metadata giả lập Kafka | Topic, partition `id % 12`, offset bằng ID |

Job tạo raw JSON `value` cùng parsed struct và metadata, repartition theo ngày/giờ rồi ghi Parquet Snappy tại:

```text
bronze/generated_streaming_transactions/dataset_version=v2_10m/
  event_date=YYYY-MM-DD/event_hour=HH/
```

Metadata Kafka trong bộ này là dữ liệu mô phỏng; generator ghi trực tiếp lên HDFS, không gửi 10M event qua broker. 96 Spark partitions là cách chia compute, không đồng nghĩa 96 file đầu ra: việc partition theo 30 ngày × 24 giờ tạo nhiều thư mục/file hơn.

Sau khi ghi, job đọc lại và đếm chính xác số dòng. Kết quả đã kiểm chứng ngày 02/10/2026: **10.000.000 dòng**, khoảng **871 MiB logic**, khoảng **2,6 GiB sau replication 3**.

Phạm vi hiện tại: bộ 10M chỉ nằm ở Bronze, chưa đi qua DAG để tạo Silver/Gold và không có nhãn default để train supervised ML.

Điểm cần phân biệt về dữ liệu: generator hiện tạo `interest_rate_monthly=1,0–5,9`, trong khi nguồn gốc dùng tỷ lệ 0–0,05; tenor cũng rộng hơn nguồn. Khi mở rộng pipeline cho bộ 10M cần chuẩn hóa đơn vị và phân bố trước. Các khác biệt này không làm thay đổi bộ Silver/Gold 100K đã kiểm chứng.

### 3.2. Fake streaming producer

Script [fake_bnpl_producer.py](../kafka/producer/fake_bnpl_producer.py) tạo event liên tục bằng Python random, seed mặc định 42. Đây là mô phỏng nguồn event; mọi xử lý lake/ML vẫn do Spark thực hiện.

- Mặc định 2 event/giây.
- Principal ₦5.000–₦500.000; rate 0–0,05; credit score 300–850.
- Tenor 14/30/60/90 ngày; provider/category theo danh sách định sẵn.
- ID gồm sequence và UUID để tránh va chạm khi restart.
- Khoảng 2% lỗi chủ đích: score 999, principal âm, ngày sai hoặc thiếu transaction ID.
- Event không có nhãn `default_30d/default_90d`.

Form Streamlit cũng là một nguồn event gửi vào cùng topic, dùng để demo hồ sơ tương tác.

## 4. Data Lake và cách lưu trữ phân tán

URI chung: `hdfs://namenode:8020/bnpl-data`.

```text
Spark executors → NameNode (metadata / vị trí block)
               → DataNode 1 / volume riêng
               → DataNode 2 / volume riêng
               → DataNode 3 / volume riêng
```

NameNode quản lý namespace và vị trí block. File được HDFS chia thành block; DataNodes giữ block data. `dfs.replication=3` yêu cầu ba bản sao mỗi block; trong cụm ba node hiện tại, block được sao chép qua cả ba DataNodes. Spark executors đọc/ghi data trực tiếp qua DataNodes sau khi lấy metadata từ NameNode.

Hai Spark workers xử lý nhiều partition song song; HDFS block, Spark partition và partition thư mục `event_date/event_hour` là ba khái niệm khác nhau.

Layout:

```text
/bnpl-data/
  bronze/historical_transactions/              Parquet source/batch
  bronze/streaming_transactions_v2/             Parquet date/hour
  bronze/generated_streaming_transactions/     Parquet 10M
  staging/validated_transactions/               Kết quả quality gate tạm
  rejected/transactions/                        Batch quarantine
  rejected/streaming_transactions/              Streaming quarantine
  silver/transactions/                          Delta
  gold/shared/enriched_transactions/            Delta
  gold/analytics/                               Delta star schema
  gold/ml/                                      Delta features/metrics/registry/test sets
  gold/data_quality/metrics/                     Delta DQ metrics
  models/                                       Saved Spark PipelineModels
  _checkpoints/                                 Streaming progress
```

Delta Lake gồm Parquet data files và `_delta_log`, cung cấp transaction log, ACID và MERGE. Bronze Parquet lưu raw payload để audit/reprocess; Silver giữ schema tin cậy; Gold chuẩn bị dữ liệu cho BI và ML. Parquet được nén Snappy nên số dòng lớn không đồng nghĩa dung lượng phải rất lớn.

### 4.1. Số liệu lưu trữ đã đo

| Vùng | Logic size | Mốc/diễn giải |
|---|---:|---|
| Bronze lịch sử, hai nguồn 100K | 25,7 MiB | Đo trước migration sang replication 3 |
| Silver | 3,5 MiB | Cùng bộ batch đã kiểm chứng |
| Gold Shared | 5,9 MiB | Cùng bộ batch |
| Gold Analytics | 8,1 MiB | Cùng bộ batch |
| Gold ML | 7,8 MiB | Cùng bộ batch |
| Generated Bronze 10M | Khoảng 871 MiB | Đối chiếu 02/10/2026 |
| Toàn `/bnpl-data` | 1.001.433.641 bytes logic | Snapshot FSCK 02/10/2026, gồm nhiều vùng |
| HDFS DFS Used | Khoảng 2,83 GiB | Snapshot report sau replication 3 |

Dung lượng vật lý xấp xỉ ba lần dung lượng logic, thêm checksum/metadata và ảnh hưởng file đang được ghi. Không dùng dung lượng nguồn upstream để thay thế dung lượng Parquet trong lake.

FSCK snapshot: 3 live DataNodes, 1.089 blocks, average block replication 3,0; under-replicated/missing/corrupt blocks đều 0; filesystem HEALTHY.

Ba DataNode có volume riêng nhưng vẫn chạy trên **cùng máy Docker Desktop**, cùng failure domain vật lý và một rack. Đây là mô phỏng cụm phân tán cho đồ án, chưa phải deployment nhiều host, nhiều rack hay NameNode HA. Capacity HDFS cộng capacity báo bởi từng DataNode; trên cùng Docker disk không nên coi tổng đó là dung lượng ổ đĩa độc lập thực có.

## 5. Batch: Airflow làm gì và kết quả cuối

DAG [bnpl_batch_pipeline.py](../airflow/dags/bnpl_batch_pipeline.py) có `schedule=None`, trigger thủ công, `max_active_runs=1`. Airflow điều phối `spark-submit` vào Spark master; Spark thực hiện xử lý dữ liệu.

Identifiers: `batch_id` xác định batch dữ liệu; `pipeline_run_id` là run Airflow; `model_version` là version model.

| Thứ tự/task | Công việc | Output |
|---|---|---|
| `ingest_data` | Đọc hai nguồn lịch sử, giữ raw JSON | Bronze Parquet source/batch |
| `validate_bronze` | Parse, normalize, quality gate nguồn BNPL | Validated staging và rejected |
| `bronze_to_silver` | Chọn PASS, MERGE theo transaction ID | Silver Delta |
| `data_quality_metrics` | Đếm row/null/duplicate/invalid/rejected/valid rate | Gold DQ + PostgreSQL metrics |
| `feature_engineering` | Transform batch Silver hiện tại | Gold Shared |
| `build_analytics` | Tạo dimensions và fact | Gold Analytics |
| `build_ml_features` | Chọn flat feature contract | Gold ML Features |
| `train_models` | Train ba thuật toán × hai horizon | 6 model artifacts, training runs, test sets |
| `evaluate_models` | Metrics, chọn champion từng horizon | Metrics và registry |
| `load_postgres` | JDBC staging rồi SQL upsert | PostgreSQL star schema/model metrics |

`build_analytics` và nhánh ML chạy song song sau feature engineering. `load_postgres` chờ cả analytics và evaluate hoàn tất. DAG có 10 task xử lý, cộng `start/end` thành 12 task.

Run `hdfs_20261002_001` đã hoàn thành cả 12 task ngày 02/10/2026, khoảng 11 phút 55 giây. Batch BNPL đi đủ ba layer; personal loans chỉ có Bronze SUCCESS, Silver/Gold giữ PENDING theo thiết kế.

## 6. Bronze → Silver: xử lý chất lượng

Code chính: [validation.py](../spark/bnpl_common/validation.py), [02_validate_bronze.py](../spark/jobs/02_validate_bronze.py), [03_bronze_to_silver.py](../spark/jobs/03_bronze_to_silver.py).

Bronze giữ `value` raw JSON và metadata source/split/batch/run/time. Historical ingestion không tự infer schema nghiệp vụ để ghi Silver; validation parse từng trường rõ ràng.

### 6.1. Chuẩn hóa

- Trim transaction/customer IDs.
- Purchase timestamp thành date.
- Merchant/state/provider thành InitCap; category lowercase.
- Principal/rate cast double; tenor/installments/score cast integer.
- Boolean chấp nhận các chuỗi tương đương như true/false, 1/0, yes/no.

Canonical Silver có 15 trường nghiệp vụ và 5 metadata fields. `first_payment_due` còn trong raw Bronze nhưng không nằm trong Silver contract hiện tại. Hai nhãn default nullable vì event inference chưa có nhãn.

### 6.2. Quality Gate

Kiểm tra JSON parse được; ID/ngày/categorical bắt buộc hợp lệ; principal > 0; rate ≥ 0; tenor/installments > 0; score 300–850; boolean hợp lệ; duplicate transaction ID trong batch.

Record PASS đi Silver. Record FAIL được giữ cùng raw JSON, danh sách `_validation_errors`, `_rejected_at`, run/batch/source. Extra fields được ghi nhận qua `_unexpected_columns`, đánh dấu compatible khi các trường bắt buộc vẫn hợp lệ; không tự thêm cột vào Silver.

### 6.3. Idempotency

Silver MERGE theo `transaction_id`: matched update, unmatched insert. Duplicate trong batch bị quality gate đánh dấu; Delta helper thêm dedup theo key trước MERGE. Chạy lại cùng batch không nhân đôi business key.

Kết quả BNPL batch đã kiểm chứng: **100.000 Silver rows, 100.000 transaction ID duy nhất, 0 duplicate**. Đợt kiểm chứng incremental cũ từ 100K lên 110K cho kết quả 110K, không phải append thành 210K; số liệu đó là bằng chứng lịch sử của cơ chế MERGE, không phải row count lake hiện tại.

## 7. Silver → Gold và kết quả

### 7.1. Gold Shared

[features.py](../spark/bnpl_common/features.py) thêm:

- Credit band: Poor <580; Fair <670; Good <740; Very Good <800; còn lại Excellent.
- Loan size: Small <₦50.000; Medium <₦200.000; còn lại Large.
- `months = tenor_days / 30`.
- `estimated_interest = principal × monthly_rate × months`.
- `estimated_total_payment = principal + estimated_interest`.
- `installment_amount = estimated_total_payment / num_installments`.
- Month, quarter và day-of-week từ purchase date.

Đây là ước tính lãi đơn trong feature contract, không phải lịch trả nợ amortization đầy đủ. Rate phải dùng đúng dạng thập phân. Transform dùng chung giữa training và streaming để giảm sai lệch feature.

Output `gold/shared/enriched_transactions`: **100.000 rows**, MERGE theo transaction ID.

### 7.2. Gold Analytics

Star Schema giữ grain **một fact row cho một BNPL transaction**. Không phải toàn bộ Gold đều bị gom thành một aggregate; fact còn chi tiết đủ để slice/filter BI.

| Bảng | Số dòng đã kiểm chứng | Mục đích |
|---|---:|---|
| `fact_bnpl_transaction` | 100.000 | Giá trị vay, measures, labels và dimension keys |
| `dim_customer` | 92.862 | Customer ID, first-time flag |
| `dim_date` | 1.095 | Ngày/tháng/quý/năm/weekend |
| `dim_location` | 37 | Bang/khu vực |
| `dim_merchant` | 4.983 | Merchant và category |
| `dim_provider` | 5 | Provider |

Dimension keys dùng deterministic `xxhash64`; dimension MERGE theo dimension key, transaction fact MERGE theo `transaction_id`. Ngày dùng date key `yyyyMMdd`.

### 7.3. Gold ML và models

`gold/ml/ml_bnpl_features` có **100.000 rows**, numeric/categorical features, engineered features, date features, labels và provenance. Training tách 80/20 với seed 42; tập test đã ghi nhận 19.825 rows cho mỗi horizon (randomSplit không bảo đảm đúng 20.000).

Pipeline MLlib: StringIndexer → OneHotEncoder → VectorAssembler → classifier. Ba candidates là Logistic Regression, Random Forest, GBT; train cho 30D và 90D tạo 6 runs.

Evaluation tính Accuracy, Precision/Recall lớp default, weighted F1 và ROC-AUC. Registry chọn **Recall giảm dần rồi ROC-AUC**, giữ 2 champion; bộ đã kiểm chứng chọn GBT cho cả hai horizon. Artifacts gồm 6 metric rows, 6 training runs, 2 registry rows và saved PipelineModels trên HDFS.

Accuracy cao cần đọc cùng Recall vì nhãn default mất cân bằng. Dữ liệu synthetic và metrics của bộ demo phục vụ kiểm chứng kỹ thuật, không đại diện chất lượng model trên khách hàng thực.

## 8. Gold dùng cho gì?

| Consumer | Dữ liệu Gold dùng | Kết quả |
|---|---|---|
| Power BI / Streamlit | Analytics fact/dim được publish PostgreSQL | Giải ngân, default rate, phân tích provider/category/state/credit band |
| Spark ML training | Flat ML features và labels | Train/evaluate/retrain model |
| Streaming inference | Registry, saved models, shared feature contract | Prediction 30D/90D cho hồ sơ mới |
| Platform monitoring | DQ metrics, training metrics và batch registry | Theo dõi quality, run status và model performance |

PostgreSQL loader đọc Gold Analytics và metrics, ghi JDBC staging rồi `ON CONFLICT` upsert để giữ serving constraints. Implementation hiện đọc toàn bộ từng Gold serving table để stage, dù kết quả upsert idempotent; đây chưa phải loader chỉ đọc changed rows.

Raw data, full feature lake, checkpoint và saved model artifacts vẫn nằm HDFS. Predictions được inference ghi trực tiếp PostgreSQL; registry model đầy đủ hiện lưu Delta trên HDFS.

## 9. Streaming cụ thể

Streaming chạy lâu dài dưới Docker Compose, độc lập với Airflow finite batch DAG. Batch training phải tạo model registry trước khi inference sử dụng.

### 9.1. Kafka transport

Kafka 3.9.1 chạy KRaft, một broker/controller, topic `bnpl.transactions.raw` có 1 partition và Kafka replication factor 1. **Kafka replication 1 khác HDFS replication 3**; ba DataNode không làm broker trở thành Kafka cluster ba node.

Bootstrap nội bộ `kafka:9092`, ngoài máy `localhost:19092`. Broker giữ named volume `kafka_data`; Bronze là nơi lưu raw dài hạn.

### 9.2. Kafka → Streaming Bronze

Job [streaming_kafka_to_bronze.py](../spark/jobs/streaming_kafka_to_bronze.py) là Kafka consumer của Spark:

- `readStream.format("kafka")`, subscribe topic.
- `startingOffsets=earliest` cho query mới; khi có checkpoint thì resume từ checkpoint.
- Giữ JSON và Kafka topic/partition/offset/timestamp.
- Parse explicit event schema và ghi `_schema_parse_ok`.
- Ghi append Parquet vào `bronze/streaming_transactions_v2`.
- Partition `event_date/event_hour` lấy theo Kafka timestamp, fallback ingestion time; đây không nhất thiết là purchase date trong payload.
- Trigger micro-batch mỗi 10 giây.
- Checkpoint `_checkpoints/kafka_to_bronze_v2` nằm HDFS.

Code đang đặt `failOnDataLoss=false`; nếu Kafka offsets mất vì retention, query có thể tiếp tục thay vì fail. Vì vậy không nên tuyên bố hệ thống bảo đảm không mất event trong mọi tình huống.

### 9.3. Bronze → Prediction

Job [streaming_predict.py](../spark/jobs/streaming_predict.py) đọc file stream Bronze bằng schema cố định:

1. Load model registry và PipelineModels lúc startup.
2. Đọc file Bronze mới, trigger mỗi 10 giây.
3. Quality Gate từng micro-batch; FAIL append quarantine.
4. PASS dùng shared feature engineering.
5. Chạy model 30D và 90D.
6. Lấy xác suất lớp 1 và predicted class.
7. Upsert vào `ml.predictions`.

Checkpoint `_checkpoints/bronze_to_prediction_v2` ghi progress file stream. Model registry thay đổi thì cần restart predictor để load model mới. Luồng hiện tại không materialize Silver/Gold cho từng live event; nó dùng Bronze → validated/features trong micro-batch → model → prediction.

### 9.4. Output và retry

Một transaction hợp lệ thường tạo 2 rows, một cho mỗi horizon. Unique key PostgreSQL là `(transaction_id, model_name, model_version, prediction_horizon)`; replay update row hiện có để tránh duplicate prediction.

Các trường: transaction/customer ID, model/version, horizon, predicted default, default probability, risk level và timestamp.

Risk bands: LOW <0,4; MEDIUM từ 0,4 đến <0,7; HIGH ≥0,7. `predicted_default` là class output của model, còn risk band là rule theo probability; hai trường không phải cùng một quyết định.

Checkpoint hỗ trợ resume; PostgreSQL upsert hỗ trợ idempotency cho prediction. Quarantine dùng append nên có thể lặp rejected rows khi micro-batch replay; không gọi toàn bộ sink là exactly-once.

### 9.5. Latency và số liệu

Hai trigger 10 giây không bảo đảm latency 10 giây: event có thể chờ trigger ở cả hai job, thêm compute/I/O/model/database và backlog. Trên máy thiếu RAM latency tăng đáng kể.

Phép đo **lịch sử ngày 21/09/2026, trước HDFS v2** ghi 9.382 transaction được predict, 18.764 prediction; một mẫu có latency 5,110 giây (30D) và 6,517 giây (90D). Các số này chỉ là tham khảo luồng cũ, không phải benchmark streaming hiện tại. File counts của rejected cũng không bằng số rejected records. Cần đo lại trên HDFS v2 để báo cáo latency mới.

## 10. Batch và Streaming đối chiếu

| Tiêu chí | Batch | Streaming |
|---|---|---|
| Dữ liệu | Lịch sử hữu hạn | Event liên tục |
| Nguồn | Hugging Face iterator | Kafka, fake producer hoặc form |
| Điều phối | Airflow DAG | Compose long-running services |
| Kích hoạt | Trigger thủ công | Micro-batches 10 giây |
| Nhãn | Có 30D/90D để train | Chưa có nhãn, dùng inference |
| Storage đầu | Historical Bronze source/batch | Streaming Bronze date/hour |
| Silver/Gold | Materialize Delta MERGE | Features tạm, dùng saved Gold models |
| ML | Train và evaluate | Score bằng model đã train |
| Output cuối | Star schema, features, models, metrics | PostgreSQL predictions |
| Retry | Batch ID, MERGE, upsert | Checkpoint + prediction upsert |
| Vòng đời | Task chạy xong kết thúc | Chạy liên tục đến khi stop |

Hugging Face `streaming=True` trong batch là cách đọc nguồn, không biến Airflow ingestion thành long-running stream. Generator 10M là offline generation, khác fake producer realtime.

## 11. Các lệnh demo và kiểm chứng

Batch: mở Airflow tại `http://localhost:8088`, trigger `bnpl_batch_pipeline` với batch/model config phù hợp.

```powershell
# Bật giao diện trên stack hiện có
docker compose --profile demo up -d --build demo-dashboard

# Bật đủ fake producer, Kafka, Bronze stream và inference
docker compose --profile demo --profile streaming --profile inference up -d

# Chỉ inference và form demo, tránh bật fake producer mới
docker compose --profile demo --profile inference up -d demo-dashboard spark-streaming-prediction

# Generate 10M offline
python scripts/run_10m_generation.py

# Kiểm chứng HDFS
docker compose exec -T namenode hdfs dfsadmin -report
docker compose exec -T namenode hdfs fsck /bnpl-data
docker compose exec -T namenode hdfs dfs -du -h /bnpl-data
```

Nếu fake producer đã chạy từ trước, profile inference không tự stop nó; dừng riêng khi demo bằng form và cần giảm tải. Khi chạy job dài, chờ kết thúc rồi mới đọc output/log theo repository rules. Giữ nguyên replication 3 và ba volume DataNode riêng.

## 12. Mẫu diễn giải khi thuyết trình

> Nguồn lịch sử của hệ thống là hai dataset synthetic Nigeria trên Hugging Face, mỗi nguồn khoảng hai triệu dòng. Pipeline demo ingest 100 nghìn dòng mỗi nguồn vào HDFS Bronze. Nguồn BNPL được Spark kiểm tra chất lượng, chuẩn hóa và Delta MERGE vào Silver; từ đó tạo Gold Star Schema và ML Features. Airflow điều phối pipeline batch, huấn luyện ba thuật toán cho hai horizon rồi chọn serving model. Job PySpark riêng sinh chính xác 10 triệu dòng để kiểm chứng quy mô Bronze. Với hồ sơ mới, Kafka đưa event vào Structured Streaming, raw được giữ ở Bronze trước khi model dự báo 30D/90D. PostgreSQL phục vụ dashboard và predictions. HDFS gồm một NameNode, ba DataNode với replication ba, được mô phỏng trên Docker Desktop.

## 13. Tài liệu/code đối chiếu

- [Architecture](architecture.md), [Data model](data_model.md), [Batch runbook](pipeline.md), [Streaming runbook](streaming.md).
- [Batch evidence](p0_evidence.md), [Historical incremental/streaming evidence](p1_evidence.md).
- [Canonical schemas](../spark/bnpl_common/schemas.py), [Validation](../spark/bnpl_common/validation.py), [Features](../spark/bnpl_common/features.py), [ML pipeline](../spark/bnpl_common/ml.py).
- [Compose](../docker-compose.yml), [HDFS configuration](../hadoop/hdfs-site.xml), [Streamlit dashboard](../dashboard/streamlit/app.py).
