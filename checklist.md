# Checklist hoàn thiện BNPL Big Data Platform

Checklist này được đối chiếu từ tài liệu `Final Bigdata_Group2.docx` và trạng thái project ngày 02/10/2026.

Quy ước:

- `[x]`: đã kiểm tra được bằng source code hoặc trạng thái chạy thực tế.
- `[ ]`: chưa hoàn thành, chưa chạy, hoặc chưa có bằng chứng đầu ra.
- Mục có ghi **Đã có code** vẫn cần được chạy và kiểm chứng trước khi đánh dấu hoàn thành.

## 1. Trạng thái hiện tại

- [x] Repository đã có source cho batch, streaming, Data Quality, ML, PostgreSQL và benchmark.
- [x] Chạy lại batch stack sau migration: PostgreSQL, HDFS NameNode/DataNode, Spark master, 2 Spark workers, Airflow webserver và scheduler.
- [ ] Kiểm tra Spark UI, HDFS NameNode UI và Airflow UI.
- [x] Airflow nhận DAG `bnpl_batch_pipeline` và không có lỗi import.
- [x] PostgreSQL đã có 4 schema nghiệp vụ, 10 bảng và 3 view phục vụ BI.
- [x] Toàn bộ schema, validation, feature engineering và integration tests đã chạy: `15 passed`.
- [x] DAG `bnpl_batch_pipeline` đã chạy lại end-to-end trên HDFS với run `hdfs_20261002_001`.
- [x] Fact, dimensions, model metrics, Data Quality, batch registry và `ml.predictions` đã có dữ liệu.
- [x] Streaming và inference profiles đã được kiểm chứng end-to-end.
- [x] Đã có Power BI Project `dashboard/BNPL Big Data Report.pbip` gồm semantic model và 5 trang báo cáo.
- [x] Đã có CSV và hai biểu đồ benchmark thực tế trong `docs/evidence/p1/`.
- [ ] Chưa có demo video trong repository.

## 2. P0 Must Have

### 2.1. Hạ tầng Docker

- [x] Có `.env.example` và `.env` cho môi trường local.
- [x] `docker compose config` hợp lệ.
- [x] PostgreSQL khởi động và health check thành công.
- [x] HDFS NameNode và DataNode khởi động, health check thành công; Spark smoke test đã ghi/đọc 3 dòng Parquet và FSCK báo filesystem `HEALTHY`, không có under-replicated block.
- [x] Spark master khởi động và health check thành công.
- [x] Có 2 Spark workers kết nối với cluster.
- [x] Airflow webserver và scheduler đang chạy.
- [ ] Chụp ảnh `docker compose ps` để làm bằng chứng cho báo cáo/demo.
- [x] Ghi lại cấu hình Docker trong `docs/p0_evidence.md`: 12 CPU logic, khoảng 15.5 GiB RAM; mỗi Spark worker 2 core/2 GiB.

### 2.2. Batch ingestion và Bronze

- [x] **Đã có code:** ingestion dataset BNPL từ Hugging Face vào Bronze Parquet.
- [x] **Đã có code:** metadata `_ingested_at`, `_source_dataset`, `_source_split`, `_pipeline_run_id`, `_batch_id`.
- [x] **Đã có code:** giới hạn số dòng bằng `INGEST_MAX_ROWS` cho môi trường demo.
- [x] Dataset personal-loan bổ sung có 100,000 dòng Bronze raw-only; dataset BNPL chính có 100,000 dòng và đi tiếp qua Silver/Gold.
- [x] Trigger batch đầu tiên với `batch_id=20260921`, run `p0_20260921_001`.
- [x] Xác nhận dữ liệu xuất hiện tại `/bnpl-data/bronze/historical_transactions/` trên HDFS cho cả hai source.
- [x] Đối chiếu Bronze có `100,000` dòng, đúng với `INGEST_MAX_ROWS=100000`.
- [ ] Chụp ảnh cấu trúc HDFS path và một mẫu record Bronze.

### 2.3. Schema Validation và Data Quality Gate

- [x] **Đã có code:** explicit schema, required/optional columns và tắt automatic schema evolution.
- [x] **Đã có code:** kiểm tra transaction ID, principal, credit score, date và duplicate.
- [x] **Đã có code:** streaming event không chứa `default_30d` hoặc `default_90d`.
- [x] **Đã có code:** record hợp lệ nhận trạng thái `PASS`, record lỗi nhận `FAIL` và danh sách nguyên nhân.
- [x] Unit test cho schema contract và validation đã pass.
- [x] Chạy Quality Gate trên batch chính và batch kiểm thử xác định `quality_demo`.
- [x] Xác nhận 2 record lỗi được lưu tại `rejected/transactions/quality_demo`, không bị drop âm thầm.
- [x] Xác nhận Data Quality metrics được ghi vào Delta và PostgreSQL.
- [x] Đã kiểm tra row count, duplicate count/rate, invalid counts, rejected count và valid rate.
- [x] Giữ lại batch `quality_demo` gồm 4 record, trong đó 2 record bị quarantine để trình diễn.

### 2.4. Silver và Gold

- [x] **Đã có code:** Bronze sang Silver Delta với chuẩn hóa kiểu dữ liệu và deduplication.
- [x] **Đã có code:** Delta `MERGE` theo `transaction_id`.
- [x] **Đã có code:** reusable feature engineering cho batch và streaming.
- [x] **Đã có code:** Gold shared enriched transactions.
- [x] **Đã có code:** Gold Analytics theo Star Schema.
- [x] **Đã có code:** Gold ML feature table.
- [x] Xác nhận valid records đã tới `silver/transactions/`.
- [x] Xác nhận các bảng Gold Analytics được tạo và có dữ liệu.
- [x] Xác nhận `gold/ml/ml_bnpl_features/` tồn tại và có feature/target cho 30D và 90D.
- [x] Run HDFS `hdfs_20261002_001` tạo `100,000` Silver rows, `100,000` enriched rows, `100,000` fact rows và `100,000` ML feature rows.
- [x] Kiểm tra `100,000` fact rows tương ứng `100,000` `transaction_id` duy nhất, không có duplicate.
- [ ] Chụp schema và sample data của Bronze, Silver và Gold để so sánh.

### 2.5. Star Schema và PostgreSQL serving

- [x] Đã tạo `fact_bnpl_transaction`.
- [x] Đã tạo `dim_customer`, `dim_date`, `dim_merchant`, `dim_provider`, `dim_location`.
- [x] Đã tạo `analytics.vw_bnpl_overview` và `analytics.vw_bnpl_transactions`.
- [x] Đã tạo `ml.predictions`, `ml.model_metrics` và `ml.vw_prediction_monitoring`.
- [x] Đã tạo `data_quality.data_quality_metrics` và `pipeline.pipeline_batches`.
- [x] Chạy task `load_postgres` thành công.
- [x] Xác nhận fact table và dimensions có dữ liệu.
- [x] Xác nhận các view Power BI trả về kết quả hợp lệ.
- [x] Kiểm tra khóa chính, khóa duy nhất và upsert không tạo duplicate.
- [x] Lưu các câu SQL kiểm tra row count trong `docs/p0_evidence.md` để dùng khi demo.

### 2.6. Machine Learning 30D và 90D

- [x] **Đã có code:** Logistic Regression, Random Forest và Gradient Boosted Trees.
- [x] **Đã có code:** hai prediction horizon `default_30d` và `default_90d`.
- [x] **Đã có code:** Accuracy, Precision, Recall, F1-score và ROC-AUC.
- [x] **Đã có code:** lưu model theo tên, horizon và version.
- [x] Train đủ 3 model cho horizon 30D.
- [x] Train đủ 3 model cho horizon 90D.
- [x] Evaluate, load lại saved model để chấm tập test và ghi 6 dòng metrics.
- [x] Phân tích Recall và False Negative của GBT: 30D bỏ sót 708/955 ca default, 90D bỏ sót 1,013/1,482 ca default trên test set.
- [x] Chọn GBT cho cả 30D và 90D theo Recall giảm dần, sau đó ROC-AUC.
- [x] Saved models đã được `PipelineModel.load` trong task evaluate và dùng để inference trên tập test.
- [ ] Chụp bảng so sánh metrics và lưu vào báo cáo.

### 2.7. Airflow orchestration

- [x] DAG có đầy đủ chuỗi `ingest -> validate -> silver -> DQ -> features -> analytics/ML -> PostgreSQL`.
- [x] DAG giới hạn một active run và cho phép truyền `batch_id`, `model_version`.
- [x] Các Spark job được tách thành file độc lập.
- [x] Unpause DAG `bnpl_batch_pipeline`.
- [x] Trigger DAG và xác nhận toàn bộ task chuyển sang trạng thái success.
- [x] Kiểm tra log của từng task Spark.
- [x] Kiểm tra dependency giữa nhánh Analytics và ML trước `load_postgres`.
- [ ] Chụp Airflow Graph/Grid của một run thành công.
- [x] Chạy lại toàn bộ DAG với cùng `batch_id=20260921`; vẫn có 100,000 fact rows, 100,000 transaction ID duy nhất và 0 duplicate.

### 2.8. Power BI

- [x] Đã có hướng dẫn kết nối Power BI với PostgreSQL trong `dashboard/README.md`.
- [x] Tạo và lưu Power BI Project `dashboard/BNPL Big Data Report.pbip` trong repository.
- [x] Trang Overview: total transactions, total amount, average loan, default rate 30D/90D.
- [x] Trang Risk Analysis: credit score band, loan size, provider, merchant category.
- [x] Trang Customer/Geography: state, first-time/returning và transaction volume.
- [x] Trang Streaming Predictions: số event, risk 30D/90D và risk level; dữ liệu sẽ có sau P1 inference.
- [x] Trang Platform Monitoring: model metrics, batch registry và Data Quality metrics.
- [x] Thêm slicer/filter thời gian, provider, state và merchant category.
- [ ] Kiểm tra refresh dữ liệu từ PostgreSQL.
- [x] Chụp ảnh 5 trang tại `docs/evidence/powerbi/`.

## 3. P1 Strongly Recommended

### 3.1. Incremental processing và idempotency

- [x] **Đã có code:** batch registry trong PostgreSQL.
- [x] **Đã có code:** Delta `MERGE` và PostgreSQL upsert.
- [x] **Đã có code:** business key `transaction_id`.
- [x] Chạy Batch 1 với dữ liệu ban đầu: 100,000 fact rows và 100,000 key duy nhất.
- [x] Chạy Batch 2 bằng run `p1_incremental_20260921_001`: 110,000 fact rows và 110,000 key duy nhất.
- [x] Chạy lại cùng `batch_id=20260921` bằng run `p0_retry_20260921_001`.
- [x] Chứng minh row count và business key không bị duplicate sau retry: 100,000 rows, 100,000 key duy nhất.
- [x] Chứng minh batch registry được upsert sang pipeline run mới và giữ đúng trạng thái từng layer.
- [x] Lưu SQL output và kết quả đối chiếu trong `docs/p0_evidence.md`.

### 3.2. Kafka và Structured Streaming

- [x] **Đã có code:** Apache Kafka KRaft service và topic initialization.
- [x] **Đã có code:** fake BNPL producer với event không chứa target tương lai.
- [x] **Đã có code:** cấu hình event rate và invalid event rate.
- [x] **Đã có code:** Spark Structured Streaming đọc Kafka bằng explicit schema.
- [x] **Đã có code:** lưu raw event và Kafka metadata vào Bronze.
- [x] **Đã có code:** streaming validation, rejected records và checkpoint.
- [x] **Đã có code:** load model 30D/90D và upsert prediction vào PostgreSQL.
- [ ] Khởi động profile `streaming` trên HDFS và kiểm tra Kafka, producer, consumer.
- [x] Xác nhận topic `bnpl.transactions.raw` nhận event: high-watermark 9,594.
- [x] Xác nhận event được persist tại `bronze/streaming_transactions/`: 448 Parquet files.
- [x] Xác nhận invalid event tới `rejected/streaming_transactions/`: 196 Parquet files.
- [x] Sau khi có saved models, khởi động profile `inference`.
- [x] Xác nhận transaction mẫu có đủ prediction 30D và 90D.
- [x] Xác nhận 18,764 predictions được ghi vào `ml.predictions` và hiện trên `ml.vw_prediction_monitoring`.
- [x] Đo độ trễ event mẫu: 5.110 giây cho 30D và 6.517 giây cho 90D.

### 3.3. Spark scalability benchmark

- [x] **Đã có code:** workload read, filter, groupBy, join, aggregate và write.
- [x] **Đã có code:** scalability benchmark có tham số dataset size, worker count và run number.
- [x] **Đã có code:** lưu kết quả idempotently vào `benchmarks/results`.
- [x] Chạy warm-up trước khi thu kết quả chính thức.
- [x] Chạy các size 100K, 500K, 1M và 2M với 1 worker.
- [x] Chạy các size 100K, 500K, 1M và 2M với 2 workers.
- [x] Mỗi cấu hình chạy 3 lần, tổng cộng 48 runs chính thức.
- [ ] Chạy lại scalability benchmark và benchmark Full Reload vs Delta MERGE thật trên HDFS.
- [ ] Xuất median runtime và records/second HDFS mới vào `benchmark_summary.csv`.
- [ ] Vẽ lại biểu đồ scalability 1 worker/2 workers từ kết quả HDFS mới.
- [ ] Cập nhật biểu đồ Full Reload vs Delta MERGE bằng kết quả HDFS mới.
- [x] Ghi nhận giới hạn 12 CPU/15.54 GiB RAM và giải thích scale không tuyến tính trong `docs/p1_evidence.md`.

### 3.4. Testing

- [x] Schema contract tests đã pass.
- [x] Data Quality validation tests đã pass.
- [x] Feature engineering tests đã pass.
- [x] Sửa lệnh Docker test trong README để thêm đúng `PYTHONPATH` cho `py4j`.
- [x] Thêm integration test tối thiểu cho Bronze -> Silver -> Gold.
- [x] Thêm test idempotency khi chạy lại cùng batch.
- [x] Thêm test streaming event hợp lệ và không hợp lệ.
- [x] Chạy toàn bộ test lần cuối: `15 passed in 26.63s`; kết quả được ghi trong `docs/p0_evidence.md` và `docs/p1_evidence.md`.

## 4. P2 Optional

- [x] Dùng Apache Kafka CLI để kiểm tra topic, partition và offsets khi demo.
- [x] Thêm GitHub Actions CI để validate Compose, build Spark image và chạy toàn bộ tests.
- [x] Thêm profile Prometheus/Grafana monitoring cho Spark.
- [x] Không xây external API vì chưa có yêu cầu cụ thể từ giảng viên.
- [x] Giữ scope hiện tại, không mở rộng sang Kubernetes, Flink, Databricks, Elasticsearch hoặc LLM.

## 5. Tài liệu và deliverables

- [x] Source code repository.
- [x] Docker Compose infrastructure.
- [x] Airflow DAG và Spark jobs.
- [x] Kafka producer, streaming ingestion và prediction code.
- [x] SQL schemas cho Analytics, ML, Data Quality và Pipeline Registry.
- [x] Architecture, data model, pipeline, streaming và benchmark docs.
- [x] Bản tài liệu thiết kế/báo cáo hiện tại đã tồn tại.
- [x] Đã thêm số liệu chạy thật và câu lệnh kiểm chứng vào `docs/p0_evidence.md`.
- [ ] Thêm ảnh kiến trúc, Airflow DAG, Spark UI, HDFS NameNode UI, PostgreSQL và Power BI.
- [x] Đã thêm bảng model metrics 30D/90D vào `docs/p0_evidence.md`.
- [x] Thêm bảng và biểu đồ benchmark.
- [x] Thêm bằng chứng incremental/idempotency trong `docs/p0_evidence.md` và `docs/p1_evidence.md`.
- [x] Thêm giới hạn của synthetic dataset và phần thảo luận kết quả.
- [ ] Tạo demo video end-to-end.
- [x] Kiểm tra README khớp với lệnh batch, streaming, test và benchmark đã chạy thực tế.
- [x] Kiểm tra Git không chứa `.env`, secret thật, dữ liệu volume, Airflow logs hoặc checkpoint runtime.

## 6. Kịch bản dry run trước khi bảo vệ

- [ ] Khởi động batch stack từ trạng thái dừng bằng `docker compose up -d`.
- [ ] Mở Spark UI, HDFS NameNode UI và Airflow UI.
- [ ] Trigger một batch DAG đã chuẩn bị sẵn cấu hình.
- [ ] Trình bày Bronze, Schema Validation, Quality Gate và Rejected records.
- [ ] Trình bày Silver Delta và Gold Analytics/ML.
- [ ] Query Star Schema, Data Quality metrics và model metrics trong PostgreSQL.
- [ ] Mở Power BI và kiểm tra refresh.
- [ ] Trình bày model 30D/90D và lý do chọn model tốt nhất.
- [ ] Khởi động streaming producer và theo dõi event Kafka -> Bronze.
- [ ] Trình bày một prediction 30D/90D mới trong PostgreSQL và Power BI.
- [ ] Trình bày kết quả 1 worker vs 2 workers và full vs incremental.
- [ ] Chạy lại toàn bộ kịch bản ít nhất 2 lần trước ngày bảo vệ.
- [ ] Chuẩn bị ảnh/video dự phòng nếu mạng hoặc máy demo gặp sự cố.

## 7. Tiêu chí sẵn sàng nộp bài

- [x] Có ít nhất một Airflow DAG run end-to-end thành công.
- [x] Bronze, Silver, Gold và rejected area đều có dữ liệu kiểm chứng được.
- [x] PostgreSQL serving tables/views và `ml.predictions` đều đã có dữ liệu kiểm chứng.
- [x] Có model và metrics cho cả 30D lẫn 90D.
- [x] Có 9,382 streaming transactions được dự đoán và lưu thành công cho cả 30D/90D.
- [x] Đã chứng minh chạy lại cùng batch không tạo duplicate.
- [x] Có benchmark tái lập với median của 3 lần chạy trên mỗi cấu hình.
- [ ] Power BI dashboard hoàn chỉnh và refresh được.
- [ ] README, báo cáo và demo phản ánh đúng hệ thống thực tế.
- [x] Toàn bộ 10 tests pass bằng lệnh được ghi chính xác trong README.

## 8. Các lệnh kiểm tra nhanh

```powershell
# Trạng thái hạ tầng
docker compose ps

# Kiểm tra DAG
docker compose exec -T airflow-webserver airflow dags list
docker compose exec -T airflow-webserver airflow dags list-runs -d bnpl_batch_pipeline

# Trigger batch
docker compose exec -T airflow-webserver airflow dags unpause bnpl_batch_pipeline
docker compose exec -T airflow-webserver airflow dags trigger bnpl_batch_pipeline --conf '{"batch_id":"batch_20260921_001","model_version":"v1"}'

# Khởi động streaming và inference
docker compose --profile streaming up -d
docker compose --profile streaming --profile inference up -d

# Test đã kiểm chứng trên Spark image
docker run --rm -e PYTHONPATH=/workspace/spark:/opt/bitnami/spark/python:/opt/bitnami/spark/python/lib/py4j-0.10.9.7-src.zip -v "${PWD}:/workspace" -w /workspace bnpl-spark:3.5.1 python -m pytest -q tests
```

## 9. P3 Big Data Hardening

Mục tiêu P3 là xử lý các điểm nghẽn đã phát hiện khi đánh giá pipeline hiện tại. Đây là phần nâng cấp sau khi P0-P2 đã chạy ổn định, không phải điều kiện bắt buộc để demo phiên bản hiện tại.

### 9.1. Batch ingestion và data layout

- [x] **Ưu tiên cao:** stage nguồn thành nhiều file Parquet trên MinIO, sau đó để Spark đọc phân tán; không còn tạo DataFrame từ Python list trên driver.
- [x] Chỉ đọc đúng đường dẫn `source_slug=.../batch_key=...` của batch cần xử lý; không còn recursive scan toàn bộ Bronze rồi mới filter.
- [ ] Historical Bronze đã partition theo source và batch; bổ sung ngày nghiệp vụ khi dữ liệu mỗi batch lớn hơn đáng kể.
- [ ] Ghi nhận riêng thời gian download, parse, Spark transform và write để xác định đúng bottleneck ingestion.
- [ ] Kiểm chứng pipeline với tập nguồn lớn hơn 110,000 dòng; ưu tiên dữ liệu thực 2 triệu dòng thay vì chỉ nhân bản record trong benchmark.

### 9.2. Streaming scalability và tính đúng đắn

- [x] **Ưu tiên cao:** tăng topic `bnpl.transactions.raw` lên 6 partitions và cấu hình số partition bằng biến môi trường.
- [ ] Đã xác nhận prediction đến từ đủ 6 Kafka partitions và Spark có 2 executors; vẫn cần lưu ảnh Spark UI cùng số liệu input rows/second.
- [x] **Ưu tiên cao:** thay `scored.collect()` và ghi PostgreSQL tập trung tại driver bằng `foreachPartition` với batched upsert trên executors.
- [x] Cấu hình `maxOffsetsPerTrigger` và processing-time trigger để kiểm soát kích thước micro-batch.
- [x] Bổ sung event-time watermark và `dropDuplicatesWithinWatermark` theo `transaction_id`.
- [ ] Quy định và kiểm thử late-event policy: event đúng hạn, event đến trễ trong watermark và event quá hạn.
- [x] Kiểm thử restart/replay từ checkpoint và xác nhận PostgreSQL vẫn chỉ có đúng hai horizon cho mỗi transaction.
- [x] Cập nhật tài liệu để mô tả rõ hai consumer độc lập hiện tại: Bronze archive và online prediction.

### 9.3. Small files và vòng đời dữ liệu

- [x] **Ưu tiên cao:** partition Streaming Bronze theo `event_date` và `event_hour`.
- [x] Compaction đã giảm Streaming Bronze từ 15 xuống 6 files cho 14,029 rows và rejected từ 26 xuống 6 files cho 661 rows; layout P1 cũ từng có 448 files cho khoảng 9,594 events.
- [x] Cấu hình `maxRecordsPerFile` và thời lượng trigger qua biến môi trường.
- [x] Thêm job compaction cho Streaming Bronze/rejected Parquet và `OPTIMIZE` cho Delta tables được chọn.
- [ ] Delta `VACUUM` đã có ngưỡng an toàn tối thiểu 168 giờ; retention/lifecycle cho Bronze, rejected, checkpoint và model artifacts vẫn cần cấu hình ở object storage.
- [x] Chỉ cho phép `VACUUM` dưới 168 giờ khi bật cờ unsafe một cách tường minh.
- [ ] Maintenance report đã có `file_count`, kích thước và số dòng trước-sau; còn thiếu `batch_duration` và tốc độ tăng dung lượng theo thời gian.

### 9.4. Data realism và data drift

- [x] Điều chỉnh fake producer theo phân phối historical: principal amount, credit score, provider, merchant category, tenor, lãi suất và first-time borrower.
- [x] Mở rộng streaming generator từ 8 lên 37 bang.
- [x] Tạo baseline từ historical Gold, tính PSI cho tám feature và lưu báo cáo drift vào Delta/DQ metrics.
- [ ] Cảnh báo khi tỷ lệ missing, invalid, category mới hoặc phân phối feature vượt ngưỡng cho phép.
- [x] Ép xác suất 90D không thấp hơn 30D và thêm view kiểm tra cả xác suất lẫn `prediction_30d <= prediction_90d`; kết quả kiểm tra có 0 vi phạm.
- [x] Giữ auxiliary personal-loan dataset ở Bronze để truy vết, chưa đưa vào Silver/Gold vì chưa có mục tiêu phân tích phù hợp.

### 9.5. Benchmark và bằng chứng scale

- [x] Tách benchmark thành source read, sample preparation, transform/shuffle, output write và total; ghi kết quả theo phase vào Delta.
- [ ] Bổ sung Kafka throughput benchmark theo số partitions, producer rate và số Spark executors.
- [x] Bổ sung view PostgreSQL cho streaming latency p50, p95 và p99 theo horizon, gồm cả cửa sổ 15 phút gần nhất.
- [ ] So sánh throughput trước và sau khi loại bỏ `collect()` khỏi streaming prediction.
- [x] Ghi rõ benchmark hiện tại dùng hai workers trên cùng một Docker host và không đại diện cho multi-node physical scale-out.
- [ ] Nếu có tài nguyên, chạy một benchmark trên nhiều máy/VM để tách ảnh hưởng tranh chấp CPU, RAM, network và MinIO.
- [x] Lưu cấu hình, raw metrics và kết luận có thể tái lập trong `docs/p3_evidence.md`; biểu đồ có thể bổ sung sau khi có benchmark matrix dài hơn.

### 9.6. Tiêu chí hoàn thành P3

- [x] Không còn `collect()` trên đường xử lý streaming theo từng micro-batch.
- [x] Kafka có 6 partitions; prediction rows ghi nhận đủ cả 6 partitions qua hai Spark executors.
- [x] Streaming restart từ checkpoint không tạo duplicate prediction.
- [ ] Có watermark, late-event test và event-time deduplication test chạy thành công.
- [x] Số file nhỏ giảm rõ rệt sau partitioning/compaction và có số liệu trước-sau.
- [x] Generator streaming có phân phối gần historical hơn và có báo cáo drift.
- [x] Báo cáo phân biệt rõ logical distributed processing trên local Docker với physical multi-node scaling.
