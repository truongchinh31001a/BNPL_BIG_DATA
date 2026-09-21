# Bằng chứng thực hiện P0

Ngày kiểm chứng: 21/09/2026.

## Airflow batch

- DAG: `bnpl_batch_pipeline`
- Run ID: `p0_20260921_001`
- Batch ID: `20260921`
- Model version: `v1`
- Thời gian: `06:25:13Z` đến `06:35:36Z`
- Kết quả: toàn bộ task từ ingest đến `load_postgres` và `end` đều `success`.
- Ingestion: `100,000` BNPL transactions và `100,000` personal-loan records phụ trợ.

### Idempotency retry

- Retry run ID: `p0_retry_20260921_001`.
- Dùng lại `batch_id=20260921`; toàn bộ 12 task đều `success` từ `07:05:32Z` đến `07:17:07Z`.
- Sau retry: `100,000` fact rows, `100,000` `transaction_id` duy nhất, `0` duplicate.
- `ml.model_metrics` vẫn có 6 dòng nhờ upsert theo model/version/horizon.
- `pipeline.pipeline_batches` vẫn có 2 dòng và `pipeline_run_id` được cập nhật sang retry run.
- DQ metrics tăng từ 20 lên 30 dòng vì được lưu theo từng `pipeline_run_id`; đây là lịch sử theo run, không phải duplicate business data.

## PostgreSQL serving

| Object | Row count |
| --- | ---: |
| `analytics.fact_bnpl_transaction` | 100,000 |
| `analytics.dim_customer` | 92,862 |
| `analytics.dim_date` | 1,095 |
| `analytics.dim_location` | 37 |
| `analytics.dim_merchant` | 4,983 |
| `analytics.dim_provider` | 5 |
| `ml.model_metrics` | 6 |
| `ml.predictions` | 0, chờ P1 streaming inference |
| `data_quality.data_quality_metrics` | 30 sau retry, gồm lịch sử theo từng run |
| `pipeline.pipeline_batches` | 2 |

`analytics.vw_bnpl_overview` trả về:

| Metric | Value |
| --- | ---: |
| Total transactions | 100,000 |
| Total BNPL amount | 4,995,623,529.83 |
| Average loan | 49,956.2352983 |
| Default 30D rate | 0.04928 |
| Default 90D rate | 0.079 |

Các câu lệnh kiểm tra nhanh:

```sql
SELECT COUNT(*) AS fact_rows,
       COUNT(DISTINCT transaction_id) AS distinct_transactions
FROM analytics.fact_bnpl_transaction;

SELECT * FROM analytics.vw_bnpl_overview;
SELECT * FROM analytics.vw_bnpl_transactions LIMIT 10;
SELECT * FROM ml.model_metrics ORDER BY prediction_horizon, recall_score DESC;
SELECT * FROM data_quality.data_quality_metrics ORDER BY created_at DESC;
SELECT * FROM pipeline.pipeline_batches ORDER BY received_at DESC;
```

## Data Quality gate

Batch `quality_demo` có 4 record được tạo bằng `spark/jobs/00_seed_quality_demo.py` để kiểm tra quarantine có thể tái lập:

- 2 record hợp lệ.
- 2 record bị reject, gồm duplicate, invalid date, negative principal và credit score ngoài miền.
- Rejected Parquet tồn tại tại `rejected/transactions/quality_demo`.

Metrics trong PostgreSQL:

| Metric | Value |
| --- | ---: |
| `row_count` | 4 |
| `duplicate_count` | 1 |
| `duplicate_rate` | 0.25 |
| `invalid_credit_score_count` | 1 |
| `invalid_date_count` | 1 |
| `invalid_principal_count` | 1 |
| `rejected_record_count` | 2 |
| `valid_record_rate` | 0.5 |

## Machine Learning

| Model | Horizon | Accuracy | Precision | Recall | F1 | ROC-AUC |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| GBT | 30D | 0.958081 | 0.667568 | 0.258639 | 0.949147 | 0.950321 |
| Logistic Regression | 30D | 0.953592 | 0.579909 | 0.132984 | 0.939488 | 0.919883 |
| Random Forest | 30D | 0.951826 | 0.000000 | 0.000000 | 0.928334 | 0.941874 |
| GBT | 90D | 0.937853 | 0.681686 | 0.316464 | 0.927142 | 0.949777 |
| Logistic Regression | 90D | 0.929479 | 0.559322 | 0.267206 | 0.917746 | 0.920611 |
| Random Forest | 90D | 0.925343 | 0.750000 | 0.002024 | 0.889661 | 0.934559 |

GBT được chọn cho cả 30D và 90D theo tiêu chí Recall giảm dần, sau đó ROC-AUC. Task evaluate đã load lại từng `PipelineModel` đã lưu và chạy inference trên tập test.

Confusion matrix của model được chọn, tạo bằng `spark/jobs/11_model_diagnostics.py`:

| Horizon | TP | FN | FP | TN | Default Recall |
| --- | ---: | ---: | ---: | ---: | ---: |
| 30D | 247 | 708 | 123 | 18,746 | 0.258639 |
| 90D | 469 | 1,013 | 219 | 18,123 | 0.316464 |

Kết quả cho thấy accuracy cao chủ yếu do dữ liệu mất cân bằng. GBT 30D vẫn bỏ sót 708/955 ca default thực tế; GBT 90D bỏ sót 1,013/1,482 ca. Khi tối ưu tiếp nên ưu tiên tăng Recall lớp Default bằng class weighting, threshold tuning hoặc resampling thay vì chỉ tối đa accuracy.

## MinIO lakehouse

- Tổng dung lượng kiểm tra: khoảng 84 MiB, 470 objects.
- Bronze historical: khoảng 26 MiB.
- Silver transactions: khoảng 3.5 MiB.
- Gold analytics: khoảng 8.1 MiB.
- Gold ML: khoảng 7.8 MiB.
- Models 30D và 90D, training runs, model registry và metrics đều tồn tại.

## Cấu hình Docker dùng cho lần chạy

- Docker engine: 12 CPU logic, 16,690,360,320 bytes RAM, khoảng 15.5 GiB.
- Spark cluster: 2 workers; mỗi worker `2` cores và `2G` RAM theo `.env`.
- Batch demo: `INGEST_MAX_ROWS=100000`.

## Power BI

- Project: `dashboard/BNPL Big Data Report.pbip`.
- PBIR validation: `succeeded`, 0 errors, 0 warnings.
- Power BI Desktop mở project thành công và nhận đủ 5 trang.
- Ảnh: `docs/evidence/powerbi/`.
- Semantic model dùng Import mode với `PostgreSQL.Database("localhost:5432", "bnpl_dw")` và không lưu password trong repository.
- Refresh dữ liệu trong Desktop chưa hoàn tất vì credential PostgreSQL phải được nhập vào credential store của Power BI ở lần đầu sử dụng.

## Tests

Lệnh đã chạy:

```powershell
docker run --rm `
  -e PYTHONPATH=/workspace/spark:/opt/bitnami/spark/python:/opt/bitnami/spark/python/lib/py4j-0.10.9.7-src.zip `
  -v "${PWD}:/workspace" `
  -w /workspace `
  bnpl-spark:3.5.1 python -m pytest -q tests
```

Kết quả kiểm tra cuối: `7 passed in 12.03s`.
