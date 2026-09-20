# Batch pipeline

## Stages

1. `01_ingest.py`: stream finite historical source thành Bronze Parquet partition theo source/batch.
2. `02_validate_bronze.py`: explicit parse, controlled schema comparison, gate PASS/FAIL và quarantine.
3. `03_bronze_to_silver.py`: Delta MERGE valid rows theo `transaction_id`.
4. `04_data_quality.py`: tính row/null/duplicate/invalid/rejected/valid metrics, ghi Delta và PostgreSQL.
5. `05_feature_engineering.py`: reusable transformation, incremental Delta MERGE.
6. `06_build_star_schema.py`: Gold Analytics dimensions/fact.
7. `07_build_ml_features.py`: Gold flat ML contract.
8. `08_train_models.py`: Logistic Regression, Random Forest, GBT cho 30D và 90D.
9. `09_evaluate_models.py`: Accuracy, default-class Precision/Recall, F1, ROC-AUC; chọn model theo Recall rồi ROC-AUC.
10. `10_load_postgres.py`: stage và PostgreSQL upsert, không drop serving constraints.

## Quality gate

Các rule bắt buộc gồm transaction ID, valid date, non-empty categorical identifiers, principal > 0, rate >= 0, tenor/installments > 0, credit score 300..850, valid booleans và duplicate trong batch. Mỗi FAIL record có `_validation_errors`, `_rejected_at`, run ID và batch ID.

## Idempotency

- Bronze path của cùng source/batch được overwrite trước khi append chunks.
- Silver/Gold dùng Delta MERGE business keys.
- DQ metrics và batch status dùng PostgreSQL `ON CONFLICT`.
- Serving load dùng staging + upsert.

## Full reload vs incremental

Incremental là default theo `BATCH_ID`. Full reload có thể dùng batch ID mới với toàn bộ source hoặc xóa volumes trong môi trường demo có chủ đích. Không dùng blind append cho Silver/Gold.
