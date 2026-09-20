"""Airflow orchestration for the finite historical BNPL batch pipeline."""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


SPARK_PACKAGES = ",".join(
    [
        "io.delta:delta-spark_2.12:3.2.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "org.postgresql:postgresql:42.7.3",
    ]
)
SPARK_SUBMIT = (
    "spark-submit --master spark://spark-master:7077 "
    f"--packages {SPARK_PACKAGES} "
    "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
    "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
    "/opt/airflow/spark/jobs/{job}"
)
RUN_ENV = {
    "PIPELINE_RUN_ID": "{{ run_id }}",
    "BATCH_ID": "{{ dag_run.conf.get('batch_id', ds_nodash) }}",
    "MODEL_VERSION": "{{ dag_run.conf.get('model_version', 'v1') }}",
}


def spark_task(task_id: str, job: str) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=SPARK_SUBMIT.format(job=job),
        env=RUN_ENV,
        append_env=True,
    )


with DAG(
    dag_id="bnpl_batch_pipeline",
    description="Historical BNPL Bronze-Silver-Gold, DQ, analytics and ML pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["bnpl", "batch", "spark", "delta"],
) as dag:
    start = EmptyOperator(task_id="start")
    ingest_data = spark_task("ingest_data", "01_ingest.py")
    validate_bronze = spark_task("validate_bronze", "02_validate_bronze.py")
    bronze_to_silver = spark_task("bronze_to_silver", "03_bronze_to_silver.py")
    data_quality_metrics = spark_task("data_quality_metrics", "04_data_quality.py")
    feature_engineering = spark_task("feature_engineering", "05_feature_engineering.py")
    build_analytics = spark_task("build_analytics", "06_build_star_schema.py")
    build_ml_features = spark_task("build_ml_features", "07_build_ml_features.py")
    train_models = spark_task("train_models", "08_train_models.py")
    evaluate_models = spark_task("evaluate_models", "09_evaluate_models.py")
    load_postgres = spark_task("load_postgres", "10_load_postgres.py")
    end = EmptyOperator(task_id="end")

    start >> ingest_data >> validate_bronze >> bronze_to_silver >> data_quality_metrics
    data_quality_metrics >> feature_engineering >> [build_analytics, build_ml_features]
    build_ml_features >> train_models >> evaluate_models
    [build_analytics, evaluate_models] >> load_postgres >> end
