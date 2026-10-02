"""Submit the PySpark 10M Bronze generator and print its result after completion."""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PACKAGES = "io.delta:delta-spark_2.12:3.2.0"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=10_000_000)
    parser.add_argument("--partitions", type=int, default=96)
    args = parser.parse_args()
    if args.rows < 1 or args.partitions < 1:
        parser.error("--rows and --partitions must be positive")

    command = [
        "docker",
        "compose",
        "exec",
        "-T",
        "airflow-webserver",
        "spark-submit",
        "--master",
        "spark://spark-master:7077",
        "--packages",
        PACKAGES,
        "--conf",
        "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
        "--conf",
        "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "/opt/airflow/spark/jobs/12_generate_10m_bronze.py",
        "--rows",
        str(args.rows),
        "--partitions",
        str(args.partitions),
    ]
    result = subprocess.run(
        command,
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    output = result.stdout + result.stderr
    if result.returncode:
        raise RuntimeError(output.strip())
    summary = [line for line in output.splitlines() if line.startswith("GENERATED_")]
    if not summary:
        raise RuntimeError("Spark completed without a generation summary")
    print("\n".join(summary))


if __name__ == "__main__":
    main()
