"""Run the reproducible Spark scalability and incremental benchmark matrices."""

from __future__ import annotations

import argparse
import os
import subprocess
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
PACKAGES = (
    "io.delta:delta-spark_2.12:3.2.0,"
    "org.postgresql:postgresql:42.7.3"
)
BENCHMARK_JOBS = {
    "scalability": "/opt/airflow/spark/jobs/benchmark_scalability.py",
    "incremental": "/opt/airflow/spark/jobs/benchmark_incremental.py",
}


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("value must be at least 1")
    return parsed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--sizes",
        nargs="+",
        type=positive_int,
        default=[100_000, 500_000, 1_000_000, 2_000_000],
        help="dataset sizes to benchmark",
    )
    parser.add_argument(
        "--workers",
        nargs="+",
        type=positive_int,
        default=[1, 2],
        help="Spark worker counts",
    )
    parser.add_argument("--runs", type=positive_int, default=3)
    parser.add_argument(
        "--benchmark",
        choices=["all", *BENCHMARK_JOBS],
        default="all",
        help="benchmark suite to execute",
    )
    parser.add_argument(
        "--incremental-fraction",
        type=float,
        default=0.25,
        help="fraction used by the incremental benchmark (0 < value < 1)",
    )
    args = parser.parse_args()
    if not 0 < args.incremental_fraction < 1:
        parser.error("--incremental-fraction must be between 0 and 1")
    return args


def run(command: list[str], *, environment: dict[str, str] | None = None) -> None:
    subprocess.run(
        command,
        cwd=REPOSITORY_ROOT,
        env=environment,
        check=True,
    )


def spark_submit(suite: str, size: int, workers: int, run_number: int, fraction: float) -> None:
    environment = os.environ.copy()
    environment.update(
        {
            "BENCHMARK_SIZE": str(size),
            "BENCHMARK_WORKER_COUNT": str(workers),
            "BENCHMARK_RUN_NUMBER": str(run_number),
            "BENCHMARK_INCREMENTAL_FRACTION": str(fraction),
        }
    )
    print(
        f"BENCHMARK suite={suite} workers={workers} size={size} run={run_number}",
        flush=True,
    )
    run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "-e",
            f"BENCHMARK_SIZE={size}",
            "-e",
            f"BENCHMARK_WORKER_COUNT={workers}",
            "-e",
            f"BENCHMARK_RUN_NUMBER={run_number}",
            "-e",
            f"BENCHMARK_INCREMENTAL_FRACTION={fraction}",
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
            BENCHMARK_JOBS[suite],
        ],
        environment=environment,
    )


def main() -> None:
    args = parse_args()
    suites = list(BENCHMARK_JOBS) if args.benchmark == "all" else [args.benchmark]

    run(
        [
            "docker",
            "compose",
            "stop",
            "fake-bnpl-producer",
            "spark-streaming-job",
            "spark-streaming-prediction",
        ]
    )

    for workers in args.workers:
        run(
            [
                "docker",
                "compose",
                "up",
                "-d",
                "--scale",
                f"spark-worker={workers}",
                "spark-worker",
            ]
        )
        for suite in suites:
            # Run number 0 is an unreported warm-up for each worker/suite pair.
            spark_submit(suite, args.sizes[0], workers, 0, args.incremental_fraction)
            for size in args.sizes:
                for run_number in range(1, args.runs + 1):
                    spark_submit(suite, size, workers, run_number, args.incremental_fraction)

    print("Benchmark matrix completed.", flush=True)


if __name__ == "__main__":
    main()
