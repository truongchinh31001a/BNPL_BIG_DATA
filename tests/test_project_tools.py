import csv
import runpy
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_benchmark_renderer_separates_suites(tmp_path, monkeypatch):
    summary = tmp_path / "summary.csv"
    fieldnames = [
        "benchmark_suite",
        "dataset_size",
        "processing_mode",
        "worker_count",
        "runs",
        "median_runtime_seconds",
        "median_records_per_second",
        "median_processed_records",
    ]
    rows = [
        ["scalability", 100000, "full", 1, 3, 10, 10000, 100000],
        ["scalability", 100000, "full", 2, 3, 7, 14000, 100000],
        ["incremental", 100000, "full", 1, 3, 10, 10000, 100000],
        ["incremental", 100000, "incremental", 1, 3, 4, 6250, 25000],
    ]
    with summary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(fieldnames)
        writer.writerows(rows)

    monkeypatch.setattr(
        sys,
        "argv",
        ["render_benchmark_charts.py", str(summary), str(tmp_path)],
    )
    runpy.run_path(
        str(ROOT / "scripts" / "render_benchmark_charts.py"), run_name="__main__"
    )

    assert (tmp_path / "benchmark_runtime.svg").is_file()
    assert (tmp_path / "benchmark_throughput.svg").is_file()
    assert (tmp_path / "benchmark_incremental_runtime.svg").is_file()
    assert (tmp_path / "benchmark_incremental_throughput.svg").is_file()
