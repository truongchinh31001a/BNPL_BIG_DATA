"""Pure split contract for the real MERGE benchmark."""

import runpy
from pathlib import Path

import pytest


BENCHMARK = runpy.run_path(str(Path(__file__).resolve().parents[1] / "spark" / "jobs" / "benchmark_incremental.py"))
split_counts = BENCHMARK["split_counts"]
expand_to_size = BENCHMARK["expand_to_size"]


def test_split_uses_actual_row_count():
    assert split_counts(100, 0.25) == (75, 25)
    assert split_counts(10, 0.25) == (8, 2)


def test_split_rejects_invalid_inputs():
    with pytest.raises(ValueError):
        split_counts(1, 0.25)
    with pytest.raises(ValueError):
        split_counts(10, 1.0)


def test_expand_to_size_reaches_requested_count_with_unique_keys(spark):
    source = spark.createDataFrame(
        [("TX-2", 2), ("TX-1", 1)], ["transaction_id", "value"]
    )

    expanded = expand_to_size(spark, source, 5)

    assert expanded.count() == 5
    assert expanded.select("transaction_id").distinct().count() == 5
    assert [row.transaction_id for row in expanded.collect()] == [
        "TX-1_B0",
        "TX-2_B0",
        "TX-1_B1",
        "TX-2_B1",
        "TX-1_B2",
    ]
