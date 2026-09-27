"""Pure split contract for the real MERGE benchmark."""

import runpy
from pathlib import Path

import pytest


BENCHMARK = runpy.run_path(str(Path(__file__).resolve().parents[1] / "spark" / "jobs" / "benchmark_incremental.py"))
split_counts = BENCHMARK["split_counts"]


def test_split_uses_actual_row_count():
    assert split_counts(100, 0.25) == (75, 25)
    assert split_counts(10, 0.25) == (8, 2)


def test_split_rejects_invalid_inputs():
    with pytest.raises(ValueError):
        split_counts(1, 0.25)
    with pytest.raises(ValueError):
        split_counts(10, 1.0)
