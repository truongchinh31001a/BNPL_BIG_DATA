"""Split a local Parquet fixture into deterministic incremental batch folders."""

import argparse
from pathlib import Path

import pyarrow.parquet as parquet


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--batch-size", type=int, default=50_000)
    args = parser.parse_args()
    table = parquet.read_table(args.source)
    args.output.mkdir(parents=True, exist_ok=True)
    for index, offset in enumerate(range(0, table.num_rows, args.batch_size), start=1):
        target = args.output / f"batch_{index:03d}.parquet"
        parquet.write_table(table.slice(offset, args.batch_size), target)
        print(f"{target}: {min(args.batch_size, table.num_rows - offset)} rows")


if __name__ == "__main__":
    main()
