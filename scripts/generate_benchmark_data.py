"""Create deterministic local benchmark subsets from a Parquet snapshot."""

import argparse
from pathlib import Path

import pyarrow.parquet as parquet


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--sizes", default="100000,500000,1000000,2000000")
    args = parser.parse_args()
    table = parquet.read_table(args.source)
    args.output.mkdir(parents=True, exist_ok=True)
    for size in (int(value) for value in args.sizes.split(",")):
        if size > table.num_rows:
            print(f"skip {size}: source has {table.num_rows} rows")
            continue
        target = args.output / f"bnpl_{size}.parquet"
        parquet.write_table(table.slice(0, size), target)
        print(target)


if __name__ == "__main__":
    main()
