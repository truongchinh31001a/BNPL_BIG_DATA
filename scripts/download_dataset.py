"""Optional explicit local snapshot tool; production ingestion streams to MinIO."""

import argparse
from itertools import islice
from pathlib import Path

from datasets import Dataset, load_dataset


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("dataset_id")
    parser.add_argument("output", type=Path)
    parser.add_argument("--split", default="train")
    parser.add_argument("--rows", type=int, default=100_000)
    args = parser.parse_args()
    rows = list(islice(load_dataset(args.dataset_id, split=args.split, streaming=True), args.rows))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    Dataset.from_list(rows).to_parquet(str(args.output))
    print(f"wrote {len(rows)} rows to {args.output}")


if __name__ == "__main__":
    main()
