"""Wait silently for an Airflow DAG run and print only its terminal state."""

from __future__ import annotations

import argparse
import re
import subprocess
import time


TERMINAL_STATES = {"success", "failed"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_id")
    parser.add_argument("--dag-id", default="bnpl_batch_pipeline")
    parser.add_argument("--timeout", type=int, default=7_200)
    parser.add_argument("--interval", type=int, default=15)
    parser.add_argument("--postgres-user", default="bnpl")
    parser.add_argument("--postgres-db", default="bnpl_dw")
    args = parser.parse_args()
    for value in (args.run_id, args.dag_id):
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", value):
            parser.error(f"unsafe identifier: {value!r}")
    return args


def read_state(args: argparse.Namespace) -> str:
    query = (
        "SELECT state FROM dag_run "
        f"WHERE dag_id='{args.dag_id}' AND run_id='{args.run_id}' "
        "ORDER BY id DESC LIMIT 1;"
    )
    result = subprocess.run(
        [
            "docker",
            "exec",
            "bnpl-postgres",
            "psql",
            "-U",
            args.postgres_user,
            "-d",
            args.postgres_db,
            "-tAc",
            query,
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip().lower()


def main() -> None:
    args = parse_args()
    deadline = time.monotonic() + args.timeout
    state = ""
    while time.monotonic() < deadline:
        state = read_state(args)
        if state in TERMINAL_STATES:
            print(f"DAG {args.dag_id} run {args.run_id}: {state}")
            raise SystemExit(0 if state == "success" else 1)
        time.sleep(args.interval)
    raise TimeoutError(
        f"DAG {args.dag_id} run {args.run_id} did not finish within "
        f"{args.timeout} seconds (last state: {state or 'not found'})"
    )


if __name__ == "__main__":
    main()
