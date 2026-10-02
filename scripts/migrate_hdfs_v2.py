"""Start the three-DataNode HDFS v2 stack and wait for replication health."""

from __future__ import annotations

import argparse
import re
import subprocess
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def run(command: list[str]) -> str:
    result = subprocess.run(
        command,
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if result.returncode:
        output = (result.stdout + result.stderr).strip()
        raise RuntimeError(f"Command failed ({result.returncode}): {' '.join(command)}\n{output}")
    return (result.stdout + result.stderr).strip()


def hdfs(*arguments: str) -> str:
    return run(
        ["docker", "compose", "exec", "-T", "namenode", "hdfs", *arguments]
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeout", type=int, default=1_800)
    parser.add_argument("--interval", type=int, default=10)
    args = parser.parse_args()

    run(["docker", "compose", "up", "-d", "--build", "--remove-orphans"])
    deadline = time.monotonic() + args.timeout
    report = ""
    while time.monotonic() < deadline:
        try:
            report = hdfs("dfsadmin", "-report")
        except RuntimeError:
            time.sleep(args.interval)
            continue
        if "Live datanodes (3)" in report:
            break
        time.sleep(args.interval)
    else:
        raise TimeoutError("HDFS did not reach three live DataNodes")

    hdfs("dfs", "-setrep", "3", "/bnpl-data")
    fsck = ""
    while time.monotonic() < deadline:
        try:
            fsck = hdfs("fsck", "/bnpl-data")
        except RuntimeError:
            time.sleep(args.interval)
            continue
        healthy = "Status: HEALTHY" in fsck and all(
            re.search(pattern, fsck, flags=re.IGNORECASE)
            for pattern in (
                r"Under[- ]replicated blocks:\s*0\b",
                r"Missing blocks:\s*0\b",
                r"Corrupt blocks:\s*0\b",
            )
        )
        if healthy:
            report = hdfs("dfsadmin", "-report")
            print(report)
            print(fsck)
            return
        time.sleep(args.interval)
    raise TimeoutError("HDFS did not finish replication within the timeout")


if __name__ == "__main__":
    main()
