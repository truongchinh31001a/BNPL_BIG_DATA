"""Wait silently until the Docker engine is ready."""

from __future__ import annotations

import argparse
import subprocess
import time


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument("--interval", type=int, default=5)
    args = parser.parse_args()

    deadline = time.monotonic() + args.timeout
    while time.monotonic() < deadline:
        result = subprocess.run(
            ["docker", "info"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        if result.returncode == 0:
            print("Docker engine is ready")
            return
        time.sleep(args.interval)
    raise TimeoutError(f"Docker engine was not ready within {args.timeout} seconds")


if __name__ == "__main__":
    main()
