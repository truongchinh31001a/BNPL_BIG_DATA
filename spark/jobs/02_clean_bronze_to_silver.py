from pathlib import Path
from runpy import run_path


def main() -> None:
    run_path(str(Path(__file__).with_name("03_bronze_to_silver.py")), run_name="__main__")


if __name__ == "__main__":
    main()
