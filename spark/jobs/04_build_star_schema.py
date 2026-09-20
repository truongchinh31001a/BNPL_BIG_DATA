from pathlib import Path
from runpy import run_path


def main() -> None:
    run_path(str(Path(__file__).with_name("06_build_star_schema.py")), run_name="__main__")


if __name__ == "__main__":
    main()
