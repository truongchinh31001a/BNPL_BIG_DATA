"""Run fast repository consistency checks without starting infrastructure."""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from urllib.parse import unquote


ROOT = Path(__file__).resolve().parents[1]
IGNORED_PARTS = {".git", ".venv", "__pycache__", ".pytest_cache", "logs"}
TEXT_SUFFIXES = {
    ".conf",
    ".env",
    ".json",
    ".md",
    ".py",
    ".sql",
    ".toml",
    ".xml",
    ".yaml",
    ".yml",
}
FORBIDDEN_STORAGE_TERMS = (
    "minio",
    "s3a://",
    "hadoop-aws",
    "aws-java-sdk",
    "redpanda",
)


def project_files() -> list[Path]:
    files: list[Path] = []
    for directory, subdirectories, filenames in os.walk(ROOT):
        subdirectories[:] = [
            name for name in subdirectories if name not in IGNORED_PARTS
        ]
        files.extend(Path(directory) / name for name in filenames)
    return files


def check_script_types(files: list[Path], errors: list[str]) -> None:
    scripts_root = ROOT / "scripts"
    invalid = [
        path.relative_to(ROOT)
        for path in files
        if path.parent == scripts_root and path.suffix.lower() not in {".py", ".ipynb"}
    ]
    if invalid:
        errors.append(f"non-Python automation in scripts/: {invalid}")


def check_obsolete_storage(files: list[Path], errors: list[str]) -> None:
    for path in files:
        if path.resolve() == Path(__file__).resolve():
            continue
        if path.suffix.lower() not in TEXT_SUFFIXES and path.name != ".env.example":
            continue
        text = path.read_text(encoding="utf-8", errors="ignore").lower()
        matches = [term for term in FORBIDDEN_STORAGE_TERMS if term in text]
        if matches:
            errors.append(f"obsolete storage term {matches} in {path.relative_to(ROOT)}")


def check_json(files: list[Path], errors: list[str]) -> int:
    checked = 0
    for path in files:
        if path.suffix.lower() != ".json":
            continue
        try:
            json.loads(path.read_text(encoding="utf-8-sig"))
            checked += 1
        except json.JSONDecodeError as exc:
            errors.append(f"invalid JSON {path.relative_to(ROOT)}: {exc}")
    return checked


def check_compose_environment(errors: list[str]) -> None:
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    referenced = set(re.findall(r"\$\{([A-Z][A-Z0-9_]*)", compose))
    template_keys = {
        match.group(1)
        for line in (ROOT / ".env.example").read_text(encoding="utf-8").splitlines()
        if (match := re.match(r"^([A-Z][A-Z0-9_]*)=", line))
    }
    missing = sorted(referenced - template_keys)
    if missing:
        errors.append(f"Compose variables missing from .env.example: {missing}")


def check_markdown_links(files: list[Path], errors: list[str]) -> int:
    checked = 0
    pattern = re.compile(r"(?<!!)\[[^\]]+\]\(([^)]+)\)")
    for path in files:
        if path.suffix.lower() != ".md":
            continue
        text = path.read_text(encoding="utf-8")
        for raw_target in pattern.findall(text):
            target = raw_target.strip().strip("<>")
            if target.startswith(("http://", "https://", "mailto:", "#")):
                continue
            target = unquote(target.split("#", 1)[0])
            if not target:
                continue
            candidate = (path.parent / target).resolve()
            checked += 1
            if not candidate.exists():
                errors.append(
                    f"broken Markdown link in {path.relative_to(ROOT)}: {raw_target}"
                )
    return checked


def check_power_bi(errors: list[str]) -> None:
    project = ROOT / "dashboard" / "BNPL Big Data Report.pbip"
    payload = json.loads(project.read_text(encoding="utf-8-sig"))
    for artifact in payload.get("artifacts", []):
        report_path = artifact.get("report", {}).get("path")
        if report_path and not (project.parent / report_path).is_dir():
            errors.append(f"Power BI report directory is missing: {report_path}")


def main() -> None:
    files = project_files()
    errors: list[str] = []
    check_script_types(files, errors)
    check_obsolete_storage(files, errors)
    json_count = check_json(files, errors)
    check_compose_environment(errors)
    link_count = check_markdown_links(files, errors)
    check_power_bi(errors)
    if errors:
        raise SystemExit("Project validation failed:\n- " + "\n- ".join(errors))
    print(
        f"Project validation passed: {len(files)} files, "
        f"{json_count} JSON documents, {link_count} local Markdown links"
    )


if __name__ == "__main__":
    main()
