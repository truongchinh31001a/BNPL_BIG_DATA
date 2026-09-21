"""Render dependency-free SVG charts from the exported benchmark summary CSV."""

from __future__ import annotations

import csv
import sys
from pathlib import Path
from xml.sax.saxutils import escape


WIDTH = 960
HEIGHT = 560
MARGIN = {"left": 92, "right": 32, "top": 58, "bottom": 78}
COLORS = {
    ("full", 1): "#C2413B",
    ("full", 2): "#196C72",
    ("incremental", 1): "#D69E2E",
    ("incremental", 2): "#276749",
}


def _fmt_size(value: int) -> str:
    if value >= 1_000_000:
        return f"{value / 1_000_000:g}M"
    return f"{value // 1_000}K"


def _render(rows: list[dict[str, str]], metric: str, title: str, y_label: str, output: Path) -> None:
    sizes = sorted({int(row["dataset_size"]) for row in rows})
    values = [float(row[metric]) for row in rows]
    max_value = max(values) * 1.12 if values else 1.0
    plot_w = WIDTH - MARGIN["left"] - MARGIN["right"]
    plot_h = HEIGHT - MARGIN["top"] - MARGIN["bottom"]

    def x_pos(size: int) -> float:
        index = sizes.index(size)
        return MARGIN["left"] + index * plot_w / max(len(sizes) - 1, 1)

    def y_pos(value: float) -> float:
        return MARGIN["top"] + plot_h * (1 - value / max_value)

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{HEIGHT}" viewBox="0 0 {WIDTH} {HEIGHT}">',
        '<rect width="100%" height="100%" fill="#F7F8F6"/>',
        f'<text x="{MARGIN["left"]}" y="32" font-family="Segoe UI,Arial" font-size="22" font-weight="600" fill="#18212A">{escape(title)}</text>',
    ]
    for tick in range(6):
        value = max_value * tick / 5
        y = y_pos(value)
        parts.append(f'<line x1="{MARGIN["left"]}" y1="{y:.1f}" x2="{WIDTH - MARGIN["right"]}" y2="{y:.1f}" stroke="#D8DDDE"/>')
        parts.append(f'<text x="{MARGIN["left"] - 12}" y="{y + 5:.1f}" text-anchor="end" font-family="Segoe UI,Arial" font-size="12" fill="#48545C">{value:,.1f}</text>')
    for size in sizes:
        x = x_pos(size)
        parts.append(f'<text x="{x:.1f}" y="{HEIGHT - 44}" text-anchor="middle" font-family="Segoe UI,Arial" font-size="13" fill="#38434A">{_fmt_size(size)}</text>')

    groups: dict[tuple[str, int], list[dict[str, str]]] = {}
    for row in rows:
        groups.setdefault((row["processing_mode"], int(row["worker_count"])), []).append(row)
    for key in sorted(groups):
        group = sorted(groups[key], key=lambda row: int(row["dataset_size"]))
        points = " ".join(f'{x_pos(int(row["dataset_size"])):.1f},{y_pos(float(row[metric])):.1f}' for row in group)
        color = COLORS[key]
        parts.append(f'<polyline points="{points}" fill="none" stroke="{color}" stroke-width="3"/>')
        for row in group:
            x = x_pos(int(row["dataset_size"]))
            y = y_pos(float(row[metric]))
            parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4.5" fill="{color}"/>')

    legend_x = MARGIN["left"]
    for index, key in enumerate(sorted(groups)):
        x = legend_x + index * 190
        label = f"{key[0].title()} - {key[1]} worker"
        parts.append(f'<line x1="{x}" y1="{HEIGHT - 16}" x2="{x + 24}" y2="{HEIGHT - 16}" stroke="{COLORS[key]}" stroke-width="3"/>')
        parts.append(f'<text x="{x + 31}" y="{HEIGHT - 11}" font-family="Segoe UI,Arial" font-size="12" fill="#263238">{escape(label)}</text>')
    parts.append(f'<text x="{WIDTH / 2}" y="{HEIGHT - 48}" text-anchor="middle" font-family="Segoe UI,Arial" font-size="13" fill="#263238">Requested dataset size</text>')
    parts.append(f'<text x="20" y="{HEIGHT / 2}" transform="rotate(-90 20 {HEIGHT / 2})" text-anchor="middle" font-family="Segoe UI,Arial" font-size="13" fill="#263238">{escape(y_label)}</text>')
    parts.append("</svg>")
    output.write_text("\n".join(parts), encoding="utf-8")


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit("Usage: render_benchmark_charts.py SUMMARY_CSV OUTPUT_DIR")
    source = Path(sys.argv[1])
    output_dir = Path(sys.argv[2])
    output_dir.mkdir(parents=True, exist_ok=True)
    with source.open(newline="", encoding="utf-8-sig") as handle:
        rows = list(csv.DictReader(handle))
    _render(rows, "median_runtime_seconds", "Spark median runtime: worker and mode comparison", "Median runtime (seconds)", output_dir / "benchmark_runtime.svg")
    _render(rows, "median_records_per_second", "Spark median throughput: worker and mode comparison", "Median records per second", output_dir / "benchmark_throughput.svg")


if __name__ == "__main__":
    main()
