"""Render the submission-ready Week 1 lakehouse architecture diagram."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch


def box(ax, x, y, text, color, width=2.25, height=0.82, fontsize=9):
    ax.add_patch(
        FancyBboxPatch(
            (x - width / 2, y - height / 2),
            width,
            height,
            boxstyle="round,pad=0.04,rounding_size=0.07",
            linewidth=1.2,
            edgecolor="#334155",
            facecolor=color,
        )
    )
    ax.text(x, y, text, ha="center", va="center", fontsize=fontsize, color="#0f172a", wrap=True)


def arrow(ax, start, end, label=None, offset=0.14):
    ax.add_patch(
        FancyArrowPatch(
            start,
            end,
            arrowstyle="-|>",
            mutation_scale=13,
            linewidth=1.25,
            color="#475569",
            shrinkA=8,
            shrinkB=8,
        )
    )
    if label:
        ax.text(
            (start[0] + end[0]) / 2,
            (start[1] + end[1]) / 2 + offset,
            label,
            ha="center",
            va="center",
            fontsize=7.4,
            color="#475569",
        )


def phase(ax, x, label):
    ax.text(x, 7.66, label, ha="center", va="center", fontsize=9, fontweight="bold", color="#334155")


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    output_path = project_root / "docs" / "architecture_diagram.png"
    colors = {
        "source": "#dbeafe",
        "contract": "#fef3c7",
        "process": "#fde68a",
        "bronze": "#fed7aa",
        "silver": "#ddd6fe",
        "gold": "#bbf7d0",
        "quality": "#e2e8f0",
        "benchmark": "#fce7f3",
    }

    fig, ax = plt.subplots(figsize=(16, 8), dpi=200)
    ax.set(xlim=(0, 16), ylim=(0, 8))
    ax.axis("off")
    fig.patch.set_facecolor("white")

    ax.text(
        8,
        7.35,
        "ID2221 Week 1: Generic Urban Data Integration Platform",
        ha="center",
        va="center",
        fontsize=17,
        fontweight="bold",
        color="#0f172a",
    )
    ax.text(
        8,
        6.96,
        "Explicit contracts, Delta Lake layers, quality isolation, and one enriched taxi-trip grain",
        ha="center",
        va="center",
        fontsize=9.5,
        color="#475569",
    )
    for x, label in [(1.25, "SOURCE INPUTS"), (4.15, "CONTRACTS"), (6.55, "INGESTION"), (9.2, "DELTA LAKEHOUSE"), (13.35, "ANALYTICS")]:
        phase(ax, x, label)

    sources = [
        ("Yellow taxi trips\nParquet, Jan-Mar 2024", 5.95),
        ("Weather\nCSV, hourly", 4.72),
        ("Air quality\nCSV/ZIP, hourly PM2.5", 3.49),
        ("Taxi zone lookup\nCSV, 265 zones", 2.26),
    ]
    for label, y in sources:
        box(ax, 1.28, y, label, colors["source"], width=2.15, height=0.76, fontsize=8.6)

    box(ax, 4.05, 4.1, "Dataset contracts\nconfig/datasets.yml\nSchemas and versions", colors["contract"], width=2.3, height=1.08, fontsize=8.7)
    box(ax, 6.55, 4.1, "Generic ingestion\nCSV/Parquet dispatch\nexplicit schema + lineage", colors["process"], width=2.35, height=1.08, fontsize=8.7)
    box(ax, 9.15, 5.38, "Bronze Delta\nraw values + source hash\nrun ID + schema version", colors["bronze"], width=2.28, height=0.98, fontsize=8.6)
    box(ax, 9.15, 3.56, "Standardize and validate\nsnake_case, timestamps\nquality flags and rules", colors["process"], width=2.28, height=1.0, fontsize=8.5)
    box(ax, 12.0, 5.38, "Silver Delta\nTaxi trips: source_file_month\nZones, weather, PM2.5: broadcast", colors["silver"], width=2.72, height=1.0, fontsize=8.35)
    box(ax, 12.0, 3.3, "Integration pipeline\nZones x 2 | weather: local hour\nPM2.5: aggregated UTC hour", colors["process"], width=2.72, height=0.98, fontsize=8.25)
    box(ax, 14.65, 3.3, "Gold Delta\nintegrated_taxi_trips\n1 row per accepted trip", colors["gold"], width=2.22, height=0.98, fontsize=8.4)
    box(ax, 6.55, 1.63, "Ingestion metadata Delta\ncounts, duration, hash, schema version", colors["quality"], width=2.55, height=0.84, fontsize=8.15)
    box(ax, 9.15, 1.63, "Quarantine Delta\ninvalid rows + rejection reason", colors["quality"], width=2.28, height=0.84, fontsize=8.25)
    box(ax, 12.0, 1.63, "Benchmark layouts\nunpartitioned vs pickup_month\nwrite, storage, query latency", colors["benchmark"], width=2.72, height=0.84, fontsize=8.1)

    for _, y in sources:
        arrow(ax, (2.38, y), (2.88, 4.1))
    arrow(ax, (5.25, 4.1), (5.38, 4.1), "contract-driven")
    arrow(ax, (7.73, 4.38), (8.0, 5.15), "raw + lineage", 0.24)
    arrow(ax, (9.15, 4.82), (9.15, 4.14), "quality split", 0.0)
    arrow(ax, (10.32, 3.72), (10.63, 5.2), "accepted", 0.2)
    arrow(ax, (10.32, 3.4), (10.63, 3.3), "join keys", 0.2)
    arrow(ax, (13.38, 3.3), (13.53, 3.3))
    arrow(ax, (6.55, 3.55), (6.55, 2.15), "run statistics", 0.08)
    arrow(ax, (9.15, 3.06), (9.15, 2.15), "rejected", 0.08)
    arrow(ax, (12.0, 4.86), (12.0, 2.15), "taxi table", 0.05)

    ax.text(
        8,
        0.45,
        "All materialized datasets, quarantine records, and ingestion metadata are Delta tables. "
        "Environment values are left NULL when no matching observation exists.",
        ha="center",
        va="center",
        fontsize=8.3,
        color="#475569",
    )
    fig.savefig(output_path, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
