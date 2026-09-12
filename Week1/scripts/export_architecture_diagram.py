"""Export the Week 1 lakehouse architecture diagram to PNG."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch


def _box(ax, xy, text, facecolor, edgecolor="#334155", width=2.35, height=0.72):
    x, y = xy
    patch = FancyBboxPatch(
        (x - width / 2, y - height / 2),
        width,
        height,
        boxstyle="round,pad=0.04,rounding_size=0.08",
        linewidth=1.4,
        edgecolor=edgecolor,
        facecolor=facecolor,
    )
    ax.add_patch(patch)
    ax.text(
        x,
        y,
        text,
        ha="center",
        va="center",
        fontsize=10,
        color="#0f172a",
        wrap=True,
    )
    return patch


def _arrow(ax, start, end):
    ax.add_patch(
        FancyArrowPatch(
            start,
            end,
            arrowstyle="-|>",
            mutation_scale=14,
            linewidth=1.4,
            color="#475569",
            shrinkA=8,
            shrinkB=8,
            connectionstyle="arc3,rad=0.0",
        )
    )


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    output_path = project_root / "docs" / "architecture_diagram.png"

    fig, ax = plt.subplots(figsize=(14, 5.5), dpi=180)
    ax.set_xlim(0, 14)
    ax.set_ylim(0, 5)
    ax.axis("off")
    fig.patch.set_facecolor("white")

    ax.text(
        7.0,
        4.55,
        "ID2221 Week 1 Lakehouse Architecture",
        ha="center",
        va="center",
        fontsize=16,
        fontweight="bold",
        color="#0f172a",
    )
    ax.text(
        7.0,
        4.15,
        "Source files -> ingestion -> Bronze/Silver/Gold Delta tables",
        ha="center",
        va="center",
        fontsize=10,
        color="#475569",
    )

    colors = {
        "source": "#dbeafe",
        "process": "#fef3c7",
        "bronze": "#fde68a",
        "silver": "#ddd6fe",
        "gold": "#bbf7d0",
        "meta": "#e2e8f0",
        "bench": "#fce7f3",
    }

    nodes = {
        "raw": (1.4, 2.5),
        "ingest": (3.8, 2.5),
        "bronze": (6.2, 3.45),
        "runs": (6.2, 1.55),
        "quarantine": (6.2, 0.55),
        "silver": (8.8, 2.5),
        "gold": (11.4, 3.2),
        "bench": (11.4, 1.8),
    }

    _box(ax, nodes["raw"], "datasets:\nCSV / Parquet", colors["source"], width=2.0)
    _box(ax, nodes["ingest"], "Explicit-schema\ningestion", colors["process"], width=2.2)
    _box(ax, nodes["bronze"], "Bronze Delta", colors["bronze"], width=2.0)
    _box(ax, nodes["runs"], "ingestion_runs\nmetadata", colors["meta"], width=2.0)
    _box(ax, nodes["quarantine"], "Quarantine Delta", colors["meta"], width=2.0)
    _box(ax, nodes["silver"], "Standardized\nSilver Delta", colors["silver"], width=2.2)
    _box(ax, nodes["gold"], "integrated_taxi_trips\nGold Delta", colors["gold"], width=2.5)
    _box(ax, nodes["bench"], "Two benchmark\nlayouts", colors["bench"], width=2.2)

    _arrow(ax, (2.45, 2.5), (2.75, 2.5))
    _arrow(ax, (4.95, 2.65), (5.15, 3.25))
    _arrow(ax, (4.95, 2.35), (5.15, 1.55))
    _arrow(ax, (4.95, 2.2), (5.15, 0.75))
    _arrow(ax, (7.25, 3.25), (7.65, 2.75))
    _arrow(ax, (9.95, 2.65), (10.05, 3.05))
    _arrow(ax, (9.95, 2.35), (10.05, 1.95))

    legend_y = 0.08
    legend_items = [
        ("Source inputs", colors["source"]),
        ("Processing", colors["process"]),
        ("Medallion layers", colors["silver"]),
        ("Metadata / QA", colors["meta"]),
    ]
    x = 1.0
    for label, color in legend_items:
        ax.add_patch(
            FancyBboxPatch(
                (x, legend_y),
                0.18,
                0.18,
                boxstyle="round,pad=0.01,rounding_size=0.03",
                linewidth=0.8,
                edgecolor="#64748b",
                facecolor=color,
            )
        )
        ax.text(x + 0.28, legend_y + 0.09, label, fontsize=9, va="center", color="#334155")
        x += 2.2

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
