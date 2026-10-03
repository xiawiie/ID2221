"""Week 4 Task 4: compare raw-path vs platform-path ML dataset construction."""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from urban_data.ml.approach_a import (
    APPROACH_A_STEPS,
    build_approach_a_training_dataset,
)
from urban_data.ml.training_dataset import (
    _dataset_output_dir,
    build_training_dataset,
    resolve_ml_problem,
)
from urban_data.paths import PROJECT_ROOT


APPROACH_B_STEPS = (
    "Read pre-integrated gold/integrated_taxi_trips Delta table",
    "Aggregate accepted trips to pickup zone and local hour",
    "Derive temporal features and lag demand columns",
    "Assign chronological train, validation, and test splits",
    "Persist reusable Task 1 training splits under lakehouse/ml/",
)


def approach_b_source_line_count() -> int:
    path = Path(__file__).resolve().parent / "training_dataset.py"
    return sum(1 for line in path.read_text(encoding="utf-8").splitlines() if line.strip())


def _dataset_summary(frame_path: Path, spark: SparkSession) -> dict[str, Any]:
    frame = spark.read.format("delta").load(str(frame_path))
    split_counts = {
        row.dataset_split: int(row["count"])
        for row in frame.groupBy("dataset_split").count().collect()
    }
    metrics = frame.agg(
        F.sum("trip_count").alias("trip_count_sum"),
        F.avg("trip_count").alias("trip_count_avg"),
    ).collect()[0]
    return {
        "row_count": int(frame.count()),
        "split_counts": split_counts,
        "trip_count_sum": float(metrics["trip_count_sum"]),
        "trip_count_avg": float(metrics["trip_count_avg"]),
    }


def _equivalence_check(
    spark: SparkSession,
    approach_a_path: Path,
    approach_b_path: Path,
) -> dict[str, Any]:
    a = _dataset_summary(approach_a_path, spark)
    b = _dataset_summary(approach_b_path, spark)
    row_delta = abs(a["row_count"] - b["row_count"])
    sum_delta = abs(a["trip_count_sum"] - b["trip_count_sum"])
    return {
        "approach_a": a,
        "approach_b": b,
        "row_count_delta": row_delta,
        "trip_count_sum_delta": sum_delta,
        "split_counts_match": a["split_counts"] == b["split_counts"],
        "equivalent_within_tolerance": row_delta == 0 and sum_delta == 0,
    }


def compare_ml_approaches(
    spark: SparkSession,
    problem_name: str | None = None,
    *,
    refresh_approach_b: bool = True,
) -> dict[str, Any]:
    """Run Approach A and Approach B dataset builds and compare engineering cost."""
    problem = resolve_ml_problem(problem_name)

    approach_a = build_approach_a_training_dataset(spark, problem_name)

    b_started = time.perf_counter()
    if refresh_approach_b:
        approach_b = build_training_dataset(spark, problem_name)
        b_duration_ms = (time.perf_counter() - b_started) * 1000.0
    else:
        b_path = _dataset_output_dir(problem) / "full"
        if not b_path.exists():
            raise FileNotFoundError(
                "Approach B dataset missing; rerun without --skip-approach-b-refresh"
            )
        b_summary = _dataset_summary(b_path, spark)
        b_duration_ms = None
        approach_b = {
            "output_paths": {"full": str(b_path)},
            "row_counts": {
                "full": b_summary["row_count"],
                **b_summary["split_counts"],
            },
        }

    equivalence = _equivalence_check(
        spark,
        Path(approach_a["output_path"]),
        Path(approach_b["output_paths"]["full"]),
    )

    a_steps = len(APPROACH_A_STEPS)
    b_steps = len(APPROACH_B_STEPS)
    a_loc = approach_a["source_line_count"]
    b_loc = approach_b_source_line_count()

    speedup = None
    if approach_a["duration_ms"] and b_duration_ms:
        speedup = approach_a["duration_ms"] / b_duration_ms

    summary = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "problem": problem["key"],
        "approach_a": approach_a,
        "approach_b": {
            "approach": "B",
            "description": "Built from integrated Gold Delta table produced by Weeks 1-3",
            "duration_ms": b_duration_ms,
            "implementation_steps": list(APPROACH_B_STEPS),
            "source_line_count": b_loc,
            "row_counts": approach_b["row_counts"],
            "output_path": approach_b["output_paths"]["full"],
        },
        "equivalence": equivalence,
        "comparison": {
            "implementation_complexity": {
                "approach_a_steps": a_steps,
                "approach_b_steps": b_steps,
                "approach_a_source_lines": a_loc,
                "approach_b_source_lines": b_loc,
                "winner": "B",
                "note": "Platform path requires fewer steps and less bespoke ETL code.",
            },
            "preprocessing_complexity": {
                "approach_a_joins_and_sources": 4,
                "approach_b_joins_and_sources": 0,
                "winner": "B",
                "note": "Approach A repeats loading, cleaning, and multi-table integration.",
            },
            "dataset_build_time_ms": {
                "approach_a": approach_a["duration_ms"],
                "approach_b": b_duration_ms,
                "platform_speedup_x": speedup,
                "note": (
                    "Approach B timing includes Task 1 persistence of full plus "
                    "train/validation/test Delta splits; Approach A timing includes "
                    "one full-table write. Raw-path filters also differ from Gold "
                    "acceptance rules, so row counts are not identical."
                ),
            },
            "reproducibility": {
                "approach_a": [
                    "Depends on ad hoc raw-path script",
                    "No ingestion hash or pipeline run lineage",
                    "Re-running requires repeating full raw ETL",
                ],
                "approach_b": [
                    "Config-driven Task 1 builder over Delta Gold",
                    "Reuses Week 1-3 validation and integration outputs",
                    "Supports incremental Gold refresh and Task 3 retrain flow",
                ],
                "winner": "B",
            },
        },
        "discussion": {
            "week1_decisions_that_helped_ml": [
                "Gold integrated_taxi_trips removed repeated joins from the ML path",
                "Quarantine and validation already applied before ML dataset generation",
                "source_file_month partitioning and YAML dataset config simplified refresh",
            ],
            "most_useful_integrated_features": [
                "pickup_zone and borough attributes",
                "weather hourly context",
                "air_pm25_mean",
            ],
            "reusable_workflow_parts": [
                "build-ml-dataset",
                "featurize-ml-dataset",
                "train-ml-model / retrain-ml-model",
            ],
            "platform_improvements": [
                "Pre-materialize zone-hour demand as a Week 2-style product",
                "Record ML dataset builds in pipeline_runs metadata",
                "Add holiday or event datasets through the same config pattern",
            ],
        },
    }

    artifact_path = PROJECT_ROOT / "artifacts" / "week4_approach_comparison.json"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text(
        json.dumps(summary, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    summary["artifact_path"] = str(artifact_path)
    return summary
