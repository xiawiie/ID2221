"""Week 3 Task 5 platform evaluation experiments."""

from __future__ import annotations

import json
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from urban_data.config import resolve_dataset
from urban_data.monitoring import register_monitoring_views
from urban_data.paths import LAKEHOUSE, PIPELINE_RUNS
from urban_data.transforms import taxi_silver_columns_by_pickup_month
from urban_data.validation.engine import apply_validation_rules
from urban_data.validation.rules import enabled_rules

ROOT = Path(__file__).resolve().parents[2]
ARTIFACT = ROOT / "artifacts" / "week3_platform_evaluation.json"
RUNS = 3

STORAGE_GROUPS = {
    "core_analytical": [
        "bronze",
        "silver",
        "gold",
        "products",
    ],
    "week3_operational_overhead": [
        "metadata",
        "quarantine",
    ],
}


def _storage_bytes(spark: Any, path: Path) -> int:
    if not path.exists():
        return 0
    row = spark.sql(
        f"DESCRIBE DETAIL delta.`{path.resolve().as_posix()}`"
    ).collect()[0]
    return int(row["sizeInBytes"])


def _tree_bytes(spark: Any, relative_root: str) -> dict[str, int]:
    root = LAKEHOUSE / relative_root
    if not root.exists():
        return {}
    if (root / "_delta_log").exists():
        return {relative_root: _storage_bytes(spark, root)}
    totals: dict[str, int] = {}
    for child in sorted(root.iterdir()):
        if not child.is_dir():
            continue
        rel = f"{relative_root}/{child.name}"
        if (child / "_delta_log").exists():
            totals[rel] = _storage_bytes(spark, child)
        else:
            totals.update(_tree_bytes(spark, rel))
    return totals


def _summarize_storage(spark: Any) -> dict[str, Any]:
    detail: dict[str, int] = {}
    for group, roots in STORAGE_GROUPS.items():
        for root_name in roots:
            detail.update(_tree_bytes(spark, root_name))

    grouped_totals = {
        group: sum(
            size
            for path, size in detail.items()
            if path.split("/")[0] in roots
        )
        for group, roots in STORAGE_GROUPS.items()
    }
    total = sum(detail.values()) or 1
    return {
        "tables_bytes": detail,
        "group_totals_bytes": grouped_totals,
        "operational_overhead_percent": round(
            100.0 * grouped_totals["week3_operational_overhead"] / total,
            4,
        ),
        "total_bytes": total,
    }


def _observed_pipeline_times(spark: Any) -> dict[str, Any]:
    register_monitoring_views(spark)
    frame = spark.table("monitoring_runs")
    if frame.limit(1).count() == 0:
        return {"available": False}

    def _latest_success(pipeline_type: str, target_key: str | None = None):
        query = frame.filter(
            (F.col("pipeline_type") == pipeline_type) & (F.col("status") == "success")
        )
        if target_key:
            query = query.filter(F.col("target_key") == target_key)
        row = query.orderBy(F.col("finished_at").desc()).limit(1).collect()
        if not row:
            return None
        item = row[0].asDict(recursive=True)
        return {
            "run_id": item["run_id"],
            "target_key": item["target_key"],
            "pipeline_type": item["pipeline_type"],
            "duration_ms": float(item["duration_ms"]),
            "rows_processed": int(item["rows_processed"]),
            "rows_inserted": int(item["rows_inserted"]),
            "finished_at": str(item["finished_at"]),
        }

    def _latest_incremental_update(target_key: str = "taxi_trips_update"):
        row = (
            frame.filter(F.col("target_key") == target_key)
            .filter(F.col("rows_inserted") > 0)
            .orderBy(F.col("finished_at").desc())
            .limit(1)
            .collect()
        )
        if not row:
            return None
        item = row[0].asDict(recursive=True)
        return {
            "run_id": item["run_id"],
            "target_key": item["target_key"],
            "pipeline_type": item["pipeline_type"],
            "duration_ms": float(item["duration_ms"]),
            "rows_processed": int(item["rows_processed"]),
            "rows_inserted": int(item["rows_inserted"]),
            "finished_at": str(item["finished_at"]),
        }

    refresh_frame = frame.filter(F.col("pipeline_type") == "product_refresh").filter(
        F.col("status") == "success"
    )
    refresh_summary = None
    if refresh_frame.limit(1).count():
        ranked = refresh_frame.withColumn(
            "row_number",
            F.row_number().over(
                Window.partitionBy("target_key").orderBy(F.col("finished_at").desc())
            ),
        ).filter(F.col("row_number") == 1)
        agg = ranked.agg(
            F.sum("duration_ms").alias("total_duration_ms"),
            F.count("*").alias("product_count"),
            F.max("finished_at").alias("latest_finished_at"),
        ).collect()[0]
        refresh_summary = {
            "product_count": int(agg["product_count"]),
            "total_duration_ms": float(agg["total_duration_ms"]),
            "latest_finished_at": str(agg["latest_finished_at"]),
            "per_product": [
                {
                    "target_key": row.target_key,
                    "duration_ms": float(row.duration_ms),
                    "rows_inserted": int(row.rows_inserted),
                }
                for row in ranked.select(
                    "target_key", "duration_ms", "rows_inserted"
                ).collect()
            ],
        }

    incremental_rows = [
        row.asDict(recursive=True)
        for row in frame.filter(F.col("pipeline_type") == "incremental")
        .filter(F.col("status") == "success")
        .select("target_key", "duration_ms", "rows_inserted", "finished_at")
        .orderBy(F.col("finished_at").desc())
        .collect()
    ]

    return {
        "available": True,
        "incremental_taxi_update": _latest_incremental_update("taxi_trips_update"),
        "incremental_runs": incremental_rows[:6],
        "product_refresh_batch": refresh_summary,
        "integrate_incremental": _latest_success("integrate", "integrated_taxi_trips"),
    }


def _validation_sample(spark: Any):
    cfg = resolve_dataset("taxi_trips_update")
    bronze_path = LAKEHOUSE / cfg["bronze_path"]
    if not bronze_path.exists():
        return None
    bronze = spark.read.format("delta").load(str(bronze_path)).limit(100_000)
    if bronze.limit(1).count() == 0:
        return None
    silver = taxi_silver_columns_by_pickup_month(bronze)
    valid_location_ids = None
    zones_path = LAKEHOUSE / "silver" / "taxi_zones"
    if zones_path.exists():
        valid_location_ids = [
            int(row.location_id)
            for row in spark.read.format("delta")
            .load(str(zones_path))
            .select("location_id")
            .distinct()
            .collect()
            if row.location_id is not None
        ]
    return silver, {"valid_location_ids": valid_location_ids}


def _benchmark_validation_overhead(spark: Any) -> dict[str, Any]:
    sample = _validation_sample(spark)
    if sample is None:
        return {"available": False, "reason": "taxi incremental bronze sample unavailable"}

    silver, context = sample
    full_rules = enabled_rules("taxi_trips")

    def _timed(rules):
        started = time.perf_counter()
        outcome = apply_validation_rules(
            silver,
            "taxi_trips",
            context=context,
            rules=rules,
        )
        _ = outcome.accepted.count()
        _ = outcome.quarantined.count()
        return round((time.perf_counter() - started) * 1000, 3)

    baseline_ms = [_timed([]) for _ in range(RUNS)]
    full_ms = [_timed(full_rules) for _ in range(RUNS)]
    baseline_median = statistics.median(baseline_ms)
    full_median = statistics.median(full_ms)
    overhead_ms = round(full_median - baseline_median, 3)
    return {
        "available": True,
        "sample_rows": 100_000,
        "rule_count": len(full_rules),
        "baseline_median_ms": round(baseline_median, 3),
        "full_validation_median_ms": round(full_median, 3),
        "overhead_ms": overhead_ms,
        "overhead_percent": round(
            100.0 * overhead_ms / full_median, 4
        )
        if full_median
        else None,
        "baseline_runs_ms": baseline_ms,
        "full_runs_ms": full_ms,
    }


def _benchmark_monitoring_overhead(spark: Any) -> dict[str, Any]:
    from urban_data.monitoring import record_pipeline_run
    from urban_data.transforms import utc_now

    durations: list[float] = []
    for _ in range(RUNS):
        started = time.perf_counter()
        record_pipeline_run(
            spark,
            run_id=uuid4().hex,
            pipeline_type="evaluation",
            target_key="evaluation_probe",
            target_name="evaluation_probe",
            schema_version="1",
            rows_processed=0,
            rows_inserted=0,
            rows_rejected=0,
            started_at=utc_now(),
            finished_at=utc_now(),
            status="success",
        )
        durations.append(round((time.perf_counter() - started) * 1000, 3))

    median_ms = round(statistics.median(durations), 3)
    return {
        "available": True,
        "runs": durations,
        "median_ms": median_ms,
        "note": "Synthetic probe rows appended to pipeline_runs during evaluation",
    }


def _discussion_summary(measurements: dict[str, Any]) -> dict[str, str]:
    return {
        "week1_decisions_that_simplified_maintenance": (
            "Configuration-driven datasets.yml, medallion layering with quarantine, "
            "source_file_month partitioning, and ingestion_runs lineage made Week 3 "
            "incremental merge and monitoring extensions incremental rather than a rewrite."
        ),
        "largest_modifications": (
            "Incremental taxi merge and Gold append, selective product refresh with "
            "dependency planning, and the validation rule engine required the most new code. "
            "Monitoring layered on top of existing run metadata."
        ),
        "future_dataset_support": (
            "New datasets add YAML entries plus optional validation rules; only new physical "
            "schemas or transform kinds need Python. Incremental and refresh wiring reuse the "
            "same ingest and product dependency patterns."
        ),
        "redesign_today": (
            "Use a single unified run-metadata schema from day one, encode product dependency "
            "graphs earlier, and treat validation rules as pure config where masks are composable. "
            "Separate operational metadata tables sooner to simplify overhead measurement."
        ),
    }


def evaluate_platform(spark: Any, *, save_artifact: bool = True) -> dict[str, Any]:
    """Measure Week 3 production-readiness metrics."""

    observed = _observed_pipeline_times(spark)
    storage = _summarize_storage(spark)
    validation = _benchmark_validation_overhead(spark)
    monitoring = _benchmark_monitoring_overhead(spark)

    report = {
        "status": "success",
        "generated_at_utc": datetime.now(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z"),
        "protocol": f"Observed pipeline_runs plus {RUNS}-run micro-benchmarks for overhead",
        "environment": {
            "spark_app": spark.sparkContext.appName,
            "lakehouse": str(LAKEHOUSE),
        },
        "measurements": {
            "incremental_update": observed.get("incremental_taxi_update"),
            "incremental_runs": observed.get("incremental_runs", []),
            "analytical_refresh": observed.get("product_refresh_batch"),
            "integrate_incremental": observed.get("integrate_incremental"),
            "storage_overhead": storage,
            "validation_overhead": validation,
            "monitoring_overhead": monitoring,
        },
        "observed_pipeline_metadata_available": observed.get("available", False),
        "discussion": _discussion_summary(
            {
                "storage": storage,
                "validation": validation,
                "monitoring": monitoring,
            }
        ),
    }

    if save_artifact:
        ARTIFACT.parent.mkdir(parents=True, exist_ok=True)
        ARTIFACT.write_text(
            json.dumps(report, default=str, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        report["artifact_path"] = str(ARTIFACT)

    return report
