"""Summarize quarantined rows and validation events."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from urban_data.config import load_datasets_config
from urban_data.paths import LAKEHOUSE, VALIDATION_EVENTS
from urban_data.validation.rules import RULE_REGISTRY, rule_categories, rule_names


def _quarantine_summary(spark: SparkSession) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    for dataset_key, cfg in sorted(load_datasets_config().items()):
        quarantine_path = LAKEHOUSE / cfg["quarantine_path"]
        if not quarantine_path.exists():
            continue
        frame = spark.read.format("delta").load(str(quarantine_path))
        if frame.limit(1).count() == 0:
            continue
        reason_col = (
            "validation_rules"
            if "validation_rules" in frame.columns
            else "quarantine_reason"
        )
        grouped = (
            frame.groupBy(reason_col)
            .agg(F.count("*").alias("row_count"))
            .orderBy(F.col("row_count").desc())
        )
        for row in grouped.collect():
            summaries.append(
                {
                    "dataset_key": dataset_key,
                    "logical_name": cfg["logical_name"],
                    "reason": row[reason_col],
                    "row_count": int(row["row_count"]),
                }
            )
    return summaries


def _validation_event_summary(spark: SparkSession) -> list[dict[str, Any]]:
    if not VALIDATION_EVENTS.exists():
        return []
    frame = spark.read.format("delta").load(str(VALIDATION_EVENTS))
    if frame.limit(1).count() == 0:
        return []
    grouped = (
        frame.groupBy("target_key", "rule_name", "pipeline_type")
        .agg(F.sum("failure_count").alias("failure_count"))
        .orderBy(F.col("failure_count").desc())
    )
    return [row.asDict(recursive=True) for row in grouped.collect()]


def build_validation_report(spark: SparkSession) -> dict[str, Any]:
    from urban_data.monitoring import register_monitoring_views

    register_monitoring_views(spark)
    rule_catalog = {
        name: {
            "category": rule.category,
            "description": rule.description,
        }
        for name, rule in RULE_REGISTRY.items()
    }
    quarantine = _quarantine_summary(spark)
    events = _validation_event_summary(spark)
    totals = spark.sql(
        """
        SELECT
            SUM(rows_rejected) AS total_quarantined_rows,
            SUM(validation_failure_count) AS total_validation_failures
        FROM monitoring_runs
        """
    ).collect()[0]
    return {
        "status": "success",
        "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "rule_categories": list(rule_categories()),
        "rule_catalog": rule_catalog,
        "enabled_rule_names": list(rule_names()),
        "quarantine_by_dataset": quarantine,
        "validation_events": events,
        "pipeline_totals": totals.asDict(recursive=True),
    }
