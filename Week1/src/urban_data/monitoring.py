"""Week 3 Task 3 platform monitoring."""

from __future__ import annotations

import json
import time
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Iterator
from uuid import uuid4

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from urban_data.paths import LAKEHOUSE, METADATA_RUNS, PIPELINE_RUNS, VALIDATION_EVENTS
from urban_data.schemas import (
    PIPELINE_RUNS_SCHEMA,
    VALIDATION_EVENTS_SCHEMA,
)
from urban_data.transforms import utc_now

MONITORING_QUERIES = {
    "validation_failures_by_dataset": """
        SELECT
            target_key,
            SUM(validation_failure_count) AS total_validation_failures,
            SUM(rows_rejected) AS total_rejected_records,
            COUNT(*) AS execution_count
        FROM monitoring_runs
        WHERE validation_failure_count > 0 OR rows_rejected > 0
        GROUP BY target_key
        ORDER BY total_validation_failures DESC, total_rejected_records DESC
    """,
    "longest_processing_by_target": """
        SELECT
            target_key,
            pipeline_type,
            MAX(duration_ms) AS max_duration_ms,
            ROUND(AVG(duration_ms), 3) AS avg_duration_ms,
            COUNT(*) AS execution_count
        FROM monitoring_runs
        WHERE status = 'success'
        GROUP BY target_key, pipeline_type
        ORDER BY max_duration_ms DESC
    """,
    "rejected_records_by_execution": """
        SELECT
            run_id,
            pipeline_type,
            target_key,
            rows_processed,
            rows_inserted,
            rows_rejected,
            validation_failure_count,
            started_at,
            finished_at
        FROM monitoring_runs
        ORDER BY started_at DESC, rows_rejected DESC
    """,
    "processing_time_trend": """
        SELECT
            target_key,
            pipeline_type,
            started_at,
            duration_ms,
            rows_processed,
            rows_inserted,
            status
        FROM monitoring_runs
        WHERE status IN ('success', 'skipped')
        ORDER BY target_key, started_at
    """,
}


def monitoring_query_names() -> tuple[str, ...]:
    return tuple(MONITORING_QUERIES)


def _append_json_row(spark: SparkSession, row: dict, schema, path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_dir = LAKEHOUSE.parent / "tools" / "spark-tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    tmp_json = tmp_dir / f"{path.name}_{row.get('run_id', uuid4().hex)}.json"
    with tmp_json.open("w", encoding="utf-8") as handle:
        json.dump(row, handle, default=str)
    try:
        frame = spark.read.schema(schema).json(str(tmp_json))
        if path.exists():
            frame.write.format("delta").mode("append").save(str(path))
        else:
            frame.write.format("delta").mode("overwrite").save(str(path))
    finally:
        tmp_json.unlink(missing_ok=True)


def _duration_ms(started_at: datetime, finished_at: datetime) -> float:
    return round((finished_at - started_at).total_seconds() * 1000, 3)


def record_validation_events(
    spark: SparkSession,
    *,
    run_id: str,
    target_key: str,
    pipeline_type: str,
    events: dict[str, int],
) -> None:
    recorded_at = utc_now()
    rows = [
        {
            "run_id": run_id,
            "target_key": target_key,
            "pipeline_type": pipeline_type,
            "rule_name": rule_name,
            "failure_count": int(count),
            "recorded_at": recorded_at,
        }
        for rule_name, count in events.items()
        if count
    ]
    if not rows:
        return
    for row in rows:
        _append_json_row(spark, row, VALIDATION_EVENTS_SCHEMA, VALIDATION_EVENTS)


def record_pipeline_run(
    spark: SparkSession,
    *,
    run_id: str,
    pipeline_type: str,
    target_key: str,
    target_name: str,
    schema_version: str,
    rows_processed: int,
    rows_inserted: int,
    rows_rejected: int,
    rows_duplicates_ignored: int = 0,
    validation_failure_count: int | None = None,
    started_at: datetime,
    finished_at: datetime | None = None,
    status: str,
    error_message: str | None = None,
    validation_events: dict[str, int] | None = None,
) -> dict[str, Any]:
    finished = finished_at or utc_now()
    failures = (
        validation_failure_count
        if validation_failure_count is not None
        else rows_rejected + rows_duplicates_ignored
    )
    row = {
        "run_id": run_id,
        "pipeline_type": pipeline_type,
        "target_key": target_key,
        "target_name": target_name,
        "schema_version": str(schema_version),
        "rows_processed": int(rows_processed),
        "rows_inserted": int(rows_inserted),
        "rows_rejected": int(rows_rejected),
        "rows_duplicates_ignored": int(rows_duplicates_ignored),
        "validation_failure_count": int(failures),
        "started_at": started_at,
        "finished_at": finished,
        "duration_ms": _duration_ms(started_at, finished),
        "status": status,
        "error_message": error_message,
    }
    _append_json_row(spark, row, PIPELINE_RUNS_SCHEMA, PIPELINE_RUNS)
    if validation_events:
        record_validation_events(
            spark,
            run_id=run_id,
            target_key=target_key,
            pipeline_type=pipeline_type,
            events=validation_events,
        )
    return row


def record_from_ingestion_summary(
    spark: SparkSession,
    summary: dict[str, Any],
    *,
    pipeline_type: str = "ingest",
    validation_rule_counts: dict[str, int] | None = None,
) -> dict[str, Any]:
    rows_inserted = int(summary.get("new_records", summary.get("rows_accepted", 0)))
    rows_duplicates = int(summary.get("duplicate_records_ignored", 0))
    rows_rejected = int(summary.get("rows_quarantined", 0))
    rows_warned = int(summary.get("rows_warned", 0))
    validation_events = dict(validation_rule_counts or {})
    if rows_rejected and "quarantined_records" not in validation_events:
        validation_events["quarantined_records"] = rows_rejected
    if rows_duplicates:
        validation_events["duplicate_records_ignored"] = rows_duplicates
    if rows_warned and rows_warned != rows_duplicates:
        validation_events["quality_warnings"] = rows_warned
    return record_pipeline_run(
        spark,
        run_id=summary["run_id"],
        pipeline_type=pipeline_type,
        target_key=summary["dataset_key"],
        target_name=summary["dataset_name"],
        schema_version=summary["schema_version"],
        rows_processed=int(summary.get("rows_read", 0)),
        rows_inserted=rows_inserted,
        rows_rejected=rows_rejected,
        rows_duplicates_ignored=rows_duplicates,
        validation_failure_count=rows_rejected + rows_warned,
        started_at=summary["started_at"],
        finished_at=summary["finished_at"],
        status=summary["status"],
        error_message=summary.get("error_message"),
        validation_events=validation_events,
    )


@contextmanager
def track_pipeline_run(
    spark: SparkSession,
    *,
    pipeline_type: str,
    target_key: str,
    target_name: str,
    schema_version: str,
    run_id: str | None = None,
) -> Iterator[dict[str, Any]]:
    context: dict[str, Any] = {
        "run_id": run_id or uuid4().hex,
        "started_at": utc_now(),
        "rows_processed": 0,
        "rows_inserted": 0,
        "rows_rejected": 0,
        "rows_duplicates_ignored": 0,
        "validation_failure_count": 0,
        "validation_events": {},
        "status": "success",
        "error_message": None,
    }
    started = time.perf_counter()
    try:
        yield context
    except Exception as exc:
        context["status"] = "failed"
        context["error_message"] = str(exc)[:2000]
        record_pipeline_run(
            spark,
            run_id=context["run_id"],
            pipeline_type=pipeline_type,
            target_key=target_key,
            target_name=target_name,
            schema_version=schema_version,
            rows_processed=int(context["rows_processed"]),
            rows_inserted=int(context["rows_inserted"]),
            rows_rejected=int(context["rows_rejected"]),
            rows_duplicates_ignored=int(context["rows_duplicates_ignored"]),
            validation_failure_count=int(context["validation_failure_count"]),
            started_at=context["started_at"],
            finished_at=utc_now(),
            status="failed",
            error_message=context["error_message"],
            validation_events=context["validation_events"],
        )
        raise
    else:
        finished_at = utc_now()
        record_pipeline_run(
            spark,
            run_id=context["run_id"],
            pipeline_type=pipeline_type,
            target_key=target_key,
            target_name=target_name,
            schema_version=schema_version,
            rows_processed=int(context["rows_processed"]),
            rows_inserted=int(context["rows_inserted"]),
            rows_rejected=int(context["rows_rejected"]),
            rows_duplicates_ignored=int(context["rows_duplicates_ignored"]),
            validation_failure_count=int(context["validation_failure_count"]),
            started_at=context["started_at"],
            finished_at=finished_at,
            status=str(context["status"]),
            error_message=context.get("error_message"),
            validation_events=context["validation_events"],
        )
        context["duration_ms"] = round((time.perf_counter() - started) * 1000, 3)


def register_monitoring_views(spark: SparkSession) -> None:
    parts: list = []
    pipeline_run_ids: set[str] = set()
    if PIPELINE_RUNS.exists():
        pipeline = (
            spark.read.format("delta")
            .load(str(PIPELINE_RUNS))
            .select(
                "run_id",
                "pipeline_type",
                "target_key",
                "target_name",
                "schema_version",
                "rows_processed",
                "rows_inserted",
                "rows_rejected",
                "rows_duplicates_ignored",
                "validation_failure_count",
                "started_at",
                "finished_at",
                "duration_ms",
                "status",
                "error_message",
            )
        )
        parts.append(pipeline)
        pipeline_run_ids = {
            row.run_id for row in pipeline.select("run_id").distinct().collect()
        }
    if METADATA_RUNS.exists():
        legacy = (
            spark.read.format("delta")
            .load(str(METADATA_RUNS))
            .select(
                "run_id",
                F.lit("ingest").alias("pipeline_type"),
                F.col("dataset_key").alias("target_key"),
                F.col("dataset_name").alias("target_name"),
                "schema_version",
                F.col("rows_read").alias("rows_processed"),
                F.col("rows_accepted").alias("rows_inserted"),
                F.col("rows_quarantined").alias("rows_rejected"),
                F.lit(0).cast("long").alias("rows_duplicates_ignored"),
                (F.col("rows_quarantined") + F.col("rows_warned")).alias(
                    "validation_failure_count"
                ),
                "started_at",
                "finished_at",
                (
                    (
                        F.unix_timestamp("finished_at")
                        - F.unix_timestamp("started_at")
                    )
                    * 1000.0
                ).alias("duration_ms"),
                "status",
                "error_message",
            )
        )
        if pipeline_run_ids:
            legacy = legacy.filter(~F.col("run_id").isin(*sorted(pipeline_run_ids)))
        parts.append(legacy)
    if not parts:
        spark.createDataFrame([], PIPELINE_RUNS_SCHEMA).createOrReplaceTempView(
            "monitoring_runs"
        )
        return
    unioned = parts[0]
    for part in parts[1:]:
        unioned = unioned.unionByName(part)
    unioned.createOrReplaceTempView("monitoring_runs")


def run_monitoring_query(spark: SparkSession, name: str):
    if name not in MONITORING_QUERIES:
        known = ", ".join(monitoring_query_names())
        raise KeyError(f"Unknown monitoring query {name!r}; known: {known}")
    register_monitoring_views(spark)
    return spark.sql(MONITORING_QUERIES[name])


def build_monitoring_report(spark: SparkSession) -> dict[str, Any]:
    register_monitoring_views(spark)
    report = {
        "status": "success",
        "generated_at_utc": datetime.utcnow().isoformat() + "Z",
        "queries": {},
    }
    for name in monitoring_query_names():
        rows = [
            row.asDict(recursive=True)
            for row in run_monitoring_query(spark, name).collect()
        ]
        report["queries"][name] = rows
    summary = spark.sql(
        """
        SELECT
            COUNT(*) AS execution_count,
            SUM(rows_processed) AS total_processed,
            SUM(rows_inserted) AS total_inserted,
            SUM(rows_rejected) AS total_rejected,
            SUM(validation_failure_count) AS total_validation_failures
        FROM monitoring_runs
        """
    ).collect()[0]
    report["summary"] = summary.asDict(recursive=True)
    return report
