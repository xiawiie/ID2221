"""Generate Week 4 ML training datasets from integrated Delta tables."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from urban_data.analytics import register_integrated_view
from urban_data.paths import LAKEHOUSE, PROJECT_ROOT


ML_TRAINING_CONFIG = PROJECT_ROOT / "config" / "ml_training.yml"
ML_ROOT = LAKEHOUSE / "ml"

LAG_COLUMN_NAMES = {
    1: "demand_lag_1h",
    24: "demand_lag_24h",
    168: "demand_lag_168h",
}


def load_ml_training_config() -> dict[str, Any]:
    return yaml.safe_load(ML_TRAINING_CONFIG.read_text(encoding="utf-8"))


def ml_problem_names() -> tuple[str, ...]:
    cfg = load_ml_training_config()
    return tuple(cfg["problems"])


def resolve_ml_problem(name: str | None = None) -> dict[str, Any]:
    cfg = load_ml_training_config()
    key = name or cfg["default_problem"]
    problems = cfg["problems"]
    if key not in problems:
        known = ", ".join(sorted(problems))
        raise KeyError(f"Unknown ML problem {key!r}; known: {known}")
    problem = dict(problems[key])
    problem["key"] = key
    problem["schema_version"] = str(cfg["schema_version"])
    problem["feature_columns"] = (
        problem["categorical_features"] + problem["numeric_features"]
    )
    return problem


def assign_dataset_split(
    service_date_col: str,
    *,
    train_end_date: str,
    validation_end_date: str,
) -> F.Column:
    """Assign train / validation / test labels using chronological cutoffs."""
    service_date = F.to_date(F.col(service_date_col))
    return (
        F.when(service_date <= F.lit(train_end_date), F.lit("train"))
        .when(service_date <= F.lit(validation_end_date), F.lit("validation"))
        .otherwise(F.lit("test"))
    )


def _aggregate_hourly_zone_demand(gold: DataFrame) -> DataFrame:
    """Roll trip-level Gold rows up to pickup zone and local hour."""
    return (
        gold.filter(F.col("pickup_zone").isNotNull())
        .groupBy(
            "pickup_zone",
            "pickup_borough",
            "pickup_service_zone",
            "pickup_hour_local",
        )
        .agg(
            F.count("*").alias("trip_count"),
            F.first("weather_temp", ignorenulls=True).alias("weather_temp"),
            F.first("weather_precipitation", ignorenulls=True).alias(
                "weather_precipitation"
            ),
            F.first("weather_wind_speed", ignorenulls=True).alias(
                "weather_wind_speed"
            ),
            F.first("weather_relative_humidity", ignorenulls=True).alias(
                "weather_relative_humidity"
            ),
            F.first("air_pm25_mean", ignorenulls=True).alias("air_pm25_mean"),
        )
    )


def _add_temporal_features(frame: DataFrame) -> DataFrame:
    return (
        frame.withColumn("service_date", F.to_date("pickup_hour_local"))
        .withColumn("pickup_month", F.date_format("pickup_hour_local", "yyyy-MM"))
        .withColumn("hour_of_day", F.hour("pickup_hour_local").cast("int"))
        .withColumn("day_of_week", F.dayofweek("pickup_hour_local").cast("int"))
        .withColumn(
            "is_weekend",
            F.dayofweek("pickup_hour_local").isin(1, 7).cast("int"),
        )
    )


def _add_lag_features(frame: DataFrame, lag_hours: list[int]) -> DataFrame:
    window = Window.partitionBy("pickup_zone").orderBy("pickup_hour_local")
    enriched = frame
    for lag in lag_hours:
        column_name = LAG_COLUMN_NAMES.get(lag, f"demand_lag_{lag}h")
        enriched = enriched.withColumn(
            column_name,
            F.lag("trip_count", lag).over(window),
        )
    return enriched


def build_hourly_zone_demand_frame(
    spark: SparkSession,
    problem: dict[str, Any],
) -> DataFrame:
    """Build the full supervised dataset before chronological splitting."""
    register_integrated_view(spark)
    gold = spark.table("integrated_taxi_trips")
    aggregated = _aggregate_hourly_zone_demand(gold)
    with_time = _add_temporal_features(aggregated)
    with_lags = _add_lag_features(with_time, list(problem["lag_hours"]))

    if problem.get("require_complete_lags", True):
        lag_columns = [
            LAG_COLUMN_NAMES.get(lag, f"demand_lag_{lag}h")
            for lag in problem["lag_hours"]
        ]
        completeness = None
        for column in lag_columns:
            condition = F.col(column).isNotNull()
            completeness = condition if completeness is None else completeness & condition
        with_lags = with_lags.filter(completeness)

    split_cfg = problem["splits"]
    return with_lags.withColumn(
        "dataset_split",
        assign_dataset_split(
            "service_date",
            train_end_date=split_cfg["train_end_date"],
            validation_end_date=split_cfg["validation_end_date"],
        ),
    )


def _dataset_output_dir(problem: dict[str, Any]) -> Path:
    return ML_ROOT / problem["output"]["dataset_key"]


def _split_counts(frame: DataFrame) -> dict[str, int]:
    return {
        row.dataset_split: int(row["count"])
        for row in frame.groupBy("dataset_split").count().collect()
    }


def _write_split_tables(frame: DataFrame, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for split_name in ("train", "validation", "test"):
        split_frame = frame.filter(F.col("dataset_split") == split_name)
        split_path = output_dir / split_name
        split_frame.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(str(split_path))


def build_training_dataset(
    spark: SparkSession,
    problem_name: str | None = None,
) -> dict[str, Any]:
    """Generate Task 1 training datasets and persist them under lakehouse/ml/."""
    problem = resolve_ml_problem(problem_name)
    if problem["key"] != "hourly_taxi_demand_by_zone":
        raise NotImplementedError(
            f"Problem {problem['key']!r} is not implemented yet."
        )

    frame = build_hourly_zone_demand_frame(spark, problem)
    output_dir = _dataset_output_dir(problem)
    full_path = output_dir / "full"
    frame.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).save(str(full_path))
    _write_split_tables(frame, output_dir)

    split_counts = _split_counts(frame)
    summary = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "problem": problem["key"],
        "schema_version": problem["schema_version"],
        "target_column": problem["target_column"],
        "feature_columns": problem["feature_columns"],
        "grain": problem["grain"],
        "splits": problem["splits"],
        "row_counts": {
            "full": int(frame.count()),
            **split_counts,
        },
        "output_paths": {
            "full": str(full_path),
            "train": str(output_dir / "train"),
            "validation": str(output_dir / "validation"),
            "test": str(output_dir / "test"),
        },
        "assumptions": [
            "Demand is measured as accepted trip count grouped by pickup zone and local hour.",
            "Only hours with at least one accepted trip are materialized; zero-demand hours are omitted.",
            "Weather and PM2.5 values are city-wide hourly context attached during Week 1 integration.",
            "Rows with incomplete lag history are excluded when require_complete_lags is true.",
            "Chronological split dates prevent future leakage into training.",
        ],
    }

    artifact_path = PROJECT_ROOT / "artifacts" / "week4_training_dataset.json"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text(
        json.dumps(summary, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    summary["artifact_path"] = str(artifact_path)
    return summary
