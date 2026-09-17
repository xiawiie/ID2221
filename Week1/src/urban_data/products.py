"""Materialized Week 2 analytical Delta products."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import yaml
from pyspark.sql import functions as F

from urban_data.analytics import register_integrated_view
from urban_data.paths import LAKEHOUSE, PROJECT_ROOT


PRODUCT_ROOT = LAKEHOUSE / "products"
ANALYTICS_CONFIG = PROJECT_ROOT / "config" / "analytics.yml"
SOURCE_TABLE = "gold.integrated_taxi_trips"

PRODUCT_SQL = {
    "daily_mobility_summary": """
        SELECT
            pickup_month,
            TO_DATE(pickup_ts_local) AS service_date,
            pickup_borough,
            COUNT(*) AS trip_count,
            ROUND(AVG(trip_distance), 6) AS average_trip_distance,
            ROUND(AVG(trip_duration_minutes), 6) AS average_trip_duration_minutes,
            ROUND(AVG(total_amount), 6) AS average_total_amount
        FROM integrated_taxi_trips
        GROUP BY pickup_month, TO_DATE(pickup_ts_local), pickup_borough
    """,
    "taxi_zone_monthly_statistics": """
        SELECT
            pickup_month,
            pickup_zone,
            pickup_borough,
            COUNT(*) AS trip_count,
            ROUND(AVG(trip_distance), 6) AS average_trip_distance,
            ROUND(AVG(trip_duration_minutes), 6) AS average_trip_duration_minutes,
            ROUND(AVG(fare_amount), 6) AS average_fare_amount
        FROM integrated_taxi_trips
        GROUP BY pickup_month, pickup_zone, pickup_borough
    """,
    "weather_impact_summary": """
        SELECT
            pickup_month,
            COALESCE(CAST(weather_condition_code AS STRING), 'unknown')
                AS weather_condition,
            COUNT(*) AS trip_count,
            ROUND(AVG(weather_temp), 6) AS average_temperature,
            ROUND(AVG(weather_precipitation), 6) AS average_precipitation,
            ROUND(AVG(trip_distance), 6) AS average_trip_distance,
            ROUND(AVG(trip_duration_minutes), 6) AS average_trip_duration_minutes
        FROM integrated_taxi_trips
        GROUP BY
            pickup_month,
            COALESCE(CAST(weather_condition_code AS STRING), 'unknown')
    """,
    "air_quality_impact_summary": """
        WITH hourly_demand AS (
            SELECT
                pickup_month,
                TO_DATE(pickup_hour_utc) AS service_date,
                pickup_hour_utc,
                MAX(air_pm25_mean) AS air_pm25_mean,
                COUNT(*) AS trip_count,
                AVG(trip_distance) AS average_trip_distance
            FROM integrated_taxi_trips
            WHERE air_quality_match_status = 'matched'
            GROUP BY pickup_month, TO_DATE(pickup_hour_utc), pickup_hour_utc
        )
        SELECT
            pickup_month,
            service_date,
            COUNT(*) AS observed_hours,
            SUM(trip_count) AS trip_count,
            ROUND(AVG(air_pm25_mean), 6) AS average_pm25,
            ROUND(CORR(trip_count, air_pm25_mean), 6) AS demand_pm25_correlation,
            ROUND(AVG(average_trip_distance), 6) AS average_trip_distance
        FROM hourly_demand
        GROUP BY pickup_month, service_date
    """,
}


def product_names() -> tuple[str, ...]:
    return tuple(PRODUCT_SQL)


def _load_config() -> dict:
    with ANALYTICS_CONFIG.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def _existing_created_at(spark: Any, path: Path) -> datetime:
    if not path.exists():
        return datetime.now(timezone.utc)
    row = (
        spark.read.format("delta")
        .load(str(path))
        .agg(F.min("created_at_utc").alias("created_at_utc"))
        .collect()[0]
    )
    return row["created_at_utc"] or datetime.now(timezone.utc)


def _storage_stats(spark: Any, path: Path) -> dict:
    row = spark.sql(
        f"DESCRIBE DETAIL delta.`{path.resolve().as_posix()}`"
    ).collect()[0]
    return {
        "data_file_count": int(row["numFiles"]),
        "data_bytes": int(row["sizeInBytes"]),
    }


def materialize_product(spark: Any, name: str) -> dict:
    """Build one Delta product and preserve its original creation timestamp."""

    if name not in PRODUCT_SQL:
        known = ", ".join(product_names())
        raise KeyError(f"Unknown analytical product {name!r}; known: {known}")

    config = _load_config()
    settings = config["products"][name]
    output_path = PRODUCT_ROOT / name
    created_at = _existing_created_at(spark, output_path)
    refreshed_at = datetime.now(timezone.utc)
    run_id = str(uuid4())

    register_integrated_view(spark)
    product = (
        spark.sql(PRODUCT_SQL[name])
        .withColumn("product_name", F.lit(name))
        .withColumn("source_table", F.lit(SOURCE_TABLE))
        .withColumn("created_at_utc", F.lit(created_at).cast("timestamp"))
        .withColumn("refreshed_at_utc", F.lit(refreshed_at).cast("timestamp"))
        .withColumn("schema_version", F.lit(str(config["schema_version"])))
        .withColumn("run_id", F.lit(run_id))
    )

    writer = product.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    )
    partition_columns = settings.get("partition_columns", [])
    if partition_columns:
        writer = writer.partitionBy(*partition_columns)
    writer.save(str(output_path))

    return {
        "product_name": name,
        "output_path": str(output_path),
        "row_count": spark.read.format("delta").load(str(output_path)).count(),
        "partition_columns": partition_columns,
        "source_table": SOURCE_TABLE,
        "created_at_utc": created_at.isoformat(),
        "refreshed_at_utc": refreshed_at.isoformat(),
        "schema_version": str(config["schema_version"]),
        "run_id": run_id,
        **_storage_stats(spark, output_path),
    }


def materialize_products(spark: Any, name: str = "all") -> list[dict]:
    """Materialize all products or one selected product."""

    names = product_names() if name == "all" else (name,)
    return [materialize_product(spark, product_name) for product_name in names]
