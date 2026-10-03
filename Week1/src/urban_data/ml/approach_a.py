"""Approach A: build the Week 4 training dataset directly from raw source files."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from urban_data.ml.training_dataset import (
    LAG_COLUMN_NAMES,
    assign_dataset_split,
)
from urban_data.ml.training_dataset import resolve_ml_problem
from urban_data.paths import DATASETS, PROJECT_ROOT
from urban_data.schemas import (
    AIR_QUALITY_RAW_SCHEMA,
    TAXI_RAW_SCHEMA,
    TAXI_ZONES_RAW_SCHEMA,
    WEATHER_RAW_SCHEMA,
)


RAW_TAXI_FILES = (
    DATASETS / "yellow_tripdata_2024-01.parquet",
    DATASETS / "yellow_tripdata_2024-02.parquet",
    DATASETS / "yellow_tripdata_2024-03.parquet",
)
RAW_ZONE_FILE = DATASETS / "taxi_zone_lookup.csv"
RAW_WEATHER_FILE = DATASETS / "weather.csv"
RAW_AIR_FILE = DATASETS / "hourly_88101_2024.csv"

APPROACH_A_OUTPUT = PROJECT_ROOT / "lakehouse" / "ml" / "hourly_zone_demand_approach_a"

APPROACH_A_STEPS = (
    "Load three yellow taxi Parquet files from datasets/",
    "Load taxi zone lookup CSV",
    "Load city weather CSV and derive hourly timestamps",
    "Load national PM2.5 CSV, filter NYC counties, aggregate to UTC hours",
    "Rename taxi columns and derive trip duration and pickup hour fields",
    "Apply raw-path cleaning rules (times, distance, zone references)",
    "Join pickup and dropoff zones, weather, and air quality",
    "Aggregate accepted trips to pickup zone and local hour",
    "Derive temporal features and lag demand columns",
    "Assign chronological train, validation, and test splits",
)


def approach_a_source_line_count() -> int:
    path = Path(__file__)
    return sum(1 for line in path.read_text(encoding="utf-8").splitlines() if line.strip())


def _load_raw_taxi_trips(spark: SparkSession) -> DataFrame:
    missing = [str(path) for path in RAW_TAXI_FILES if not path.exists()]
    if missing:
        raise FileNotFoundError(
            "Approach A requires raw taxi Parquet files: " + ", ".join(missing)
        )
    frames = [
        spark.read.schema(TAXI_RAW_SCHEMA).parquet(str(path))
        for path in RAW_TAXI_FILES
    ]
    combined = frames[0]
    for frame in frames[1:]:
        combined = combined.unionByName(frame, allowMissingColumns=True)
    return combined


def _load_raw_zones(spark: SparkSession) -> DataFrame:
    if not RAW_ZONE_FILE.exists():
        raise FileNotFoundError(f"Missing zone lookup file: {RAW_ZONE_FILE}")
    return (
        spark.read.schema(TAXI_ZONES_RAW_SCHEMA)
        .option("header", True)
        .csv(str(RAW_ZONE_FILE))
        .select(
            F.col("LocationID").alias("location_id"),
            F.col("Borough").alias("borough"),
            F.col("Zone").alias("zone"),
            F.col("service_zone"),
        )
    )


def _load_raw_weather(spark: SparkSession) -> DataFrame:
    if not RAW_WEATHER_FILE.exists():
        raise FileNotFoundError(f"Missing weather file: {RAW_WEATHER_FILE}")
    weather = spark.read.schema(WEATHER_RAW_SCHEMA).option("header", True).csv(
        str(RAW_WEATHER_FILE)
    )
    return weather.withColumn(
        "observation_ts_local",
        F.make_timestamp(
            F.col("year").cast("int"),
            F.col("month").cast("int"),
            F.col("day").cast("int"),
            F.col("hour").cast("int"),
            F.lit(0),
            F.lit(0),
        ),
    ).select(
        "observation_ts_local",
        F.col("temp").alias("weather_temp"),
        F.col("prcp").alias("weather_precipitation"),
        F.col("wspd").alias("weather_wind_speed"),
        F.col("rhum").alias("weather_relative_humidity"),
    )


def _load_raw_air_hourly(spark: SparkSession) -> DataFrame:
    if not RAW_AIR_FILE.exists():
        raise FileNotFoundError(f"Missing air quality file: {RAW_AIR_FILE}")
    air = (
        spark.read.schema(AIR_QUALITY_RAW_SCHEMA)
        .option("header", True)
        .csv(str(RAW_AIR_FILE))
        .withColumnRenamed("State Code", "state_code")
        .withColumnRenamed("County Name", "county_name")
        .withColumnRenamed("Date GMT", "date_gmt")
        .withColumnRenamed("Time GMT", "time_gmt")
        .withColumnRenamed("Sample Measurement", "pm25")
        .withColumnRenamed("Parameter Code", "parameter_code")
        .filter(
            (F.col("state_code") == "36")
            & (
                F.col("county_name").isin(
                    "Bronx", "Kings", "New York", "Queens", "Richmond"
                )
            )
            & (F.col("parameter_code") == "88101")
        )
        .withColumn(
            "observation_ts_utc",
            F.to_timestamp(F.concat_ws(" ", F.col("date_gmt"), F.col("time_gmt"))),
        )
    )
    return air.groupBy("observation_ts_utc").agg(
        F.avg("pm25").alias("air_pm25_mean"),
    )


def _prepare_taxi_trips(taxi: DataFrame, zones: DataFrame) -> DataFrame:
    valid_ids = zones.select(F.col("location_id").cast("int").alias("location_id"))
    renamed = taxi.select(
        F.col("tpep_pickup_datetime").alias("pickup_ts_local"),
        F.col("tpep_dropoff_datetime").alias("dropoff_ts_local"),
        F.col("trip_distance"),
        F.col("PULocationID").alias("pu_location_id"),
        F.col("DOLocationID").alias("do_location_id"),
    )
    cleaned = renamed.filter(
        F.col("pickup_ts_local").isNotNull()
        & F.col("dropoff_ts_local").isNotNull()
        & (F.col("dropoff_ts_local") > F.col("pickup_ts_local"))
        & F.col("trip_distance").isNotNull()
        & (F.col("trip_distance") > 0)
    )
    cleaned = cleaned.join(
        valid_ids.alias("pu"),
        cleaned.pu_location_id == F.col("pu.location_id"),
        "inner",
    ).drop("location_id")
    cleaned = cleaned.join(
        valid_ids.alias("do"),
        cleaned.do_location_id == F.col("do.location_id"),
        "inner",
    ).drop("location_id")
    return cleaned.withColumn(
        "pickup_hour_local", F.date_trunc("hour", F.col("pickup_ts_local"))
    ).withColumn(
        "pickup_ts_utc",
        F.to_utc_timestamp(F.col("pickup_ts_local"), "America/New_York"),
    ).withColumn(
        "pickup_hour_utc", F.date_trunc("hour", F.col("pickup_ts_utc"))
    )


def _join_dimensions(
    trips: DataFrame,
    zones: DataFrame,
    weather: DataFrame,
    air: DataFrame,
) -> DataFrame:
    pickup_zones = zones.select(
        F.col("location_id").alias("pu_location_id"),
        F.col("zone").alias("pickup_zone"),
        F.col("borough").alias("pickup_borough"),
        F.col("service_zone").alias("pickup_service_zone"),
    )
    enriched = trips.join(pickup_zones, "pu_location_id", "inner")
    enriched = enriched.join(
        F.broadcast(weather),
        enriched.pickup_hour_local == weather.observation_ts_local,
        "left",
    )
    return enriched.join(
        F.broadcast(air),
        enriched.pickup_hour_utc == air.observation_ts_utc,
        "left",
    )


def _aggregate_hourly_zone_demand(trips: DataFrame) -> DataFrame:
    return (
        trips.groupBy(
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


def build_approach_a_training_frame(
    spark: SparkSession,
    problem: dict[str, Any],
) -> DataFrame:
    """Build the Task 1 training dataset without using integrated Delta tables."""
    taxi = _load_raw_taxi_trips(spark)
    zones = _load_raw_zones(spark)
    weather = _load_raw_weather(spark)
    air = _load_raw_air_hourly(spark)
    trips = _prepare_taxi_trips(taxi, zones)
    enriched = _join_dimensions(trips, zones, weather, air)
    aggregated = _aggregate_hourly_zone_demand(enriched)
    with_time = _add_temporal_features(aggregated)
    with_lags = _add_lag_features(with_time, list(problem["lag_hours"]))

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


def build_approach_a_training_dataset(
    spark: SparkSession,
    problem_name: str | None = None,
) -> dict[str, Any]:
    """Materialize Approach A output and return timing metadata."""
    problem = resolve_ml_problem(problem_name)
    started = time.perf_counter()
    frame = build_approach_a_training_frame(spark, problem)
    row_count = int(frame.count())
    elapsed_ms = (time.perf_counter() - started) * 1000.0

    output_dir = APPROACH_A_OUTPUT
    output_dir.mkdir(parents=True, exist_ok=True)
    full_path = output_dir / "full"
    frame.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).save(str(full_path))

    split_counts = {
        row.dataset_split: int(row["count"])
        for row in frame.groupBy("dataset_split").count().collect()
    }
    return {
        "approach": "A",
        "description": "Built directly from raw TLC, zone, weather, and PM2.5 files",
        "duration_ms": elapsed_ms,
        "implementation_steps": list(APPROACH_A_STEPS),
        "source_line_count": approach_a_source_line_count(),
        "row_counts": {"full": row_count, **split_counts},
        "output_path": str(full_path),
        "raw_inputs": [
            str(path) for path in (*RAW_TAXI_FILES, RAW_ZONE_FILE, RAW_WEATHER_FILE, RAW_AIR_FILE)
        ],
    }
