"""Generate Week 3 incremental update files and manifest documentation."""

from __future__ import annotations

import csv
import json
import random
from datetime import datetime, timedelta, timezone
from typing import Any

from pyspark.sql import functions as F

from urban_data.config import UPDATES_DIR, UPDATE_MANIFEST, resolve_dataset
from urban_data.paths import DATASETS
from urban_data.schemas import TAXI_RAW_SCHEMA

NEW_TRIP_FRACTION = 0.07
DUPLICATE_TRIP_FRACTION = 0.015
WEATHER_UPDATE_HOURS = 168
AIR_UPDATE_HOURS = 168


def _taxi_source_paths() -> list:
    return [
        resolve_dataset(f"taxi_2024_{month}")["source_path"]
        for month in ("01", "02", "03")
    ]


def _generate_taxi_update(spark) -> dict[str, Any]:
    paths = _taxi_source_paths()
    total_rows = sum(
        resolve_dataset(f"taxi_2024_{month}")["expected_rows"]
        for month in ("01", "02", "03")
    )
    new_count = max(1, int(total_rows * NEW_TRIP_FRACTION))
    duplicate_count = max(1, int(total_rows * DUPLICATE_TRIP_FRACTION))

    combined_pickup = None
    max_pickup = None
    for path in paths:
        candidate = (
            spark.read.parquet(str(path))
            .agg(F.max("tpep_pickup_datetime"))
            .collect()[0][0]
        )
        if max_pickup is None or candidate > max_pickup:
            max_pickup = candidate

    base = spark.read.schema(TAXI_RAW_SCHEMA).parquet(str(paths[-1]))
    base_count = base.count()
    new_fraction = min(1.0, (new_count / base_count) * 1.05)
    dup_fraction = min(1.0, (duplicate_count / base_count) * 1.05)

    sampled = base.sample(withReplacement=False, fraction=new_fraction, seed=42).limit(
        new_count
    )
    min_in_sample = sampled.agg(F.min("tpep_pickup_datetime")).collect()[0][0]
    duration_seconds = (
        F.unix_timestamp("tpep_dropoff_datetime")
        - F.unix_timestamp("tpep_pickup_datetime")
    )
    relative_seconds = F.unix_timestamp("tpep_pickup_datetime") - F.lit(
        int(min_in_sample.timestamp())
    )
    shifted_pickup = F.to_timestamp(
        F.from_unixtime(
            F.lit(int(max_pickup.timestamp()) + 3600) + relative_seconds
        )
    )
    new_trips = (
        sampled.withColumn("_duration_seconds", duration_seconds)
        .withColumn("tpep_pickup_datetime", shifted_pickup)
        .withColumn(
            "tpep_dropoff_datetime",
            F.to_timestamp(
                F.from_unixtime(
                    F.unix_timestamp("tpep_pickup_datetime") + F.col("_duration_seconds")
                )
            ),
        )
        .drop("_duration_seconds")
    )
    duplicates = base.sample(withReplacement=False, fraction=dup_fraction, seed=24).limit(
        duplicate_count
    )
    update_path = UPDATES_DIR / "taxi_trips_update.parquet"
    UPDATES_DIR.mkdir(parents=True, exist_ok=True)
    new_trips.unionByName(duplicates).write.mode("overwrite").parquet(str(update_path))

    min_pickup = base.agg(F.min("tpep_pickup_datetime")).collect()[0][0]
    shift_seconds = int(max_pickup.timestamp()) - int(min_pickup.timestamp()) + 3600
    return {
        "dataset": "taxi_trips",
        "update_file": str(update_path.relative_to(DATASETS.parent)),
        "new_records": new_count,
        "duplicate_records": duplicate_count,
        "total_update_rows": new_count + duplicate_count,
        "schema_changes": [],
        "notes": (
            f"New trips start after {max_pickup.isoformat()} "
            f"(shifted by {shift_seconds} seconds from sampled baseline)."
        ),
    }


def _read_weather_template() -> dict[str, str]:
    with (DATASETS / "weather.csv").open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    return rows[-1]


def _generate_weather_update() -> dict[str, Any]:
    template = _read_weather_template()
    update_path = UPDATES_DIR / "weather_update.csv"
    UPDATES_DIR.mkdir(parents=True, exist_ok=True)
    fieldnames = list(template.keys()) + ["humidity"]
    start = datetime(2025, 1, 1, 0, 0, 0)
    rng = random.Random(15)

    with update_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for hour_idx in range(WEATHER_UPDATE_HOURS):
            ts = start + timedelta(hours=hour_idx)
            rhum = int(template["rhum"] or 50) + rng.randint(-3, 3)
            writer.writerow(
                {
                    "year": ts.year,
                    "month": ts.month,
                    "day": ts.day,
                    "hour": ts.hour,
                    "temp": round(float(template["temp"]) + rng.uniform(-2, 2), 1),
                    "temp_source": template["temp_source"],
                    "rhum": rhum,
                    "rhum_source": template["rhum_source"],
                    "prcp": template["prcp"],
                    "prcp_source": template["prcp_source"],
                    "snwd": template["snwd"],
                    "snwd_source": template["snwd_source"],
                    "wdir": template["wdir"],
                    "wdir_source": template["wdir_source"],
                    "wspd": round(float(template["wspd"] or 0) + rng.uniform(-1, 1), 1),
                    "wspd_source": template["wspd_source"],
                    "wpgt": template["wpgt"],
                    "wpgt_source": template["wpgt_source"],
                    "pres": round(float(template["pres"] or 1013) + rng.uniform(-1, 1), 1),
                    "pres_source": template["pres_source"],
                    "cldc": template["cldc"],
                    "cldc_source": template["cldc_source"],
                    "coco": template["coco"],
                    "coco_source": template["coco_source"],
                    "humidity": round(rng.uniform(20, 100), 1),
                }
            )

    return {
        "dataset": "weather_hourly",
        "update_file": str(update_path.relative_to(DATASETS.parent)),
        "new_records": WEATHER_UPDATE_HOURS,
        "duplicate_records": 0,
        "schema_changes": [
            "Added numeric column humidity (relative humidity percentage, 20-100)."
        ],
        "notes": "Hourly observations for 2025-01-01 through 2025-01-07 (168 hours).",
    }


def _read_air_template() -> dict[str, str]:
    with (DATASETS / "hourly_88101_2024.csv").open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["State Code"] == "36" and row["County Name"] in {
                "Bronx",
                "Kings",
                "New York",
                "Queens",
                "Richmond",
            }:
                return row
    raise RuntimeError("Could not find NYC air-quality template row")


def _generate_air_quality_update() -> dict[str, Any]:
    template = _read_air_template()
    update_path = UPDATES_DIR / "air_quality_update.csv"
    UPDATES_DIR.mkdir(parents=True, exist_ok=True)
    fieldnames = list(template.keys()) + ["aqi"]
    counties = [
        ("Bronx", "005"),
        ("Kings", "047"),
        ("New York", "061"),
        ("Queens", "081"),
        ("Richmond", "085"),
    ]
    sites = ["0001", "0002", "0003"]
    start = datetime(2025, 1, 1, 0, 0, 0)
    rng = random.Random(21)
    rows_written = 0

    with update_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for hour_idx in range(AIR_UPDATE_HOURS):
            ts = start + timedelta(hours=hour_idx)
            for county_name, county_code in counties:
                for site_num in sites:
                    pm25 = round(rng.uniform(8, 28), 1)
                    aqi = round(min(500.0, max(0.0, pm25 * 2.5 + 20)), 1)
                    writer.writerow(
                        {
                            "State Code": "36",
                            "County Code": county_code,
                            "Site Num": site_num,
                            "Parameter Code": "88101",
                            "POC": template["POC"],
                            "Latitude": template["Latitude"],
                            "Longitude": template["Longitude"],
                            "Datum": template["Datum"],
                            "Parameter Name": template["Parameter Name"],
                            "Date Local": ts.strftime("%Y-%m-%d"),
                            "Time Local": ts.strftime("%H:%M"),
                            "Date GMT": ts.strftime("%Y-%m-%d"),
                            "Time GMT": ts.strftime("%H:%M"),
                            "Sample Measurement": pm25,
                            "Units of Measure": template["Units of Measure"],
                            "MDL": template["MDL"],
                            "Uncertainty": template["Uncertainty"],
                            "Qualifier": template["Qualifier"],
                            "Method Type": template["Method Type"],
                            "Method Code": template["Method Code"],
                            "Method Name": template["Method Name"],
                            "State Name": "New York",
                            "County Name": county_name,
                            "Date of Last Change": "2025-01-01",
                            "aqi": aqi,
                        }
                    )
                    rows_written += 1

    return {
        "dataset": "air_quality_hourly_nyc",
        "update_file": str(update_path.relative_to(DATASETS.parent)),
        "new_records": rows_written,
        "duplicate_records": 0,
        "schema_changes": [
            "Added numeric column aqi (Air Quality Index, 0-500)."
        ],
        "notes": (
            "Station-level NYC PM2.5 observations for 168 UTC hours starting "
            "2025-01-01; hourly silver adds 168 aggregated rows."
        ),
    }


def generate_incremental_updates(spark=None) -> dict[str, Any]:
    """Create update files and manifest for Week 3 Task 1."""

    owns_spark = spark is None
    if owns_spark:
        from urban_data.spark_session import build_spark

        spark = build_spark("urban-data-week3-generate-updates")
    try:
        manifest = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "datasets": {
                "taxi_trips": _generate_taxi_update(spark),
                "weather_hourly": _generate_weather_update(),
                "air_quality_hourly_nyc": _generate_air_quality_update(),
            },
        }
    finally:
        if owns_spark:
            spark.stop()

    UPDATES_DIR.mkdir(parents=True, exist_ok=True)
    UPDATE_MANIFEST.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return manifest
