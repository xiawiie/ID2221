"""Configurable validation rules for Week 3 Task 4."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from urban_data.config import load_validation_rules_config

MaskBuilder = Callable[[DataFrame, dict[str, Any]], Any]

AIR_CANDIDATE_KEY_COLUMNS = (
    "state_code",
    "county_code",
    "site_num",
    "parameter_code",
    "poc",
    "date_local",
    "time_local",
)


@dataclass(frozen=True)
class ValidationRule:
    name: str
    category: str
    description: str
    build_mask: MaskBuilder


def _null_or_invalid_trip_times(df: DataFrame, _context: dict[str, Any]):
    return (
        F.col("pickup_ts_local").isNull()
        | F.col("dropoff_ts_local").isNull()
        | (F.col("dropoff_ts_local") <= F.col("pickup_ts_local"))
    )


def _missing_zone_reference(df: DataFrame, context: dict[str, Any]):
    valid_ids = context.get("valid_location_ids")
    if not valid_ids:
        return F.lit(False)
    return F.col("pu_location_id").isNull() | ~F.col("pu_location_id").isin(
        *valid_ids
    ) | F.col("do_location_id").isNull() | ~F.col("do_location_id").isin(*valid_ids)


def _duplicate_trip_fingerprint(df: DataFrame, _context: dict[str, Any]):
    if "trip_id" not in df.columns:
        return F.lit(False)
    return (
        F.count("*").over(Window.partitionBy("trip_id")) > 1
    )


def _missing_location_id(df: DataFrame, _context: dict[str, Any]):
    return F.col("location_id").isNull()


def _duplicate_location_id(df: DataFrame, _context: dict[str, Any]):
    return F.count("*").over(Window.partitionBy("location_id")) > 1


def _incomplete_time_parts(df: DataFrame, _context: dict[str, Any]):
    return (
        F.col("year").isNull()
        | F.col("month").isNull()
        | F.col("day").isNull()
        | F.col("hour").isNull()
        | F.col("observation_ts_local").isNull()
    )


def _duplicate_hourly_observation(df: DataFrame, _context: dict[str, Any]):
    return (
        F.count("*").over(Window.partitionBy("observation_ts_local")) > 1
    )


def _null_observation_timestamp(df: DataFrame, _context: dict[str, Any]):
    return F.col("observation_ts_utc").isNull()


def _invalid_pm25_value(df: DataFrame, _context: dict[str, Any]):
    return F.col("pm25").isNull() | (F.col("pm25") < 0)


def _duplicate_candidate_key(df: DataFrame, _context: dict[str, Any]):
    return F.col("_candidate_key_count") > 1


RULE_REGISTRY: dict[str, ValidationRule] = {
    "null_or_invalid_trip_times": ValidationRule(
        "null_or_invalid_trip_times",
        "incomplete",
        "Pickup or dropoff timestamp is missing or dropoff is not after pickup.",
        _null_or_invalid_trip_times,
    ),
    "missing_zone_reference": ValidationRule(
        "missing_zone_reference",
        "missing_reference",
        "Pickup or dropoff LocationID is absent from the taxi zone lookup.",
        _missing_zone_reference,
    ),
    "duplicate_trip_fingerprint": ValidationRule(
        "duplicate_trip_fingerprint",
        "duplicate",
        "More than one row shares the same taxi trip fingerprint in the batch.",
        _duplicate_trip_fingerprint,
    ),
    "missing_location_id": ValidationRule(
        "missing_location_id",
        "incomplete",
        "Taxi zone lookup row is missing LocationID.",
        _missing_location_id,
    ),
    "duplicate_location_id": ValidationRule(
        "duplicate_location_id",
        "duplicate",
        "More than one taxi zone row shares the same LocationID.",
        _duplicate_location_id,
    ),
    "incomplete_time_parts": ValidationRule(
        "incomplete_time_parts",
        "incomplete",
        "Weather row is missing one or more date/time parts or derived timestamp.",
        _incomplete_time_parts,
    ),
    "duplicate_hourly_observation": ValidationRule(
        "duplicate_hourly_observation",
        "duplicate",
        "More than one weather row shares the same hourly observation timestamp.",
        _duplicate_hourly_observation,
    ),
    "null_observation_timestamp": ValidationRule(
        "null_observation_timestamp",
        "incomplete",
        "Air-quality row has an unparseable GMT timestamp.",
        _null_observation_timestamp,
    ),
    "invalid_pm25_value": ValidationRule(
        "invalid_pm25_value",
        "invalid_value",
        "PM2.5 measurement is NULL or negative.",
        _invalid_pm25_value,
    ),
    "duplicate_candidate_key": ValidationRule(
        "duplicate_candidate_key",
        "duplicate",
        "More than one air-quality row shares the same site/time candidate key.",
        _duplicate_candidate_key,
    ),
}


def rule_names() -> tuple[str, ...]:
    return tuple(RULE_REGISTRY)


def rule_categories() -> tuple[str, ...]:
    return ("duplicate", "invalid_value", "missing_reference", "incomplete", "schema")


def enabled_rules(dataset_kind: str) -> list[ValidationRule]:
    config = load_validation_rules_config()
    names = config["rule_sets"].get(dataset_kind, [])
    missing = [name for name in names if name not in RULE_REGISTRY]
    if missing:
        known = ", ".join(rule_names())
        raise KeyError(
            f"Unknown validation rule(s) {missing!r} for kind {dataset_kind!r}; known: {known}"
        )
    return [RULE_REGISTRY[name] for name in names]
