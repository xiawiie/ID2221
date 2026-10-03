"""Week 3 incremental update pipeline."""

from __future__ import annotations

from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from urban_data.config import incremental_update_keys, resolve_dataset
from urban_data.ingest import (
    _append_failed_run,
    _append_skipped_run,
    _finish_run,
    _lake_path,
    _prior_success,
    _read_parquet_with_schema,
    _skip_payload,
    _valid_location_ids,
    _write_quarantine,
)
from urban_data.integrate import integrate_taxi_trips_incremental
from urban_data.schemas import (
    AIR_QUALITY_RAW_SCHEMA,
    AIR_QUALITY_UPDATE_RAW_SCHEMA,
    TAXI_RAW_SCHEMA,
    WEATHER_RAW_SCHEMA,
    WEATHER_UPDATE_RAW_SCHEMA,
)
from urban_data.transforms import (
    aggregate_air_hourly,
    file_sha256,
    filter_air_quality_nyc,
    new_run_id,
    normalize_air_quality_columns,
    split_air_station_quality,
    split_taxi_quality,
    split_taxi_quality_incremental,
    split_weather_quality,
    utc_now,
    with_bronze_metadata,
)
from urban_data.validation import validate_schema_evolution_names


def _read_csv_evolved(
    spark: SparkSession,
    source_path,
    base_schema: StructType,
    evolved_schema: StructType,
):
    physical = spark.read.option("header", True).csv(str(source_path))
    validate_schema_evolution_names(physical.columns, base_schema, evolved_schema)
    return (
        spark.read.schema(evolved_schema)
        .option("header", True)
        .option("mode", "FAILFAST")
        .csv(str(source_path))
    )


def _append_bronze(df, bronze_path) -> None:
    writer = df.write.format("delta").mode("append").option("mergeSchema", "true")
    bronze_path.parent.mkdir(parents=True, exist_ok=True)
    if bronze_path.exists():
        writer.save(str(bronze_path))
        return
    df.write.format("delta").mode("overwrite").save(str(bronze_path))


def _merge_silver(
    spark: SparkSession,
    accepted,
    silver_path,
    merge_condition: str,
    *,
    partition_by: list[str] | None = None,
) -> int:
    if not silver_path.exists():
        writer = accepted.write.format("delta").mode("overwrite").option(
            "mergeSchema", "true"
        )
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(str(silver_path))
        return accepted.count()

    accepted.createOrReplaceTempView("incremental_silver_updates")
    silver_before = spark.read.format("delta").load(str(silver_path)).count()
    delta = DeltaTable.forPath(spark, str(silver_path))
    (
        delta.alias("target")
        .merge(
            accepted.alias("source"),
            merge_condition,
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
    silver_after = spark.read.format("delta").load(str(silver_path)).count()
    return silver_after - silver_before


def _count_existing_matches(accepted, silver_path, key: str) -> int:
    if not silver_path.exists():
        return 0
    existing = accepted.sparkSession.read.format("delta").load(str(silver_path)).select(
        key
    )
    return accepted.join(existing, on=key, how="inner").count()


def apply_taxi_incremental(
    spark: SparkSession, cfg: dict, *, force: bool = False
) -> dict[str, Any]:
    source_path = cfg["source_path"]
    if not source_path.exists():
        raise FileNotFoundError(source_path)

    source_sha256 = file_sha256(source_path)
    rel_source = cfg["path"].replace("\\", "/")
    if not force and _prior_success(
        spark, cfg["key"], source_sha256, cfg["schema_version"]
    ):
        run_id = _append_skipped_run(spark, cfg, source_sha256, rel_source, pipeline_type="incremental")
        return _skip_payload(cfg["key"], source_sha256, run_id)

    run_id = new_run_id()
    started_at = utc_now()
    bronze_path = _lake_path(cfg["bronze_path"])
    silver_path = _lake_path(cfg["silver_path"])
    quarantine_path = _lake_path(cfg["quarantine_path"])

    try:
        raw = _read_parquet_with_schema(spark, source_path, TAXI_RAW_SCHEMA)
        rows_read = raw.count()
        bronze = with_bronze_metadata(
            raw,
            source_file=rel_source,
            source_sha256=source_sha256,
            run_id=run_id,
            schema_version=cfg["schema_version"],
            ingested_at=started_at,
        )
        _append_bronze(bronze, bronze_path)

        accepted_outcome = split_taxi_quality_incremental(
            bronze, valid_location_ids=_valid_location_ids(spark)
        )
        accepted = accepted_outcome.accepted
        quarantined = accepted_outcome.quarantined
        validation_rule_counts = accepted_outcome.rule_counts
        duplicate_records = _count_existing_matches(accepted, silver_path, "trip_id")
        rows_inserted = _merge_silver(
            spark,
            accepted,
            silver_path,
            "target.trip_id = source.trip_id",
            partition_by=["source_file_month"],
        )
        rows_quarantined = quarantined.count()
        _write_quarantine(
            spark,
            quarantined,
            quarantine_path,
            rel_source,
            "invalid_trip_times_or_null_ts",
        )
        integration = integrate_taxi_trips_incremental(spark)

        summary = _finish_run(
            spark,
            cfg=cfg,
            run_id=run_id,
            started_at=started_at,
            source_file=rel_source,
            source_sha256=source_sha256,
            rows_read=rows_read,
            rows_accepted=rows_inserted,
            rows_quarantined=rows_quarantined,
            rows_warned=duplicate_records,
            bronze_path=bronze_path,
            silver_path=silver_path,
            quarantine_path=quarantine_path,
            pipeline_type="incremental",
            validation_rule_counts=validation_rule_counts,
        )
        summary.update(
            {
                "mode": "incremental",
                "new_records": rows_inserted,
                "duplicate_records_ignored": duplicate_records,
                "schema_changes": [],
                "gold_integration": integration,
            }
        )
        return summary
    except Exception as exc:
        _append_failed_run(
            spark,
            cfg,
            run_id=run_id,
            started_at=started_at,
            rel_source=rel_source,
            source_sha256=source_sha256,
            exc=exc,
            pipeline_type="incremental",
        )
        raise


def apply_weather_incremental(
    spark: SparkSession, cfg: dict, *, force: bool = False
) -> dict[str, Any]:
    source_path = cfg["source_path"]
    if not source_path.exists():
        raise FileNotFoundError(source_path)

    source_sha256 = file_sha256(source_path)
    rel_source = cfg["path"].replace("\\", "/")
    if not force and _prior_success(
        spark, cfg["key"], source_sha256, cfg["schema_version"]
    ):
        run_id = _append_skipped_run(spark, cfg, source_sha256, rel_source, pipeline_type="incremental")
        return _skip_payload(cfg["key"], source_sha256, run_id)

    run_id = new_run_id()
    started_at = utc_now()
    bronze_path = _lake_path(cfg["bronze_path"])
    silver_path = _lake_path(cfg["silver_path"])
    quarantine_path = _lake_path(cfg["quarantine_path"])

    try:
        raw = _read_csv_evolved(
            spark,
            source_path,
            WEATHER_RAW_SCHEMA,
            WEATHER_UPDATE_RAW_SCHEMA,
        )
        rows_read = raw.count()
        bronze = with_bronze_metadata(
            raw,
            source_file=rel_source,
            source_sha256=source_sha256,
            run_id=run_id,
            schema_version=cfg["schema_version"],
            ingested_at=started_at,
        )
        _append_bronze(bronze, bronze_path)

        weather_outcome = split_weather_quality(bronze)
        accepted = weather_outcome.accepted
        quarantined = weather_outcome.quarantined
        validation_rule_counts = weather_outcome.rule_counts
        duplicate_records = _count_existing_matches(
            accepted, silver_path, "observation_ts_local"
        )
        rows_inserted = _merge_silver(
            spark,
            accepted,
            silver_path,
            "target.observation_ts_local = source.observation_ts_local",
        )
        rows_quarantined = quarantined.count()
        _write_quarantine(
            spark,
            quarantined,
            quarantine_path,
            rel_source,
            cfg["kind"],
        )

        summary = _finish_run(
            spark,
            cfg=cfg,
            run_id=run_id,
            started_at=started_at,
            source_file=rel_source,
            source_sha256=source_sha256,
            rows_read=rows_read,
            rows_accepted=rows_inserted,
            rows_quarantined=rows_quarantined,
            rows_warned=duplicate_records,
            bronze_path=bronze_path,
            silver_path=silver_path,
            quarantine_path=quarantine_path,
            pipeline_type="incremental",
            validation_rule_counts=validation_rule_counts,
        )
        summary.update(
            {
                "mode": "incremental",
                "new_records": rows_inserted,
                "duplicate_records_ignored": duplicate_records,
                "schema_changes": [
                    "Added numeric column humidity (relative humidity percentage)."
                ],
            }
        )
        return summary
    except Exception as exc:
        _append_failed_run(
            spark,
            cfg,
            run_id=run_id,
            started_at=started_at,
            rel_source=rel_source,
            source_sha256=source_sha256,
            exc=exc,
            pipeline_type="incremental",
        )
        raise


def apply_air_quality_incremental(
    spark: SparkSession, cfg: dict, *, force: bool = False
) -> dict[str, Any]:
    source_path = cfg["source_path"]
    if not source_path.exists():
        raise FileNotFoundError(source_path)

    source_sha256 = file_sha256(source_path)
    rel_source = cfg["path"].replace("\\", "/")
    if not force and _prior_success(
        spark, cfg["key"], source_sha256, cfg["schema_version"]
    ):
        run_id = _append_skipped_run(spark, cfg, source_sha256, rel_source, pipeline_type="incremental")
        return _skip_payload(cfg["key"], source_sha256, run_id)

    run_id = new_run_id()
    started_at = utc_now()
    bronze_path = _lake_path(cfg["bronze_path"])
    silver_path = _lake_path(cfg["silver_path"])
    quarantine_path = _lake_path(cfg["quarantine_path"])

    try:
        raw = _read_csv_evolved(
            spark,
            source_path,
            AIR_QUALITY_RAW_SCHEMA,
            AIR_QUALITY_UPDATE_RAW_SCHEMA,
        )
        normalized = normalize_air_quality_columns(raw)
        filtered = filter_air_quality_nyc(normalized)
        rows_read = filtered.count()

        bronze = with_bronze_metadata(
            filtered,
            source_file=rel_source,
            source_sha256=source_sha256,
            run_id=run_id,
            schema_version=cfg["schema_version"],
            ingested_at=started_at,
        )
        _append_bronze(bronze, bronze_path)

        air_outcome = split_air_station_quality(bronze)
        stations = air_outcome.accepted
        quarantined = air_outcome.quarantined
        validation_rule_counts = air_outcome.rule_counts
        hourly = aggregate_air_hourly(stations)
        duplicate_records = _count_existing_matches(
            hourly, silver_path, "observation_ts_utc"
        )
        rows_inserted = _merge_silver(
            spark,
            hourly,
            silver_path,
            "target.observation_ts_utc = source.observation_ts_utc",
        )
        rows_quarantined = quarantined.count()
        _write_quarantine(
            spark,
            quarantined,
            quarantine_path,
            rel_source,
            "invalid_air_quality_measurement",
        )

        summary = _finish_run(
            spark,
            cfg=cfg,
            run_id=run_id,
            started_at=started_at,
            source_file=rel_source,
            source_sha256=source_sha256,
            rows_read=rows_read,
            rows_accepted=rows_inserted,
            rows_quarantined=rows_quarantined,
            rows_warned=duplicate_records,
            bronze_path=bronze_path,
            silver_path=silver_path,
            quarantine_path=quarantine_path,
            pipeline_type="incremental",
            validation_rule_counts=validation_rule_counts,
        )
        summary.update(
            {
                "mode": "incremental",
                "new_records": rows_inserted,
                "duplicate_records_ignored": duplicate_records,
                "schema_changes": ["Added numeric column aqi (Air Quality Index)."],
            }
        )
        return summary
    except Exception as exc:
        _append_failed_run(
            spark,
            cfg,
            run_id=run_id,
            started_at=started_at,
            rel_source=rel_source,
            source_sha256=source_sha256,
            exc=exc,
            pipeline_type="incremental",
        )
        raise


def apply_incremental_update(
    spark: SparkSession, cfg: dict, *, force: bool = False
) -> dict[str, Any]:
    kind = cfg["kind"]
    if kind == "taxi_trips":
        return apply_taxi_incremental(spark, cfg, force=force)
    if kind == "weather_hourly":
        return apply_weather_incremental(spark, cfg, force=force)
    if kind == "air_quality_hourly_nyc":
        return apply_air_quality_incremental(spark, cfg, force=force)
    raise ValueError(f"Unsupported incremental dataset kind {kind!r}")


def apply_incremental_updates(
    spark: SparkSession,
    dataset: str = "all",
    *,
    force: bool = False,
) -> list[dict[str, Any]]:
    keys = incremental_update_keys()
    if dataset != "all":
        if dataset not in keys:
            known = ", ".join(keys)
            raise KeyError(f"Unknown incremental dataset {dataset!r}; known: {known}")
        keys = [dataset]

    results = []
    for key in keys:
        cfg = resolve_dataset(key)
        results.append(apply_incremental_update(spark, cfg, force=force))
    return results
