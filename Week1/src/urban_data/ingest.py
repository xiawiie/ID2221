from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from urban_data.paths import LAKEHOUSE, METADATA_RUNS
from urban_data.schemas import (
    AIR_QUALITY_RAW_SCHEMA,
    INGESTION_RUNS_SCHEMA,
    TAXI_RAW_SCHEMA,
    TAXI_ZONES_RAW_SCHEMA,
    WEATHER_RAW_SCHEMA,
)
from urban_data.transforms import (
    aggregate_air_hourly,
    file_sha256,
    filter_air_quality_nyc,
    new_run_id,
    split_air_station_quality,
    split_taxi_quality,
    split_weather_quality,
    split_zone_quality,
    utc_now,
    with_bronze_metadata,
)


def _lake_path(relative: str):
    return LAKEHOUSE / relative


def _validate_columns(df, schema) -> None:
    expected = [field.name for field in schema.fields]
    actual = df.columns
    if actual != expected:
        raise ValueError(f"Schema mismatch: expected {expected}, got {actual}")


def _prior_success(
    spark: SparkSession, dataset_key: str, source_sha256: str, schema_version: str
) -> bool:
    if not METADATA_RUNS.exists():
        return False
    runs = spark.read.format("delta").load(str(METADATA_RUNS))
    hit = runs.filter(
        (F.col("dataset_key") == dataset_key)
        & (F.col("source_sha256") == source_sha256)
        & (F.col("schema_version") == schema_version)
        & (F.col("status") == "success")
    )
    return hit.limit(1).count() > 0


def _append_run(spark: SparkSession, row: dict) -> None:
    METADATA_RUNS.parent.mkdir(parents=True, exist_ok=True)
    frame = spark.createDataFrame([row], schema=INGESTION_RUNS_SCHEMA)
    if METADATA_RUNS.exists():
        frame.write.format("delta").mode("append").save(str(METADATA_RUNS))
    else:
        frame.write.format("delta").mode("overwrite").save(str(METADATA_RUNS))


def _skip_payload(dataset_key: str, source_sha256: str) -> dict:
    return {
        "status": "skipped",
        "dataset_key": dataset_key,
        "source_sha256": source_sha256,
        "reason": "already ingested with same hash and schema version",
    }


def _write_delta(df, path, *, partition_by: list[str] | None = None, mode: str = "overwrite"):
    spark = df.sparkSession
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    writer = df.write.format("delta").mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(str(path))


def _finish_run(
    spark: SparkSession,
    *,
    cfg: dict,
    run_id: str,
    started_at,
    source_file: str,
    source_sha256: str,
    rows_read: int,
    rows_accepted: int,
    rows_quarantined: int,
    rows_warned: int,
    bronze_path,
    silver_path,
    quarantine_path,
) -> dict:
    finished_at = utc_now()
    summary = {
        "run_id": run_id,
        "dataset_key": cfg["key"],
        "dataset_name": cfg["logical_name"],
        "source_file": source_file,
        "source_sha256": source_sha256,
        "schema_version": cfg["schema_version"],
        "rows_read": rows_read,
        "rows_accepted": rows_accepted,
        "rows_quarantined": rows_quarantined,
        "rows_warned": rows_warned,
        "started_at": started_at,
        "finished_at": finished_at,
        "status": "success",
        "error_message": None,
    }
    _append_run(spark, summary)
    summary["bronze_path"] = str(bronze_path)
    summary["silver_path"] = str(silver_path)
    summary["quarantine_path"] = str(quarantine_path)
    return summary


def ingest_taxi_dataset(spark: SparkSession, cfg: dict, *, force: bool = False) -> dict:
    source_path = cfg["source_path"]
    if not source_path.is_file():
        raise FileNotFoundError(source_path)

    source_sha256 = file_sha256(source_path)
    rel_source = cfg["path"].replace("\\", "/")
    if not force and _prior_success(
        spark, cfg["key"], source_sha256, cfg["schema_version"]
    ):
        return _skip_payload(cfg["key"], source_sha256)

    run_id = new_run_id()
    started_at = utc_now()
    bronze_path = _lake_path(cfg["bronze_path"])
    silver_path = _lake_path(cfg["silver_path"])
    quarantine_path = _lake_path(cfg["quarantine_path"])

    raw = spark.read.schema(TAXI_RAW_SCHEMA).parquet(str(source_path))
    _validate_columns(raw, TAXI_RAW_SCHEMA)
    rows_read = raw.count()
    if rows_read != cfg["expected_rows"]:
        raise ValueError(
            f"Row count mismatch for {rel_source}: expected {cfg['expected_rows']}, got {rows_read}"
        )

    bronze = with_bronze_metadata(
        raw,
        source_file=rel_source,
        source_sha256=source_sha256,
        run_id=run_id,
        schema_version=cfg["schema_version"],
        ingested_at=started_at,
    )
    bronze.write.format("delta").mode("overwrite").save(str(bronze_path))

    accepted, quarantined = split_taxi_quality(bronze, cfg["file_month"])
    rows_accepted = accepted.count()
    rows_quarantined = quarantined.count()
    if rows_read != rows_accepted + rows_quarantined:
        raise ValueError(
            "Quality split mismatch: "
            f"read={rows_read}, accepted={rows_accepted}, quarantined={rows_quarantined}"
        )

    rows_warned = accepted.filter(F.col("quality_flags").isNotNull()).count()
    _write_delta(accepted, silver_path, partition_by=["pickup_month"], mode="overwrite")
    quarantined = quarantined.withColumn(
        "quarantine_reason", F.lit("invalid_trip_times_or_null_ts")
    )
    _write_delta(quarantined, quarantine_path, partition_by=["pickup_month"], mode="overwrite")

    return _finish_run(
        spark,
        cfg=cfg,
        run_id=run_id,
        started_at=started_at,
        source_file=rel_source,
        source_sha256=source_sha256,
        rows_read=rows_read,
        rows_accepted=rows_accepted,
        rows_quarantined=rows_quarantined,
        rows_warned=rows_warned,
        bronze_path=bronze_path,
        silver_path=silver_path,
        quarantine_path=quarantine_path,
    )


def ingest_csv_dataset(
    spark: SparkSession,
    cfg: dict,
    *,
    schema,
    transform_split,
    force: bool = False,
    prefilter=None,
    expected_rows_key: str = "expected_rows",
    silver_partition_by: list[str] | None = None,
) -> dict:
    source_path = cfg["source_path"]
    if not source_path.is_file():
        raise FileNotFoundError(source_path)

    source_sha256 = file_sha256(source_path)
    rel_source = cfg["path"].replace("\\", "/")
    if not force and _prior_success(
        spark, cfg["key"], source_sha256, cfg["schema_version"]
    ):
        return _skip_payload(cfg["key"], source_sha256)

    run_id = new_run_id()
    started_at = utc_now()
    bronze_path = _lake_path(cfg["bronze_path"])
    silver_path = _lake_path(cfg["silver_path"])
    quarantine_path = _lake_path(cfg["quarantine_path"])

    raw = (
        spark.read.schema(schema)
        .option("header", True)
        .option("mode", "FAILFAST")
        .csv(str(source_path))
    )
    _validate_columns(raw, schema)
    if prefilter is not None:
        raw = prefilter(raw)
    rows_read = raw.count()
    expected = cfg[expected_rows_key]
    if rows_read != expected:
        raise ValueError(
            f"Row count mismatch for {rel_source}: expected {expected}, got {rows_read}"
        )

    bronze = with_bronze_metadata(
        raw,
        source_file=rel_source,
        source_sha256=source_sha256,
        run_id=run_id,
        schema_version=cfg["schema_version"],
        ingested_at=started_at,
    )
    bronze.write.format("delta").mode("overwrite").save(str(bronze_path))

    accepted, quarantined = transform_split(bronze)
    rows_accepted = accepted.count()
    rows_quarantined = quarantined.count()
    if cfg["kind"] != "air_quality_hourly_nyc" and rows_read != rows_accepted + rows_quarantined:
        raise ValueError(
            "Quality split mismatch: "
            f"read={rows_read}, accepted={rows_accepted}, quarantined={rows_quarantined}"
        )

    rows_warned = 0
    if "quality_flags" in accepted.columns:
        rows_warned = accepted.filter(F.col("quality_flags").isNotNull()).count()
    elif cfg["kind"] == "taxi_zones":
        rows_warned = accepted.filter(
            F.col("borough").isNull()
            | F.col("zone").isNull()
            | F.col("service_zone").isNull()
        ).count()

    _write_delta(
        accepted,
        silver_path,
        partition_by=silver_partition_by,
        mode="overwrite",
    )
    if rows_quarantined:
        quarantined = quarantined.withColumn("quarantine_reason", F.lit(cfg["kind"]))
        _write_delta(quarantined, quarantine_path, mode="overwrite")
    elif quarantine_path.exists():
        pass
    else:
        spark.createDataFrame([], quarantined.schema).write.format("delta").mode(
            "overwrite"
        ).save(str(quarantine_path))

    return _finish_run(
        spark,
        cfg=cfg,
        run_id=run_id,
        started_at=started_at,
        source_file=rel_source,
        source_sha256=source_sha256,
        rows_read=rows_read,
        rows_accepted=rows_accepted,
        rows_quarantined=rows_quarantined,
        rows_warned=rows_warned,
        bronze_path=bronze_path,
        silver_path=silver_path,
        quarantine_path=quarantine_path,
    )


def ingest_zones_dataset(spark: SparkSession, cfg: dict, *, force: bool = False) -> dict:
    return ingest_csv_dataset(
        spark,
        cfg,
        schema=TAXI_ZONES_RAW_SCHEMA,
        transform_split=split_zone_quality,
        force=force,
    )


def ingest_weather_dataset(spark: SparkSession, cfg: dict, *, force: bool = False) -> dict:
    return ingest_csv_dataset(
        spark,
        cfg,
        schema=WEATHER_RAW_SCHEMA,
        transform_split=split_weather_quality,
        force=force,
    )


def ingest_air_quality_dataset(spark: SparkSession, cfg: dict, *, force: bool = False) -> dict:
    source_path = cfg["source_path"]
    if not source_path.is_file():
        raise FileNotFoundError(source_path)

    source_sha256 = file_sha256(source_path)
    rel_source = cfg["path"].replace("\\", "/")
    if not force and _prior_success(
        spark, cfg["key"], source_sha256, cfg["schema_version"]
    ):
        return _skip_payload(cfg["key"], source_sha256)

    run_id = new_run_id()
    started_at = utc_now()
    bronze_path = _lake_path(cfg["bronze_path"])
    silver_path = _lake_path(cfg["silver_path"])
    quarantine_path = _lake_path(cfg["quarantine_path"])

    raw = (
        spark.read.schema(AIR_QUALITY_RAW_SCHEMA)
        .option("header", True)
        .option("mode", "FAILFAST")
        .csv(str(source_path))
    )
    _validate_columns(raw, AIR_QUALITY_RAW_SCHEMA)
    filtered = filter_air_quality_nyc(raw)
    rows_read = filtered.count()
    if rows_read != cfg["expected_bronze_rows"]:
        raise ValueError(
            f"NYC air-quality row count mismatch: expected {cfg['expected_bronze_rows']}, got {rows_read}"
        )

    bronze = with_bronze_metadata(
        filtered,
        source_file=rel_source,
        source_sha256=source_sha256,
        run_id=run_id,
        schema_version=cfg["schema_version"],
        ingested_at=started_at,
    )
    bronze.write.format("delta").mode("overwrite").save(str(bronze_path))

    stations, quarantined = split_air_station_quality(bronze)
    station_count = stations.count()
    rows_quarantined = quarantined.count()
    if rows_read != station_count + rows_quarantined:
        raise ValueError(
            "Station-level split mismatch: "
            f"read={rows_read}, stations={station_count}, quarantined={rows_quarantined}"
        )
    hourly = aggregate_air_hourly(stations)
    rows_accepted = hourly.count()
    if rows_accepted != cfg["expected_silver_rows"]:
        raise ValueError(
            f"Hourly aggregation mismatch: expected {cfg['expected_silver_rows']}, got {rows_accepted}"
        )

    _write_delta(hourly, silver_path, mode="overwrite")
    if rows_quarantined:
        quarantined = quarantined.withColumn(
            "quarantine_reason", F.lit("invalid_air_quality_measurement")
        )
        _write_delta(quarantined, quarantine_path, mode="overwrite")

    return _finish_run(
        spark,
        cfg=cfg,
        run_id=run_id,
        started_at=started_at,
        source_file=rel_source,
        source_sha256=source_sha256,
        rows_read=rows_read,
        rows_accepted=rows_accepted,
        rows_quarantined=rows_quarantined,
        rows_warned=0,
        bronze_path=bronze_path,
        silver_path=silver_path,
        quarantine_path=quarantine_path,
    )


def ingest_dataset(spark: SparkSession, cfg: dict, *, force: bool = False) -> dict:
    kind = cfg["kind"]
    if kind == "taxi_trips":
        return ingest_taxi_dataset(spark, cfg, force=force)
    if kind == "taxi_zones":
        return ingest_zones_dataset(spark, cfg, force=force)
    if kind == "weather_hourly":
        return ingest_weather_dataset(spark, cfg, force=force)
    if kind == "air_quality_hourly_nyc":
        return ingest_air_quality_dataset(spark, cfg, force=force)
    raise ValueError(f"Unsupported dataset kind {kind!r}")
