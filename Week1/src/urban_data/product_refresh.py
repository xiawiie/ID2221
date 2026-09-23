"""Week 3 Task 2 selective and incremental analytical product refresh."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from pyspark.sql import functions as F

from urban_data.config import UPDATE_MANIFEST
from urban_data.monitoring import record_pipeline_run
from urban_data.paths import LAKEHOUSE, METADATA_RUNS
from urban_data.transforms import utc_now
from urban_data.analytics import register_integrated_view
from urban_data.products import (
    PRODUCT_ROOT,
    PRODUCT_SQL,
    SOURCE_TABLE,
    _existing_created_at,
    _load_config,
    _storage_stats,
    materialize_product,
    product_names,
)

INCREMENTAL_DATASET_KEYS = (
    "taxi_trips_update",
    "weather_update",
    "air_quality_update",
)

INCREMENTAL_LOGICAL_NAMES = {
    "taxi_trips_update": "taxi_trips",
    "weather_update": "weather_hourly",
    "air_quality_update": "air_quality_hourly_nyc",
}


def _sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _dependency_config() -> dict:
    return _load_config()["source_dataset_dependencies"]


def products_for_logical_dataset(logical_name: str) -> list[str]:
    cfg = _dependency_config().get(logical_name, {})
    return list(cfg.get("products", []))


def products_affected_by_dataset_keys(dataset_keys: list[str]) -> list[str]:
    affected: set[str] = set()
    for key in dataset_keys:
        logical = INCREMENTAL_LOGICAL_NAMES.get(key, key)
        affected.update(products_for_logical_dataset(logical))
    return sorted(affected)


def latest_successful_incremental_keys(spark: Any) -> list[str]:
    if not METADATA_RUNS.exists():
        return []
    runs = (
        spark.read.format("delta")
        .load(str(METADATA_RUNS))
        .filter(F.col("status") == "success")
        .filter(F.col("dataset_key").isin(*INCREMENTAL_DATASET_KEYS))
    )
    keys: list[str] = []
    for key in INCREMENTAL_DATASET_KEYS:
        hit = runs.filter(F.col("dataset_key") == key).orderBy(
            F.col("finished_at").desc()
        )
        if hit.limit(1).count():
            keys.append(key)
    return keys


def taxi_incremental_pickup_months(spark: Any) -> list[str]:
    silver_path = LAKEHOUSE / "silver" / "taxi_trips"
    if not silver_path.exists():
        return []
    frame = spark.read.format("delta").load(str(silver_path)).filter(
        F.col("_source_file").contains("datasets/updates/taxi_trips_update")
    )
    if frame.limit(1).count() == 0:
        return []
    return sorted(
        row.pickup_month
        for row in frame.select("pickup_month").distinct().collect()
        if row.pickup_month
    )


def weather_trip_overlap_months(spark: Any) -> list[str]:
    weather_path = LAKEHOUSE / "silver" / "weather_hourly"
    if not weather_path.exists():
        return []
    weather = spark.read.format("delta").load(str(weather_path)).filter(
        F.col("_source_file").contains("datasets/updates/weather_update")
    )
    if weather.limit(1).count() == 0:
        return []
    if "humidity" not in weather.columns:
        weather = weather.withColumn("humidity", F.lit(None).cast("double"))
    register_integrated_view(spark)
    gold = spark.table("integrated_taxi_trips").select(
        "pickup_month", "pickup_hour_local", "weather_observation_ts_local"
    )
    joined = (
        gold.join(
            weather.select(
                F.col("observation_ts_local").alias("weather_observation_ts_local"),
                F.col("humidity").alias("weather_humidity"),
            ),
            on="weather_observation_ts_local",
            how="inner",
        )
        .select("pickup_month")
        .distinct()
    )
    return sorted(row.pickup_month for row in joined.collect() if row.pickup_month)


def air_trip_overlap_months(spark: Any) -> list[str]:
    register_integrated_view(spark)
    frame = (
        spark.table("integrated_taxi_trips")
        .filter(F.col("air_quality_match_status") == "matched")
        .filter(F.col("pickup_month") >= "2025-01")
        .select("pickup_month")
        .distinct()
    )
    return sorted(row.pickup_month for row in frame.collect() if row.pickup_month)


MONTH_SCOPE_RESOLVERS = {
    "taxi_incremental_pickup_months": taxi_incremental_pickup_months,
    "weather_trip_overlap_months": weather_trip_overlap_months,
    "air_trip_overlap_months": air_trip_overlap_months,
}


def pickup_months_for_dataset(spark: Any, logical_name: str) -> list[str]:
    cfg = _dependency_config().get(logical_name, {})
    resolver_name = cfg.get("month_scope")
    if not resolver_name:
        return []
    resolver = MONTH_SCOPE_RESOLVERS[resolver_name]
    return resolver(spark)


def resolve_refresh_plan(
    spark: Any,
    dataset_keys: list[str] | None = None,
) -> dict[str, Any]:
    keys = dataset_keys or latest_successful_incremental_keys(spark)
    product_months: dict[str, set[str]] = {}
    dataset_details: dict[str, dict[str, Any]] = {}

    for key in keys:
        logical = INCREMENTAL_LOGICAL_NAMES[key]
        months = pickup_months_for_dataset(spark, logical)
        dataset_details[key] = {
            "logical_name": logical,
            "pickup_months": months,
            "products": products_for_logical_dataset(logical),
        }
        for product_name in dataset_details[key]["products"]:
            product_months.setdefault(product_name, set()).update(months)

    return {
        "dataset_keys": keys,
        "datasets": dataset_details,
        "products": {
            name: sorted(months)
            for name, months in sorted(product_months.items())
        },
    }


def _decorate_product_frame(
    spark: Any,
    name: str,
    frame,
    *,
    created_at: datetime,
    refreshed_at: datetime,
    run_id: str,
):
    config = _load_config()
    return (
        frame.withColumn("product_name", F.lit(name))
        .withColumn("source_table", F.lit(SOURCE_TABLE))
        .withColumn("created_at_utc", F.lit(created_at).cast("timestamp"))
        .withColumn("refreshed_at_utc", F.lit(refreshed_at).cast("timestamp"))
        .withColumn("schema_version", F.lit(str(config["schema_version"])))
        .withColumn("run_id", F.lit(run_id))
    )


def _product_frame(spark: Any, name: str, pickup_months: list[str] | None):
    register_integrated_view(spark)
    frame = spark.sql(PRODUCT_SQL[name])
    if pickup_months:
        frame = frame.filter(F.col("pickup_month").isin(pickup_months))
    return frame


def refresh_product_partitions(
    spark: Any,
    name: str,
    pickup_months: list[str],
    *,
    mode: str = "partition_replace",
) -> dict[str, Any]:
    if name not in PRODUCT_SQL:
        known = ", ".join(product_names())
        raise KeyError(f"Unknown analytical product {name!r}; known: {known}")

    started_at = utc_now()
    config = _load_config()
    schema_version = str(config["schema_version"])

    if not pickup_months:
        result = {
            "product_name": name,
            "status": "skipped",
            "reason": "no affected pickup_month partitions",
            "refresh_mode": mode,
            "pickup_months": [],
            "run_id": str(uuid4()),
        }
        record_pipeline_run(
            spark,
            run_id=result["run_id"],
            pipeline_type="product_refresh",
            target_key=name,
            target_name=name,
            schema_version=schema_version,
            rows_processed=0,
            rows_inserted=0,
            rows_rejected=0,
            started_at=started_at,
            finished_at=utc_now(),
            status="skipped",
            error_message=result["reason"],
        )
        return result

    settings = _load_config()["products"][name]
    output_path = PRODUCT_ROOT / name
    if not output_path.exists() or mode == "full":
        result = materialize_product(spark, name)
        result["status"] = "success"
        result["refresh_mode"] = "full"
        result["pickup_months"] = pickup_months
        record_pipeline_run(
            spark,
            run_id=result["run_id"],
            pipeline_type="product_refresh",
            target_key=name,
            target_name=name,
            schema_version=schema_version,
            rows_processed=int(result.get("row_count", 0)),
            rows_inserted=int(result.get("row_count", 0)),
            rows_rejected=0,
            started_at=started_at,
            finished_at=utc_now(),
            status="success",
        )
        return result

    created_at = _existing_created_at(spark, output_path)
    refreshed_at = datetime.now(timezone.utc)
    run_id = str(uuid4())
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    partition_columns = settings.get("partition_columns", [])
    product = _decorate_product_frame(
        spark,
        name,
        _product_frame(spark, name, pickup_months),
        created_at=created_at,
        refreshed_at=refreshed_at,
        run_id=run_id,
    )

    if mode == "full_partition":
        for month in pickup_months:
            part = product.filter(F.col("pickup_month") == month)
            (
                part.write.format("delta")
                .mode("overwrite")
                .option("mergeSchema", "true")
                .option("replaceWhere", f"pickup_month = '{_sql_literal(month)}'")
                .save(str(output_path))
            )
    else:
        writer = (
            product.write.format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
        )
        if partition_columns:
            writer = writer.partitionBy(*partition_columns)
        replace_predicate = " OR ".join(
            f"pickup_month = '{_sql_literal(month)}'" for month in pickup_months
        )
        writer.option("replaceWhere", replace_predicate).save(str(output_path))

    result = {
        "product_name": name,
        "status": "success",
        "refresh_mode": mode,
        "pickup_months": pickup_months,
        "output_path": str(output_path),
        "row_count": spark.read.format("delta").load(str(output_path)).count(),
        "partition_columns": partition_columns,
        "source_table": SOURCE_TABLE,
        "created_at_utc": created_at.isoformat(),
        "refreshed_at_utc": refreshed_at.isoformat(),
        "schema_version": schema_version,
        "run_id": run_id,
        **_storage_stats(spark, output_path),
    }
    record_pipeline_run(
        spark,
        run_id=run_id,
        pipeline_type="product_refresh",
        target_key=name,
        target_name=name,
        schema_version=schema_version,
        rows_processed=int(result["row_count"]),
        rows_inserted=int(result["row_count"]),
        rows_rejected=0,
        started_at=started_at,
        finished_at=utc_now(),
        status="success",
    )
    return result


def refresh_affected_products(
    spark: Any,
    *,
    dataset_keys: list[str] | None = None,
    product: str = "all",
) -> dict[str, Any]:
    plan = resolve_refresh_plan(spark, dataset_keys=dataset_keys)
    targets = plan["products"]
    if product != "all":
        if product not in targets:
            targets = {product: targets.get(product, [])}
        else:
            targets = {product: targets[product]}

    results = []
    for product_name in sorted(targets):
        months = targets[product_name]
        refresh_mode = _load_config()["products"][product_name].get(
            "refresh_mode", "partition_replace"
        )
        results.append(
            refresh_product_partitions(
                spark,
                product_name,
                months,
                mode=refresh_mode,
            )
        )

    manifest_exists = UPDATE_MANIFEST.is_file()
    return {
        "status": "success",
        "plan": plan,
        "manifest_available": manifest_exists,
        "results": results,
    }
