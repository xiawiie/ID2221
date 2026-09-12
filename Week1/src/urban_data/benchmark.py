import hashlib
import json
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from urban_data.paths import LAKEHOUSE
from urban_data.taxi_sources import load_accepted_taxi_trips_from_sources


ROOT = Path(__file__).resolve().parents[2]
ARTIFACT = ROOT / "artifacts" / "benchmark_report.json"
PLAN_ARTIFACT = ROOT / "artifacts" / "benchmark_physical_plans.json"
BENCHMARK_ROOT = LAKEHOUSE / "benchmark"
STRATEGIES = {
    "unpartitioned": BENCHMARK_ROOT / "taxi_unpartitioned",
    "pickup_month_partitioned": BENCHMARK_ROOT / "taxi_pickup_month_partitioned",
}
WRITE_TIME_DEFINITION = (
    "read configured taxi Parquet source files, apply Silver acceptance transforms, "
    "and commit the benchmark Delta layout"
)


def _storage_stats(spark: SparkSession, path: Path) -> dict:
    sql_path = path.resolve().as_posix()
    row = spark.sql(f"DESCRIBE DETAIL delta.`{sql_path}`").collect()[0]
    data_bytes = int(row["sizeInBytes"])
    data_file_count = int(row["numFiles"])
    return {
        "data_file_count": data_file_count,
        "data_bytes": data_bytes,
        "average_data_file_bytes": round(data_bytes / data_file_count)
        if data_file_count
        else 0,
        "delta_total_bytes": data_bytes,
    }


def _write_strategy(
    spark: SparkSession, trips: DataFrame, output_path: Path, partitioned: bool
) -> dict:
    started = time.perf_counter()
    writer = trips.write.format("delta").mode("overwrite")
    if partitioned:
        writer = writer.partitionBy("pickup_month")
    writer.save(str(output_path))
    write_seconds = time.perf_counter() - started
    rows = spark.read.format("delta").load(str(output_path)).count()
    return {
        "write_seconds": round(write_seconds, 3),
        "rows": rows,
        **_storage_stats(spark, output_path),
    }


def _ingest_strategy(
    spark: SparkSession, output_path: Path, partitioned: bool
) -> dict:
    started = time.perf_counter()
    trips, _source_files = load_accepted_taxi_trips_from_sources(spark)
    writer = trips.write.format("delta").mode("overwrite")
    if partitioned:
        writer = writer.partitionBy("pickup_month")
    writer.save(str(output_path))
    write_seconds = time.perf_counter() - started
    rows = spark.read.format("delta").load(str(output_path)).count()
    return {
        "write_seconds": round(write_seconds, 3),
        "rows": rows,
        **_storage_stats(spark, output_path),
    }


def _borough_query(trips: DataFrame, zones: DataFrame, metric: str) -> DataFrame:
    if metric == "trip_count":
        aggregate = F.count("*").alias("trip_count")
    else:
        aggregate = F.round(F.avg("fare_amount"), 6).alias("average_fare")
    return (
        trips.join(
            F.broadcast(zones),
            trips.pu_location_id == zones.location_id,
            "left",
        )
        .groupBy(zones.borough)
        .agg(aggregate)
        .orderBy(zones.borough)
    )


def _build_query(
    spark: SparkSession, strategy_path: Path, zones: DataFrame, query: str
) -> DataFrame:
    trips = spark.read.format("delta").load(str(strategy_path))
    if query == "trips_per_borough":
        return _borough_query(trips, zones, "trip_count")
    if query == "average_trip_duration_per_day":
        return (
            trips.withColumn(
                "pickup_day", F.to_date("pickup_ts_local")
            )
            .groupBy("pickup_day")
            .agg(F.round(F.avg("trip_duration_minutes"), 6).alias("average_duration"))
            .orderBy("pickup_day")
        )
    if query == "average_fare_per_borough":
        return _borough_query(trips, zones, "average_fare")
    raise ValueError(f"Unsupported benchmark query {query!r}")


def _signature(rows: list) -> str:
    payload = [{key: str(value) for key, value in row.asDict().items()} for row in rows]
    encoded = json.dumps(payload, sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _measure_queries(spark: SparkSession, zones: DataFrame, path: Path) -> dict:
    results = {}
    for query in (
        "trips_per_borough",
        "average_trip_duration_per_day",
        "average_fare_per_borough",
    ):
        warmup = _build_query(spark, path, zones, query)
        plan = warmup._jdf.queryExecution().executedPlan().toString()
        started = time.perf_counter()
        warmup.collect()
        warmup_ms = round((time.perf_counter() - started) * 1000, 3)

        run_ms = []
        row_count = 0
        signature = ""
        for _ in range(3):
            frame = _build_query(spark, path, zones, query)
            started = time.perf_counter()
            rows = frame.collect()
            run_ms.append(round((time.perf_counter() - started) * 1000, 3))
            row_count = len(rows)
            signature = _signature(rows)

        results[query] = {
            "warmup_ms": warmup_ms,
            "hot_run_ms": run_ms,
            "median_ms": round(statistics.median(run_ms), 3),
            "result_rows": row_count,
            "result_sha256": signature,
            "physical_plan": plan,
        }
    return results


def benchmark_taxi_storage(spark: SparkSession) -> dict:
    spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
    _, source_files = load_accepted_taxi_trips_from_sources(spark)
    zones = spark.read.format("delta").load(
        str(LAKEHOUSE / "silver" / "taxi_zones")
    ).select("location_id", "borough")

    strategies = {
        "unpartitioned": _ingest_strategy(
            spark, STRATEGIES["unpartitioned"], partitioned=False
        ),
        "pickup_month_partitioned": _ingest_strategy(
            spark, STRATEGIES["pickup_month_partitioned"], partitioned=True
        ),
    }
    for name, metrics in strategies.items():
        if metrics["rows"] != 9_551_977:
            raise ValueError(f"Benchmark strategy {name} has {metrics['rows']} rows")

    queries = {
        name: _measure_queries(spark, zones, path)
        for name, path in STRATEGIES.items()
    }
    for query in queries["unpartitioned"]:
        left = queries["unpartitioned"][query]["result_sha256"]
        right = queries["pickup_month_partitioned"][query]["result_sha256"]
        if left != right:
            raise ValueError(f"Benchmark result mismatch for {query}")

    plans = {
        strategy: {query: metrics["physical_plan"] for query, metrics in runs.items()}
        for strategy, runs in queries.items()
    }
    for runs in queries.values():
        for metrics in runs.values():
            metrics.pop("physical_plan")

    report = {
        "status": "success",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "input": source_files,
        "input_rows": 9_551_977,
        "write_time_definition": WRITE_TIME_DEFINITION,
        "compression": "snappy",
        "hot_query_protocol": "1 warmup + 3 measured runs; median reported",
        "strategies": strategies,
        "queries": queries,
    }
    ARTIFACT.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    PLAN_ARTIFACT.write_text(
        json.dumps(plans, indent=2) + "\n", encoding="utf-8"
    )
    return report
