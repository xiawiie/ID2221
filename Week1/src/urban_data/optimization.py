"""Repeatable Week 2 query-optimization experiments."""

from __future__ import annotations

import hashlib
import json
import statistics
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from urban_data.analytics import register_integrated_view
from urban_data.paths import LAKEHOUSE
from urban_data.products import PRODUCT_ROOT, product_names


ROOT = Path(__file__).resolve().parents[2]
ARTIFACT = ROOT / "artifacts" / "week2_optimization_benchmark.json"
PLAN_ARTIFACT = ROOT / "artifacts" / "week2_optimization_plans.json"
RUNS = 3
CACHE_VIEW = "taxi_demand_projection"

CACHE_SQL = """
    SELECT
        pickup_month,
        pickup_zone,
        pickup_borough,
        COUNT(*) AS taxi_demand
    FROM taxi_demand_projection
    GROUP BY pickup_month, pickup_zone, pickup_borough
    ORDER BY pickup_month, taxi_demand DESC, pickup_zone
"""

PARTITION_BASELINE_SQL = """
    SELECT pickup_borough, COUNT(*) AS taxi_demand
    FROM integrated_taxi_trips
    WHERE SUBSTRING(source_file_month, 1, 7) = '2024-01'
    GROUP BY pickup_borough
    ORDER BY pickup_borough
"""

PARTITION_PRUNED_SQL = """
    SELECT pickup_borough, COUNT(*) AS taxi_demand
    FROM integrated_taxi_trips
    WHERE source_file_month = '2024-01'
    GROUP BY pickup_borough
    ORDER BY pickup_borough
"""

BROADCAST_BASELINE_SQL = """
    SELECT z.borough, COUNT(*) AS taxi_demand
    FROM silver_taxi_trips AS t
    LEFT JOIN silver_taxi_zones AS z
        ON t.pu_location_id = z.location_id
    GROUP BY z.borough
    ORDER BY z.borough
"""

BROADCAST_OPTIMIZED_SQL = """
    SELECT /*+ BROADCAST(z) */ z.borough, COUNT(*) AS taxi_demand
    FROM silver_taxi_trips AS t
    LEFT JOIN silver_taxi_zones AS z
        ON t.pu_location_id = z.location_id
    GROUP BY z.borough
    ORDER BY z.borough
"""

AQE_SQL = """
    SELECT
        pickup_month,
        pickup_borough,
        COALESCE(CAST(weather_condition_code AS STRING), 'unknown') AS weather_condition,
        COUNT(*) AS taxi_demand,
        ROUND(AVG(trip_distance), 6) AS average_trip_distance
    FROM integrated_taxi_trips
    GROUP BY
        pickup_month,
        pickup_borough,
        COALESCE(CAST(weather_condition_code AS STRING), 'unknown')
    ORDER BY pickup_month, pickup_borough, weather_condition
"""


def _signature(rows: list[Any]) -> str:
    payload = [{key: str(value) for key, value in row.asDict().items()} for row in rows]
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def _explain_formatted(spark: Any, sql: str) -> str:
    return "\n".join(row[0] for row in spark.sql(f"EXPLAIN FORMATTED {sql}").collect())


def _measure_sql(spark: Any, sql: str) -> dict:
    plan = _explain_formatted(spark, sql)
    started = time.perf_counter()
    spark.sql(sql).collect()
    warmup_ms = round((time.perf_counter() - started) * 1000, 3)

    run_ms = []
    rows = []
    for _ in range(RUNS):
        started = time.perf_counter()
        rows = spark.sql(sql).collect()
        run_ms.append(round((time.perf_counter() - started) * 1000, 3))
    return {
        "warmup_ms": warmup_ms,
        "hot_run_ms": run_ms,
        "median_ms": round(statistics.median(run_ms), 3),
        "result_rows": len(rows),
        "result_sha256": _signature(rows),
        "explain_formatted": plan,
    }


def _comparison(baseline: dict, optimized: dict) -> dict:
    if baseline["result_sha256"] != optimized["result_sha256"]:
        raise ValueError("Optimized query result does not match baseline")
    speedup = baseline["median_ms"] / optimized["median_ms"] if optimized["median_ms"] else None
    return {
        "baseline": baseline,
        "optimized": optimized,
        "result_match": True,
        "speedup": round(speedup, 4) if speedup else None,
    }


@contextmanager
def _temporary_conf(spark: Any, key: str, value: str) -> Iterator[None]:
    previous = spark.conf.get(key, None)
    spark.conf.set(key, value)
    try:
        yield
    finally:
        if previous is None:
            spark.conf.unset(key)
        else:
            spark.conf.set(key, previous)


def _register_cache_view(spark: Any) -> None:
    """Expose only the columns reused by the repeated demand aggregation."""

    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW taxi_demand_projection AS
        SELECT pickup_month, pickup_zone, pickup_borough
        FROM integrated_taxi_trips
        """
    )


def _register_silver_views(spark: Any) -> None:
    spark.read.format("delta").load(str(LAKEHOUSE / "silver" / "taxi_trips")).createOrReplaceTempView(
        "silver_taxi_trips"
    )
    spark.read.format("delta").load(str(LAKEHOUSE / "silver" / "taxi_zones")).createOrReplaceTempView(
        "silver_taxi_zones"
    )


def _product_storage(spark: Any) -> dict:
    metrics = {}
    for name in product_names():
        path = PRODUCT_ROOT / name
        if not path.exists():
            continue
        row = spark.sql(
            f"DESCRIBE DETAIL delta.`{path.resolve().as_posix()}`"
        ).collect()[0]
        metrics[name] = {
            "data_file_count": int(row["numFiles"]),
            "data_bytes": int(row["sizeInBytes"]),
        }
    return metrics


def benchmark_analytical_optimizations(spark: Any) -> dict:
    """Measure caching, partition pruning, broadcast joins, and AQE."""

    register_integrated_view(spark)
    plans: dict[str, dict[str, str]] = {}

    _register_cache_view(spark)
    spark.catalog.clearCache()
    cache_baseline = _measure_sql(spark, CACHE_SQL)
    cache_started = time.perf_counter()
    spark.catalog.cacheTable(CACHE_VIEW)
    spark.table(CACHE_VIEW).count()
    cache_fill_ms = round((time.perf_counter() - cache_started) * 1000, 3)
    cache_optimized = _measure_sql(spark, CACHE_SQL)
    spark.catalog.uncacheTable(CACHE_VIEW)
    caching = _comparison(cache_baseline, cache_optimized)
    caching["cache_fill_ms"] = cache_fill_ms
    plans["caching"] = {
        "baseline": caching["baseline"].pop("explain_formatted"),
        "optimized": caching["optimized"].pop("explain_formatted"),
    }

    partitioning = _comparison(
        _measure_sql(spark, PARTITION_BASELINE_SQL),
        _measure_sql(spark, PARTITION_PRUNED_SQL),
    )
    plans["partition_pruning"] = {
        "baseline": partitioning["baseline"].pop("explain_formatted"),
        "optimized": partitioning["optimized"].pop("explain_formatted"),
    }

    _register_silver_views(spark)
    with _temporary_conf(spark, "spark.sql.autoBroadcastJoinThreshold", "-1"):
        broadcasting = _comparison(
            _measure_sql(spark, BROADCAST_BASELINE_SQL),
            _measure_sql(spark, BROADCAST_OPTIMIZED_SQL),
        )
    plans["broadcast_join"] = {
        "baseline": broadcasting["baseline"].pop("explain_formatted"),
        "optimized": broadcasting["optimized"].pop("explain_formatted"),
    }

    with _temporary_conf(spark, "spark.sql.adaptive.enabled", "false"):
        aqe_disabled = _measure_sql(spark, AQE_SQL)
    with _temporary_conf(spark, "spark.sql.adaptive.enabled", "true"):
        aqe_enabled = _measure_sql(spark, AQE_SQL)
    aqe = _comparison(aqe_disabled, aqe_enabled)
    plans["adaptive_query_execution"] = {
        "baseline": aqe["baseline"].pop("explain_formatted"),
        "optimized": aqe["optimized"].pop("explain_formatted"),
    }

    report = {
        "status": "success",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "protocol": f"1 warmup + {RUNS} measured runs; median reported",
        "experiments": {
            "caching": caching,
            "partition_pruning": partitioning,
            "broadcast_join": broadcasting,
            "adaptive_query_execution": aqe,
        },
        "analytical_product_storage": _product_storage(spark),
    }
    ARTIFACT.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    PLAN_ARTIFACT.write_text(json.dumps(plans, indent=2) + "\n", encoding="utf-8")
    return report
