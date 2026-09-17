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

from urban_data.analytics import ANALYTIC_QUERY_SQL, query_names, register_integrated_view
from urban_data.paths import LAKEHOUSE
from urban_data.products import PRODUCT_ROOT, product_names


ROOT = Path(__file__).resolve().parents[2]
ARTIFACT = ROOT / "artifacts" / "week2_optimization_benchmark.json"
PLAN_ARTIFACT = ROOT / "artifacts" / "week2_optimization_plans.json"
RUNS = 3
CACHE_VIEW = "taxi_demand_projection"
CACHE_VIEW_PREFIX = "analytic_cache_"

ANALYTICAL_QUERY_TECHNIQUES = {
    "monthly_taxi_demand_by_zone": "caching",
    "average_trip_distance_by_weather": "adaptive_query_execution",
    "air_quality_taxi_demand_relationship": "adaptive_query_execution",
    "zone_weather_demand_variation": "adaptive_query_execution",
    "peak_travel_hours_by_day_of_week": "caching",
    "monthly_taxi_demand_trends": "partition_pruning",
}

CACHE_COLUMNS = {
    "monthly_taxi_demand_by_zone": (
        "pickup_month",
        "pickup_zone",
        "pickup_borough",
    ),
    "peak_travel_hours_by_day_of_week": ("pickup_ts_local",),
}

MONTHLY_TRENDS_PARTITION_BASELINE = """
        WITH monthly_demand AS (
            SELECT pickup_month, COUNT(*) AS taxi_demand
            FROM integrated_taxi_trips
            WHERE SUBSTRING(source_file_month, 1, 7) IN ('2024-01', '2024-02', '2024-03')
            GROUP BY pickup_month
        ), with_previous_month AS (
            SELECT
                pickup_month,
                taxi_demand,
                LAG(taxi_demand) OVER (ORDER BY pickup_month) AS previous_month_demand
            FROM monthly_demand
        )
        SELECT
            pickup_month,
            taxi_demand,
            previous_month_demand,
            CASE
                WHEN previous_month_demand IS NULL OR previous_month_demand = 0 THEN NULL
                ELSE ROUND(
                    100.0 * (taxi_demand - previous_month_demand) / previous_month_demand,
                    6
                )
            END AS month_over_month_percent_change
        FROM with_previous_month
        ORDER BY pickup_month
"""

MONTHLY_TRENDS_PARTITION_OPTIMIZED = """
        WITH monthly_demand AS (
            SELECT pickup_month, COUNT(*) AS taxi_demand
            FROM integrated_taxi_trips
            WHERE source_file_month IN ('2024-01', '2024-02', '2024-03')
            GROUP BY pickup_month
        ), with_previous_month AS (
            SELECT
                pickup_month,
                taxi_demand,
                LAG(taxi_demand) OVER (ORDER BY pickup_month) AS previous_month_demand
            FROM monthly_demand
        )
        SELECT
            pickup_month,
            taxi_demand,
            previous_month_demand,
            CASE
                WHEN previous_month_demand IS NULL OR previous_month_demand = 0 THEN NULL
                ELSE ROUND(
                    100.0 * (taxi_demand - previous_month_demand) / previous_month_demand,
                    6
                )
            END AS month_over_month_percent_change
        FROM with_previous_month
        ORDER BY pickup_month
"""

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


def _store_plan(plans: dict, key: str, comparison: dict) -> None:
    plans[key] = {
        "baseline": comparison["baseline"].pop("explain_formatted"),
        "optimized": comparison["optimized"].pop("explain_formatted"),
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


def _benchmark_query_caching(spark: Any, query_name: str) -> dict:
    sql = ANALYTIC_QUERY_SQL[query_name]
    columns = ", ".join(CACHE_COLUMNS[query_name])
    view_name = f"{CACHE_VIEW_PREFIX}{query_name}"

    baseline = _measure_sql(spark, sql)
    spark.sql(
        f"""
        CREATE OR REPLACE TEMP VIEW {view_name} AS
        SELECT {columns}
        FROM integrated_taxi_trips
        """
    )
    cache_started = time.perf_counter()
    spark.catalog.cacheTable(view_name)
    spark.table(view_name).count()
    cache_fill_ms = round((time.perf_counter() - cache_started) * 1000, 3)

    optimized_sql = sql.replace("integrated_taxi_trips", view_name)
    optimized = _measure_sql(spark, optimized_sql)
    spark.catalog.uncacheTable(view_name)

    result = _comparison(baseline, optimized)
    result["cache_fill_ms"] = cache_fill_ms
    return result


def _benchmark_query_aqe(spark: Any, query_name: str) -> dict:
    sql = ANALYTIC_QUERY_SQL[query_name]
    with _temporary_conf(spark, "spark.sql.adaptive.enabled", "false"):
        baseline = _measure_sql(spark, sql)
    with _temporary_conf(spark, "spark.sql.adaptive.enabled", "true"):
        optimized = _measure_sql(spark, sql)
    return _comparison(baseline, optimized)


def _benchmark_query_partition_pruning(spark: Any) -> dict:
    return _comparison(
        _measure_sql(spark, MONTHLY_TRENDS_PARTITION_BASELINE),
        _measure_sql(spark, MONTHLY_TRENDS_PARTITION_OPTIMIZED),
    )


def _benchmark_analytical_queries(spark: Any, plans: dict) -> dict:
    results = {}
    for query_name in query_names():
        technique = ANALYTICAL_QUERY_TECHNIQUES[query_name]
        if technique == "caching":
            comparison = _benchmark_query_caching(spark, query_name)
        elif technique == "adaptive_query_execution":
            comparison = _benchmark_query_aqe(spark, query_name)
        elif technique == "partition_pruning":
            comparison = _benchmark_query_partition_pruning(spark)
        else:
            raise ValueError(f"Unsupported analytical optimization {technique!r}")

        _store_plan(plans, f"analytical::{query_name}", comparison)
        payload = {
            "optimization_technique": technique,
            **comparison,
        }
        if technique == "caching":
            payload["cache_fill_ms"] = comparison["cache_fill_ms"]
        results[query_name] = payload
    return results


def _platform_evaluation(
    analytical_queries: dict,
    experiments: dict,
    product_storage: dict,
) -> dict:
    speedups: list[tuple[str, str, float]] = []
    for name, payload in experiments.items():
        speedup = payload.get("speedup")
        if speedup is not None:
            speedups.append(("technique_experiment", name, speedup))
    for name, payload in analytical_queries.items():
        speedup = payload.get("speedup")
        if speedup is not None:
            speedups.append(("analytical_query", name, speedup))

    largest = max(speedups, key=lambda item: item[2])
    smallest = min(speedups, key=lambda item: item[2])

    expensive = sorted(
        (
            {
                "query_name": name,
                "optimization_technique": payload["optimization_technique"],
                "baseline_median_ms": payload["baseline"]["median_ms"],
                "optimized_median_ms": payload["optimized"]["median_ms"],
            }
            for name, payload in analytical_queries.items()
        ),
        key=lambda item: item["baseline_median_ms"],
        reverse=True,
    )

    total_product_bytes = sum(item["data_bytes"] for item in product_storage.values())
    return {
        "largest_improvement": {
            "scope": largest[0],
            "name": largest[1],
            "speedup": largest[2],
        },
        "smallest_improvement": {
            "scope": smallest[0],
            "name": smallest[1],
            "speedup": smallest[2],
        },
        "most_expensive_analytical_queries": expensive,
        "analytical_product_storage_total_bytes": total_product_bytes,
        "ten_city_recommendations": [
            "Partition by city and stable month keys before data volume makes pruning mandatory.",
            "Materialize city-level analytical products and refresh them incrementally instead of scanning global Gold tables.",
            "Keep small reference tables broadcastable per city and avoid cross-city shuffle joins.",
            "Re-run the same benchmark protocol per city after scaling; do not assume one-city tuning transfers globally.",
            "Scale driver and executor memory with city count and enforce Delta file compaction targets.",
        ],
    }


def benchmark_analytical_optimizations(spark: Any) -> dict:
    """Measure required analytical queries and the four optimization techniques."""

    register_integrated_view(spark)
    plans: dict[str, dict[str, str]] = {}

    analytical_queries = _benchmark_analytical_queries(spark, plans)

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
    _store_plan(plans, "technique::caching", caching)

    partitioning = _comparison(
        _measure_sql(spark, PARTITION_BASELINE_SQL),
        _measure_sql(spark, PARTITION_PRUNED_SQL),
    )
    _store_plan(plans, "technique::partition_pruning", partitioning)

    _register_silver_views(spark)
    with _temporary_conf(spark, "spark.sql.autoBroadcastJoinThreshold", "-1"):
        broadcasting = _comparison(
            _measure_sql(spark, BROADCAST_BASELINE_SQL),
            _measure_sql(spark, BROADCAST_OPTIMIZED_SQL),
        )
    _store_plan(plans, "technique::broadcast_join", broadcasting)

    with _temporary_conf(spark, "spark.sql.adaptive.enabled", "false"):
        aqe_disabled = _measure_sql(spark, AQE_SQL)
    with _temporary_conf(spark, "spark.sql.adaptive.enabled", "true"):
        aqe_enabled = _measure_sql(spark, AQE_SQL)
    aqe = _comparison(aqe_disabled, aqe_enabled)
    _store_plan(plans, "technique::adaptive_query_execution", aqe)

    experiments = {
        "caching": caching,
        "partition_pruning": partitioning,
        "broadcast_join": broadcasting,
        "adaptive_query_execution": aqe,
    }
    product_storage = _product_storage(spark)
    platform_evaluation = _platform_evaluation(
        analytical_queries,
        experiments,
        product_storage,
    )

    report = {
        "status": "success",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "protocol": f"1 warmup + {RUNS} measured runs; median reported",
        "analytical_queries": analytical_queries,
        "experiments": experiments,
        "platform_evaluation": platform_evaluation,
        "analytical_product_storage": product_storage,
    }
    ARTIFACT.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    PLAN_ARTIFACT.write_text(json.dumps(plans, indent=2) + "\n", encoding="utf-8")
    return report
