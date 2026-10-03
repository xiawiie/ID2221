"""Reusable Spark SQL queries over the Week 1 integrated Delta table."""

from __future__ import annotations

from typing import Any

from urban_data.paths import LAKEHOUSE


GOLD_PATH = LAKEHOUSE / "gold" / "integrated_taxi_trips"
INTEGRATED_VIEW = "integrated_taxi_trips"

ANALYTIC_QUERY_SQL = {
    "monthly_taxi_demand_by_zone": """
        SELECT
            pickup_month,
            pickup_zone,
            pickup_borough,
            COUNT(*) AS taxi_demand
        FROM integrated_taxi_trips
        GROUP BY pickup_month, pickup_zone, pickup_borough
        ORDER BY pickup_month, taxi_demand DESC, pickup_zone
    """,
    "average_trip_distance_by_weather": """
        SELECT
            COALESCE(CAST(weather_condition_code AS STRING), 'unknown')
                AS weather_condition,
            COUNT(*) AS trip_count,
            ROUND(AVG(trip_distance), 6) AS average_trip_distance
        FROM integrated_taxi_trips
        GROUP BY COALESCE(CAST(weather_condition_code AS STRING), 'unknown')
        ORDER BY average_trip_distance DESC, weather_condition
    """,
    "air_quality_taxi_demand_relationship": """
        WITH hourly_demand AS (
            SELECT
                pickup_month,
                pickup_hour_utc,
                MAX(air_pm25_mean) AS air_pm25_mean,
                COUNT(*) AS taxi_demand
            FROM integrated_taxi_trips
            WHERE air_quality_match_status = 'matched'
            GROUP BY pickup_month, pickup_hour_utc
        )
        SELECT
            pickup_month,
            COUNT(*) AS observed_hours,
            ROUND(AVG(air_pm25_mean), 6) AS average_pm25,
            SUM(taxi_demand) AS taxi_demand,
            ROUND(CORR(taxi_demand, air_pm25_mean), 6) AS demand_pm25_correlation
        FROM hourly_demand
        GROUP BY pickup_month
        ORDER BY pickup_month
    """,
    "zone_weather_demand_variation": """
        WITH demand_by_weather AS (
            SELECT
                pickup_zone,
                pickup_borough,
                COALESCE(CAST(weather_condition_code AS STRING), 'unknown')
                    AS weather_condition,
                COUNT(*) AS taxi_demand
            FROM integrated_taxi_trips
            GROUP BY
                pickup_zone,
                pickup_borough,
                COALESCE(CAST(weather_condition_code AS STRING), 'unknown')
        )
        SELECT
            pickup_zone,
            pickup_borough,
            COUNT(*) AS weather_conditions_observed,
            ROUND(STDDEV_POP(taxi_demand), 6) AS demand_standard_deviation,
            MAX(taxi_demand) - MIN(taxi_demand) AS demand_range
        FROM demand_by_weather
        GROUP BY pickup_zone, pickup_borough
        ORDER BY demand_standard_deviation DESC, demand_range DESC, pickup_zone
    """,
    "peak_travel_hours_by_day_of_week": """
        WITH hourly_demand AS (
            SELECT
                DAYOFWEEK(pickup_ts_local) AS day_of_week_number,
                DATE_FORMAT(pickup_ts_local, 'EEEE') AS day_of_week,
                HOUR(pickup_ts_local) AS travel_hour,
                COUNT(*) AS taxi_demand
            FROM integrated_taxi_trips
            GROUP BY
                DAYOFWEEK(pickup_ts_local),
                DATE_FORMAT(pickup_ts_local, 'EEEE'),
                HOUR(pickup_ts_local)
        ), ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY day_of_week_number
                    ORDER BY taxi_demand DESC, travel_hour
                ) AS demand_rank
            FROM hourly_demand
        )
        SELECT day_of_week_number, day_of_week, travel_hour, taxi_demand
        FROM ranked
        WHERE demand_rank = 1
        ORDER BY day_of_week_number
    """,
    "monthly_taxi_demand_trends": """
        WITH monthly_demand AS (
            SELECT pickup_month, COUNT(*) AS taxi_demand
            FROM integrated_taxi_trips
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
    """,
}


def query_names() -> tuple[str, ...]:
    """Return stable public names for the six required analytical queries."""

    return tuple(ANALYTIC_QUERY_SQL)


def register_integrated_view(spark: Any) -> None:
    """Register the Week 1 Gold Delta table for direct Spark SQL queries."""

    spark.read.format("delta").load(str(GOLD_PATH)).createOrReplaceTempView(
        INTEGRATED_VIEW
    )


def run_analytic_query(spark: Any, name: str):
    """Execute one named analytical query against the integrated Gold table."""

    try:
        sql = ANALYTIC_QUERY_SQL[name]
    except KeyError as exc:
        known = ", ".join(query_names())
        raise KeyError(f"Unknown analytical query {name!r}; known: {known}") from exc
    register_integrated_view(spark)
    return spark.sql(sql)
