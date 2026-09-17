# Week 2 Analytics Design

## Scope

Week 2 uses Spark SQL directly over the Week 1 Delta Gold table,
`gold/integrated_taxi_trips`. The input remains immutable. Query results and
analytical products are derived data written under `lakehouse/`.

## Required Queries

| Query | Question answered |
| --- | --- |
| `monthly_taxi_demand_by_zone` | How does taxi demand vary by pickup zone and month? |
| `average_trip_distance_by_weather` | How does average trip distance differ by weather condition? |
| `air_quality_taxi_demand_relationship` | What is the hourly PM2.5 and taxi-demand relationship for each month? |
| `zone_weather_demand_variation` | Which pickup zones have the largest demand variation across weather conditions? |
| `peak_travel_hours_by_day_of_week` | Which hour has the greatest taxi demand on each day of the week? |
| `monthly_taxi_demand_trends` | What are the monthly demand totals and month-over-month changes? |

`src/urban_data/analytics.py` keeps each query as named Spark SQL and registers
the Gold Delta table as a temporary view. The CLI returns each query result as
JSON.

## Materialized Products

| Delta product | Intended user and use | Refresh and partitioning |
| --- | --- | --- |
| `daily_mobility_summary` | Operations analysts monitoring borough-level daily activity | Full refresh; partitioned by `pickup_month` |
| `taxi_zone_monthly_statistics` | Planners comparing zone demand, distance, duration, and fare | Full refresh; partitioned by `pickup_month` |
| `weather_impact_summary` | Analysts assessing weather-related trip behavior | Full refresh; partitioned by `pickup_month` |
| `air_quality_impact_summary` | Policy and environmental analysts examining PM2.5 and mobility | Full refresh; partitioned by `pickup_month` |

Every materialized table has `product_name`, `source_table`,
`created_at_utc`, `refreshed_at_utc`, `schema_version`, and `run_id` metadata.
The original creation time is retained when a product is refreshed.

## Optimization Experiments

`benchmark-analytics` records a warm-up plus three measured executions, reports
the median, validates a SHA-256 signature of the result rows, and writes
`EXPLAIN FORMATTED` plans. It evaluates:

1. Caching the narrow three-column projection reused by a repeated zone-and-month aggregation.
2. Direct filtering on the physical `source_file_month` partition column versus
   an equivalent expression that prevents partition pruning.
3. A broadcast hint for the small taxi-zone dimension versus a disabled
   auto-broadcast baseline.
4. The same aggregation with Adaptive Query Execution disabled and enabled.

The benchmark also records the Delta storage size and number of data files for
any materialized products that have been refreshed. Actual timing and storage
figures belong in the benchmark report only after an execution with the course
dataset and the documented Spark environment.

## Task 5 Platform Evaluation

`benchmark-analytics` now benchmarks all six required analytical queries in
addition to the four technique experiments. Each analytical query records:

- baseline execution time and `EXPLAIN FORMATTED` plan,
- an optimized execution using one assigned technique,
- result-row SHA-256 equivalence,
- median latency speedup.

| Query | Assigned optimization |
| --- | --- |
| `monthly_taxi_demand_by_zone` | Narrow projection cache |
| `average_trip_distance_by_weather` | Adaptive Query Execution |
| `air_quality_taxi_demand_relationship` | Adaptive Query Execution |
| `zone_weather_demand_variation` | Adaptive Query Execution |
| `peak_travel_hours_by_day_of_week` | Cache pickup timestamp column |
| `monthly_taxi_demand_trends` | Direct `source_file_month` partition filter |

The JSON report adds `analytical_queries` and `platform_evaluation`. The
latter summarizes the largest and smallest measured speedups, the most
expensive analytical queries, total analytical-product storage overhead, and
ten-city scaling recommendations derived from the measured evidence.
