# Week 4 Task 1 Training Dataset Design

## Selected Prediction Problem

**Hourly taxi demand by pickup zone**

Predict `trip_count`: the number of accepted yellow-taxi pickups in each TLC pickup zone for each New York local hour.

This extends Week 2 descriptive demand queries (`monthly_taxi_demand_by_zone`, `peak_travel_hours_by_day_of_week`) from historical reporting to supervised learning on a repeatable training dataset.

## Target Variable

| Item | Definition |
| --- | --- |
| Name | `trip_count` |
| Grain | `(pickup_zone, pickup_hour_local)` |
| Source | `COUNT(*)` over accepted rows in `gold/integrated_taxi_trips` |
| Type | Non-negative integer regression target |

Each training row represents one observed zone-hour with at least one accepted trip. Zero-demand zone-hours are not imputed in Task 1; that keeps the first version simple and avoids inventing labels for unobserved hours.

## Feature Columns

Features are generated automatically from the integrated Gold table. Categorical and numeric columns are kept in semantic form; Spark ML encoding and scaling belong to Task 2.

| Group | Columns | Rationale |
| --- | --- | --- |
| Location | `pickup_zone`, `pickup_borough`, `pickup_service_zone` | Captures persistent spatial demand patterns and borough/service-zone effects |
| Time | `hour_of_day`, `day_of_week`, `is_weekend`, `pickup_month` (metadata) | Models hourly, weekly, and seasonal regularities |
| Weather | `weather_temp`, `weather_precipitation`, `weather_wind_speed`, `weather_relative_humidity` | City-wide hourly context already joined in Week 1 Gold integration |
| Air quality | `air_pm25_mean` | Optional environmental context attached at pickup UTC hour |
| Lag demand | `demand_lag_1h`, `demand_lag_24h`, `demand_lag_168h` | Recent, daily, and weekly demand history for the same zone |

## Dataset Contributions

| Platform dataset | Role in Task 1 |
| --- | --- |
| `gold/integrated_taxi_trips` | Primary source for target aggregation and all features |
| Taxi zones (via Gold join) | Borough and service-zone attributes |
| Weather hourly (via Gold join) | Hourly environmental context |
| Air quality hourly NYC (via Gold join) | PM2.5 context |

Task 1 does not re-read raw Parquet or CSV files. That separation is intentional and supports the Week 4 Task 4 comparison.

## Train / Validation / Test Splits

Chronological split on `service_date = DATE(pickup_hour_local)`:

| Split | Date range |
| --- | --- |
| Train | `<= 2024-02-29` |
| Validation | `2024-03-01` through `2024-03-15` |
| Test | `>= 2024-03-16` |

Random splits are avoided to prevent temporal leakage through lag features.

Rows with incomplete lag history (`require_complete_lags: true`) are excluded, mainly affecting the first 168 hours per zone in the calendar.

## Assumptions

1. Pickup-side demand is the municipal planning target; dropoff zones are out of scope for Task 1.
2. Weather and PM2.5 values are treated as city-wide hourly context, consistent with Week 1 integration semantics.
3. Accepted-trip filtering already happened in Week 1 Silver validation; Task 1 aggregates only Gold rows.
4. Lag features use contiguous prior rows per zone in timestamp order; missing intermediate hours reduce lag quality but are accepted in this first version.

## Generation Command

```powershell
.\scripts\run_win.ps1 build-ml-dataset --problem hourly_taxi_demand_by_zone
```

Outputs:

- `lakehouse/ml/hourly_zone_demand/full`
- `lakehouse/ml/hourly_zone_demand/train`
- `lakehouse/ml/hourly_zone_demand/validation`
- `lakehouse/ml/hourly_zone_demand/test`
- `artifacts/week4_training_dataset.json`
