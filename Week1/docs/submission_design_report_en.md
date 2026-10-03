# ID2221 Week 1 Design Report

**Project:** Generic Urban Data Integration Platform  
**Group members:** Add names and KTH IDs before submission  
**Evidence date:** 12 September 2026

## 1. Objective and Verified Outcome

The project implements a reusable Spark and Delta Lake platform for integrating heterogeneous urban data. It ingests three months of NYC yellow-taxi trips (Parquet), hourly weather (CSV), hourly PM2.5 observations (CSV), and the TLC taxi-zone lookup (CSV). The platform validates source contracts, standardizes data, records lineage and run metadata, quarantines structurally invalid rows, and writes Bronze, Silver, and Gold Delta tables.

The verified run accepted 9,551,977 taxi trips and quarantined 2,801 trips whose dropoff time was not later than pickup. The Silver layer contains 265 zones, 8,784 weather hours, and 8,784 aggregated NYC PM2.5 hours. The Gold table, `integrated_taxi_trips`, contains exactly 9,551,977 rows: one enriched row per accepted trip. Pickup and dropoff zone joins both matched 100% of trips. Weather and air-quality context were missing for 19 trips with pickup times outside 2024; these records were retained with NULL context values and an explicit match status.

## 2. Data Catalog

| Dataset | Entity and record key | Joins and time | Categorical values and growth |
| --- | --- | --- | --- |
| Yellow taxi trips | One trip. The source has no proven unique natural key; an SHA-256 `trip_id` fingerprint is an audit key, not a business primary key. | `PULocationID` and `DOLocationID` join zones. Pickup time joins weather at local hour and PM2.5 at UTC hour. Pickup and dropoff timestamps define trip duration. | Vendor, rate code, payment type, store-and-forward flag, and zone IDs are categorical. New monthly trip files continually increase the fact table. |
| Weather | One city-level hourly observation. `year + month + day + hour` is unique and becomes `observation_ts_local`. | The derived hourly timestamp joins a taxi pickup timestamp truncated to the local hour. | Weather codes and `*_source` fields are categorical. The dataset grows by one row per hour. |
| Taxi zones | One TLC taxi zone. `LocationID` is unique and non-null. | `LocationID` joins to both taxi location IDs. It has no temporal field. | Borough, zone, and service zone are categorical. This is a slowly changing lookup table. |
| Air quality | One PM2.5 observation for a site, instrument channel, and hour. Candidate key: state, county, site, parameter, POC, local date, and local time. | GMT date and time become `observation_ts_utc`. After filtering to NYC and aggregating sites, the UTC hour joins taxi pickup time converted to UTC. | Site, county, parameter, method, unit, and qualifier fields are categorical. Measurements grow per site and hour. |

The taxi source contains optional NULL fields and negative amount values that may represent corrections. Negative fare or total values, an out-of-file-month pickup, and an extreme distance are flagged but retained. A missing core timestamp, non-positive trip duration, duplicate weather hour, invalid zone key, or invalid PM2.5 value causes quarantine. This distinguishes suspicious business events from structurally unusable records.

## 3. Storage Architecture and Common Model

Source files remain immutable under `datasets/`. Dataset contracts, paths, expected schemas, schema versions, and source-file months are stored in `config/datasets.yml`. Generated tables use this structure:

```text
lakehouse/
  bronze/       raw source values plus lineage fields
  silver/       standardized, quality-checked datasets
  gold/         integrated_taxi_trips
  quarantine/   rejected rows and rejection reasons
  metadata/     ingestion_runs records
  benchmark/    alternative Taxi Trips layouts
```

Bronze adds `_source_file`, `_source_sha256`, `_ingested_at`, `_run_id`, and `_schema_version` uniformly. Silver and Gold use lowercase `snake_case`; IDs are integers, measures are doubles, flags are strings, and event fields are timestamps. Optional values and unmatched context remain NULL rather than being replaced with zero.

Taxi timestamps are retained as New York local wall-clock fields (`pickup_ts_local`, `dropoff_ts_local`). Weather time is constructed from its four date parts and labeled as a local wall-clock value with unconfirmed source timezone. Air-quality timestamps are parsed from GMT fields and stored in UTC. This makes the distinct time semantics explicit: weather uses local-hour matching, while PM2.5 uses taxi pickup converted to a UTC hour.

`taxi_zones`, `weather_hourly`, and aggregated `air_quality_hourly_nyc` are small tables and are not partitioned; they are broadcast during joins. The production taxi table is partitioned by `source_file_month`, a stable overwrite key. `pickup_month` remains a data column because a few trips fall outside the month stated by their source filename. Partitioning by high-cardinality columns, skewed values, or columns absent from query filters would create small files and metadata overhead without pruning benefit. At 20x volume, the platform should keep incremental source-hash ingestion, measure actual filtering patterns before changing partitions, compact small files, and consider clustering on proven query predicates rather than partitioning speculatively.

The accompanying architecture diagram is `docs/architecture_diagram.png`: source CSV/Parquet files flow through explicit-schema ingestion into Bronze, Silver, and Gold Delta layers, with separate metadata, quarantine, and benchmark outputs.

## 4. Generic Ingestion Framework

One orchestration path dispatches CSV and Parquet readers, applies explicit schemas, injects Bronze lineage, runs validation, writes Delta tables, appends quarantine rows, and records successful ingestion statistics. The run-metadata table records dataset key, source file and hash, schema version, run identifier, row counts read/accepted/quarantined, timestamps, and status. An unchanged source hash and schema version is skipped, making reruns idempotent.

Dataset-specific concerns are intentionally isolated: PySpark schemas, rename maps, timestamp construction, taxi quality rules, the NYC PM2.5 filter, and PM2.5 hourly aggregation reflect different source grains and business semantics. Configuration handles operational variation, while transformation functions are selected by dataset kind. This prevents a separate ingestion script for each monthly taxi file. Adding 20 future datasets primarily requires new YAML entries; only genuinely new physical schemas or transformation kinds require code and tests.

## 5. Integration Strategy

`integrated_taxi_trips` is created with sequential left joins, with a row-count assertion after each join to prevent accidental fan-out.

| Context | Rule | Missing-data policy |
| --- | --- | --- |
| Pickup and dropoff zone/borough | Equi-join `pu_location_id` and `do_location_id` to `location_id`. | A match-status column is retained; all observed trips matched. |
| Weather | Truncate `pickup_ts_local` to hour and join `observation_ts_local`. | Leave weather fields NULL and mark `missing`. |
| Air quality | Convert pickup time to UTC, truncate to hour, and join an hourly NYC PM2.5 aggregate. | Leave PM2.5 fields NULL and mark `missing`. |

The air-quality source has several site measurements per hour. Direct joining would multiply taxi records, so the platform filters to NYC PM2.5 data and computes one hourly city estimate, including median, mean, minimum, maximum, and site count. This preserves the taxi trip grain. The strategy has limits: it does not interpolate within an hour, select the nearest monitor, or represent zone-level exposure; Manhattan and Staten Island have no observed sites in the filtered data. The weather source provides neither a station identifier nor a declared timezone, so its local-hour association is documented as an assumption rather than a proven spatial match.

## 6. Benchmark, Decisions, and Trade-offs

Two Delta layouts materialized the same 9,551,977 accepted taxi trips: unpartitioned and partitioned by `pickup_month`. A warm-up and three measured runs were used; medians are reported below.

| Metric | Unpartitioned | `pickup_month` partitioned |
| --- | ---: | ---: |
| Source-to-Delta write time | 18.453 s | 17.344 s |
| Data size / files | 815.0 MiB / 10 | 820.0 MiB / 23 |
| Trips per borough | 527.601 ms | 558.957 ms |
| Average trip duration per day | 491.303 ms | 505.720 ms |
| Average fare per borough | 539.199 ms | 509.505 ms |

Both layouts returned identical result signatures. The three assigned queries scan the full range and do not filter by month, so partition pruning does not apply. The observed write and query times are effectively tied, while the partitioned table creates more than twice as many files. The recommended default for this workload is therefore unpartitioned benchmark storage; month partitioning should be reconsidered only for genuinely month-filtered or retention-driven workloads. Full method and results are provided separately in `docs/benchmark_report.md`.

## 7. Limitations and Reproducibility

The source offers no proven globally unique taxi-trip ID, so `trip_id` is audit-only. Weather timezone and geography are not declared. Negative taxi amounts may be valid reversals and are retained with flags. PM2.5 is a city-level estimate rather than pickup-zone exposure. These limitations are visible in the output rather than hidden by imputation or deletion.

Reproduction instructions and environment setup are in `README.md` and `docs/local_environment.md`. Machine-readable evidence is retained in `artifacts/data_profile.json`, `artifacts/ingestion_verification.json`, `artifacts/integration_verification.json`, and `artifacts/benchmark_report.json`.
