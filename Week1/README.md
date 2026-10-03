# ID2221 Urban Data Integration Platform

This Spark + Delta project covers Week 1 data integration, Week 2 analytics, Week 3 operations, and Week 4 machine learning. It ingests and validates three taxi months, taxi zones, hourly weather, and NYC hourly PM2.5; builds the integrated Gold table; materializes analytical products; supports incremental updates and monitoring; and trains a reusable Spark MLlib pipeline for hourly taxi demand by pickup zone.

The verified runtime on this machine is **Windows native** (Conda + OpenJDK 17). A legacy WSL workflow remains available under `scripts/run_wsl.sh` and `scripts/bootstrap_wsl.sh`, but the commands below are the ones used for the current artifacts.

## Verified Result

- Bronze and Silver ingestion: 9,554,778 taxi source rows, 9,551,977 accepted rows, 2,801 quarantined rows.
- Dimension and environment tables: 265 zones, 8,784 weather hours, and 8,784 NYC PM2.5 hours.
- Gold `integrated_taxi_trips`: 9,551,977 rows, one row per accepted taxi trip.
- Zone matching is complete. Weather and PM2.5 each have 19 unmatched historical out-of-2024 trips; NULL values and match statuses are retained.
- Benchmark (Windows, 2026-09-12): full-range query latency is effectively tied between layouts; unpartitioned storage keeps fewer, larger files. See [benchmark report](docs/benchmark_report.md).
- Week 4 ML (Windows, 2026-10-03): 254,586 zone-hour training rows; GBT test metrics RMSE 15.04, MAE 6.96, R² 0.924; beats a 24-hour lag baseline. See [Week 4 Task 3 ML pipeline design](docs/week4_task3_ml_pipeline.md).

## Requirements

- Windows 10/11
- Conda (Anaconda or Miniconda)
- Approximately 4 GB available to the Spark Driver
- The six source files listed in `datasets/SHA256SUMS.txt`, placed directly in `datasets/`

The source datasets are read-only inputs. The pipeline writes generated Delta tables under `lakehouse/` and verification results under `artifacts/`.

## Setup

Open PowerShell in this `Week1` directory. The commands below use only relative paths, so the repository may live anywhere on the machine.

```powershell
conda create -n id2221-week1 python=3.12 openjdk=17 -y
conda activate id2221-week1
python -m pip install -r requirements.txt
```

The repository includes Windows Hadoop helpers under `tools/hadoop/` (`winutils.exe`). The Windows launcher configures `HADOOP_HOME`, `JAVA_HOME`, `PYTHONPATH`, and Spark temporary directories from the active Conda environment. For a non-Conda setup, set `ID2221_PYTHON` to `python.exe` and `ID2221_JAVA_HOME` to the JDK home before running the launcher.

## Dataset Setup

Download the following files from the course data folder and place them directly under `datasets/`:

```text
air_quality.zip
taxi_zone_lookup.csv
weather.csv
yellow_tripdata_2024-01.parquet
yellow_tripdata_2024-02.parquet
yellow_tripdata_2024-03.parquet
```

Validate the downloads before running the pipeline:

```powershell
Get-Content datasets\SHA256SUMS.txt | ForEach-Object {
    $expected, $name = $_ -split '\s+', 2
    $actual = (Get-FileHash (Join-Path datasets $name) -Algorithm SHA256).Hash
    if ($actual -ne $expected) { throw "Checksum mismatch: $name" }
    Write-Host "Verified: $name"
}
```

## Run

From the active `id2221-week1` environment and this project directory, run the full reproduction sequence:

```powershell
# Unit and configuration tests
$env:PYTHONPATH = (Join-Path (Get-Location) "src")
python -m unittest discover -s tests

# Ingest and verify all configured datasets
.\scripts\run_win.ps1 ingest --dataset all
python .\scripts\verify_ingestion.py

# Build and verify the Gold table
.\scripts\run_win.ps1 integrate
python .\scripts\verify_integration.py

# Compare two Taxi Trips storage layouts
.\scripts\run_win.ps1 benchmark
```

Ingestion skips an unchanged source hash and schema version, so rerunning the command is idempotent. Use `--force` only when deliberately rebuilding a selected dataset. The benchmark command creates both comparison layouts and uses one warm-up plus three measured runs per query.

For targeted debugging, ingest one dataset at a time:

```powershell
.\scripts\run_win.ps1 ingest --dataset taxi_2024_01
.\scripts\run_win.ps1 ingest --dataset weather
```

Optional source profiling requires pandas and PyArrow:

```powershell
python .\scripts\profile_data.py
```

## Week 2 Analytics

After the Week 1 `integrate` command has built Gold data, run any of the six analytical Spark SQL queries:

```powershell
.\scripts\run_win.ps1 query --name monthly_taxi_demand_by_zone
.\scripts\run_win.ps1 products --product all
.\scripts\run_win.ps1 benchmark-analytics
```

`query` accepts `monthly_taxi_demand_by_zone`, `average_trip_distance_by_weather`, `air_quality_taxi_demand_relationship`, `zone_weather_demand_variation`, `peak_travel_hours_by_day_of_week`, and `monthly_taxi_demand_trends`. `products` refreshes four Delta data products with source, refresh, and schema metadata. The optimization benchmark takes one warm-up plus three measured runs, verifies result equivalence, and saves timing data plus `EXPLAIN FORMATTED` plans under `artifacts/`.

## Week 3 Operations

Week 3 extends the platform with incremental updates, selective analytical refresh, and operational monitoring. Run these commands after Week 1 ingestion and Week 2 product materialization.

```powershell
# Task 1: generate incremental update files and manifest
.\scripts\run_win.ps1 generate-updates

# Task 1: apply incremental Bronze/Silver/Gold updates (idempotent; skips unchanged hashes)
.\scripts\run_win.ps1 update --dataset all

# Task 2: refresh only analytical products affected by new data
.\scripts\run_win.ps1 refresh-products --dataset taxi_trips_update
.\scripts\run_win.ps1 refresh-products --dataset latest

# Task 3: platform monitoring
.\scripts\run_win.ps1 monitor --report
.\scripts\run_win.ps1 monitor --query validation_failures_by_dataset
.\scripts\run_win.ps1 monitor --query longest_processing_by_target
.\scripts\run_win.ps1 monitor --query rejected_records_by_execution
.\scripts\run_win.ps1 monitor --query processing_time_trend

# Task 4: validation summary report
.\scripts\run_win.ps1 validate-report

# Task 5: platform evaluation
.\scripts\run_win.ps1 evaluate-platform
```

Use `--force` on `update` only when deliberately re-applying the same update file. Incremental taxi updates append new trips to Silver and Gold without rebuilding the full lakehouse. Product refresh uses partition replace for most products and full-partition recompute where correlation requires it (see `config/analytics.yml`).

Monitoring records every pipeline execution (ingest, incremental, integrate, products, product refresh) into Delta metadata tables. A full JSON report is written to `artifacts/week3_monitoring_report.json` when you run the Python API or redirect CLI output. Design rationale and discussion questions are documented in [Week 3 monitoring design](docs/week3_monitoring_design.md).

The validation framework quarantines invalid rows, records rule-level events, and summarizes quarantine totals via `validate-report`. Rule sets live in `config/validation_rules.yml`; design notes are in [Week 3 validation design](docs/week3_validation_design.md).

Platform evaluation (`evaluate-platform`) measures incremental update time, product refresh time, storage overhead, and validation/monitoring micro-benchmarks. Results are saved to `artifacts/week3_platform_evaluation.json`; see [Week 3 evaluation report](docs/week3_evaluation_report.md).

Week 3 unit tests:

```powershell
$env:PYTHONPATH = (Join-Path (Get-Location) "src")
python -m unittest tests.test_week3_task1 tests.test_week3_task2 tests.test_week3_task3 tests.test_week3_task4 tests.test_week3_task5
```

## Outputs

- `lakehouse/bronze/`: raw-value Delta tables with source, hash, run ID, and schema lineage
- `lakehouse/silver/`: standardized taxi, zone, weather, and PM2.5 tables
- `lakehouse/quarantine/`: rejected records and reasons
- `lakehouse/gold/integrated_taxi_trips/`: integrated one-row-per-trip table
- `lakehouse/metadata/ingestion_runs/`: Week 1 ingestion execution statistics and lineage
- `lakehouse/metadata/pipeline_runs/`: Week 3 unified pipeline monitoring (timing, row counts, schema version, validation failures)
- `lakehouse/metadata/validation_events/`: optional rule-level validation failure detail per run
- `datasets/updates/`: generated incremental update files and `manifest.json` (Task 1)
- `lakehouse/benchmark/`: unpartitioned and month-partitioned comparison tables
- `lakehouse/products/`: materialized Week 2 analytical Delta products with source, refresh, and schema metadata
- `artifacts/`: machine-readable profile, ingestion, integration, benchmark, and plan evidence
- `artifacts/week2_optimization_benchmark.json`: timing, equivalence, and storage-overhead measurements
- `artifacts/week2_optimization_plans.json`: `EXPLAIN FORMATTED` output for every compared query variant
- `artifacts/week3_monitoring_report.json`: monitoring summary and four operational SQL query results
- `artifacts/week3_validation_report.json`: validation rule catalog and quarantine summary
- `artifacts/week3_platform_evaluation.json`: Task 5 platform evaluation measurements
- `lakehouse/ml/hourly_zone_demand/`: Week 4 Task 1 training splits (`full`, `train`, `validation`, `test`)
- `lakehouse/ml/hourly_zone_demand_featurized/`: Week 4 Task 2 ML-ready splits with `label` and `features`
- `lakehouse/ml/models/hourly_zone_demand/`: saved feature pipeline and GBT regressor
- `lakehouse/ml/hourly_zone_demand_predictions/`: validation and test prediction tables
- `lakehouse/ml/hourly_zone_demand_approach_a/`: Task 4 raw-path comparison dataset
- `artifacts/week4_training_dataset.json`: Task 1 dataset summary
- `artifacts/week4_feature_pipeline.json`: Task 2 feature pipeline summary
- `artifacts/week4_model_evaluation.json`: Task 3 model metrics and baseline comparison
- `artifacts/week4_approach_comparison.json`: Task 4 Approach A vs B comparison

## Documentation

- [Local environment setup record](docs/local_environment.md)
- [Design report](docs/design_report.md)
- [Benchmark report](docs/benchmark_report.md)
- [Week 3 monitoring design (Task 3)](docs/week3_monitoring_design.md)
- [Week 3 validation design (Task 4)](docs/week3_validation_design.md)
- [Week 3 evaluation report (Task 5)](docs/week3_evaluation_report.md)
- [Week 4 Task 1 training dataset design](docs/week4_task1_training_dataset_design.md)
- [Week 4 Task 2 feature engineering design](docs/week4_task2_feature_engineering.md)
- [Week 4 Task 3 ML pipeline design](docs/week4_task3_ml_pipeline.md)
- [Week 4 Task 4 engineering comparison](docs/week4_task4_engineering_comparison.md)
- [Week 2 analytical query and product definitions](config/analytics.yml)
- [Week 4 ML training and model configuration](config/ml_training.yml)
- [Assignment translation and implementation details](docs/assignment_and_implementation.md)
- [Machine-readable data profile](artifacts/data_profile.json)
- [Task plan](task_plan.md)
- [Findings](findings.md)
- [Progress](progress.md)

## Week 4 Machine Learning

Week 4 adds a reproducible Spark MLlib workflow on top of the integrated platform. The selected prediction problem is **hourly taxi demand by pickup zone**: predict `trip_count`, the number of accepted pickups in each TLC zone for each New York local hour.

Prerequisites:

- Week 1 `integrate` has built `lakehouse/gold/integrated_taxi_trips/`
- Conda environment includes `numpy` from `requirements.txt` (required by PySpark MLlib)
- Raw files used by Task 4 Approach A must exist under `datasets/` (same six files as Week 1)

Configuration lives in `config/ml_training.yml`:

- `problems.hourly_taxi_demand_by_zone`: target, feature columns, lag hours, chronological split dates
- `feature_engineering`: imputer, scaling, metadata columns, featurized output paths
- `model`: GBT hyperparameters, evaluation metrics, baseline definition

### Full reproduction sequence

Run from the active `id2221-week1` environment and this project directory:

```powershell
# Prerequisites: Week 1 Gold table must already exist
# .\scripts\run_win.ps1 integrate

# Task 1: build training dataset from integrated Gold
.\scripts\run_win.ps1 build-ml-dataset --problem hourly_taxi_demand_by_zone

# Task 2: fit feature pipeline on train and featurize all splits
.\scripts\run_win.ps1 featurize-ml-dataset --problem hourly_taxi_demand_by_zone

# Task 3: train GBTRegressor, evaluate, save model
.\scripts\run_win.ps1 train-ml-model --problem hourly_taxi_demand_by_zone

# Task 4: compare raw-path vs platform-path dataset construction
.\scripts\run_win.ps1 compare-ml-approaches --problem hourly_taxi_demand_by_zone
```

Task 1 writes zone-hour rows with target `trip_count`, 15 feature columns, and chronological splits:

| Split | Date range |
| --- | --- |
| Train | through 2024-02-29 |
| Validation | 2024-03-01 through 2024-03-15 |
| Test | from 2024-03-16 |

Verified row counts (2026-10-03): train 115,133; validation 41,453; test 98,000; full 254,586 after lag filtering.

Task 2 applies a reusable Spark ML pipeline:

1. `StringIndexer` on categorical zone and month columns
2. `Imputer` with median strategy on numeric weather, PM2.5, temporal, and lag columns
3. `VectorAssembler`
4. `StandardScaler`

The fitted feature pipeline is saved to `lakehouse/ml/models/hourly_zone_demand/feature_pipeline`. Featurized splits contain metadata columns, `label`, and dense `features`.

Task 3 trains `GBTRegressor` on the featurized train split, evaluates RMSE, MAE, and R² on validation and test, and compares against a seasonal naive baseline that predicts `demand_lag_24h`. Verified test metrics (2026-10-03): RMSE 15.04, MAE 6.96, R² 0.924.

Task 4 builds the same semantic dataset twice:

- **Approach A:** directly from raw taxi Parquet, zone CSV, weather CSV, and PM2.5 CSV
- **Approach B:** from integrated Gold via the Task 1 platform builder

Use `--skip-approach-b-refresh` to reuse an existing Approach B dataset and skip its rebuild timing.

### Retrain when new data arrives

After Week 3 incremental updates refresh Gold, rerun the ML workflow:

```powershell
# Option A: refresh Task 1 splits from Gold, then refit features and model
.\scripts\run_win.ps1 retrain-ml-model --rebuild-dataset

# Option B: manual step-by-step
.\scripts\run_win.ps1 build-ml-dataset --problem hourly_taxi_demand_by_zone
.\scripts\run_win.ps1 featurize-ml-dataset --problem hourly_taxi_demand_by_zone
.\scripts\run_win.ps1 train-ml-model --problem hourly_taxi_demand_by_zone
```

`retrain-ml-model` always refits the feature pipeline and retrains the regressor. Add `--rebuild-dataset` when Gold changed and Task 1 splits must be regenerated first.

### Week 4 CLI reference

| Command | Purpose |
| --- | --- |
| `build-ml-dataset --problem hourly_taxi_demand_by_zone` | Task 1 dataset from Gold |
| `featurize-ml-dataset --problem hourly_taxi_demand_by_zone` | Task 2 feature pipeline |
| `train-ml-model [--model-type gbt_regressor]` | Task 3 train and evaluate |
| `retrain-ml-model [--rebuild-dataset]` | Refit features and retrain |
| `compare-ml-approaches [--skip-approach-b-refresh]` | Task 4 engineering comparison |

Source modules:

- `src/urban_data/ml/training_dataset.py` — Task 1 platform builder
- `src/urban_data/ml/features.py` — Task 2 feature pipeline
- `src/urban_data/ml/train.py` — Task 3 training and evaluation
- `src/urban_data/ml/approach_a.py` — Task 4 raw-path builder
- `src/urban_data/ml/compare.py` — Task 4 comparison experiment

Week 4 unit tests:

```powershell
$env:PYTHONPATH = (Join-Path (Get-Location) "src")
python -m unittest tests.test_week4_task1 tests.test_week4_task2 tests.test_week4_task3 tests.test_week4_task4
```

## Legacy WSL Workflow

If WSL Ubuntu is available, the original commands still work after replacing the path with your WSL mount point:

```powershell
wsl bash -lc "cd /mnt/d/DD2221/ID2221-main/Week1 && bash scripts/bootstrap_wsl.sh"
wsl bash -lc "cd /mnt/d/DD2221/ID2221-main/Week1 && bash scripts/run_wsl.sh ingest --dataset all"
```

The Windows-native workflow above is the one used to generate the current `artifacts/*.json` results.
