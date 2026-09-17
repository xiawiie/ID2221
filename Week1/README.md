# ID2221 Urban Data Integration Platform

This Spark + Delta project covers Week 1 data integration and Week 2 analytics. It ingests and validates three taxi months, taxi zones, hourly weather, and NYC hourly PM2.5; builds the integrated Gold table; materializes analytical products; and benchmarks storage and query optimizations.

The verified runtime on this machine is **Windows native** (Conda + OpenJDK 17). A legacy WSL workflow remains available under `scripts/run_wsl.sh` and `scripts/bootstrap_wsl.sh`, but the commands below are the ones used for the current artifacts.

## Verified Result

- Bronze and Silver ingestion: 9,554,778 taxi source rows, 9,551,977 accepted rows, 2,801 quarantined rows.
- Dimension and environment tables: 265 zones, 8,784 weather hours, and 8,784 NYC PM2.5 hours.
- Gold `integrated_taxi_trips`: 9,551,977 rows, one row per accepted taxi trip.
- Zone matching is complete. Weather and PM2.5 each have 19 unmatched historical out-of-2024 trips; NULL values and match statuses are retained.
- Benchmark (Windows, 2026-09-12): full-range query latency is effectively tied between layouts; unpartitioned storage keeps fewer, larger files. See [benchmark report](docs/benchmark_report.md).

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
## Outputs

- `lakehouse/bronze/`: raw-value Delta tables with source, hash, run ID, and schema lineage
- `lakehouse/silver/`: standardized taxi, zone, weather, and PM2.5 tables
- `lakehouse/quarantine/`: rejected records and reasons
- `lakehouse/gold/integrated_taxi_trips/`: integrated one-row-per-trip table
- `lakehouse/metadata/ingestion_runs/`: execution statistics and lineage
- `lakehouse/benchmark/`: unpartitioned and month-partitioned comparison tables
- `lakehouse/products/`: materialized Week 2 analytical Delta products with source, refresh, and schema metadata
- `artifacts/`: machine-readable profile, ingestion, integration, benchmark, and plan evidence
- `artifacts/week2_optimization_benchmark.json`: timing, equivalence, and storage-overhead measurements
- `artifacts/week2_optimization_plans.json`: `EXPLAIN FORMATTED` output for every compared query variant

## Documentation

- [Local environment setup record](docs/local_environment.md)
- [Design report](docs/design_report.md)
- [Benchmark report](docs/benchmark_report.md)
- [Week 2 analytical query and product definitions](config/analytics.yml)
- [Assignment translation and implementation details](docs/assignment_and_implementation.md)
- [Machine-readable data profile](artifacts/data_profile.json)
- [Task plan](task_plan.md)
- [Findings](findings.md)
- [Progress](progress.md)

## Legacy WSL Workflow

If WSL Ubuntu is available, the original commands still work after replacing the path with your WSL mount point:

```powershell
wsl bash -lc "cd /mnt/d/DD2221/ID2221-main/Week1 && bash scripts/bootstrap_wsl.sh"
wsl bash -lc "cd /mnt/d/DD2221/ID2221-main/Week1 && bash scripts/run_wsl.sh ingest --dataset all"
```

The Windows-native workflow above is the one used to generate the current `artifacts/*.json` results.
