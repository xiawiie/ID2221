# ID2221 Urban Data Integration Platform

This is the Week 1 Spark + Delta project. It ingests and validates three taxi months, taxi zones, hourly weather, and NYC hourly PM2.5; builds the integrated Gold table; and benchmarks two taxi storage layouts.

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
- Approximately 4 GB memory available to the Spark Driver
- Project datasets under `datasets/`; source files are read-only inputs

Optional: host Python with pandas and PyArrow for `scripts/profile_data.py`.

## Setup

Create the Conda environment and install locked dependencies:

```powershell
conda create -n id2221-week1 python=3.12 openjdk=17 -y
conda activate id2221-week1
pip install -r "D:\DD2221\ID2221-main\Week1\requirements.txt"
```

The project also includes Windows Hadoop helpers under `tools/hadoop/` (`winutils.exe`). `scripts/run_win.ps1` sets `HADOOP_HOME` and `JAVA_HOME` automatically.

## Run

Run the standard checks and pipeline from PowerShell. Set `JAVA_HOME` once per session if you invoke Python directly:

```powershell
$project = "D:\DD2221\ID2221-main\Week1"
$env:JAVA_HOME = "D:\anaconda3\envs\id2221-week1\Library"

# Config tests
$env:PYTHONPATH = "$project\src"
& "D:\anaconda3\envs\id2221-week1\python.exe" -m unittest discover -s "$project\tests"

# Ingest all configured datasets
& "$project\scripts\run_win.ps1" ingest --dataset all

# Verify ingestion
& "D:\anaconda3\envs\id2221-week1\python.exe" "$project\scripts\verify_ingestion.py"

# Build Gold integrated table
& "$project\scripts\run_win.ps1" integrate

# Verify integration
& "D:\anaconda3\envs\id2221-week1\python.exe" "$project\scripts\verify_integration.py"

# Benchmark two taxi storage layouts
& "$project\scripts\run_win.ps1" benchmark
```

Ingestion is idempotent for an unchanged source hash and schema version. `--force` rebuilds a selected dataset deliberately. The benchmark command materializes two comparison tables and runs one warmup plus three measured executions per query.

Ingest one dataset at a time when debugging:

```powershell
& "$project\scripts\run_win.ps1" ingest --dataset taxi_2024_01
& "$project\scripts\run_win.ps1" ingest --dataset weather
```

Optional source profiling:

```powershell
python "$project\scripts\profile_data.py"
```

## Outputs

- `lakehouse/bronze/`: raw-value Delta tables with source, hash, run ID, and schema lineage
- `lakehouse/silver/`: standardized taxi, zone, weather, and PM2.5 tables
- `lakehouse/quarantine/`: rejected records and reasons
- `lakehouse/gold/integrated_taxi_trips/`: integrated one-row-per-trip table
- `lakehouse/metadata/ingestion_runs/`: execution statistics and lineage
- `lakehouse/benchmark/`: unpartitioned and month-partitioned comparison tables
- `artifacts/`: machine-readable profile, ingestion, integration, benchmark, and plan evidence

## Documentation

- [Local environment setup record](docs/local_environment.md)
- [Design report](docs/design_report.md)
- [Benchmark report](docs/benchmark_report.md)
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
