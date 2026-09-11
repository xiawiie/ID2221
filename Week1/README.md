# ID2221 Urban Data Integration Platform

This is the complete Week 1 Spark + Delta project. It ingests and validates three taxi months, taxi zones, hourly weather, and NYC hourly PM2.5; builds the integrated Gold table; and benchmarks two taxi storage layouts.

## Verified Result

- Bronze and Silver ingestion: 9,554,778 taxi source rows, 9,551,977 accepted rows, 2,801 quarantined rows.
- Dimension and environment tables: 265 zones, 8,784 weather hours, and 8,784 NYC PM2.5 hours.
- Gold `integrated_taxi_trips`: 9,551,977 rows, one row per accepted taxi trip.
- Zone matching is complete. Weather and PM2.5 each have 19 unmatched historical out-of-2024 trips; NULL values and match statuses are retained.
- Benchmark: the unpartitioned layout is faster for all three assigned full-range queries. See [benchmark report](docs/benchmark_report.md).

## Requirements

- Windows with WSL Ubuntu
- OpenJDK 17 available inside WSL
- Python 3.12 support in WSL
- Approximately 4 GB memory available to the Spark Driver
- Project datasets under `datasets/`; source files are read-only inputs

The profiling utility also needs a host Python with pandas and PyArrow. The Spark pipeline itself uses the isolated WSL environment.

## Setup

```powershell
wsl bash -lc "cd /mnt/c/Users/goahe/Desktop/ID2221/Week1 && bash scripts/bootstrap_wsl.sh"
```

This creates `.venv-wsl` and installs the locked PySpark, Delta, and PyYAML versions.

## Run

Run the standard checks and pipeline from PowerShell:

```powershell
wsl bash -lc "cd /mnt/c/Users/goahe/Desktop/ID2221/Week1 && PYTHONPATH=src .venv-wsl/bin/python -m unittest discover -s tests"
wsl bash -lc "cd /mnt/c/Users/goahe/Desktop/ID2221/Week1 && bash scripts/run_wsl.sh ingest --dataset all"
wsl bash -lc "cd /mnt/c/Users/goahe/Desktop/ID2221/Week1 && PYTHONPATH=src .venv-wsl/bin/python scripts/verify_ingestion.py"
wsl bash -lc "cd /mnt/c/Users/goahe/Desktop/ID2221/Week1 && bash scripts/run_wsl.sh integrate"
wsl bash -lc "cd /mnt/c/Users/goahe/Desktop/ID2221/Week1 && PYTHONPATH=src .venv-wsl/bin/python scripts/verify_integration.py"
wsl bash -lc "cd /mnt/c/Users/goahe/Desktop/ID2221/Week1 && bash scripts/run_wsl.sh benchmark"
```

Ingestion is idempotent for an unchanged source hash and schema version. `--force` rebuilds a selected dataset deliberately. The benchmark command materializes two comparison tables and runs one warmup plus three measured executions per query.

Optional source profiling, using a host Python with pandas and PyArrow:

```powershell
python scripts/profile_data.py
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

- [Design report](docs/design_report.md)
- [Benchmark report](docs/benchmark_report.md)
- [Assignment translation and implementation details](docs/assignment_and_implementation.md)
- [Machine-readable data profile](artifacts/data_profile.json)
- [Task plan](task_plan.md)
- [Findings](findings.md)
- [Progress](progress.md)

The sibling `C:/Users/goahe/Desktop/ID2221/datasets` directory is a retained mirror. This project does not read, modify, delete, or synchronize it.
