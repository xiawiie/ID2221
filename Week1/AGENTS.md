# ID2221 Project Instructions

## Project

This directory is the completed project root for the Week 1 urban data integration assignment. Preserve the smallest reliable Spark + Delta implementation that ingests, validates, standardizes, integrates, and benchmarks the supplied datasets.

## Current State

- Raw-input integrity checks, profiling, generic ingestion, quarantine, metadata, Gold integration, verification, and storage benchmarking are implemented.
- Verified runtime: WSL Ubuntu, OpenJDK 17, Python 3.12, PySpark 3.5.7, delta-spark 3.3.2.
- Gold has 9,551,977 rows and matches accepted Silver taxi rows.
- Machine-readable evidence is under `artifacts/`; final reports are under `docs/`.

## Commands

- Test config: `PYTHONPATH=src .venv-wsl/bin/python -m unittest discover -s tests`
- Ingest all configured datasets: `bash scripts/run_wsl.sh ingest --dataset all`
- Verify ingestion: `PYTHONPATH=src .venv-wsl/bin/python scripts/verify_ingestion.py`
- Integrate Gold: `bash scripts/run_wsl.sh integrate`
- Verify Gold: `PYTHONPATH=src .venv-wsl/bin/python scripts/verify_integration.py`
- Run storage benchmark: `bash scripts/run_wsl.sh benchmark`

## Layout

- `datasets/` contains immutable project source inputs.
- `C:/Users/goahe/Desktop/ID2221/datasets` is a retained mirror, not a runtime dependency. Do not modify, delete, or auto-sync it.
- `src/urban_data/` contains ingestion, integration, benchmarking, schemas, configuration, and Spark setup.
- `scripts/` contains setup, running, profiling, and verification utilities.
- `lakehouse/` contains generated Bronze, Silver, Gold, quarantine, metadata, and benchmark outputs.
- `artifacts/` contains generated evidence; `docs/` contains the reports.

## Engineering Rules

- Never overwrite, rename, move, or delete source datasets without explicit user approval.
- Resolve data paths relative to the project root.
- Treat `air_quality.zip` as the authoritative download and the verified CSV as the runtime extraction cache.
- Keep Spark timestamps deterministic: process `TZ`, SQL session timezone, and Driver JVM timezone are UTC.
- Use explicit schemas, lowercase `snake_case`, documented timestamp semantics, and NULL for missing values.
- Keep shared ingestion logic generic; keep dataset-specific transformations explicit and small.
- Preserve one Gold row per accepted taxi trip and assert row counts after every join.
- Quarantine invalid records with reasons; do not silently drop records or impute missing environmental values.
- Partition taxi data by stable `source_file_month`; do not dynamically overwrite on business-derived `pickup_month`.
- Measure partition choices rather than assuming they improve performance.
