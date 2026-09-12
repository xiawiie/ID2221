# ID2221 Project Instructions

## Project

This directory is the completed project root for the Week 1 urban data integration assignment. Preserve the smallest reliable Spark + Delta implementation that ingests, validates, standardizes, integrates, and benchmarks the supplied datasets.

## Current State

- Raw-input integrity checks, profiling, generic ingestion, quarantine, metadata, Gold integration, verification, and storage benchmarking are implemented.
- Verified runtime: Windows 11 native, Conda `id2221-week1`, OpenJDK 17, Python 3.12, PySpark 3.5.7, delta-spark 3.3.2.
- Gold has 9,551,977 rows and matches accepted Silver taxi rows.
- Machine-readable evidence is under `artifacts/`; final reports are under `docs/`.

## Commands

Primary (Windows): see `README.md` and `scripts/run_win.ps1`.

- Test config: `PYTHONPATH=src python -m unittest discover -s tests` (Conda env)
- Ingest all: `scripts/run_win.ps1 ingest --dataset all`
- Verify ingestion: `python scripts/verify_ingestion.py`
- Integrate Gold: `scripts/run_win.ps1 integrate`
- Verify Gold: `python scripts/verify_integration.py`
- Benchmark: `scripts/run_win.ps1 benchmark`

Legacy WSL commands remain in `scripts/run_wsl.sh`.

## Layout

- `datasets/` contains immutable project source inputs.
- Optional data mirror under `D:/DD2221/data/` is not a runtime dependency. Do not modify project inputs without explicit user approval.
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
