# ID2221 Project Instructions

## Project

This directory is the project root for the Week 1 generic urban data integration assignment. Build the smallest complete Spark + Delta pipeline that ingests, validates, standardizes, integrates, and benchmarks the supplied datasets.

## Current State

- Raw inputs, integrity checks, data profiling, and design documentation are complete.
- The project data path is `datasets/`, and the profiling command passes after the directory migration.
- The Spark + Delta pipeline for one taxi month (2024-01) is implemented and verified in WSL.
- Java, Spark, PySpark, and Delta are configured for WSL via `.venv-wsl` and `scripts/bootstrap_wsl.sh`.
- The directory is not currently a Git repository.

## Commands

- Rebuild and verify the data profile: `python scripts/profile_data.py`
- WSL Spark ingest (2024-01 taxi): `bash scripts/run_wsl.sh ingest --dataset taxi_2024_01`
- Delta smoke test (WSL): `.venv-wsl/bin/python scripts/smoke_delta.py`

## Stack

- Current profiling: Python, pandas, PyArrow.
- Planned implementation: PySpark and Delta Lake.
- Confirm course-compatible Java, Spark, Delta, and Python versions before adding dependency files.

## Layout

- `datasets/` contains the immutable project source inputs.
- `C:/Users/goahe/Desktop/ID2221/datasets` is a retained mirror, not a runtime dependency. Do not modify, delete, or auto-sync it.
- `scripts/` contains local verification utilities.
- `artifacts/` contains generated machine-readable evidence.
- `docs/` contains the authoritative assignment and implementation design.
- Future runtime data belongs under `lakehouse/`; tests belong under `tests/`.

## Engineering Rules

- Never overwrite, rename, move, or delete source datasets without explicit user approval.
- Resolve all project data paths relative to the project root; do not hardcode the sibling mirror path.
- Treat `air_quality.zip` as the authoritative source and `hourly_88101_2024.csv` as its verified extraction cache.
- Use explicit schemas, lowercase `snake_case`, documented timestamp semantics, and `NULL` for missing values.
- Keep generic ingestion logic shared; keep dataset-specific transformations explicit and small.
- Preserve one output row per accepted taxi trip. Check row counts before and after every join.
- Quarantine invalid records with reasons; do not silently drop them or turn missing values into zero.
- Measure partition choices instead of assuming they improve performance.
- Add only the narrowest test that directly covers each non-trivial rule.

## Next Step

Extend the shared ingest path to taxi zones, weather, and air quality before building Gold integration.
