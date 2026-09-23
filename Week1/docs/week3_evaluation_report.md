# Week 3 Task 5 Platform Evaluation Report

Evidence artifact: `artifacts/week3_platform_evaluation.json` (regenerate with `.\scripts\run_win.ps1 evaluate-platform`).

## 1. Objective

Task 5 measures whether the Week 3 extensions preserve production readiness: incremental processing time, selective analytical refresh time, operational storage overhead, and the runtime cost of validation and monitoring hooks.

## 2. Measurement Protocol

| Metric | Method |
| --- | --- |
| Incremental update time | Latest successful `incremental` run for `taxi_trips_update` from `monitoring_runs` |
| Analytical refresh time | Sum of latest successful `product_refresh` duration per product |
| Storage overhead | Delta `sizeInBytes` for metadata + quarantine vs core lakehouse tables |
| Validation overhead | 100k-row taxi sample: median runtime with zero rules vs all enabled rules (3 runs each) |
| Monitoring overhead | Median runtime of three synthetic `record_pipeline_run` appends |

Spark session: Conda `id2221-week1`, local mode, existing lakehouse after Tasks 1–4.

## 3. Results Summary (2026-09-23, Windows local)

| Metric | Measurement |
| --- | --- |
| Incremental taxi update | Latest insert run for `taxi_trips_update` from `monitoring_runs` (see artifact; historical full merge up to ~425s) |
| Analytical refresh | **30.9 s** total across 4 products (partition refresh for 2024-04/05) |
| Storage overhead | **0.026%** of lakehouse bytes (metadata + quarantine vs core tables) |
| Validation overhead | **3.16 s** median incremental cost on 100k-row sample (~91% of validation pass; dominated by rule column evaluation + counts) |
| Monitoring overhead | **703 ms** median per `record_pipeline_run` append |

Storage breakdown: core analytical ~2.24 GB; operational overhead ~571 KB (metadata 136 KB, quarantine 435 KB).

Product refresh detail: `air_quality_impact_summary` 13.0s, `daily_mobility_summary` 6.2s, `taxi_zone_monthly_statistics` 5.9s, `weather_impact_summary` 5.8s.

## 4. Discussion

### Which Week 1 design decisions simplified maintenance?

- **YAML-driven datasets** let Week 3 add incremental keys and schema version 2 without new CLI entry points per file.
- **Medallion + quarantine** provided the isolation model Task 4 extended into a rule engine.
- **`source_file_month` partitioning** enabled incremental Silver replace and selective product refresh by month.
- **`ingestion_runs` lineage** evolved into unified `pipeline_runs` monitoring with backward-compatible views.

### Which components required the largest modifications?

1. **Incremental ingest and Gold append** (merge keys, dedup, dynamic partitions).
2. **Selective product refresh** (dependency graph, partition replace vs full-partition CORR recomputation).
3. **Validation rule engine** (registry + configurable rule sets).
4. Monitoring layered on existing hooks with modest incremental code.

### How well does the platform support future datasets?

Adding a dataset requires: YAML config, schema definitions, optional transform/split wiring, and validation rule names in `validation_rules.yml`. Incremental behavior reuses merge patterns when a business key exists. Product impact is declared in `analytics.yml` dependencies rather than hard-coded refresh lists.

### If redesigned today?

- Unify run metadata schema from Week 1 (avoid dual ingestion_runs + pipeline_runs migration).
- Declare analytical dependencies in Week 2 before building full product overwrites.
- Keep validation rules fully data-driven where possible; use Spark SQL expressions in config for simple masks.
- Separate operational metadata tables early to simplify overhead accounting.

### Evidence-based conclusion

The platform remains operable after Week 3 extensions: incremental updates avoid full lakehouse rebuilds, selective refresh limits product recomputation, validation and monitoring add measurable but comparatively small overhead on this hardware, and operational tables remain a bounded fraction of total storage. Production deployment would still require scheduled evaluation runs, alerting on quarantine growth, and compaction policies for metadata tables.

## 5. Reproduction

```powershell
conda activate id2221-week1
cd Week1
.\scripts\run_win.ps1 evaluate-platform
```

Related reports:

- `artifacts/week3_monitoring_report.json`
- `artifacts/week3_validation_report.json`
