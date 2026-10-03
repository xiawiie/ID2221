# Week 3 Task 3: Platform Monitoring Design

## 1. Objective

A production data platform must expose operational status continuously. Task 3 adds a monitoring layer that records metadata for every pipeline execution, stores it in Delta tables, and answers common maintenance questions with Spark SQL.

Verified evidence: `artifacts/week3_monitoring_report.json` (generated 2026-09-23 after seeding `pipeline_runs` via incremental update and product refresh commands).

## 2. Architecture

```text
ingest / incremental / integrate / products / product_refresh
        |
        v
  monitoring.py  (record_pipeline_run, record_validation_events)
        |
        +--> lakehouse/metadata/pipeline_runs        (one row per execution)
        +--> lakehouse/metadata/validation_events    (optional rule-level detail)
        |
        v
  register_monitoring_views()  -->  temp view monitoring_runs
        |
        +--> legacy ingestion_runs (deduplicated by run_id when pipeline_runs exists)
        |
        v
  monitor --report / monitor --query <name>
```

Every pipeline stage writes a single append-only row. Legacy Week 1 `ingestion_runs` remains for backward compatibility; the monitoring view prefers `pipeline_runs` and excludes duplicate `run_id` values from the legacy table.

## 3. Recorded Fields

Each row in `pipeline_runs` captures the minimum fields required by the assignment:

| Field | Meaning |
| --- | --- |
| `run_id` | Unique execution identifier |
| `pipeline_type` | Stage: `ingest`, `incremental`, `integrate`, `products`, `product_refresh` |
| `target_key` / `target_name` | Dataset or product affected |
| `schema_version` | Contract version at execution time |
| `rows_processed` | Input rows evaluated |
| `rows_inserted` | Rows written or newly integrated |
| `rows_rejected` | Rows quarantined or structurally rejected |
| `rows_duplicates_ignored` | Duplicate records skipped (incremental taxi/weather/air) |
| `validation_failure_count` | Aggregate validation issues (quarantine + warnings + duplicates) |
| `started_at`, `finished_at`, `duration_ms` | Execution timing |
| `status` | `success`, `skipped`, or `failed` |
| `error_message` | Failure detail when applicable |

Rule-level detail (for example `quarantined_records`, `duplicate_records_ignored`) is stored in `validation_events` when counts are non-zero.

## 4. Pipeline Coverage

| Pipeline | Module | When recorded |
| --- | --- | --- |
| Full ingestion | `ingest.py` | Success, skip, or failure of each configured dataset |
| Incremental update | `incremental.py` | Bronze/Silver merge, Gold incremental integrate |
| Gold integration | `integrate.py` | Full or incremental `integrated_taxi_trips` build |
| Product materialization | `products.py` | Full product overwrite |
| Selective refresh | `product_refresh.py` | Partition replace or full partition refresh |

Skipped executions (same source hash and schema version) are still recorded with `status=skipped`, which supports idempotency auditing.

## 5. Monitoring Queries

Four Spark SQL queries answer the assignment questions via the `monitoring_runs` view:

| Query name | Assignment question |
| --- | --- |
| `validation_failures_by_dataset` | Which dataset fails validation most frequently? |
| `longest_processing_by_target` | Which dataset requires the longest processing time? |
| `rejected_records_by_execution` | How many records were rejected during each execution? |
| `processing_time_trend` | How has processing time changed over multiple executions? |

CLI usage:

```powershell
.\scripts\run_win.ps1 monitor --report
.\scripts\run_win.ps1 monitor --query validation_failures_by_dataset
```

## 6. Discussion

### Which operational metrics are most useful?

The most actionable metrics for this platform are:

1. **`duration_ms` by `pipeline_type` and `target_key`** — identifies slow stages (for example incremental taxi merge vs. product refresh) and supports capacity planning.
2. **`rows_rejected` and `validation_failure_count`** — surfaces data-quality regressions when new source files arrive or schema evolution is misconfigured.
3. **`rows_inserted` vs. `rows_processed`** — distinguishes healthy incremental loads from runs that mostly deduplicate or skip.
4. **`status` and `schema_version`** — shows whether failures correlate with contract changes.
5. **`rows_duplicates_ignored`** — confirms incremental deduplication is working for taxi trip fingerprints and hourly keys.

Aggregate counters alone are insufficient; trend queries (`processing_time_trend`) reveal performance drift after platform changes.

### How could this information support debugging and system maintenance?

- **Failed or skipped runs:** Filter `monitoring_runs` by `status != 'success'` and inspect `error_message` to locate the first failing stage without re-running the full pipeline.
- **Validation spikes:** Join `validation_events` to `pipeline_runs` on `run_id` to see whether quarantine growth comes from duplicate trips, invalid timestamps, or schema mismatch.
- **Incremental correctness:** Compare `rows_inserted` on `taxi_trips_update` with manifest expectations; large `rows_duplicates_ignored` with zero inserts indicates a re-delivered file rather than new data.
- **Refresh scope:** `product_refresh` rows list affected partitions implicitly through row counts and timing; unexpected full refresh duration suggests the dependency resolver selected too many months.
- **Historical baseline:** Multiple executions of the same `target_key` make it possible to detect slowdowns after validation or monitoring hooks are added (Task 5 evaluation).

### What additional monitoring information would be valuable in a production system?

Production deployments would typically extend this design with:

| Category | Examples |
| --- | --- |
| Data freshness | Time since last successful insert per dataset; SLA alerts when Gold or products lag source delivery |
| Resource usage | Spark stage duration, shuffle bytes, executor memory spill — not visible in row counts alone |
| Lineage | Upstream `source_sha256`, output Delta table paths, and product `run_id` propagation (partially present today) |
| Alerting | Thresholds on `validation_failure_count`, consecutive failures, or duration anomalies |
| Cost | Storage growth of quarantine and metadata tables; partition file counts after incremental appends |
| Schema drift | Explicit unsupported-change events separate from supported evolution (`humidity`, `aqi`) |
| End-to-end traces | Correlating ingest, integrate, and refresh runs triggered by the same municipal delivery batch |

The current implementation deliberately keeps monitoring append-only and queryable in Spark SQL so it can run on the same lakehouse without external services; production would add scheduled `monitor --report` jobs and export to an observability stack.

## 7. Engineering Decisions

- **Separate `pipeline_runs` from legacy `ingestion_runs`:** avoids breaking Week 1 verification while unifying all pipeline stages under one schema.
- **JSON staging + Delta append:** matches the existing ingestion metadata pattern and works on Windows without JDBC.
- **Deduplication in the monitoring view:** prevents double-counting when both tables contain the same `run_id`.
- **Validation events as a child table:** keeps the main run row compact while supporting drill-down by rule name.

## 8. Trade-offs

| Choice | Benefit | Cost |
| --- | --- | --- |
| Append-only Delta metadata | Simple audit trail, time-travel compatible | Table grows; compaction policy needed at scale |
| Unified view over two tables | Backward compatible with Week 1 runs | Slightly more complex SQL registration |
| Synchronous recording in pipeline | Immediate consistency for `monitor --report` | Small write overhead per run (measured in Task 5) |
| Rule names as free text | Easy to extend without schema migration | Less strict than a normalized rule catalog |
