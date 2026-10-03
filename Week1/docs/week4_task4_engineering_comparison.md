# Week 4 Task 4 Engineering Comparison

## Experiment

Task 4 compares two ways to produce the same Week 4 Task 1 training dataset for `hourly_taxi_demand_by_zone`:

| Approach | Starting point | Implementation |
| --- | --- | --- |
| **A** | Raw TLC taxi Parquet, zone CSV, weather CSV, PM2.5 CSV | `src/urban_data/ml/approach_a.py` |
| **B** | Integrated Delta table `gold/integrated_taxi_trips` | `src/urban_data/ml/training_dataset.py` |

Both paths derive the same semantic columns: target `trip_count`, feature columns, lag history, and chronological train/validation/test labels.

## Command

```powershell
.\scripts\run_win.ps1 compare-ml-approaches --problem hourly_taxi_demand_by_zone
```

Optional:

```powershell
.\scripts\run_win.ps1 compare-ml-approaches --skip-approach-b-refresh
```

Results are written to `artifacts/week4_approach_comparison.json`.

## Comparison Dimensions

| Dimension | Approach A | Approach B |
| --- | --- | --- |
| Implementation complexity | More ETL steps and bespoke code | Fewer steps; reuses integrated Gold |
| Preprocessing complexity | Reloads, cleans, joins four raw sources | Aggregation over enriched Gold only |
| Dataset build time | Measured end-to-end in milliseconds | Measured end-to-end in milliseconds |
| Reproducibility | Ad hoc script, no ingestion lineage | Config-driven builder over validated Delta outputs |

## Expected Findings

- **Week 1-3 platform work pays off before feature engineering:** Approach B avoids repeating joins and validation already captured in Gold.
- **Most useful integrated inputs:** zone attributes, weather context, PM2.5 context attached once in Gold and reused by ML.
- **Reusable workflow:** Task 1 builder, Task 2 feature pipeline, and Task 3 retrain commands compose without rewriting raw ETL.
- **Future datasets:** add config + integration once, then extend `ml_training.yml` rather than duplicating Approach A logic.

## Measured Example (2026-10-03, Windows local)

| Metric | Approach A | Approach B |
| --- | ---: | ---: |
| Implementation steps | 10 | 5 |
| Source lines (single module) | 289 | 192 |
| Dataset build time | 8.4 s | 36.7 s |
| Full split rows | 192,544 | 254,586 |

Approach B took longer in this run because Task 1 also materializes train, validation, and test Delta tables, while Approach A persisted only the full comparison table. Build-time alone is therefore a secondary signal; implementation and preprocessing complexity favor the platform path.

## Row-Count Note

Approach A applies a self-contained raw cleaning policy (valid times, positive distance, known zone IDs). Approach B uses Week 1-3 accepted Gold rows with the full validation framework. Row-count differences are expected and should be interpreted as validation-policy differences, not as a failure of the comparison experiment.
