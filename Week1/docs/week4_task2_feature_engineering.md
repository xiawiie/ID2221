# Week 4 Task 2 Feature Engineering Design

## Input and Output

| Item | Path |
| --- | --- |
| Input train/validation/test | `lakehouse/ml/hourly_zone_demand/{train,validation,test}` |
| Featurized splits | `lakehouse/ml/hourly_zone_demand_featurized/{train,validation,test}` |
| Saved pipeline model | `lakehouse/ml/models/hourly_zone_demand/feature_pipeline` |
| Summary artifact | `artifacts/week4_feature_pipeline.json` |

Each featurized row contains metadata columns, a double `label`, and a dense MLlib `features` vector.

## Pipeline Stages

The reusable Spark ML `Pipeline` applies the following steps in order:

1. **StringIndexer** on categorical columns: `pickup_zone`, `pickup_borough`, `pickup_service_zone`, `pickup_month`
2. **Imputer** with median strategy on numeric weather, PM2.5, temporal, and lag columns
3. **VectorAssembler** combining indexed categories and numeric columns into `features_raw`
4. **StandardScaler** producing the final `features` vector

One-hot encoding is disabled by default because tree-based models in Task 3 work well with indexed categories; it can be enabled in `config/ml_training.yml` for linear models.

## Feature Selection Rationale

| Feature group | Columns | Why |
| --- | --- | --- |
| Location | zone, borough, service zone | Captures stable spatial demand structure |
| Time | hour, day of week, weekend, month | Models hourly, weekly, and seasonal patterns |
| Environment | weather and PM2.5 numerics | Uses Week 1 integrated context without re-joining raw files |
| Lag demand | 1h, 24h, 168h | Strongest predictors for short-term demand continuity |

Temporal columns are generated in Task 1; Task 2 focuses on encoding, imputation, assembly, and scaling so the same semantic dataset can feed multiple ML algorithms.

## Preprocessing Notes

- **Most preprocessing:** `pickup_zone` indexing and median imputation for sparse environmental nulls.
- **Most valuable features:** lag demand columns, `hour_of_day`, and `pickup_zone`.
- **Removed from model input:** raw identifiers and split labels remain as metadata only; they are not assembled into `features`.

## Extension Points

- Add a column to `categorical_features` or `numeric_features` in `config/ml_training.yml`.
- Toggle `one_hot_encode` or `scale_numeric` without changing Python code.
- Reuse the saved `PipelineModel` during Task 3 retraining on refreshed Task 1 splits.

## Command

```powershell
.\scripts\run_win.ps1 featurize-ml-dataset --problem hourly_taxi_demand_by_zone
```

Run `build-ml-dataset` first if the Task 1 splits are missing or stale.
