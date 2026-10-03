# Week 4 Task 3 ML Pipeline Design

## Workflow

```text
build-ml-dataset
  -> featurize-ml-dataset (fit feature PipelineModel on train)
  -> train-ml-model (fit GBTRegressor on featurized train)
  -> evaluate on validation/test
  -> save regressor + prediction Delta tables
```

Retraining reuses the same commands through `retrain-ml-model`, optionally rebuilding Task 1 splits from Gold when new integrated data arrives.

## Model

| Item | Value |
| --- | --- |
| Algorithm | `GBTRegressor` |
| Target | `label` (`trip_count`) |
| Features | Task 2 `features` vector |
| Seed | 42 |
| Baseline | Seasonal naive using `demand_lag_24h` on raw test split |

Hyperparameters live in `config/ml_training.yml` under `model.types.gbt_regressor.params`.

## Evaluation

Metrics reported on validation and test splits:

- RMSE
- MAE
- R²

The baseline provides a simple comparison against lag-only forecasting without MLlib training.

## Saved Artifacts

| Artifact | Path |
| --- | --- |
| Trained regressor | `lakehouse/ml/models/hourly_zone_demand/gbt_regressor` |
| Validation/test predictions | `lakehouse/ml/hourly_zone_demand_predictions/{validation,test}` |
| Evaluation JSON | `artifacts/week4_model_evaluation.json` |

## Reusability

- **Feature pipeline:** saved separately and refit during retraining.
- **Model config:** switch `model.default_type` or add new entries under `model.types`.
- **Multiple tasks:** add another `problems.*` block with its own target and dataset key.
- **New features:** update YAML, rerun `featurize-ml-dataset`, then `train-ml-model`.

## Commands

```powershell
.\scripts\run_win.ps1 train-ml-model --problem hourly_taxi_demand_by_zone
.\scripts\run_win.ps1 retrain-ml-model --rebuild-dataset
```
