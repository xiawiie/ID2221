"""Week 4 Task 3: reproducible Spark ML training, evaluation, and retraining."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import GBTRegressor, GBTRegressionModel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from urban_data.ml.features import (
    featurize_training_dataset,
    featurized_dataset_dir,
    load_feature_engineering_config,
)
from urban_data.ml.training_dataset import (
    ML_ROOT,
    _dataset_output_dir,
    load_ml_training_config,
    resolve_ml_problem,
)
from urban_data.paths import PROJECT_ROOT


METRIC_TO_EVALUATOR = {
    "rmse": "rmse",
    "mae": "mae",
    "r2": "r2",
}


def load_model_config() -> dict[str, Any]:
    cfg = load_ml_training_config()
    return dict(cfg["model"])


def resolve_model_type(model_type: str | None = None) -> tuple[str, dict[str, Any]]:
    model_cfg = load_model_config()
    key = model_type or model_cfg["default_type"]
    types = model_cfg["types"]
    if key not in types:
        known = ", ".join(sorted(types))
        raise KeyError(f"Unknown model type {key!r}; known: {known}")
    return key, dict(types[key])


def model_artifact_dir(
    problem: dict[str, Any],
    model_type_cfg: dict[str, Any],
    fe_config: dict[str, Any] | None = None,
) -> Path:
    fe = fe_config or load_feature_engineering_config()
    dataset_key = problem["output"]["dataset_key"]
    artifact_name = model_type_cfg["artifact_name"]
    return ML_ROOT / fe["output"]["model_dir"] / dataset_key / artifact_name


def build_regressor(
    model_type: str,
    model_type_cfg: dict[str, Any],
    model_cfg: dict[str, Any],
) -> GBTRegressor:
    if model_type != "gbt_regressor":
        raise NotImplementedError(f"Model type {model_type!r} is not implemented yet.")
    params = dict(model_type_cfg["params"])
    return GBTRegressor(
        featuresCol=model_cfg["features_column"],
        labelCol=model_cfg["label_column"],
        predictionCol=model_cfg["prediction_column"],
        **params,
    )


def _load_featurized_split(
    spark: SparkSession,
    featurized_dir: Path,
    split: str,
) -> DataFrame:
    split_path = featurized_dir / split
    if not split_path.exists():
        raise FileNotFoundError(
            f"Featurized split not found at {split_path}. Run featurize-ml-dataset first."
        )
    return spark.read.format("delta").load(str(split_path))


def evaluate_regression(
    frame: DataFrame,
    *,
    label_column: str,
    prediction_column: str,
    metrics: list[str],
) -> dict[str, float]:
    results: dict[str, float] = {}
    for metric in metrics:
        evaluator = RegressionEvaluator(
            labelCol=label_column,
            predictionCol=prediction_column,
            metricName=METRIC_TO_EVALUATOR[metric],
        )
        results[metric] = float(evaluator.evaluate(frame))
    return results


def evaluate_seasonal_naive_baseline(
    spark: SparkSession,
    problem: dict[str, Any],
    model_cfg: dict[str, Any],
    *,
    split: str = "test",
) -> dict[str, float]:
    """Baseline that predicts trip_count using the 24-hour lag feature."""
    baseline_cfg = model_cfg["baseline"]
    raw_dir = _dataset_output_dir(problem)
    frame = spark.read.format("delta").load(str(raw_dir / split))
    target = problem["target_column"]
    prediction_column = baseline_cfg["prediction_column"]
    scored = frame.withColumn("label", F.col(target).cast("double")).withColumn(
        model_cfg["prediction_column"],
        F.col(prediction_column).cast("double"),
    )
    return evaluate_regression(
        scored,
        label_column="label",
        prediction_column=model_cfg["prediction_column"],
        metrics=list(model_cfg["metrics"]),
    )


def train_ml_model(
    spark: SparkSession,
    problem_name: str | None = None,
    *,
    model_type: str | None = None,
) -> dict[str, Any]:
    """Train, evaluate, and persist a Spark MLlib model on featurized splits."""
    problem = resolve_ml_problem(problem_name)
    model_cfg = load_model_config()
    fe_config = load_feature_engineering_config()
    model_key, model_type_cfg = resolve_model_type(model_type)

    featurized_dir = featurized_dataset_dir(problem, fe_config)
    train_frame = _load_featurized_split(spark, featurized_dir, "train")
    validation_frame = _load_featurized_split(spark, featurized_dir, "validation")
    test_frame = _load_featurized_split(spark, featurized_dir, "test")

    regressor = build_regressor(model_key, model_type_cfg, model_cfg)
    started = datetime.now(timezone.utc)
    trained_model = regressor.fit(train_frame)
    finished = datetime.now(timezone.utc)

    val_predictions = trained_model.transform(validation_frame)
    test_predictions = trained_model.transform(test_frame)

    label_column = model_cfg["label_column"]
    prediction_column = model_cfg["prediction_column"]
    metrics = list(model_cfg["metrics"])

    validation_metrics = evaluate_regression(
        val_predictions,
        label_column=label_column,
        prediction_column=prediction_column,
        metrics=metrics,
    )
    test_metrics = evaluate_regression(
        test_predictions,
        label_column=label_column,
        prediction_column=prediction_column,
        metrics=metrics,
    )
    baseline_test_metrics = evaluate_seasonal_naive_baseline(
        spark,
        problem,
        model_cfg,
        split="test",
    )

    model_dir = model_artifact_dir(problem, model_type_cfg, fe_config)
    model_dir.parent.mkdir(parents=True, exist_ok=True)
    trained_model.write().overwrite().save(str(model_dir))

    predictions_dir = featurized_dir.parent / f"{problem['output']['dataset_key']}_predictions"
    for split_name, split_frame in (
        ("validation", val_predictions),
        ("test", test_predictions),
    ):
        output = (
            split_frame.select(
                *[column for column in split_frame.columns if column != "features"],
            )
        )
        path = predictions_dir / split_name
        output.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(str(path))

    run_id = uuid4().hex
    summary = {
        "generated_at_utc": finished.isoformat(),
        "run_id": run_id,
        "problem": problem["key"],
        "model_type": model_key,
        "model_params": model_type_cfg["params"],
        "training_duration_ms": (finished - started).total_seconds() * 1000.0,
        "label_column": label_column,
        "features_column": model_cfg["features_column"],
        "prediction_column": prediction_column,
        "metrics": metrics,
        "evaluation": {
            "validation": validation_metrics,
            "test": test_metrics,
            "baseline_test": {
                "name": model_cfg["baseline"]["name"],
                **baseline_test_metrics,
            },
        },
        "model_path": str(model_dir),
        "featurized_input_dir": str(featurized_dir),
        "prediction_output_dir": str(predictions_dir),
        "reproducibility": {
            "feature_pipeline_path": str(
                ML_ROOT
                / fe_config["output"]["model_dir"]
                / problem["output"]["dataset_key"]
                / "feature_pipeline"
            ),
            "random_seed": model_type_cfg["params"].get("seed"),
            "retrain_steps": [
                "build-ml-dataset",
                "featurize-ml-dataset",
                "train-ml-model",
            ],
        },
        "discussion": {
            "reusable_components": [
                "Task 1 dataset builder",
                "Task 2 feature PipelineModel",
                "Task 3 regressor artifact and evaluation helpers",
            ],
            "new_features": "Extend ml_training.yml and rerun featurize-ml-dataset",
            "multiple_tasks": "Add a new problems.* entry with its own target and model config",
        },
    }

    artifact_path = PROJECT_ROOT / "artifacts" / "week4_model_evaluation.json"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text(
        json.dumps(summary, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    summary["artifact_path"] = str(artifact_path)
    return summary


def retrain_ml_model(
    spark: SparkSession,
    problem_name: str | None = None,
    *,
    rebuild_dataset: bool = False,
    model_type: str | None = None,
) -> dict[str, Any]:
    """Refresh datasets and models when new integrated data is available."""
    from urban_data.ml.training_dataset import build_training_dataset

    dataset_refresh = None
    if rebuild_dataset:
        dataset_refresh = build_training_dataset(spark, problem_name)
    featurize_summary = featurize_training_dataset(spark, problem_name)
    train_summary = train_ml_model(spark, problem_name, model_type=model_type)
    return {
        "rebuild_dataset": rebuild_dataset,
        "dataset_refresh": dataset_refresh,
        "featurize_summary": featurize_summary,
        "train_summary": train_summary,
    }


def load_trained_model(
    spark: SparkSession,
    problem_name: str | None = None,
    *,
    model_type: str | None = None,
) -> GBTRegressionModel:
    problem = resolve_ml_problem(problem_name)
    _, model_type_cfg = resolve_model_type(model_type)
    fe_config = load_feature_engineering_config()
    model_path = model_artifact_dir(problem, model_type_cfg, fe_config)
    return GBTRegressionModel.load(str(model_path))
