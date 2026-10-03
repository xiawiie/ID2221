"""Week 4 Task 2: reusable Spark ML feature engineering pipeline."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import (
    Imputer,
    OneHotEncoder,
    StandardScaler,
    StringIndexer,
    VectorAssembler,
)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from urban_data.ml.training_dataset import (
    ML_ROOT,
    _dataset_output_dir,
    load_ml_training_config,
    resolve_ml_problem,
)
from urban_data.paths import PROJECT_ROOT


def load_feature_engineering_config() -> dict[str, Any]:
    cfg = load_ml_training_config()
    return dict(cfg["feature_engineering"])


def indexed_column_name(column: str) -> str:
    return f"{column}_idx"


def assembled_column_name(column: str, *, one_hot: bool) -> str:
    if one_hot:
        return f"{column}_ohe"
    return indexed_column_name(column)


def feature_pipeline_stage_names(
    problem: dict[str, Any],
    fe_config: dict[str, Any],
) -> list[str]:
    stages = [
        f"StringIndexer({column})" for column in problem["categorical_features"]
    ]
    stages.append(f"Imputer(strategy={fe_config['imputer_strategy']})")
    if fe_config.get("one_hot_encode", False):
        stages.extend(
            f"OneHotEncoder({column})"
            for column in problem["categorical_features"]
        )
    stages.append("VectorAssembler")
    if fe_config.get("scale_numeric", True):
        stages.append("StandardScaler")
    return stages


def build_feature_stages(
    problem: dict[str, Any],
    fe_config: dict[str, Any],
) -> list[Any]:
    """Build Spark ML stages for encoding, imputation, assembly, and scaling."""
    categorical = problem["categorical_features"]
    numeric = problem["numeric_features"]
    one_hot = bool(fe_config.get("one_hot_encode", False))

    indexers = [
        StringIndexer(
            inputCol=column,
            outputCol=indexed_column_name(column),
            handleInvalid="keep",
        )
        for column in categorical
    ]

    imputer = Imputer(
        inputCols=numeric,
        outputCols=numeric,
        strategy=fe_config["imputer_strategy"],
    )

    stages: list[Any] = [*indexers, imputer]

    if one_hot:
        encoders = [
            OneHotEncoder(
                inputCol=indexed_column_name(column),
                outputCol=assembled_column_name(column, one_hot=True),
                handleInvalid="keep",
            )
            for column in categorical
        ]
        stages.extend(encoders)
        assembler_inputs = [
            assembled_column_name(column, one_hot=True) for column in categorical
        ] + numeric
    else:
        assembler_inputs = [
            indexed_column_name(column) for column in categorical
        ] + numeric

    if fe_config.get("scale_numeric", True):
        assembler = VectorAssembler(
            inputCols=assembler_inputs,
            outputCol="features_raw",
            handleInvalid="skip",
        )
        stages.append(assembler)
        stages.append(
            StandardScaler(
                inputCol="features_raw",
                outputCol="features",
                withMean=True,
                withStd=True,
            )
        )
    else:
        stages.append(
            VectorAssembler(
                inputCols=assembler_inputs,
                outputCol="features",
                handleInvalid="skip",
            )
        )

    return stages


def build_feature_pipeline(
    problem: dict[str, Any],
    fe_config: dict[str, Any] | None = None,
) -> Pipeline:
    fe = fe_config or load_feature_engineering_config()
    return Pipeline(stages=build_feature_stages(problem, fe))


def _training_dataset_dir(problem: dict[str, Any]) -> Path:
    return _dataset_output_dir(problem)


def featurized_dataset_dir(
    problem: dict[str, Any] | None = None,
    fe_config: dict[str, Any] | None = None,
    *,
    problem_name: str | None = None,
) -> Path:
    resolved_problem = problem or resolve_ml_problem(problem_name)
    resolved_fe = fe_config or load_feature_engineering_config()
    dataset_key = resolved_problem["output"]["dataset_key"]
    suffix = resolved_fe["output"]["featurized_suffix"]
    return ML_ROOT / f"{dataset_key}_{suffix}"


def _featurized_output_dir(problem: dict[str, Any], fe_config: dict[str, Any]) -> Path:
    return featurized_dataset_dir(problem, fe_config)


def _feature_model_dir(problem: dict[str, Any], fe_config: dict[str, Any]) -> Path:
    dataset_key = problem["output"]["dataset_key"]
    return ML_ROOT / fe_config["output"]["model_dir"] / dataset_key / "feature_pipeline"


def _load_split(spark: SparkSession, dataset_dir: Path, split: str) -> DataFrame:
    return spark.read.format("delta").load(str(dataset_dir / split))


def prepare_supervised_frame(frame: DataFrame, target_column: str) -> DataFrame:
    return frame.withColumn("label", F.col(target_column).cast("double"))


def _select_featurized_columns(
    frame: DataFrame,
    metadata_columns: list[str],
) -> DataFrame:
    keep = [*metadata_columns, "label", "features"]
    missing = [column for column in keep if column not in frame.columns]
    if missing:
        raise ValueError(f"Featurized frame missing columns: {missing}")
    return frame.select(*keep)


def featurize_training_dataset(
    spark: SparkSession,
    problem_name: str | None = None,
) -> dict[str, Any]:
    """Fit the feature pipeline on train and featurize all splits."""
    problem = resolve_ml_problem(problem_name)
    fe_config = load_feature_engineering_config()
    dataset_dir = _training_dataset_dir(problem)
    if not (dataset_dir / "train").exists():
        raise FileNotFoundError(
            f"Training split not found at {dataset_dir / 'train'}. "
            "Run build-ml-dataset first."
        )

    train_frame = prepare_supervised_frame(
        _load_split(spark, dataset_dir, "train"),
        problem["target_column"],
    )
    pipeline = build_feature_pipeline(problem, fe_config)
    model = pipeline.fit(train_frame)

    featurized_dir = _featurized_output_dir(problem, fe_config)
    model_dir = _feature_model_dir(problem, fe_config)
    model_dir.parent.mkdir(parents=True, exist_ok=True)
    model.write().overwrite().save(str(model_dir))

    metadata_columns = list(fe_config["metadata_columns"])
    split_counts: dict[str, int] = {}
    output_paths: dict[str, str] = {}

    for split_name in ("train", "validation", "test"):
        split_frame = prepare_supervised_frame(
            _load_split(spark, dataset_dir, split_name),
            problem["target_column"],
        )
        transformed = model.transform(split_frame)
        featurized = _select_featurized_columns(transformed, metadata_columns)
        split_path = featurized_dir / split_name
        featurized.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(str(split_path))
        split_counts[split_name] = int(featurized.count())
        output_paths[split_name] = str(split_path)

    summary = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "problem": problem["key"],
        "target_column": problem["target_column"],
        "label_column": "label",
        "features_column": "features",
        "categorical_features": problem["categorical_features"],
        "numeric_features": problem["numeric_features"],
        "metadata_columns": metadata_columns,
        "pipeline_stages": feature_pipeline_stage_names(problem, fe_config),
        "feature_engineering": {
            "imputer_strategy": fe_config["imputer_strategy"],
            "scale_numeric": fe_config["scale_numeric"],
            "one_hot_encode": fe_config["one_hot_encode"],
        },
        "row_counts": split_counts,
        "input_dataset_dir": str(dataset_dir),
        "output_paths": output_paths,
        "model_path": str(model_dir),
        "discussion": {
            "most_preprocessing": [
                "pickup_zone StringIndexer (265 TLC zones)",
                "Imputer on weather and PM2.5 numeric columns",
                "VectorAssembler combining indexed categories and numeric lags",
            ],
            "most_valuable_features": [
                "demand_lag_1h",
                "demand_lag_24h",
                "demand_lag_168h",
                "hour_of_day",
                "pickup_zone",
            ],
            "extension_points": [
                "Add columns to ml_training.yml categorical_features or numeric_features",
                "Toggle one_hot_encode for linear models",
                "Reuse the saved PipelineModel during retraining in Task 3",
            ],
        },
    }

    artifact_path = PROJECT_ROOT / "artifacts" / "week4_feature_pipeline.json"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text(
        json.dumps(summary, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
    summary["artifact_path"] = str(artifact_path)
    return summary


def load_feature_pipeline_model(
    spark: SparkSession,
    problem_name: str | None = None,
) -> PipelineModel:
    problem = resolve_ml_problem(problem_name)
    fe_config = load_feature_engineering_config()
    model_path = _feature_model_dir(problem, fe_config)
    return PipelineModel.load(str(model_path))
