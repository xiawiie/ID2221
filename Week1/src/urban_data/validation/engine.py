"""Apply registered validation rules and split accepted/quarantined rows."""

from __future__ import annotations

from dataclasses import dataclass, field
from functools import reduce
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from urban_data.validation.rules import ValidationRule, enabled_rules


@dataclass
class ValidationOutcome:
    accepted: DataFrame
    quarantined: DataFrame
    rule_counts: dict[str, int] = field(default_factory=dict)
    dataset_kind: str = ""


def _validation_rules_column(rule_columns: list[tuple[str, str]]):
    parts = [
        F.when(F.col(column_name), F.lit(rule_name))
        for rule_name, column_name in rule_columns
    ]
    if not parts:
        return F.lit(None).cast("string")
    return F.nullif(F.trim(F.concat_ws(",", *parts)), F.lit(""))


def apply_validation_rules(
    df: DataFrame,
    dataset_kind: str,
    *,
    context: dict[str, Any] | None = None,
    rules: list[ValidationRule] | None = None,
) -> ValidationOutcome:
    """Run enabled rules for a dataset kind without interrupting the pipeline."""

    rules = rules if rules is not None else enabled_rules(dataset_kind)
    context = context or {}
    working = df
    rule_columns: list[tuple[str, str]] = []

    for rule in rules:
        column_name = f"_validation_{rule.name}"
        working = working.withColumn(column_name, rule.build_mask(working, context))
        rule_columns.append((rule.name, column_name))

    if not rule_columns:
        return ValidationOutcome(
            accepted=working,
            quarantined=working.limit(0),
            rule_counts={},
            dataset_kind=dataset_kind,
        )

    failed_mask = reduce(
        lambda left, right: left | right,
        (F.col(column_name) for _, column_name in rule_columns),
    )
    validation_rules = _validation_rules_column(rule_columns)
    quarantined = working.filter(failed_mask).withColumn(
        "validation_rules", validation_rules
    )
    accepted = working.filter(~failed_mask)
    drop_columns = [column_name for _, column_name in rule_columns]
    accepted = accepted.drop(*drop_columns)
    quarantined = quarantined.drop(*drop_columns)

    rule_counts = {
        rule_name: quarantined.filter(
            F.col("validation_rules").contains(rule_name)
        ).count()
        for rule_name, _ in rule_columns
    }
    rule_counts = {name: count for name, count in rule_counts.items() if count}

    return ValidationOutcome(
        accepted=accepted,
        quarantined=quarantined,
        rule_counts=rule_counts,
        dataset_kind=dataset_kind,
    )


def prepare_air_quality_validation(df: DataFrame) -> DataFrame:
    """Attach duplicate-key counts required by air-quality rules."""

    from urban_data.validation.rules import AIR_CANDIDATE_KEY_COLUMNS

    key_counts = df.groupBy(*AIR_CANDIDATE_KEY_COLUMNS).agg(
        F.count("*").alias("_candidate_key_count")
    )
    return df.join(key_counts, on=list(AIR_CANDIDATE_KEY_COLUMNS), how="left")


def rule_metadata(rule: ValidationRule) -> dict[str, str]:
    return {
        "name": rule.name,
        "category": rule.category,
        "description": rule.description,
    }
