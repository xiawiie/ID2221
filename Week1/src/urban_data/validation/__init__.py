"""Week 3 Task 4 extensible data validation framework."""

from urban_data.validation.engine import (
    ValidationOutcome,
    apply_validation_rules,
    prepare_air_quality_validation,
    rule_metadata,
)
from urban_data.validation.report import build_validation_report
from urban_data.validation.rules import (
    ValidationRule,
    enabled_rules,
    rule_categories,
    rule_names,
)
from urban_data.validation.schema import (
    expected_column_names,
    is_schema_validation_error,
    types_compatible,
    validate_column_names,
    validate_parquet_schema,
    validate_schema_evolution,
    validate_schema_evolution_names,
)

__all__ = [
    "ValidationOutcome",
    "ValidationRule",
    "apply_validation_rules",
    "build_validation_report",
    "enabled_rules",
    "expected_column_names",
    "is_schema_validation_error",
    "prepare_air_quality_validation",
    "rule_categories",
    "rule_metadata",
    "rule_names",
    "types_compatible",
    "validate_column_names",
    "validate_parquet_schema",
    "validate_schema_evolution",
    "validate_schema_evolution_names",
]
