from pyspark.sql.types import DataType, StructType

COMPATIBLE_TYPE_PAIRS = frozenset(
    {
        ("int", "bigint"),
        ("bigint", "int"),
        ("float", "double"),
        ("double", "float"),
        ("timestamp", "timestamp_ntz"),
        ("timestamp_ntz", "timestamp"),
    }
)


def expected_column_names(schema: StructType) -> list[str]:
    return [field.name for field in schema.fields]


def type_signature(data_type: DataType) -> str:
    return data_type.simpleString().lower()


def types_compatible(actual: DataType, expected: DataType) -> bool:
    if actual == expected:
        return True
    actual_sig = type_signature(actual)
    expected_sig = type_signature(expected)
    if actual_sig == expected_sig:
        return True
    return (actual_sig, expected_sig) in COMPATIBLE_TYPE_PAIRS


def validate_column_names(actual: list[str], expected: list[str]) -> None:
    if actual != expected:
        raise ValueError(f"Schema mismatch: expected {expected}, got {actual}")


def validate_parquet_schema(physical: StructType, expected: StructType) -> None:
    validate_column_names(
        [field.name for field in physical.fields],
        expected_column_names(expected),
    )
    for field in expected.fields:
        actual_type = physical[field.name].dataType
        if not types_compatible(actual_type, field.dataType):
            raise ValueError(
                "Type mismatch for "
                f"{field.name!r}: expected {field.dataType.simpleString()}, "
                f"got {actual_type.simpleString()}"
            )
