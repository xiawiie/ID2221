import unittest

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from urban_data.schemas import TAXI_RAW_SCHEMA
from urban_data.validation import (
    expected_column_names,
    types_compatible,
    validate_column_names,
    validate_parquet_schema,
)


class ValidationTest(unittest.TestCase):
    def test_expected_column_names_match_taxi_schema(self):
        names = expected_column_names(TAXI_RAW_SCHEMA)
        self.assertEqual(names[0], "VendorID")
        self.assertEqual(len(names), len(TAXI_RAW_SCHEMA.fields))

    def test_validate_column_names_accepts_exact_match(self):
        expected = expected_column_names(TAXI_RAW_SCHEMA)
        validate_column_names(expected, expected)

    def test_validate_column_names_rejects_wrong_name(self):
        expected = expected_column_names(TAXI_RAW_SCHEMA)
        actual = expected.copy()
        actual[0] = "wrong_id"
        with self.assertRaisesRegex(ValueError, "Schema mismatch"):
            validate_column_names(actual, expected)

    def test_validate_column_names_rejects_missing_column(self):
        expected = expected_column_names(TAXI_RAW_SCHEMA)
        with self.assertRaisesRegex(ValueError, "Schema mismatch"):
            validate_column_names(expected[:-1], expected)

    def test_validate_column_names_rejects_extra_column(self):
        expected = expected_column_names(TAXI_RAW_SCHEMA)
        actual = ["extra"] + expected
        with self.assertRaisesRegex(ValueError, "Schema mismatch"):
            validate_column_names(actual, expected)


class SchemaContractTest(unittest.TestCase):
    def test_taxi_schema_field_count(self):
        self.assertEqual(len(TAXI_RAW_SCHEMA.fields), 19)

    def test_custom_schema_order_is_enforced(self):
        schema = StructType(
            [
                StructField("a", IntegerType(), True),
                StructField("b", IntegerType(), True),
            ]
        )
        validate_column_names(["a", "b"], expected_column_names(schema))
        with self.assertRaises(ValueError):
            validate_column_names(["b", "a"], expected_column_names(schema))


class TypeCompatibilityTest(unittest.TestCase):
    def test_exact_type_match(self):
        self.assertTrue(types_compatible(IntegerType(), IntegerType()))

    def test_int_long_are_compatible(self):
        self.assertTrue(types_compatible(LongType(), IntegerType()))
        self.assertTrue(types_compatible(IntegerType(), LongType()))

    def test_string_int_are_incompatible(self):
        self.assertFalse(types_compatible(StringType(), IntegerType()))

    def test_validate_parquet_schema_rejects_wrong_type(self):
        expected = StructType(
            [
                StructField("VendorID", IntegerType(), True),
                StructField("fare_amount", DoubleType(), True),
            ]
        )
        physical = StructType(
            [
                StructField("VendorID", StringType(), True),
                StructField("fare_amount", DoubleType(), True),
            ]
        )
        with self.assertRaisesRegex(ValueError, "Type mismatch for 'VendorID'"):
            validate_parquet_schema(physical, expected)

    def test_validate_parquet_schema_accepts_compatible_types(self):
        expected = StructType([StructField("passenger_count", LongType(), True)])
        physical = StructType([StructField("passenger_count", IntegerType(), True)])
        validate_parquet_schema(physical, expected)


class AirQualityKeyLogicTest(unittest.TestCase):
    def test_duplicate_candidate_keys_are_detected(self):
        from urban_data.transforms import AIR_CANDIDATE_KEY_COLUMNS, candidate_key_counts

        row = {column: "value" for column in AIR_CANDIDATE_KEY_COLUMNS}
        counts = candidate_key_counts([row, row])
        self.assertEqual(counts[tuple(row[column] for column in AIR_CANDIDATE_KEY_COLUMNS)], 2)

    def test_unique_candidate_keys_have_count_one(self):
        from urban_data.transforms import AIR_CANDIDATE_KEY_COLUMNS, candidate_key_counts

        first = {column: "a" for column in AIR_CANDIDATE_KEY_COLUMNS}
        second = dict(first)
        second["site_num"] = "0002"
        counts = candidate_key_counts([first, second])
        self.assertTrue(all(value == 1 for value in counts.values()))


if __name__ == "__main__":
    unittest.main()
