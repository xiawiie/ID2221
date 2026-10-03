import unittest

from pyspark.sql.types import DoubleType, LongType, StructField, StructType

from urban_data.config import incremental_update_keys, resolve_dataset
from urban_data.schemas import WEATHER_RAW_SCHEMA, WEATHER_UPDATE_RAW_SCHEMA
from urban_data.validation import validate_schema_evolution


class Week3Task1ConfigTest(unittest.TestCase):
    def test_incremental_update_keys(self):
        keys = incremental_update_keys()
        self.assertEqual(
            keys,
            ["air_quality_update", "taxi_trips_update", "weather_update"],
        )

    def test_incremental_datasets_use_schema_version_two(self):
        for key in incremental_update_keys():
            cfg = resolve_dataset(key)
            self.assertEqual(cfg["schema_version"], "2")
            self.assertEqual(cfg["mode"], "incremental")


class Week3SchemaEvolutionTest(unittest.TestCase):
    def test_weather_update_schema_extends_base(self):
        base_names = [field.name for field in WEATHER_RAW_SCHEMA.fields]
        evolved_names = [field.name for field in WEATHER_UPDATE_RAW_SCHEMA.fields]
        self.assertEqual(evolved_names[: len(base_names)], base_names)
        self.assertEqual(evolved_names[-1], "humidity")
        self.assertIsInstance(
            WEATHER_UPDATE_RAW_SCHEMA["humidity"].dataType, DoubleType
        )

    def test_validate_schema_evolution_accepts_humidity(self):
        physical = StructType(
            list(WEATHER_RAW_SCHEMA.fields)
            + [StructField("humidity", DoubleType(), True)]
        )
        validate_schema_evolution(
            physical, WEATHER_RAW_SCHEMA, WEATHER_UPDATE_RAW_SCHEMA
        )

    def test_validate_schema_evolution_rejects_missing_base_column(self):
        physical = StructType([StructField("year", LongType(), True)])
        with self.assertRaisesRegex(ValueError, "Schema evolution mismatch"):
            validate_schema_evolution(
                physical, WEATHER_RAW_SCHEMA, WEATHER_UPDATE_RAW_SCHEMA
            )


if __name__ == "__main__":
    unittest.main()
