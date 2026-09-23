import unittest

from urban_data.product_refresh import (
    INCREMENTAL_LOGICAL_NAMES,
    products_affected_by_dataset_keys,
)
from urban_data.products import product_names


class Week3Task2RefreshPlanTest(unittest.TestCase):
    def test_taxi_update_affects_all_products(self):
        affected = products_affected_by_dataset_keys(["taxi_trips_update"])
        self.assertEqual(set(affected), set(product_names()))

    def test_weather_update_affects_weather_product_only(self):
        affected = products_affected_by_dataset_keys(["weather_update"])
        self.assertEqual(affected, ["weather_impact_summary"])

    def test_air_update_affects_air_product_only(self):
        affected = products_affected_by_dataset_keys(["air_quality_update"])
        self.assertEqual(affected, ["air_quality_impact_summary"])

    def test_incremental_logical_name_mapping(self):
        self.assertEqual(
            INCREMENTAL_LOGICAL_NAMES["taxi_trips_update"],
            "taxi_trips",
        )


if __name__ == "__main__":
    unittest.main()
