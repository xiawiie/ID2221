import unittest

from urban_data.ml.training_dataset import (
    LAG_COLUMN_NAMES,
    load_ml_training_config,
    ml_problem_names,
    resolve_ml_problem,
)


class Week4Task1ConfigTest(unittest.TestCase):
    def test_default_problem_is_hourly_zone_demand(self):
        cfg = load_ml_training_config()
        self.assertEqual(cfg["default_problem"], "hourly_taxi_demand_by_zone")

    def test_problem_catalog(self):
        names = ml_problem_names()
        self.assertEqual(names, ("hourly_taxi_demand_by_zone",))

    def test_resolved_problem_includes_features_and_target(self):
        problem = resolve_ml_problem()
        self.assertEqual(problem["target_column"], "trip_count")
        self.assertIn("pickup_zone", problem["categorical_features"])
        self.assertIn("demand_lag_24h", problem["numeric_features"])
        self.assertEqual(
            problem["feature_columns"],
            problem["categorical_features"] + problem["numeric_features"],
        )

    def test_lag_columns_cover_configured_hours(self):
        problem = resolve_ml_problem()
        expected = {LAG_COLUMN_NAMES[lag] for lag in problem["lag_hours"]}
        self.assertEqual(
            expected,
            {"demand_lag_1h", "demand_lag_24h", "demand_lag_168h"},
        )

    def test_chronological_split_dates_are_ordered(self):
        splits = resolve_ml_problem()["splits"]
        self.assertLessEqual(splits["train_end_date"], splits["validation_end_date"])


if __name__ == "__main__":
    unittest.main()
