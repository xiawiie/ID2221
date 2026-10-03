import unittest

from urban_data.ml.features import (
    feature_pipeline_stage_names,
    indexed_column_name,
    load_feature_engineering_config,
)
from urban_data.ml.training_dataset import resolve_ml_problem


class Week4Task2FeaturePipelineTest(unittest.TestCase):
    def test_feature_engineering_config_defaults(self):
        cfg = load_feature_engineering_config()
        self.assertEqual(cfg["imputer_strategy"], "median")
        self.assertTrue(cfg["scale_numeric"])
        self.assertFalse(cfg["one_hot_encode"])
        self.assertIn("pickup_zone", cfg["metadata_columns"])

    def test_indexed_column_naming(self):
        self.assertEqual(indexed_column_name("pickup_zone"), "pickup_zone_idx")

    def test_pipeline_includes_required_stages(self):
        problem = resolve_ml_problem()
        fe = load_feature_engineering_config()
        stages = feature_pipeline_stage_names(problem, fe)
        self.assertTrue(any(stage.startswith("StringIndexer(pickup_zone)") for stage in stages))
        self.assertIn("Imputer(strategy=median)", stages)
        self.assertIn("VectorAssembler", stages)
        self.assertIn("StandardScaler", stages)

    def test_pickup_month_is_categorical_feature(self):
        problem = resolve_ml_problem()
        self.assertIn("pickup_month", problem["categorical_features"])


if __name__ == "__main__":
    unittest.main()
