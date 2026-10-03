import unittest

from urban_data.ml.train import (
    METRIC_TO_EVALUATOR,
    load_model_config,
    resolve_model_type,
)


class Week4Task3ModelConfigTest(unittest.TestCase):
    def test_default_model_is_gbt(self):
        model_cfg = load_model_config()
        self.assertEqual(model_cfg["default_type"], "gbt_regressor")

    def test_model_metrics_include_assignment_set(self):
        metrics = load_model_config()["metrics"]
        self.assertEqual(metrics, ["rmse", "mae", "r2"])

    def test_gbt_has_fixed_seed(self):
        _, model_type_cfg = resolve_model_type()
        self.assertEqual(model_type_cfg["params"]["seed"], 42)

    def test_evaluator_mapping_is_complete(self):
        for metric in load_model_config()["metrics"]:
            self.assertIn(metric, METRIC_TO_EVALUATOR)


if __name__ == "__main__":
    unittest.main()
