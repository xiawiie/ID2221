import unittest

from urban_data.evaluation import ARTIFACT, RUNS, STORAGE_GROUPS


class Week3Task5EvaluationTest(unittest.TestCase):
    def test_artifact_path(self):
        self.assertTrue(str(ARTIFACT).endswith("week3_platform_evaluation.json"))

    def test_storage_groups(self):
        self.assertIn("core_analytical", STORAGE_GROUPS)
        self.assertIn("week3_operational_overhead", STORAGE_GROUPS)
        self.assertIn("metadata", STORAGE_GROUPS["week3_operational_overhead"])
        self.assertIn("quarantine", STORAGE_GROUPS["week3_operational_overhead"])

    def test_benchmark_run_count(self):
        self.assertGreaterEqual(RUNS, 3)


if __name__ == "__main__":
    unittest.main()
