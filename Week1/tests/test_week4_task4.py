import unittest

from urban_data.ml.approach_a import APPROACH_A_STEPS, approach_a_source_line_count
from urban_data.ml.compare import APPROACH_B_STEPS, approach_b_source_line_count


class Week4Task4ComparisonTest(unittest.TestCase):
    def test_approach_a_has_more_steps_than_platform_path(self):
        self.assertGreater(len(APPROACH_A_STEPS), len(APPROACH_B_STEPS))

    def test_approach_a_source_is_substantial(self):
        self.assertGreater(approach_a_source_line_count(), 100)

    def test_approach_b_source_is_smaller(self):
        self.assertLess(approach_b_source_line_count(), approach_a_source_line_count())

    def test_step_catalogues_cover_integration_and_aggregation(self):
        joined = " ".join(APPROACH_A_STEPS).lower()
        self.assertIn("parquet", joined)
        self.assertIn("join", joined)
        self.assertIn("aggregate", joined)
        platform = " ".join(APPROACH_B_STEPS).lower()
        self.assertIn("gold", platform)


if __name__ == "__main__":
    unittest.main()
