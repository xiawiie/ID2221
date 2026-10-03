import unittest

from urban_data.analytics import ANALYTIC_QUERY_SQL, query_names
from urban_data.optimization import ANALYTICAL_QUERY_TECHNIQUES


class Week2QueryDefinitionTest(unittest.TestCase):
    def test_six_required_queries_are_registered(self):
        self.assertEqual(len(query_names()), 6)
        self.assertEqual(
            set(query_names()),
            {
                "monthly_taxi_demand_by_zone",
                "average_trip_distance_by_weather",
                "air_quality_taxi_demand_relationship",
                "zone_weather_demand_variation",
                "peak_travel_hours_by_day_of_week",
                "monthly_taxi_demand_trends",
            },
        )

    def test_queries_target_the_integrated_dataset(self):
        for sql in ANALYTIC_QUERY_SQL.values():
            self.assertIn("integrated_taxi_trips", sql)

    def test_peak_hour_query_uses_a_window_function(self):
        self.assertIn("ROW_NUMBER() OVER", ANALYTIC_QUERY_SQL["peak_travel_hours_by_day_of_week"])

    def test_monthly_trend_query_uses_lag(self):
        self.assertIn("LAG(taxi_demand)", ANALYTIC_QUERY_SQL["monthly_taxi_demand_trends"])

    def test_each_analytical_query_has_task5_technique(self):
        self.assertEqual(set(ANALYTICAL_QUERY_TECHNIQUES), set(query_names()))
        self.assertEqual(
            ANALYTICAL_QUERY_TECHNIQUES["monthly_taxi_demand_by_zone"],
            "caching",
        )
        self.assertEqual(
            ANALYTICAL_QUERY_TECHNIQUES["monthly_taxi_demand_trends"],
            "partition_pruning",
        )
