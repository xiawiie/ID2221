import unittest

from urban_data.config import load_validation_rules_config
from urban_data.validation import (
    enabled_rules,
    is_schema_validation_error,
    rule_categories,
    rule_names,
)
from urban_data.validation.rules import RULE_REGISTRY


class Week3Task4ValidationConfigTest(unittest.TestCase):
    def test_rule_sets_cover_all_dataset_kinds(self):
        config = load_validation_rules_config()
        self.assertEqual(
            set(config["rule_sets"]),
            {
                "taxi_trips",
                "taxi_zones",
                "weather_hourly",
                "air_quality_hourly_nyc",
            },
        )

    def test_enabled_rules_resolve_from_registry(self):
        for kind in (
            "taxi_trips",
            "taxi_zones",
            "weather_hourly",
            "air_quality_hourly_nyc",
        ):
            rules = enabled_rules(kind)
            self.assertTrue(rules)
            for rule in rules:
                self.assertEqual(rule, RULE_REGISTRY[rule.name])

    def test_assignment_categories_are_represented(self):
        categories = {rule.category for rule in RULE_REGISTRY.values()}
        self.assertTrue(
            {
                "duplicate",
                "invalid_value",
                "missing_reference",
                "incomplete",
            }.issubset(categories)
        )
        self.assertEqual(set(rule_categories()), categories | {"schema"})


class Week3Task4SchemaErrorTest(unittest.TestCase):
    def test_schema_validation_error_detection(self):
        self.assertTrue(
            is_schema_validation_error(
                ValueError("Unsupported schema change: expected evolved columns")
            )
        )
        self.assertFalse(is_schema_validation_error(ValueError("Row count mismatch")))


class Week3Task4RuleCatalogTest(unittest.TestCase):
    def test_taxi_rules_include_reference_and_duplicate_checks(self):
        names = {rule.name for rule in enabled_rules("taxi_trips")}
        self.assertIn("null_or_invalid_trip_times", names)
        self.assertIn("missing_zone_reference", names)
        self.assertIn("duplicate_trip_fingerprint", names)

    def test_catalog_lists_all_registered_rules(self):
        self.assertEqual(set(rule_names()), set(RULE_REGISTRY))


if __name__ == "__main__":
    unittest.main()
