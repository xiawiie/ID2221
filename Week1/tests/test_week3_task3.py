import unittest
from datetime import datetime, timezone

from urban_data.monitoring import (
    MONITORING_QUERIES,
    monitoring_query_names,
    record_from_ingestion_summary,
)
from urban_data.schemas import PIPELINE_RUNS_SCHEMA, VALIDATION_EVENTS_SCHEMA


class Week3Task3MonitoringTest(unittest.TestCase):
    def test_monitoring_query_catalog(self):
        names = monitoring_query_names()
        self.assertEqual(len(names), 4)
        self.assertEqual(set(names), set(MONITORING_QUERIES))
        for name in names:
            sql = MONITORING_QUERIES[name]
            self.assertIn("monitoring_runs", sql)

    def test_pipeline_runs_schema_fields(self):
        field_names = [field.name for field in PIPELINE_RUNS_SCHEMA.fields]
        self.assertIn("duration_ms", field_names)
        self.assertIn("validation_failure_count", field_names)
        self.assertIn("rows_duplicates_ignored", field_names)

    def test_validation_events_schema_fields(self):
        field_names = [field.name for field in VALIDATION_EVENTS_SCHEMA.fields]
        self.assertEqual(
            field_names,
            [
                "run_id",
                "target_key",
                "pipeline_type",
                "rule_name",
                "failure_count",
                "recorded_at",
            ],
        )

    def test_record_from_ingestion_summary_mapping(self):
        started = datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)
        finished = datetime(2026, 3, 1, 12, 5, tzinfo=timezone.utc)
        summary = {
            "run_id": "run-test",
            "dataset_key": "taxi_trips_update",
            "dataset_name": "taxi_trips",
            "schema_version": "2",
            "rows_read": 1000,
            "rows_accepted": 800,
            "rows_quarantined": 50,
            "rows_warned": 150,
            "new_records": 700,
            "duplicate_records_ignored": 150,
            "started_at": started,
            "finished_at": finished,
            "status": "success",
        }

        class FakeSpark:
            appended = []

            @staticmethod
            def read():
                raise AssertionError("read should not be called in this unit test")

        captured = {}

        def fake_record_pipeline_run(spark, **kwargs):
            captured.update(kwargs)
            return kwargs

        original = record_from_ingestion_summary.__globals__["record_pipeline_run"]
        record_from_ingestion_summary.__globals__["record_pipeline_run"] = (
            fake_record_pipeline_run
        )
        try:
            record_from_ingestion_summary(FakeSpark(), summary, pipeline_type="incremental")
        finally:
            record_from_ingestion_summary.__globals__["record_pipeline_run"] = original

        self.assertEqual(captured["pipeline_type"], "incremental")
        self.assertEqual(captured["rows_processed"], 1000)
        self.assertEqual(captured["rows_inserted"], 700)
        self.assertEqual(captured["rows_rejected"], 50)
        self.assertEqual(captured["rows_duplicates_ignored"], 150)
        self.assertEqual(captured["validation_failure_count"], 200)
        self.assertEqual(
            captured["validation_events"],
            {
                "quarantined_records": 50,
                "duplicate_records_ignored": 150,
            },
        )


if __name__ == "__main__":
    unittest.main()
