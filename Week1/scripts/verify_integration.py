import json
from pathlib import Path

from urban_data.paths import LAKEHOUSE
from urban_data.spark_session import build_spark


ROOT = Path(__file__).resolve().parents[1]
ARTIFACT = ROOT / "artifacts" / "integration_verification.json"
EXPECTED_PARTITIONS = {
    "2024-01": 2_963_754,
    "2024-02": 3_006_723,
    "2024-03": 3_581_500,
}


def _status_counts(frame, column: str) -> dict[str, int]:
    return {
        row[column]: row["count"]
        for row in frame.groupBy(column).count().collect()
    }


def main() -> None:
    spark = build_spark("verify-integration")
    try:
        silver = spark.read.format("delta").load(str(LAKEHOUSE / "silver" / "taxi_trips"))
        gold = spark.read.format("delta").load(
            str(LAKEHOUSE / "gold" / "integrated_taxi_trips")
        )
        silver_count = silver.count()
        gold_count = gold.count()
        partition_counts = {
            row.source_file_month: row["count"]
            for row in gold.groupBy("source_file_month").count().collect()
        }
        pickup_zone_statuses = _status_counts(gold, "pickup_zone_match_status")
        dropoff_zone_statuses = _status_counts(gold, "dropoff_zone_match_status")
        zone_statuses = {
            **{f"pickup_{key}": value for key, value in pickup_zone_statuses.items()},
            **{
                f"dropoff_{key}": value
                for key, value in dropoff_zone_statuses.items()
            },
        }
        weather_statuses = _status_counts(gold, "weather_match_status")
        air_statuses = _status_counts(gold, "air_quality_match_status")
        duplicate_trip_ids = (
            gold.groupBy("trip_id").count().filter("count > 1").count()
        )

        assert silver_count == gold_count == 9_551_977
        assert partition_counts == EXPECTED_PARTITIONS, partition_counts
        assert zone_statuses == {
            "pickup_matched": 9_551_977,
            "dropoff_matched": 9_551_977,
        }, zone_statuses
        assert sum(weather_statuses.values()) == gold_count, weather_statuses
        assert sum(air_statuses.values()) == gold_count, air_statuses
        for column in (
            "weather_observation_ts_local",
            "air_observation_ts_utc",
            "weather_lag_minutes",
            "air_quality_lag_minutes",
        ):
            assert column in gold.columns, column

        result = {
            "status": "passed",
            "silver_rows": silver_count,
            "gold_rows": gold_count,
            "partition_counts": partition_counts,
            "zone_statuses": zone_statuses,
            "weather_statuses": weather_statuses,
            "air_quality_statuses": air_statuses,
            "duplicate_trip_ids": duplicate_trip_ids,
            "trip_id_note": "Source has no proven unique trip key; duplicates are reported, not removed.",
        }
    finally:
        spark.stop()

    ARTIFACT.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
