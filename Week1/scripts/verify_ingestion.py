import json
from pathlib import Path

from urban_data.spark_session import build_spark


ROOT = Path(__file__).resolve().parents[1]
ARTIFACT = ROOT / "artifacts" / "ingestion_verification.json"

BRONZE_EXPECTED = {
    "taxi_trips/yellow_tripdata_2024-01": 2_964_624,
    "taxi_trips/yellow_tripdata_2024-02": 3_007_526,
    "taxi_trips/yellow_tripdata_2024-03": 3_582_628,
    "taxi_zones": 265,
    "weather_hourly": 8_784,
    "air_quality_hourly_nyc": 51_885,
}

SILVER_EXPECTED = {
    "taxi_trips": 9_551_977,
    "taxi_zones": 265,
    "weather_hourly": 8_784,
    "air_quality_hourly_nyc": 8_784,
}

METADATA_EXPECTED = {
    "taxi_2024_01": (2_964_624, 2_963_754, 870),
    "taxi_2024_02": (3_007_526, 3_006_723, 803),
    "taxi_2024_03": (3_582_628, 3_581_500, 1_128),
    "taxi_zones": (265, 265, 0),
    "weather": (8_784, 8_784, 0),
    "air_quality_nyc": (51_885, 8_784, 0),
}


def _table_count(spark, relative: str) -> int:
    return spark.read.format("delta").load(str(ROOT / "lakehouse" / relative)).count()


def _verify_unique(spark, relative: str, column: str) -> int:
    frame = spark.read.format("delta").load(str(ROOT / "lakehouse" / relative))
    return frame.select(column).distinct().count()


def main() -> None:
    spark = build_spark("verify-ingestion")
    try:
        bronze = {name: _table_count(spark, f"bronze/{name}") for name in BRONZE_EXPECTED}
        silver = {name: _table_count(spark, f"silver/{name}") for name in SILVER_EXPECTED}
        quarantine_taxi = _table_count(spark, "quarantine/taxi_trips")

        metadata = {}
        runs = spark.read.format("delta").load(
            str(ROOT / "lakehouse" / "metadata" / "ingestion_runs")
        )
        for row in runs.filter(runs.status == "success").collect():
            key = row.dataset_key
            counts = (row.rows_read, row.rows_accepted, row.rows_quarantined)
            metadata[key] = max(metadata.get(key, counts), counts)

        uniqueness = {
            "taxi_zones.location_id": _verify_unique(
                spark, "silver/taxi_zones", "location_id"
            ),
            "weather_hourly.observation_ts_local": _verify_unique(
                spark, "silver/weather_hourly", "observation_ts_local"
            ),
            "air_quality_hourly_nyc.observation_ts_utc": _verify_unique(
                spark, "silver/air_quality_hourly_nyc", "observation_ts_utc"
            ),
        }

        assert bronze == BRONZE_EXPECTED, bronze
        assert silver == SILVER_EXPECTED, silver
        assert quarantine_taxi == 2_801, quarantine_taxi
        assert metadata == METADATA_EXPECTED, metadata
        assert uniqueness == {
            "taxi_zones.location_id": 265,
            "weather_hourly.observation_ts_local": 8_784,
            "air_quality_hourly_nyc.observation_ts_utc": 8_784,
        }, uniqueness

        result = {
            "status": "passed",
            "bronze": bronze,
            "silver": silver,
            "quarantine": {"taxi_trips": quarantine_taxi},
            "metadata": {
                key: {
                    "rows_read": value[0],
                    "rows_accepted": value[1],
                    "rows_quarantined": value[2],
                }
                for key, value in metadata.items()
            },
            "unique_keys": uniqueness,
        }
    finally:
        spark.stop()

    ARTIFACT.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
