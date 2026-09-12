from pyspark.sql import DataFrame, SparkSession

from urban_data.config import resolve_dataset
from urban_data.ingest import _read_parquet_with_schema
from urban_data.schemas import TAXI_RAW_SCHEMA
from urban_data.transforms import (
    new_run_id,
    split_taxi_quality,
    utc_now,
    with_bronze_metadata,
)

TAXI_SOURCE_KEYS = ("taxi_2024_01", "taxi_2024_02", "taxi_2024_03")


def load_accepted_taxi_trips_from_sources(spark: SparkSession) -> tuple[DataFrame, list[str]]:
    """Read configured taxi Parquet sources and return accepted Silver-shaped rows."""
    frames: list[DataFrame] = []
    source_files: list[str] = []
    for key in TAXI_SOURCE_KEYS:
        cfg = resolve_dataset(key)
        rel_source = cfg["path"].replace("\\", "/")
        raw = _read_parquet_with_schema(spark, cfg["source_path"], TAXI_RAW_SCHEMA)
        prepared = with_bronze_metadata(
            raw,
            source_file=rel_source,
            source_sha256="benchmark",
            run_id=new_run_id(),
            schema_version=cfg["schema_version"],
            ingested_at=utc_now(),
        )
        accepted, _quarantined = split_taxi_quality(prepared, cfg["file_month"])
        frames.append(accepted)
        source_files.append(rel_source)

    combined = frames[0]
    for frame in frames[1:]:
        combined = combined.unionByName(frame)
    return combined, source_files
