import hashlib
import json
import platform
import zipfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq


ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "datasets"
OUTPUT = ROOT / "artifacts" / "data_profile.json"
TAXI_FILES = sorted(DATA.glob("yellow_tripdata_2024-*.parquet"))


def json_value(value):
    if value is None or pd.isna(value):
        return None
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.isoformat()
    return value.item() if hasattr(value, "item") else value


def sha256(stream):
    return hashlib.file_digest(stream, "sha256").hexdigest()


def verify_integrity():
    manifest = {}
    for line in (DATA / "SHA256SUMS.txt").read_text(encoding="utf-8").splitlines():
        expected, name = line.split(maxsplit=1)
        manifest[name] = expected.lower()

    actual = {}
    for name, expected in manifest.items():
        with (DATA / name).open("rb") as stream:
            actual[name] = sha256(stream)
        if actual[name] != expected:
            raise ValueError(f"SHA-256 mismatch: {name}")

    air_csv = DATA / "hourly_88101_2024.csv"
    with air_csv.open("rb") as stream:
        csv_hash = sha256(stream)
    with zipfile.ZipFile(DATA / "air_quality.zip") as archive:
        entries = archive.infolist()
        if len(entries) != 1 or entries[0].filename != air_csv.name:
            raise ValueError("air_quality.zip must contain only hourly_88101_2024.csv")
        with archive.open(entries[0]) as stream:
            zip_entry_hash = sha256(stream)
        if entries[0].file_size != air_csv.stat().st_size or zip_entry_hash != csv_hash:
            raise ValueError("Extracted air-quality CSV differs from ZIP entry")

    return {
        "manifest_entries": len(manifest),
        "manifest_matches": True,
        "source_sha256": actual,
        "air_csv_sha256": csv_hash,
        "air_csv_matches_zip_entry": True,
    }


def profile_taxi(path):
    parquet = pq.ParquetFile(path)
    month = int(path.stem[-2:])
    month_start = datetime(2024, month, 1)
    month_end = datetime(2024 + (month == 12), month % 12 + 1, 1)
    nulls = Counter()
    minima = {}
    maxima = {}
    distinct = {name: set() for name in [
        "VendorID", "RatecodeID", "store_and_fwd_flag",
        "PULocationID", "DOLocationID", "payment_type",
    ]}
    quality = Counter()

    for batch in parquet.iter_batches(batch_size=262_144):
        columns = {name: batch.column(i) for i, name in enumerate(batch.schema.names)}
        for name, array in columns.items():
            nulls[name] += array.null_count
            if name in distinct:
                distinct[name].update(x for x in pc.unique(array).to_pylist() if x is not None)
            if name in {
                "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
                "trip_distance", "fare_amount", "total_amount",
            }:
                stats = pc.min_max(array).as_py()
                if stats["min"] is not None:
                    minima[name] = min(minima.get(name, stats["min"]), stats["min"])
                    maxima[name] = max(maxima.get(name, stats["max"]), stats["max"])

        quality["dropoff_not_after_pickup"] += pc.sum(pc.fill_null(pc.less_equal(
            columns["tpep_dropoff_datetime"], columns["tpep_pickup_datetime"]
        ), False)).as_py()
        quality["pickup_outside_file_month"] += pc.sum(pc.fill_null(pc.or_(
            pc.less(columns["tpep_pickup_datetime"], month_start),
            pc.greater_equal(columns["tpep_pickup_datetime"], month_end),
        ), False)).as_py()
        for name in ["trip_distance", "passenger_count", "fare_amount", "total_amount"]:
            quality[f"negative_{name}"] += pc.sum(
                pc.fill_null(pc.less(columns[name], 0), False)
            ).as_py()

    return {
        "file": path.name,
        "bytes": path.stat().st_size,
        "rows": parquet.metadata.num_rows,
        "row_groups": parquet.metadata.num_row_groups,
        "schema": [
            {"name": field.name, "type": str(field.type), "nullable": field.nullable}
            for field in parquet.schema_arrow
        ],
        "null_counts": dict(nulls),
        "min": {key: json_value(value) for key, value in minima.items()},
        "max": {key: json_value(value) for key, value in maxima.items()},
        "distinct_values": {
            key: sorted(values, key=str) for key, values in distinct.items()
        },
        "quality_counts": dict(quality),
    }


def profile_weather():
    path = DATA / "weather.csv"
    data = pd.read_csv(path)
    timestamps = pd.to_datetime(data[["year", "month", "day", "hour"]])
    return {
        "file": path.name,
        "bytes": path.stat().st_size,
        "rows": len(data),
        "schema": [{"name": name, "type": str(dtype)} for name, dtype in data.dtypes.items()],
        "timestamp_min": timestamps.min().isoformat(),
        "timestamp_max": timestamps.max().isoformat(),
        "unique_timestamps": int(timestamps.nunique()),
        "duplicate_timestamps": int(timestamps.duplicated().sum()),
        "null_counts": {key: int(value) for key, value in data.isna().sum().items()},
        "fully_null_columns": [name for name in data if data[name].isna().all()],
        "numeric_min": {
            key: json_value(value) for key, value in data.min(numeric_only=True).items()
        },
        "numeric_max": {
            key: json_value(value) for key, value in data.max(numeric_only=True).items()
        },
        "source_values": {
            name: sorted(data[name].dropna().astype(str).unique().tolist())
            for name in data if name.endswith("_source")
        },
    }


def profile_zones(taxi_profiles):
    path = DATA / "taxi_zone_lookup.csv"
    data = pd.read_csv(path)
    zone_ids = set(data["LocationID"].dropna().astype(int))
    pickup_ids = set().union(*(
        set(profile["distinct_values"]["PULocationID"]) for profile in taxi_profiles
    ))
    dropoff_ids = set().union(*(
        set(profile["distinct_values"]["DOLocationID"]) for profile in taxi_profiles
    ))
    return {
        "file": path.name,
        "bytes": path.stat().st_size,
        "rows": len(data),
        "schema": [{"name": name, "type": str(dtype)} for name, dtype in data.dtypes.items()],
        "location_id_unique": bool(data["LocationID"].is_unique),
        "location_id_min": int(data["LocationID"].min()),
        "location_id_max": int(data["LocationID"].max()),
        "duplicate_rows": int(data.duplicated().sum()),
        "null_counts": {key: int(value) for key, value in data.isna().sum().items()},
        "borough_values": sorted(data["Borough"].dropna().unique().tolist()),
        "service_zone_values": sorted(data["service_zone"].dropna().unique().tolist()),
        "unmatched_pickup_location_ids": sorted(pickup_ids - zone_ids),
        "unmatched_dropoff_location_ids": sorted(dropoff_ids - zone_ids),
    }


def profile_air_quality():
    path = DATA / "hourly_88101_2024.csv"
    string_columns = {
        name: pa.string() for name in [
            "State Code", "County Code", "Site Num", "Parameter Code", "POC",
            "Date Local", "Time Local", "Date GMT", "Time GMT", "Method Code",
        ]
    }
    reader = pacsv.open_csv(
        path,
        read_options=pacsv.ReadOptions(block_size=16 << 20),
        convert_options=pacsv.ConvertOptions(
            column_types=string_columns, strings_can_be_null=True
        ),
    )
    nyc_county_codes = pa.array(["005", "047", "061", "081", "085"])
    nulls = Counter()
    distinct = {name: set() for name in [
        "State Code", "Parameter Code", "Units of Measure", "Method Type", "Method Code"
    ]}
    sites = set()
    nyc_sites = set()
    nyc_counties = set()
    nyc_keys = set()
    nyc_hours = Counter()
    nyc_site_rows = Counter()
    rows = nyc_rows = negative = nyc_negative = duplicate_keys = 0
    value_min = value_max = nyc_value_min = nyc_value_max = None
    date_min = date_max = nyc_date_min = nyc_date_max = None

    for batch in reader:
        rows += batch.num_rows
        columns = {name: batch.column(i) for i, name in enumerate(batch.schema.names)}
        for name, array in columns.items():
            nulls[name] += array.null_count
            if name in distinct:
                distinct[name].update(x for x in pc.unique(array).to_pylist() if x is not None)
        sites.update(zip(
            columns["State Code"].to_pylist(), columns["County Code"].to_pylist(),
            columns["Site Num"].to_pylist(), columns["Latitude"].to_pylist(),
            columns["Longitude"].to_pylist(),
        ))
        dates = columns["Date Local"].to_pylist()
        date_min = min(date_min or dates[0], min(dates))
        date_max = max(date_max or dates[0], max(dates))
        stats = pc.min_max(columns["Sample Measurement"]).as_py()
        value_min = min(value_min if value_min is not None else stats["min"], stats["min"])
        value_max = max(value_max if value_max is not None else stats["max"], stats["max"])
        negative += pc.sum(pc.less(columns["Sample Measurement"], 0)).as_py()

        mask = pc.and_(
            pc.equal(columns["State Code"], "36"),
            pc.is_in(columns["County Code"], value_set=nyc_county_codes),
        )
        nyc = batch.filter(mask)
        if not nyc.num_rows:
            continue
        nyc_rows += nyc.num_rows
        nc = {name: nyc.column(i) for i, name in enumerate(nyc.schema.names)}
        nyc_counties.update(pc.unique(nc["County Name"]).to_pylist())
        nyc_sites.update(zip(
            nc["County Code"].to_pylist(), nc["Site Num"].to_pylist(),
            nc["Latitude"].to_pylist(), nc["Longitude"].to_pylist(),
        ))
        nyc_dates = nc["Date Local"].to_pylist()
        nyc_date_min = min(nyc_date_min or nyc_dates[0], min(nyc_dates))
        nyc_date_max = max(nyc_date_max or nyc_dates[0], max(nyc_dates))
        nyc_stats = pc.min_max(nc["Sample Measurement"]).as_py()
        nyc_value_min = min(
            nyc_value_min if nyc_value_min is not None else nyc_stats["min"], nyc_stats["min"]
        )
        nyc_value_max = max(
            nyc_value_max if nyc_value_max is not None else nyc_stats["max"], nyc_stats["max"]
        )
        nyc_negative += pc.sum(pc.less(nc["Sample Measurement"], 0)).as_py()

        key_columns = [
            "State Code", "County Code", "Site Num", "Parameter Code", "POC",
            "Date Local", "Time Local",
        ]
        for key in zip(*(nc[name].to_pylist() for name in key_columns)):
            duplicate_keys += key in nyc_keys
            nyc_keys.add(key)
        for date, time, county, site in zip(
            nc["Date Local"].to_pylist(), nc["Time Local"].to_pylist(),
            nc["County Name"].to_pylist(), nc["Site Num"].to_pylist(),
        ):
            nyc_hours[(date, time)] += 1
            nyc_site_rows[(county, site)] += 1

    return {
        "file": path.name,
        "bytes": path.stat().st_size,
        "rows": rows,
        "schema": [{"name": field.name, "type": str(field.type)} for field in reader.schema],
        "null_counts": dict(nulls),
        "date_local_min": date_min,
        "date_local_max": date_max,
        "sample_measurement_min": value_min,
        "sample_measurement_max": value_max,
        "negative_measurements": negative,
        "site_count": len(sites),
        "distinct_values": {key: sorted(values) for key, values in distinct.items()},
        "nyc": {
            "rows": nyc_rows,
            "county_names": sorted(nyc_counties),
            "site_count": len(nyc_sites),
            "date_local_min": nyc_date_min,
            "date_local_max": nyc_date_max,
            "sample_measurement_min": nyc_value_min,
            "sample_measurement_max": nyc_value_max,
            "negative_measurements": nyc_negative,
            "candidate_key_duplicate_rows": duplicate_keys,
            "unique_local_hours": len(nyc_hours),
            "observations_per_hour_min": min(nyc_hours.values()),
            "observations_per_hour_max": max(nyc_hours.values()),
            "observations_per_hour_mean": nyc_rows / len(nyc_hours),
            "site_row_counts": {
                f"{county}/{site}": count
                for (county, site), count in sorted(nyc_site_rows.items())
            },
        },
    }


def main():
    taxi = [profile_taxi(path) for path in TAXI_FILES]
    profile = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "runtime": {
            "python": platform.python_version(),
            "pandas": pd.__version__,
            "pyarrow": pa.__version__,
        },
        "integrity": verify_integrity(),
        "taxi": taxi,
        "taxi_total_rows": sum(item["rows"] for item in taxi),
        "weather": profile_weather(),
        "taxi_zones": profile_zones(taxi),
        "air_quality": profile_air_quality(),
    }
    OUTPUT.parent.mkdir(exist_ok=True)
    OUTPUT.write_text(json.dumps(profile, ensure_ascii=False, indent=2), encoding="utf-8")
    assert profile["integrity"]["manifest_matches"]
    assert profile["taxi_total_rows"] == sum(item["rows"] for item in profile["taxi"])
    print(json.dumps({
        "output": str(OUTPUT),
        "taxi_rows": profile["taxi_total_rows"],
        "weather_rows": profile["weather"]["rows"],
        "zone_rows": profile["taxi_zones"]["rows"],
        "air_quality_rows": profile["air_quality"]["rows"],
        "nyc_air_quality_rows": profile["air_quality"]["nyc"]["rows"],
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
