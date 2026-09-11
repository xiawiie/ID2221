import hashlib
from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def file_sha256(path) -> str:
    with open(path, "rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def new_run_id() -> str:
    return uuid4().hex


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def with_bronze_metadata(
    df: DataFrame,
    *,
    source_file: str,
    source_sha256: str,
    run_id: str,
    schema_version: str,
    ingested_at: datetime,
) -> DataFrame:
    return (
        df.withColumn("_source_file", F.lit(source_file))
        .withColumn("_source_sha256", F.lit(source_sha256))
        .withColumn("_ingested_at", F.lit(ingested_at))
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_schema_version", F.lit(schema_version))
    )


def rename_taxi_columns(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("VendorID").alias("vendor_id"),
        F.col("tpep_pickup_datetime").alias("pickup_ts_local"),
        F.col("tpep_dropoff_datetime").alias("dropoff_ts_local"),
        F.col("passenger_count"),
        F.col("trip_distance"),
        F.col("RatecodeID").alias("ratecode_id"),
        F.col("store_and_fwd_flag"),
        F.col("PULocationID").alias("pu_location_id"),
        F.col("DOLocationID").alias("do_location_id"),
        F.col("payment_type"),
        F.col("fare_amount"),
        F.col("extra"),
        F.col("mta_tax"),
        F.col("tip_amount"),
        F.col("tolls_amount"),
        F.col("improvement_surcharge"),
        F.col("total_amount"),
        F.col("congestion_surcharge"),
        F.col("Airport_fee").alias("airport_fee"),
        F.col("_source_file"),
        F.col("_source_sha256"),
        F.col("_ingested_at"),
        F.col("_run_id"),
        F.col("_schema_version"),
    )


def taxi_silver_columns(df: DataFrame, file_month: str) -> DataFrame:
    month_start = F.to_timestamp(F.lit(f"{file_month}-01"))
    month_end = F.add_months(month_start, 1)
    duration_seconds = F.unix_timestamp("dropoff_ts_local") - F.unix_timestamp(
        "pickup_ts_local"
    )
    return (
        df.withColumn("source_file_month", F.lit(file_month))
        .withColumn(
            "trip_duration_minutes",
            (duration_seconds / F.lit(60.0)).cast("double"),
        )
        .withColumn("pickup_month", F.date_format("pickup_ts_local", "yyyy-MM"))
        .withColumn(
            "trip_id",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.col("vendor_id").cast("string"),
                    F.col("pickup_ts_local").cast("string"),
                    F.col("dropoff_ts_local").cast("string"),
                    F.col("pu_location_id").cast("string"),
                    F.col("do_location_id").cast("string"),
                    F.col("fare_amount").cast("string"),
                    F.col("total_amount").cast("string"),
                ),
                256,
            ),
        )
        .withColumn(
            "has_negative_fare",
            F.col("fare_amount").isNotNull() & (F.col("fare_amount") < 0),
        )
        .withColumn(
            "has_negative_total",
            F.col("total_amount").isNotNull() & (F.col("total_amount") < 0),
        )
        .withColumn(
            "pickup_outside_file_month",
            F.col("pickup_ts_local").isNull()
            | (F.col("pickup_ts_local") < month_start)
            | (F.col("pickup_ts_local") >= month_end),
        )
        .withColumn(
            "extreme_trip_distance",
            F.col("trip_distance").isNotNull() & (F.col("trip_distance") > 100),
        )
        .withColumn(
            "quality_flags",
            F.nullif(
                F.trim(
                    F.concat_ws(
                        ",",
                        F.when(F.col("has_negative_fare"), F.lit("negative_fare")),
                        F.when(F.col("has_negative_total"), F.lit("negative_total")),
                        F.when(
                            F.col("pickup_outside_file_month"),
                            F.lit("pickup_outside_file_month"),
                        ),
                        F.when(
                            F.col("extreme_trip_distance"),
                            F.lit("extreme_trip_distance"),
                        ),
                    )
                ),
                F.lit(""),
            ),
        )
    )


def quarantine_mask(df: DataFrame) -> F.Column:
    return (
        F.col("pickup_ts_local").isNull()
        | F.col("dropoff_ts_local").isNull()
        | (F.col("dropoff_ts_local") <= F.col("pickup_ts_local"))
    )


def split_taxi_quality(df: DataFrame, file_month: str) -> tuple[DataFrame, DataFrame]:
    silver = taxi_silver_columns(rename_taxi_columns(df), file_month)
    bad = silver.filter(quarantine_mask(silver))
    good = silver.filter(~quarantine_mask(silver))
    return good, bad


def rename_zone_columns(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("LocationID").alias("location_id"),
        F.col("Borough").alias("borough"),
        F.col("Zone").alias("zone"),
        F.col("service_zone"),
        F.col("_source_file"),
        F.col("_source_sha256"),
        F.col("_ingested_at"),
        F.col("_run_id"),
        F.col("_schema_version"),
    )


def split_zone_quality(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    renamed = rename_zone_columns(df).withColumn(
        "missing_location_id",
        F.col("location_id").isNull(),
    ).withColumn(
        "duplicate_location_id",
        F.count("*").over(Window.partitionBy("location_id")) > 1,
    )
    bad = renamed.filter(F.col("missing_location_id") | F.col("duplicate_location_id"))
    good = renamed.filter(~F.col("missing_location_id") & ~F.col("duplicate_location_id"))
    return good.drop("missing_location_id", "duplicate_location_id"), bad.drop(
        "missing_location_id", "duplicate_location_id"
    )


def rename_weather_columns(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("year"),
        F.col("month"),
        F.col("day"),
        F.col("hour"),
        F.col("temp"),
        F.col("temp_source"),
        F.col("rhum"),
        F.col("rhum_source"),
        F.col("prcp"),
        F.col("prcp_source"),
        F.col("snwd"),
        F.col("snwd_source"),
        F.col("wdir"),
        F.col("wdir_source"),
        F.col("wspd"),
        F.col("wspd_source"),
        F.col("wpgt"),
        F.col("wpgt_source"),
        F.col("pres"),
        F.col("pres_source"),
        F.col("cldc"),
        F.col("cldc_source"),
        F.col("coco"),
        F.col("coco_source"),
        F.col("_source_file"),
        F.col("_source_sha256"),
        F.col("_ingested_at"),
        F.col("_run_id"),
        F.col("_schema_version"),
    )


def weather_silver(df: DataFrame) -> DataFrame:
    renamed = rename_weather_columns(df)
    return renamed.withColumn(
        "observation_ts_local",
        F.make_timestamp(
            F.col("year").cast("int"),
            F.col("month").cast("int"),
            F.col("day").cast("int"),
            F.col("hour").cast("int"),
            F.lit(0),
            F.lit(0),
        ),
    ).withColumn(
        "timestamp_semantics",
        F.lit("local_wall_clock_unconfirmed_timezone"),
    )


def split_weather_quality(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    silver = weather_silver(df).withColumn(
        "observation_ts_count",
        F.count("*").over(Window.partitionBy("observation_ts_local")),
    )
    bad = silver.filter(
        F.col("year").isNull()
        | F.col("month").isNull()
        | F.col("day").isNull()
        | F.col("hour").isNull()
        | F.col("observation_ts_local").isNull()
        | (F.col("observation_ts_count") > 1)
    )
    good = silver.filter(
        F.col("year").isNotNull()
        & F.col("month").isNotNull()
        & F.col("day").isNotNull()
        & F.col("hour").isNotNull()
        & F.col("observation_ts_local").isNotNull()
        & (F.col("observation_ts_count") == 1)
    )
    return good.drop("observation_ts_count"), bad.drop("observation_ts_count")


def filter_air_quality_nyc(df: DataFrame) -> DataFrame:
    return df.filter(
        (F.col("state_code") == "36")
        & (
            F.col("county_name").isin(
                "Bronx", "Kings", "New York", "Queens", "Richmond"
            )
        )
        & (F.col("parameter_code") == "88101")
    )


def normalize_air_quality_columns(df: DataFrame) -> DataFrame:
    return (
        df.withColumnRenamed("State Code", "state_code")
        .withColumnRenamed("County Code", "county_code")
        .withColumnRenamed("Site Num", "site_num")
        .withColumnRenamed("Parameter Code", "parameter_code")
        .withColumnRenamed("POC", "poc")
        .withColumnRenamed("Latitude", "latitude")
        .withColumnRenamed("Longitude", "longitude")
        .withColumnRenamed("Datum", "datum")
        .withColumnRenamed("Parameter Name", "parameter_name")
        .withColumnRenamed("Date Local", "date_local")
        .withColumnRenamed("Time Local", "time_local")
        .withColumnRenamed("Date GMT", "date_gmt")
        .withColumnRenamed("Time GMT", "time_gmt")
        .withColumnRenamed("Sample Measurement", "pm25")
        .withColumnRenamed("Units of Measure", "units_of_measure")
        .withColumnRenamed("MDL", "mdl")
        .withColumnRenamed("Uncertainty", "uncertainty")
        .withColumnRenamed("Qualifier", "qualifier")
        .withColumnRenamed("Method Type", "method_type")
        .withColumnRenamed("Method Code", "method_code")
        .withColumnRenamed("Method Name", "method_name")
        .withColumnRenamed("State Name", "state_name")
        .withColumnRenamed("County Name", "county_name")
        .withColumnRenamed("Date of Last Change", "date_of_last_change")
    )


def air_station_silver(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "observation_ts_utc",
        F.to_timestamp(F.concat_ws(" ", F.col("date_gmt"), F.col("time_gmt"))),
    ).withColumn(
        "site_key",
        F.concat_ws("/", F.col("county_name"), F.col("site_num")),
    )


def split_air_station_quality(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    silver = air_station_silver(df)
    bad = silver.filter(
        F.col("observation_ts_utc").isNull()
        | F.col("pm25").isNull()
        | (F.col("pm25") < 0)
    )
    good = silver.filter(
        F.col("observation_ts_utc").isNotNull()
        & F.col("pm25").isNotNull()
        & (F.col("pm25") >= 0)
    )
    return good, bad


def aggregate_air_hourly(stations: DataFrame) -> DataFrame:
    return stations.groupBy("observation_ts_utc").agg(
        F.expr("percentile_approx(pm25, 0.5)").alias("pm25_median"),
        F.avg("pm25").alias("pm25_mean"),
        F.min("pm25").alias("pm25_min"),
        F.max("pm25").alias("pm25_max"),
        F.count("*").alias("site_observation_count"),
        F.countDistinct("site_key").alias("site_count"),
    )
