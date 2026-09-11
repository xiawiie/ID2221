from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from urban_data.paths import LAKEHOUSE


GOLD_PATH = LAKEHOUSE / "gold" / "integrated_taxi_trips"


def _read_silver(spark: SparkSession, name: str) -> DataFrame:
    return spark.read.format("delta").load(str(LAKEHOUSE / "silver" / name))


def _assert_unique(df: DataFrame, key: str, name: str) -> None:
    duplicates = df.groupBy(key).count().filter(F.col("count") > 1).limit(1).count()
    if duplicates:
        raise ValueError(f"{name} has duplicate join key {key!r}")


def _assert_unchanged_count(before: DataFrame, after: DataFrame, step: str) -> int:
    before_count = before.count()
    after_count = after.count()
    if before_count != after_count:
        raise ValueError(
            f"Join changed row count at {step}: before={before_count}, after={after_count}"
        )
    return after_count


def _zone_columns(zones: DataFrame, prefix: str) -> DataFrame:
    return zones.select(
        F.col("location_id").alias(f"{prefix}_location_id"),
        F.col("zone").alias(f"{prefix}_zone"),
        F.col("borough").alias(f"{prefix}_borough"),
        F.col("service_zone").alias(f"{prefix}_service_zone"),
        F.lit(True).alias(f"_{prefix}_zone_matched"),
    )


def _weather_columns(weather: DataFrame) -> DataFrame:
    return weather.select(
        F.col("observation_ts_local").alias("weather_observation_ts_local"),
        F.col("temp").alias("weather_temp"),
        F.col("rhum").alias("weather_relative_humidity"),
        F.col("prcp").alias("weather_precipitation"),
        F.col("snwd").alias("weather_snow_depth"),
        F.col("wdir").alias("weather_wind_direction"),
        F.col("wspd").alias("weather_wind_speed"),
        F.col("wpgt").alias("weather_wind_peak_gust"),
        F.col("pres").alias("weather_pressure"),
        F.col("cldc").alias("weather_cloud_cover"),
        F.col("coco").alias("weather_condition_code"),
        F.col("timestamp_semantics").alias("weather_timestamp_semantics"),
        F.lit(True).alias("_weather_matched"),
    )


def _air_quality_columns(air_quality: DataFrame) -> DataFrame:
    return air_quality.select(
        F.col("observation_ts_utc").alias("air_observation_ts_utc"),
        F.col("pm25_median").alias("air_pm25_median"),
        F.col("pm25_mean").alias("air_pm25_mean"),
        F.col("pm25_min").alias("air_pm25_min"),
        F.col("pm25_max").alias("air_pm25_max"),
        F.col("site_count").alias("air_site_count"),
        F.col("site_observation_count").alias("air_site_observation_count"),
        F.lit(True).alias("_air_quality_matched"),
    )


def build_integrated_taxi_trips(
    spark: SparkSession,
    *,
    taxi: DataFrame,
    zones: DataFrame,
    weather: DataFrame,
    air_quality: DataFrame,
) -> tuple[DataFrame, dict[str, int]]:
    _assert_unique(zones, "location_id", "taxi_zones")
    _assert_unique(weather, "observation_ts_local", "weather_hourly")
    _assert_unique(air_quality, "observation_ts_utc", "air_quality_hourly_nyc")

    enriched = taxi.withColumn(
        "pickup_hour_local", F.date_trunc("hour", F.col("pickup_ts_local"))
    ).withColumn(
        "pickup_ts_utc",
        F.to_utc_timestamp(F.col("pickup_ts_local"), "America/New_York"),
    )
    enriched = enriched.withColumn(
        "pickup_hour_utc", F.date_trunc("hour", F.col("pickup_ts_utc"))
    )

    pickup_zones = _zone_columns(zones, "pickup")
    dropoff_zones = _zone_columns(zones, "dropoff")
    weather = _weather_columns(weather)
    air_quality = _air_quality_columns(air_quality)

    enriched = enriched.join(
        F.broadcast(pickup_zones),
        enriched.pu_location_id == pickup_zones.pickup_location_id,
        "left",
    )
    rows_after_pickup_zone = _assert_unchanged_count(taxi, enriched, "pickup zone")

    enriched = enriched.join(
        F.broadcast(dropoff_zones),
        enriched.do_location_id == dropoff_zones.dropoff_location_id,
        "left",
    )
    rows_after_dropoff_zone = _assert_unchanged_count(
        taxi, enriched, "dropoff zone"
    )

    enriched = enriched.join(
        F.broadcast(weather),
        enriched.pickup_hour_local == weather.weather_observation_ts_local,
        "left",
    )
    rows_after_weather = _assert_unchanged_count(taxi, enriched, "weather")

    enriched = enriched.join(
        F.broadcast(air_quality),
        enriched.pickup_hour_utc == air_quality.air_observation_ts_utc,
        "left",
    )
    rows_after_air_quality = _assert_unchanged_count(
        taxi, enriched, "air quality"
    )

    enriched = (
        enriched.withColumn(
            "pickup_zone_match_status",
            F.when(F.col("_pickup_zone_matched"), "matched").otherwise("missing"),
        )
        .withColumn(
            "dropoff_zone_match_status",
            F.when(F.col("_dropoff_zone_matched"), "matched").otherwise("missing"),
        )
        .withColumn(
            "weather_match_status",
            F.when(F.col("_weather_matched"), "matched").otherwise("missing"),
        )
        .withColumn(
            "air_quality_match_status",
            F.when(F.col("_air_quality_matched"), "matched").otherwise("missing"),
        )
        .drop(
            "_pickup_zone_matched",
            "_dropoff_zone_matched",
            "_weather_matched",
            "_air_quality_matched",
            "pickup_location_id",
            "dropoff_location_id",
            "weather_observation_ts_local",
            "air_observation_ts_utc",
        )
    )

    pickup_zone_unmatched = enriched.filter(
        F.col("pickup_zone_match_status") == "missing"
    ).count()
    dropoff_zone_unmatched = enriched.filter(
        F.col("dropoff_zone_match_status") == "missing"
    ).count()
    if pickup_zone_unmatched or dropoff_zone_unmatched:
        raise ValueError(
            "Taxi zone lookup misses: "
            f"pickup={pickup_zone_unmatched}, dropoff={dropoff_zone_unmatched}"
        )

    metrics = {
        "taxi_rows": taxi.count(),
        "rows_after_pickup_zone": rows_after_pickup_zone,
        "rows_after_dropoff_zone": rows_after_dropoff_zone,
        "rows_after_weather": rows_after_weather,
        "rows_after_air_quality": rows_after_air_quality,
        "pickup_zone_unmatched": pickup_zone_unmatched,
        "dropoff_zone_unmatched": dropoff_zone_unmatched,
        "weather_unmatched": enriched.filter(
            F.col("weather_match_status") == "missing"
        ).count(),
        "air_quality_unmatched": enriched.filter(
            F.col("air_quality_match_status") == "missing"
        ).count(),
    }
    return enriched, metrics


def integrate_taxi_trips(spark: SparkSession) -> dict:
    taxi = _read_silver(spark, "taxi_trips")
    zones = _read_silver(spark, "taxi_zones")
    weather = _read_silver(spark, "weather_hourly")
    air_quality = _read_silver(spark, "air_quality_hourly_nyc")

    integrated, metrics = build_integrated_taxi_trips(
        spark, taxi=taxi, zones=zones, weather=weather, air_quality=air_quality
    )
    integrated.write.format("delta").mode("overwrite").partitionBy(
        "source_file_month"
    ).save(str(GOLD_PATH))

    return {
        "status": "success",
        "output_path": str(GOLD_PATH),
        "row_count_checks": metrics,
        "gold_rows": integrated.count(),
        "weather_join": "pickup local hour to city hourly weather",
        "air_quality_join": "pickup UTC hour to NYC hourly PM2.5 aggregate",
        "missing_environment_policy": "left join; preserve NULL and match status",
    }
