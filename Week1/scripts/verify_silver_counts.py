from urban_data.spark_session import build_spark

spark = build_spark("verify-all-silver")
try:
    taxi = spark.read.format("delta").load("lakehouse/silver/taxi_trips").count()
    zones = spark.read.format("delta").load("lakehouse/silver/taxi_zones").count()
    weather = spark.read.format("delta").load("lakehouse/silver/weather_hourly").count()
    air = spark.read.format("delta").load("lakehouse/silver/air_quality_hourly_nyc").count()
    print(
        {
            "taxi_trips": taxi,
            "taxi_zones": zones,
            "weather_hourly": weather,
            "air_quality_hourly_nyc": air,
        }
    )
finally:
    spark.stop()
