from urban_data.spark_session import build_spark

spark = build_spark("verify-taxi-delta")
try:
    counts = {
        "bronze": spark.read.format("delta")
        .load("lakehouse/bronze/taxi_trips/yellow_tripdata_2024-01")
        .count(),
        "silver": spark.read.format("delta")
        .load("lakehouse/silver/taxi_trips")
        .count(),
        "quarantine": spark.read.format("delta")
        .load("lakehouse/quarantine/taxi_trips")
        .count(),
    }
    print(counts)
finally:
    spark.stop()
