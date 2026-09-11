from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def build_spark(app_name: str = "urban-data") -> SparkSession:
    builder = (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "8")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
