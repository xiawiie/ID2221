from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "lakehouse" / "_smoke_delta"

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


builder = (
    SparkSession.builder.master("local[2]")
    .appName("week1-delta-smoke")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
try:
    spark.createDataFrame([(1, "ok"), (2, "ok")], ["id", "status"]).write.format(
        "delta"
    ).mode("overwrite").save(str(OUTPUT))
    rows = spark.read.format("delta").load(str(OUTPUT)).orderBy("id").collect()
    assert [(row.id, row.status) for row in rows] == [(1, "ok"), (2, "ok")]
    print({"spark_version": spark.version, "delta_path": str(OUTPUT), "rows": 2})
finally:
    spark.stop()
