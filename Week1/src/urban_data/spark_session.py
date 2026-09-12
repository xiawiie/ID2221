import os
import sys
import time
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parents[2]
HADOOP_HOME = PROJECT_ROOT / "tools" / "hadoop"
SPARK_TMP = PROJECT_ROOT / "tools" / "spark-tmp"

os.environ["TZ"] = "UTC"
if hasattr(time, "tzset"):
    time.tzset()

if sys.platform.startswith("win"):
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("HADOOP_HOME", str(HADOOP_HOME))
    os.environ["PATH"] = str(HADOOP_HOME / "bin") + os.pathsep + os.environ.get("PATH", "")
    SPARK_TMP.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("SPARK_LOCAL_DIRS", str(SPARK_TMP))
    os.environ.setdefault("TMP", str(SPARK_TMP))
    os.environ.setdefault("TEMP", str(SPARK_TMP))
    if "PYSPARK_PYTHON" not in os.environ:
        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def build_spark(app_name: str = "urban-data") -> SparkSession:
    java_opts = "-Duser.timezone=UTC"
    if sys.platform.startswith("win"):
        hadoop_home = HADOOP_HOME.as_posix()
        hadoop_bin = (HADOOP_HOME / "bin").as_posix()
        java_opts = (
            f"-Duser.timezone=UTC "
            f"-Dhadoop.home.dir={hadoop_home} "
            f"-Djava.library.path={hadoop_bin}"
        )

    builder = (
        SparkSession.builder.master("local[8]")
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", java_opts)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "8")
    )
    if sys.platform.startswith("win"):
        builder = builder.config("spark.hadoop.hadoop.home.dir", HADOOP_HOME.as_posix())
    return configure_spark_with_delta_pip(builder).getOrCreate()
