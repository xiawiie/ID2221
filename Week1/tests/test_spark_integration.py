import os
import shutil
import tempfile
import unittest
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from urban_data.config import resolve_dataset
from urban_data.ingest import _read_parquet_with_schema
from urban_data.schemas import TAXI_RAW_SCHEMA
from urban_data.spark_session import build_spark
from urban_data.taxi_sources import load_accepted_taxi_trips_from_sources

PROJECT_ROOT = Path(__file__).resolve().parents[1]
HAS_JAVA = bool(shutil.which("java") or os.environ.get("JAVA_HOME"))


@unittest.skipUnless(HAS_JAVA, "Java runtime required for Spark integration tests")
class SparkIntegrationTest(unittest.TestCase):
    spark = None
    tmp_dir: Path | None = None

    @classmethod
    def setUpClass(cls):
        cls.tmp_dir = Path(tempfile.mkdtemp(prefix="week1-spark-it-"))
        cls.spark = build_spark("week1-spark-integration-tests")

    @classmethod
    def tearDownClass(cls):
        if cls.spark is not None:
            cls.spark.stop()
        if cls.tmp_dir is not None:
            shutil.rmtree(cls.tmp_dir, ignore_errors=True)

    def test_rejects_wrong_parquet_column_name(self):
        source = resolve_dataset("taxi_2024_01")["source_path"]
        self.assertTrue(source.is_file(), "taxi fixture missing")
        bad_path = self.tmp_dir / "bad_name.parquet"
        (
            self.spark.read.parquet(str(source))
            .withColumnRenamed("VendorID", "wrong_id")
            .write.mode("overwrite")
            .parquet(str(bad_path))
        )
        with self.assertRaisesRegex(ValueError, "Schema mismatch"):
            _read_parquet_with_schema(self.spark, bad_path, TAXI_RAW_SCHEMA)

    def test_rejects_wrong_parquet_column_type(self):
        source = resolve_dataset("taxi_2024_01")["source_path"]
        bad_path = self.tmp_dir / "bad_type.parquet"
        (
            self.spark.read.parquet(str(source))
            .withColumn("VendorID", F.col("VendorID").cast("string"))
            .write.mode("overwrite")
            .parquet(str(bad_path))
        )
        with self.assertRaisesRegex(ValueError, "Type mismatch for 'VendorID'"):
            _read_parquet_with_schema(self.spark, bad_path, TAXI_RAW_SCHEMA)

    def test_load_accepted_taxi_from_sources_matches_expected_rows(self):
        trips, source_files = load_accepted_taxi_trips_from_sources(self.spark)
        self.assertEqual(len(source_files), 3)
        self.assertEqual(trips.count(), 9_551_977)


if __name__ == "__main__":
    unittest.main()
