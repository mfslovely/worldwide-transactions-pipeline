import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

PYSPARK_AVAILABLE = importlib.util.find_spec("pyspark") is not None


@unittest.skipUnless(PYSPARK_AVAILABLE, "PySpark optional dependency is not installed")
class SparkIntegrationTests(unittest.TestCase):
    def test_distributed_pipeline_balances_loaded_and_rejected_rows(self) -> None:
        from world_transactions_pipeline.spark_pipeline import create_spark_session, run_spark_pipeline

        spark = create_spark_session("local[2]", 2)
        try:
            with tempfile.TemporaryDirectory() as directory:
                summary = run_spark_pipeline(spark, 10_000, Path(directory), seed=7, partitions=2)
            self.assertEqual(summary["engine"], "PySpark")
            self.assertEqual(summary["extracted_count"], 10_001)
            self.assertEqual(
                summary["loaded_count"] + summary["rejected_count"], summary["extracted_count"]
            )
            self.assertGreater(summary["rejected_count"], 1)
            self.assertGreater(summary["gross_volume_usd"], 0)
        finally:
            spark.stop()
