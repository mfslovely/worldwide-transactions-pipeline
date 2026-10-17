import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from world_transactions_pipeline.spark_cli import build_parser
from world_transactions_pipeline.spark_pipeline import DEFAULT_RECORD_COUNT, SUPPORTED_COUNTRIES


class SparkContractTests(unittest.TestCase):
    def test_default_run_is_one_million_records(self) -> None:
        args = build_parser().parse_args([])
        self.assertEqual(args.count, 1_000_000)
        self.assertEqual(DEFAULT_RECORD_COUNT, 1_000_000)

    def test_worldwide_generator_has_ten_countries(self) -> None:
        self.assertEqual(len(SUPPORTED_COUNTRIES), 10)
        self.assertIn("IN", SUPPORTED_COUNTRIES)
        self.assertIn("US", SUPPORTED_COUNTRIES)


if __name__ == "__main__":
    unittest.main()
