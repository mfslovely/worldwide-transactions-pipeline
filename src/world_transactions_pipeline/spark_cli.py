import argparse
import json
from pathlib import Path

from .spark_pipeline import DEFAULT_RECORD_COUNT, create_spark_session, run_spark_pipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Distributed worldwide transaction pipeline")
    parser.add_argument("--count", type=int, default=DEFAULT_RECORD_COUNT)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--partitions", type=int, default=8)
    parser.add_argument("--master", default="local[*]")
    parser.add_argument("--output-dir", type=Path, default=Path("output/spark-million"))
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if args.count < 1:
        raise SystemExit("--count must be at least 1")
    if args.partitions < 1:
        raise SystemExit("--partitions must be at least 1")

    spark = create_spark_session(args.master, args.partitions)
    try:
        summary = run_spark_pipeline(
            spark=spark,
            count=args.count,
            output_dir=args.output_dir,
            seed=args.seed,
            partitions=args.partitions,
        )
        print(json.dumps(summary, indent=2))
    finally:
        spark.stop()
