"""Run the million-row PySpark pipeline without installing the package."""

from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from world_transactions_pipeline.spark_cli import main  # noqa: E402


if __name__ == "__main__":
    main()
