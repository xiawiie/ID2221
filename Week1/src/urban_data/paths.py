from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATASETS = PROJECT_ROOT / "datasets"
CONFIG = PROJECT_ROOT / "config" / "datasets.yml"
LAKEHOUSE = PROJECT_ROOT / "lakehouse"
METADATA_RUNS = LAKEHOUSE / "metadata" / "ingestion_runs"
