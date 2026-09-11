import argparse
import json
import sys

from urban_data.config import load_datasets_config, resolve_dataset
from urban_data.ingest import ingest_dataset
from urban_data.spark_session import build_spark


def _dataset_keys(name: str) -> list[str]:
    datasets = load_datasets_config()
    if name == "all":
        return sorted(datasets)
    if name not in datasets:
        known = ", ".join(sorted(datasets))
        raise KeyError(f"Unknown dataset {name!r}; known: {known}, or use 'all'")
    return [name]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="ID2221 Week1 urban data platform")
    sub = parser.add_subparsers(dest="command", required=True)

    ingest = sub.add_parser("ingest", help="Ingest one configured dataset")
    ingest.add_argument(
        "--dataset",
        required=True,
        help="Dataset key from config/datasets.yml, or 'all'",
    )
    ingest.add_argument(
        "--force",
        action="store_true",
        help="Re-ingest even if the same source hash already succeeded",
    )

    args = parser.parse_args(argv)
    if args.command == "ingest":
        results = []
        for key in _dataset_keys(args.dataset):
            cfg = resolve_dataset(key)
            spark = build_spark(app_name=f"urban-data-ingest-{key}")
            try:
                results.append(ingest_dataset(spark, cfg, force=args.force))
            finally:
                spark.stop()
        print(json.dumps(results if len(results) > 1 else results[0], default=str, indent=2, sort_keys=True))
        return 0

    return 1


if __name__ == "__main__":
    sys.exit(main())
