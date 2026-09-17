import argparse
import json
import sys

from urban_data.analytics import query_names, run_analytic_query
from urban_data.config import load_datasets_config, resolve_dataset
from urban_data.benchmark import benchmark_taxi_storage
from urban_data.integrate import integrate_taxi_trips
from urban_data.ingest import ingest_dataset
from urban_data.optimization import benchmark_analytical_optimizations
from urban_data.products import materialize_products, product_names
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

    sub.add_parser("integrate", help="Build the integrated taxi-trips Gold table")
    sub.add_parser("benchmark", help="Compare two taxi Delta storage strategies")

    query = sub.add_parser("query", help="Run one Week 2 analytical Spark SQL query")
    query.add_argument("--name", required=True, choices=query_names())

    products = sub.add_parser("products", help="Materialize Week 2 Delta data products")
    products.add_argument(
        "--product", default="all", choices=("all", *product_names()),
        help="Product to refresh, or all products (default)",
    )
    sub.add_parser("benchmark-analytics", help="Benchmark Week 2 query optimizations")

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

    if args.command == "integrate":
        spark = build_spark(app_name="urban-data-integrate")
        try:
            print(json.dumps(integrate_taxi_trips(spark), default=str, indent=2, sort_keys=True))
        finally:
            spark.stop()
        return 0

    if args.command == "benchmark":
        spark = build_spark(app_name="urban-data-benchmark")
        try:
            print(json.dumps(benchmark_taxi_storage(spark), default=str, indent=2, sort_keys=True))
        finally:
            spark.stop()
        return 0

    if args.command == "query":
        spark = build_spark(app_name="urban-data-week2-query")
        try:
            rows = [row.asDict(recursive=True) for row in run_analytic_query(spark, args.name).collect()]
            print(json.dumps(rows, default=str, indent=2, sort_keys=True))
        finally:
            spark.stop()
        return 0

    if args.command == "products":
        spark = build_spark(app_name="urban-data-week2-products")
        try:
            results = materialize_products(spark, args.product)
            print(json.dumps(results, default=str, indent=2, sort_keys=True))
        finally:
            spark.stop()
        return 0

    if args.command == "benchmark-analytics":
        spark = build_spark(app_name="urban-data-week2-benchmark")
        try:
            report = benchmark_analytical_optimizations(spark)
            print(json.dumps(report, default=str, indent=2, sort_keys=True))
        finally:
            spark.stop()
        return 0

    return 1


if __name__ == "__main__":
    sys.exit(main())
