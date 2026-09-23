import argparse
import json
import sys

from urban_data.evaluation import evaluate_platform
from urban_data.validation.report import build_validation_report
from urban_data.monitoring import (
    build_monitoring_report,
    monitoring_query_names,
    run_monitoring_query,
)
from urban_data.analytics import query_names, run_analytic_query
from urban_data.config import load_datasets_config, resolve_dataset
from urban_data.benchmark import benchmark_taxi_storage
from urban_data.generate_updates import generate_incremental_updates
from urban_data.incremental import apply_incremental_updates
from urban_data.integrate import integrate_taxi_trips
from urban_data.ingest import ingest_dataset
from urban_data.optimization import benchmark_analytical_optimizations
from urban_data.products import materialize_products, product_names
from urban_data.product_refresh import refresh_affected_products, resolve_refresh_plan
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

    sub.add_parser(
        "generate-updates",
        help="Generate Week 3 incremental update files and manifest",
    )

    update = sub.add_parser("update", help="Apply Week 3 incremental dataset updates")
    update.add_argument(
        "--dataset",
        default="all",
        help="Incremental dataset key from config/datasets.yml, or 'all'",
    )
    update.add_argument(
        "--force",
        action="store_true",
        help="Re-apply even if the same update hash already succeeded",
    )

    refresh = sub.add_parser(
        "refresh-products",
        help="Refresh Week 2 products affected by incremental updates",
    )
    refresh.add_argument(
        "--product",
        default="all",
        choices=("all", *product_names()),
        help="Product to refresh, or all affected products (default)",
    )
    refresh.add_argument(
        "--dataset",
        default="latest",
        help="Comma-separated incremental dataset keys, or 'latest'",
    )

    monitor = sub.add_parser("monitor", help="Run Week 3 platform monitoring queries")
    monitor.add_argument(
        "--query",
        choices=monitoring_query_names(),
        help="Run one monitoring SQL query",
    )
    monitor.add_argument(
        "--report",
        action="store_true",
        help="Build the full monitoring report (all queries plus summary)",
    )

    validate = sub.add_parser(
        "validate-report",
        help="Build Week 3 validation summary from quarantine and validation events",
    )

    sub.add_parser(
        "evaluate-platform",
        help="Run Week 3 platform evaluation experiments and save the report artifact",
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

    if args.command == "generate-updates":
        manifest = generate_incremental_updates()
        print(json.dumps(manifest, default=str, indent=2, sort_keys=True))
        return 0

    if args.command == "update":
        spark = build_spark(app_name="urban-data-week3-update")
        try:
            results = apply_incremental_updates(
                spark, args.dataset, force=args.force
            )
            print(json.dumps(results if len(results) > 1 else results[0], default=str, indent=2, sort_keys=True))
        finally:
            spark.stop()
        return 0

    if args.command == "refresh-products":
        spark = build_spark(app_name="urban-data-week3-refresh-products")
        try:
            dataset_keys = None
            if args.dataset != "latest":
                dataset_keys = [item.strip() for item in args.dataset.split(",") if item.strip()]
            report = refresh_affected_products(
                spark,
                dataset_keys=dataset_keys,
                product=args.product,
            )
            print(json.dumps(report, default=str, indent=2, sort_keys=True))
        finally:
            spark.stop()
        return 0

    if args.command == "monitor":
        if not args.report and not args.query:
            parser.error("monitor requires --report or --query")
        spark = build_spark(app_name="urban-data-week3-monitor")
        try:
            if args.report:
                print(json.dumps(build_monitoring_report(spark), default=str, indent=2, sort_keys=True))
            else:
                rows = [
                    row.asDict(recursive=True)
                    for row in run_monitoring_query(spark, args.query).collect()
                ]
                print(json.dumps(rows, default=str, indent=2, sort_keys=True))
        finally:
            spark.stop()
        return 0

    if args.command == "validate-report":
        spark = build_spark(app_name="urban-data-week3-validate-report")
        try:
            print(json.dumps(build_validation_report(spark), default=str, indent=2, sort_keys=True))
        finally:
            spark.stop()
        return 0

    if args.command == "evaluate-platform":
        spark = build_spark(app_name="urban-data-week3-evaluate")
        try:
            print(json.dumps(evaluate_platform(spark), default=str, indent=2, sort_keys=True))
        finally:
            spark.stop()
        return 0

    return 1


if __name__ == "__main__":
    sys.exit(main())
