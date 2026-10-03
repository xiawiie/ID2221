# ID2221 Week 1 Benchmark Report

**Project:** Generic Urban Data Integration Platform  
**Group members:** Add names and KTH IDs before submission  
**Benchmark evidence date:** 12 September 2026

## Objective and Method

This benchmark evaluates two physical Delta Lake layouts for the Taxi Trips dataset:

1. **Unpartitioned Delta:** one table without a partition column.
2. **Partitioned Delta:** the same table partitioned by `pickup_month`.

Both layouts materialize the same 9,551,977 accepted taxi trips from the three configured Parquet source files. The write-time measurement includes source read, the Silver acceptance transforms, and the Delta commit. Snappy compression was used. Each query received one warm-up execution followed by three measured executions; the median measured latency is reported. The run used Windows 11 native, Python 3.12, PySpark 3.5.7, Delta Lake 3.3.2, OpenJDK 17, Spark `local[8]`, and a 4 GB driver heap.

## Storage Results

| Strategy | Rows | Write time | Data size | Files | Average file size |
| --- | ---: | ---: | ---: | ---: | ---: |
| Unpartitioned | 9,551,977 | 18.453 s | 815.0 MiB | 10 | 81.5 MiB |
| Partitioned by `pickup_month` | 9,551,977 | 17.344 s | 820.0 MiB | 23 | 35.7 MiB |

The layouts have nearly identical write time. Partitioning created more than twice as many files, smaller average files, and a slightly larger active data footprint.

## Query Results

| Required query | Unpartitioned median | Partitioned median | Faster layout |
| --- | ---: | ---: | --- |
| Number of taxi trips per pickup borough | 527.601 ms | 558.957 ms | Unpartitioned |
| Average trip duration per day | 491.303 ms | 505.720 ms | Unpartitioned |
| Average fare per pickup borough | 539.199 ms | 509.505 ms | Partitioned |

For every query, the two layouts returned the same row count and the same SHA-256 result signature. The result equivalence check confirms that the comparison concerns physical storage behavior rather than different logical data.

## Performance Discussion and Recommendation

All three assigned queries aggregate the full data range and do not filter by `pickup_month`. Consequently, neither query can use partition pruning. The observed latencies are close: the partitioned layout is slightly faster for average fare, while the unpartitioned layout is faster for the other two aggregates. This variation does not justify the operational cost of 23 smaller files for the present workload.

We recommend the **unpartitioned layout** for these assigned full-range analytical queries because it produces fewer, larger files with effectively equivalent write and query performance. The `pickup_month` layout should be reconsidered when real workloads commonly filter by month, support month-based retention, or ingest enough data for partition pruning to outweigh small-file overhead. At a larger scale, the same benchmark should be repeated with representative month-filtered queries before changing the production design.

Machine-readable measurements and physical plans are retained in `artifacts/benchmark_report.json` and `artifacts/benchmark_physical_plans.json`.
