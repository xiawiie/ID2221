# Taxi Storage Benchmark Report

## Run Setup

- Input: three taxi Parquet files under `datasets/` (`yellow_tripdata_2024-01/02/03.parquet`)
- Rows: 9,551,977 accepted trips after the same Silver quality split used in ingestion
- Runtime: Windows 11 native, Conda env `id2221-week1`, OpenJDK 17, Python 3.12, PySpark 3.5.7, Delta Lake 3.3.2
- Spark master and memory: `local[8]`, Driver 4 GB
- Compression: Snappy
- Hadoop on Windows: `tools/hadoop/` (`HADOOP_HOME`, `winutils.exe`)
- Timing protocol: one warmup followed by three measured runs; the median is reported
- Generated at (UTC): 2026-09-12T08:47:38
- Machine-readable result: `artifacts/benchmark_report.json`
- Physical execution plans: `artifacts/benchmark_physical_plans.json`

Write time means "read configured taxi Parquet source files, apply Silver acceptance transforms, and commit the benchmark Delta layout". Each strategy is timed independently from source read through Delta commit.

## Storage Metrics

| Strategy | Rows | Write seconds | Data size | Data files | Average file size |
| --- | ---: | ---: | ---: | ---: | ---: |
| Unpartitioned | 9,551,977 | 18.453 | 815.0 MiB | 10 | 81.5 MiB |
| Partitioned by `pickup_month` | 9,551,977 | 17.344 | 820.0 MiB | 23 | 35.7 MiB |

Delta metadata adds less than 0.2 MiB in both cases. The partitioned layout produces more than twice as many files and smaller average files. In this run, ingestion-to-Delta time was effectively tied between layouts.

## Query Latency

| Query | Unpartitioned median | Partitioned median | Unpartitioned / partitioned |
| --- | ---: | ---: | ---: |
| Trips per pickup borough | 527.601 ms | 558.957 ms | 0.94x |
| Average trip duration per day | 491.303 ms | 505.720 ms | 0.97x |
| Average fare per pickup borough | 539.199 ms | 509.505 ms | 1.06x |

Each query returned the same number of rows and the same SHA-256 result signature on both layouts. On this Windows local run, the three assigned full-range aggregates perform almost identically. The physical plans still scan the full dataset; none of the queries filters by month, so partition pruning does not apply.

## Conclusion

For the assigned full-range queries on this Windows local Spark setup, query latency is effectively a tie. The unpartitioned table remains simpler to operate: fewer files, larger average file size, and similar query performance. Source-to-Delta ingestion time was also effectively tied in this run.

Default recommendation for the current query workload: **unpartitioned storage**, because it delivers the same answers with fewer files and no loss in aggregate-query performance on this machine. Revisit month partitioning only when month-filtered reads or incremental retention become dominant. At 20x volume, rerun this benchmark with realistic filtered workloads before changing the production layout.

## Reproduce

From PowerShell, in the project root:

```powershell
$env:JAVA_HOME = "D:\anaconda3\envs\id2221-week1\Library"
& "D:\DD2221\ID2221-main\Week1\scripts\run_win.ps1" benchmark
```

Adjust `JAVA_HOME` if your Conda environment is installed elsewhere.
