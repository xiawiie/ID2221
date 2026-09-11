# Taxi Storage Benchmark Report

## Run Setup

- Input: `lakehouse/silver/taxi_trips`
- Rows: 9,551,977
- Runtime: WSL, OpenJDK 17, PySpark 3.5.7, Delta Lake 3.3.2
- Spark master and memory: `local[8]`, Driver 4 GB
- Compression: Snappy
- Timing protocol: one warmup followed by three measured runs; the median is reported
- Machine-readable result: `artifacts/benchmark_report.json`
- Physical execution plans: `artifacts/benchmark_physical_plans.json`

Write time means "read the canonical Silver Delta table and materialize the benchmark strategy". It is not a repeated statistical sample; the query timings are the stronger comparison because each has three hot runs.

## Storage Metrics

| Strategy | Rows | Write seconds | Data size | Data files | Average file size |
| --- | ---: | ---: | ---: | ---: | ---: |
| Unpartitioned | 9,551,977 | 67.166 | 827.74 MiB | 20 | 41.39 MiB |
| Partitioned by `pickup_month` | 9,551,977 | 54.622 | 833.02 MiB | 46 | 18.11 MiB |

Delta metadata adds less than 0.2 MiB in both cases. The partitioned layout produces more than twice as many files and smaller average files. Its one-off write was faster, but that single measurement should not be treated as a stable throughput claim.

## Query Latency

| Query | Unpartitioned median | Partitioned median | Partitioned / unpartitioned |
| --- | ---: | ---: | ---: |
| Trips per pickup borough | 1,670.160 ms | 3,255.404 ms | 1.95x |
| Average trip duration per day | 1,468.995 ms | 3,608.642 ms | 2.46x |
| Average fare per pickup borough | 1,639.759 ms | 3,391.770 ms | 2.07x |

Each query returned the same number of rows and the same SHA-256 result signature on both layouts. The physical plans show full scans and aggregation; none of the assigned queries filters by month, so partition pruning cannot offset the extra file and scheduling overhead.

## Conclusion

For the assigned workload, the unpartitioned table is the better default: all three full-range aggregates are roughly two times faster and the table has fewer, larger files. The month-partitioned layout should be reconsidered only when month-filtered queries become common or incremental retention is required. At 20x volume, rerun this benchmark after adding a realistic filtered workload and before changing the production layout.

## Reproduce

```powershell
wsl bash -lc "cd /mnt/c/Users/goahe/Desktop/ID2221/Week1 && bash scripts/run_wsl.sh benchmark"
```
