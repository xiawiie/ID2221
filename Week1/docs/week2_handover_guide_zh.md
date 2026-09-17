# Week 2 交接指南

## 当前状态

截至 2026-09-17，Week 1 摄取、Gold 集成、四个 Week 2 Delta 数据产品和 Week 2 优化基准均已在本机成功运行。原始数据在 `datasets/` 下保持不变；生成的 Delta 表位于被 Git 忽略的 `lakehouse/` 下。

- Gold `integrated_taxi_trips`：9,551,977 行。
- 区域匹配：上车与下车区域均完整匹配。
- 环境数据：天气与 PM2.5 各有 19 条历史行程未匹配，行程保留且环境字段为 `NULL`。

## 关键代码与证据

| 目的 | 文件或目录 |
| --- | --- |
| 六个 Spark SQL 查询 | `src/urban_data/analytics.py` |
| 四个 Delta 数据产品 | `src/urban_data/products.py` |
| 优化实验、计时和结果签名验证 | `src/urban_data/optimization.py` |
| 产品分区和 schema 配置 | `config/analytics.yml` |
| 最终 benchmark 数据 | `artifacts/week2_optimization_benchmark.json` |
| `EXPLAIN FORMATTED` 物理计划 | `artifacts/week2_optimization_plans.json` |
| 查询和产品设计说明 | `docs/week2_analytics_design.md` |

## 最终基准结果

协议为每个版本 1 次预热加 3 次测量，记录中位数。每组优化均用结果 SHA-256 验证与基线输出相同。

| 实验 | 基线中位数 | 优化后中位数 | 加速比 | 报告结论 |
| --- | ---: | ---: | ---: | --- |
| 窄投影缓存 | 864.098 ms | 684.214 ms | 1.2629x | 缓存三列复用投影有效；不要缓存整张 Gold 表。填充缓存需 2,285.727 ms，因此仅适合重复读取。 |
| 分区裁剪 | 338.762 ms | 347.847 ms | 0.9739x | 当前三个月、本地存储和低成本聚合下无稳定收益；不要声称它带来加速。物理计划仍可用于说明直接分区谓词的语义。 |
| Broadcast Join | 1,240.648 ms | 487.026 ms | 2.5474x | 最大且稳定的收益。Taxi Zone 是小维度，应广播到大型出租车事实表的执行端。 |
| AQE | 725.674 ms | 637.088 ms | 1.1390x | 小幅稳定改善；建议保持启用。 |

四个数据产品的总 Delta 数据大小为 178,065 bytes：

| 产品 | 行数 | 数据文件数 | 数据大小 |
| --- | ---: | ---: | ---: |
| `daily_mobility_summary` | 726 | 8 | 56,053 bytes |
| `taxi_zone_monthly_statistics` | 790 | 8 | 67,876 bytes |
| `weather_impact_summary` | 38 | 8 | 34,278 bytes |
| `air_quality_impact_summary` | 95 | 4 | 19,858 bytes |

## 报告写作清单

### 设计报告

1. 说明六个分析问题及对应的 Gold 字段、分组粒度和输出含义。
2. 描述四个数据产品的用户、用途、物化理由、Delta 存储、月份分区和元数据字段。
3. 说明工程选择：Gold 的 `source_file_month` 分区、窄投影缓存、小维度广播、AQE、结果签名验证和 UTC 时间语义。
4. 说明取舍：缓存填充成本和内存约束、当前规模下分区裁剪收益有限、环境缺失值不被填补或丢弃。
5. 对十城市扩展提出建议：按城市与稳定时间分区、增量摄取、数据文件压实、按真实过滤模式重新评估分区或聚簇，并扩展计算资源。

### Benchmark 报告

1. 写明环境、1 次预热加 3 次测量和中位数作为报告值。
2. 使用上表填写前后时间、加速比、结果一致性和存储开销。
3. 从 `week2_optimization_plans.json` 提取证据：缓存的 `Scan In-memory table`、广播 Join、分区过滤和 AQE 计划变化。
4. 明确结论：Broadcast Join 最有效，窄投影缓存仅在反复使用时合理，AQE 小幅改善，当前规模下分区裁剪没有稳定加速。
5. 不要将第一次“缓存完整 Gold 表导致磁盘溢写”的临时结果当作最终 benchmark；最终 JSON 已被窄投影缓存的复现实验覆盖。

## 复现命令

在已准备 Python、Java、PySpark 和 Delta 的环境中，从 `Week1/` 运行：

```powershell
.\scripts\run_win.ps1 ingest --dataset all
.\scripts\run_win.ps1 integrate
.\scripts\run_win.ps1 products --product all
.\scripts\run_win.ps1 benchmark-analytics
```

也可以运行单个查询：

```powershell
.\scripts\run_win.ps1 query --name monthly_taxi_demand_by_zone
```

运行数据集不应提交到 Git；`datasets/`、`lakehouse/`、虚拟环境和 Spark 临时文件均已被忽略。
