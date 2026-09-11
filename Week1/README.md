# ID2221 Urban Data Integration Platform

本目录是 Week 1 Assignment 的项目根目录。目标是用 Spark 和 Delta Lake 构建可复用的数据摄取、验证、标准化、集成和基准测试流水线。

## 当前状态

- 6 个权威源文件的 SHA-256 已全部通过。
- `hourly_88101_2024.csv` 已确认与 `air_quality.zip` 内唯一条目逐字节一致。
- 四类数据的 schema、行数、时间范围、空值、候选键和主要异常已画像。
- Assignment 中文翻译、数据目录、架构和详细实现路径已整理。
- 项目数据路径已统一为 `datasets/`，迁移后的画像命令已重新验证。
- WSL 运行环境已就绪：OpenJDK 17、PySpark 3.5.7、delta-spark 3.3.2；Delta 写读冒烟测试通过。
- 2024-01 出租车数据已实现 Bronze → Silver 端到端摄取（含 quarantine 与幂等跳过）。
- 区域、天气、空气质量摄取与 Gold 集成、基准测试尚未实现。
- 当前目录尚未初始化为 Git 仓库。

## 数据概览

| 逻辑数据集 | 输入文件 | 已确认规模 |
| --- | --- | --- |
| 出租车行程 | `datasets/yellow_tripdata_2024-01/02/03.parquet` | 9,554,778 行 |
| 天气 | `datasets/weather.csv` | 8,784 行，2024 全年逐小时 |
| 空气质量 | `datasets/air_quality.zip` / 等价解压 CSV | 全国 8,139,551 行；纽约市子集 51,885 行 |
| 出租车区域 | `datasets/taxi_zone_lookup.csv` | 265 行，`LocationID` 唯一 |

`Week1/datasets/` 中的 CSV、Parquet 和 ZIP 是项目的只读输入。后续 Delta 表和运行产物写入 `lakehouse/`，不得覆盖源文件。

同级目录 `C:/Users/goahe/Desktop/ID2221/datasets` 按用户要求保留为镜像。项目代码不读取该镜像，也不自动同步两个目录；运行与提交均以 `Week1/datasets/` 为准。

## 现有命令

数据完整性检查与画像：

```powershell
python scripts/profile_data.py
```

Spark + Delta（在 WSL 中，需先执行一次 `bash scripts/bootstrap_wsl.sh`）：

```powershell
wsl bash -lc "cd /mnt/c/Users/goahe/Desktop/ID2221/Week1 && bash scripts/run_wsl.sh ingest --dataset taxi_2024_01"
wsl bash -lc "cd /mnt/c/Users/goahe/Desktop/ID2221/Week1 && .venv-wsl/bin/python scripts/smoke_delta.py"
```

成功摄取后会写入 `lakehouse/bronze/`、`lakehouse/silver/`、`lakehouse/quarantine/` 与 `lakehouse/metadata/ingestion_runs/`。

`.gitignore` 已配置为忽略大型数据文件、Delta 运行目录和 Python/Spark 缓存，同时允许跟踪 `datasets/SHA256SUMS.txt`。

## 文档入口

- [Assignment、数据目录与实现方案](docs/assignment_and_implementation.md)
- [机器可读数据画像](artifacts/data_profile.json)
- [任务计划](task_plan.md)
- [研究发现](findings.md)
- [进度记录](progress.md)

## 实现顺序

1. ~~确认课程要求的 Java、Spark、Delta Lake 和 Python 版本，建立隔离环境并通过 Spark/Delta 写读冒烟测试~~（WSL：JDK 17、PySpark 3.5.7、delta-spark 3.3.2）。
2. ~~建立最小 PySpark 项目，让一个出租车月份完成 Bronze → Silver Delta 的端到端流程~~（已完成 `taxi_2024_01`）。
3. 抽取通用摄取路径，再接入区域、天气和空气质量数据。
4. 构建一行一行程的 `integrated_taxi_trips`，逐步验证连接前后行数不膨胀。
5. 比较未分区与按 `pickup_month` 分区两种策略，运行指定三条查询并记录实际指标。
6. 根据实测结果完成设计报告、架构图、基准报告和最终 README。

计划中的 CLI 与验收标准见实现文档。未实现的命令不会在这里写成可运行命令。
