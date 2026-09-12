# 本地运行环境配置记录

> 记录本机实际用于完成 Week1 流水线（ingest / integrate / benchmark）的环境。  
> 最后验证日期：**2026-09-12**  
> 项目根目录：`D:/DD2221/ID2221-main/Week1`

此前环境信息分散在 `README.md`、`findings.md`、`scripts/run_win.ps1`、`src/urban_data/spark_session.py`；本文档为**集中版**，便于换机或重装时复现。

---

## 1. 机器与路径

| 项目 | 值 |
| --- | --- |
| 操作系统 | Windows 11（10.0.26200） |
| CPU | AMD Ryzen 9 8945HX |
| 内存 | 约 34 GiB |
| 项目路径 | `D:\DD2221\ID2221-main\Week1` |
| Conda 安装位置 | `D:\anaconda3` |
| Spark 主环境 | Conda 环境 `id2221-week1` |
| Python（Spark） | `D:\anaconda3\envs\id2221-week1\python.exe`（3.12） |
| Java | Conda 内 OpenJDK 17 → `JAVA_HOME=D:\anaconda3\envs\id2221-week1\Library` |
| 宿主机 Python（可选） | Anaconda 3.13，仅用于 `scripts/profile_data.py` |

**为何用 Windows 原生而非 WSL：** 本机当时无法安装 WSL（需管理员权限）。因此在 Windows 上补齐 Hadoop 辅助文件与 `run_win.ps1`，并完成全部验收。

---

## 2. 版本锁定

| 组件 | 版本 | 来源 |
| --- | --- | --- |
| Python | 3.12 | Conda |
| OpenJDK | 17 | Conda `openjdk=17` |
| PySpark | 3.5.7 | `requirements.txt` |
| delta-spark | 3.3.2 | `requirements.txt` |
| PyYAML | 6.0.2 | `requirements.txt` |

版本依据：Delta Lake 3.3.x 对应 Spark 3.5.x；官方文档支持 JDK 17 与 Python 3.8+。

`requirements.txt`：

```text
pyspark==3.5.7
delta-spark==3.3.2
PyYAML==6.0.2
```

---

## 3. 从零安装（Windows）

### 3.1 创建 Conda 环境

```powershell
conda create -n id2221-week1 python=3.12 openjdk=17 -y
conda activate id2221-week1
pip install -r "D:\DD2221\ID2221-main\Week1\requirements.txt"
```

若 Conda 不在 `D:\anaconda3`，需同步修改 `scripts/run_win.ps1` 第 12 行的 `$python` 路径。

### 3.2 Windows Hadoop 辅助（必需）

Spark/Delta 在 Windows 写本地文件时需要 `winutils.exe`。

目录结构：

```text
Week1/tools/hadoop/
└── bin/
    ├── winutils.exe
    └── hadoop.dll
```

运行时由 `run_win.ps1` 和 `spark_session.py` 自动设置：

- `HADOOP_HOME=Week1/tools/hadoop`
- `PATH` 追加 `%HADOOP_HOME%\bin`

### 3.3 数据文件

6 个源文件放在 `Week1/datasets/`（约 2.4 GB）。校验清单：`datasets/SHA256SUMS.txt`。

可选镜像 `D:/DD2221/data/` **不是**运行依赖，代码只读 `Week1/datasets/`。

---

## 4. 环境变量（由 `run_win.ps1` 自动注入）

| 变量 | 值 |
| --- | --- |
| `JAVA_HOME` | `D:\anaconda3\envs\id2221-week1\Library` |
| `HADOOP_HOME` | `Week1\tools\hadoop` |
| `PYTHONPATH` | `Week1\src` |
| `PYSPARK_PYTHON` | Conda env 的 `python.exe` |
| `PYSPARK_DRIVER_PYTHON` | 同上 |
| `SPARK_LOCAL_IP` | `127.0.0.1` |
| `SPARK_LOCAL_DIRS` | `Week1\tools\spark-tmp` |
| `TMP` / `TEMP` | `Week1\tools\spark-tmp` |

直接调用 Python 时，至少手动设置：

```powershell
$project = "D:\DD2221\ID2221-main\Week1"
$env:JAVA_HOME = "D:\anaconda3\envs\id2221-week1\Library"
$env:PYTHONPATH = "$project\src"
```

推荐一律通过 `scripts/run_win.ps1` 启动 Spark 命令，避免漏设变量。

---

## 5. Spark 运行时配置

定义于 `src/urban_data/spark_session.py`：

| 配置项 | 值 |
| --- | --- |
| Master | `local[8]` |
| Driver memory | `4g` |
| Shuffle partitions | `8` |
| Session timezone | `UTC` |
| JVM timezone | `TZ=UTC` + `-Duser.timezone=UTC` |
| Delta 扩展 | `DeltaSparkSessionExtension` + `DeltaCatalog` |
| Spark UI | 关闭（`spark.ui.enabled=false`） |

Windows 额外 JVM 参数：

- `-Dhadoop.home.dir=<项目>/tools/hadoop`（正斜杠路径）
- `-Djava.library.path=<项目>/tools/hadoop/bin`

---

## 6. Windows 专项代码修复

配置环境时同步改过以下代码（已在仓库中）：

| 问题 | 处理 |
| --- | --- |
| `time.tzset()` 在 Windows 不存在 | `spark_session.py` 用 `hasattr(time, "tzset")` 保护 |
| Java 路径反斜杠被 Spark 吃掉 | Hadoop 相关路径改用 `.as_posix()` 正斜杠 |
| Delta 写权限 | 依赖 `winutils.exe`，不用 WSL 权限模型 |
| JVM 本地时间函数与 UTC 不一致 | 进程级 `TZ=UTC` + Driver `-Duser.timezone=UTC` |

---

## 7. 推荐运行方式

```powershell
$project = "D:\DD2221\ID2221-main\Week1"

# 单元测试（19 tests）
$env:JAVA_HOME = "D:\anaconda3\envs\id2221-week1\Library"
$env:PYTHONPATH = "$project\src"
& "D:\anaconda3\envs\id2221-week1\python.exe" -m unittest discover -s "$project\tests"

# 主流水线（环境变量由脚本注入）
& "$project\scripts\run_win.ps1" ingest --dataset all
& "D:\anaconda3\envs\id2221-week1\python.exe" "$project\scripts\verify_ingestion.py"
& "$project\scripts\run_win.ps1" integrate
& "D:\anaconda3\envs\id2221-week1\python.exe" "$project\scripts\verify_integration.py"
& "$project\scripts\run_win.ps1" benchmark
```

调试时可逐个数据集 ingest，例如：

```powershell
& "$project\scripts\run_win.ps1" ingest --dataset taxi_2024_01
```

---

## 8. 冒烟验证（装完环境后）

```powershell
$env:JAVA_HOME = "D:\anaconda3\envs\id2221-week1\Library"
$env:PYTHONPATH = "D:\DD2221\ID2221-main\Week1\src"
& "D:\anaconda3\envs\id2221-week1\python.exe" -c "from pyspark.sql import SparkSession; s=SparkSession.builder.master('local[1]').getOrCreate(); print('spark ok', s.range(1).count()); s.stop()"
```

Delta 读写由项目内 `build_spark()` 负责；完整 Delta 冒烟见历史 `lakehouse/_smoke_delta*` 测试记录。

---

## 9. 可选：数据画像脚本

`scripts/profile_data.py` 可在**宿主机 Anaconda Python 3.13** 上运行（需 pandas、pyArrow），不进入 Spark 流水线环境。用于生成 `artifacts/data_profile.json`。

---

## 10. Legacy：WSL 路径（未作为本机主环境）

仓库仍保留：

- `scripts/bootstrap_wsl.sh`
- `scripts/run_wsl.sh`

若将来启用 WSL Ubuntu，可参照 `README.md` 的 Legacy 节；本机验收结果均来自 **Windows 原生**流程。

---

## 11. 已知现象与清理

| 现象 | 说明 |
| --- | --- |
| Spark 退出时 `tools/spark-tmp/` 删除警告 | Windows 文件锁导致，一般无害 |
| `run_win.ps1` 偶发非零退出 | Ivy 下载 stderr 干扰；可直接 `python -m urban_data ...` |
| `tools/spark-tmp/` 体积增长 | Spark 临时文件；**未运行 Spark 时**可手动清空 |
| `lakehouse/` 已删 | 不影响 `artifacts/*.json` 验收证据；需要表时按 §7 重跑 |

---

## 12. 验收证据（2026-09-12）

本环境曾产出并通过：

- `artifacts/ingestion_verification.json` → `passed`
- `artifacts/integration_verification.json` → `passed`
- `artifacts/benchmark_report.json` → `success`

---

## 13. 相关文件索引

| 文件 | 作用 |
| --- | --- |
| `scripts/run_win.ps1` | Windows 入口，注入全部环境变量 |
| `src/urban_data/spark_session.py` | SparkSession 与 Delta 配置 |
| `requirements.txt` | Python 依赖锁定 |
| `README.md` | 对外运行说明（精简版） |
| `findings.md` | 环境发现与版本依据（工作笔记） |
| `AGENTS.md` | Agent 用的运行摘要 |
