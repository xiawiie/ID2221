# 项目进度

## 2026-09-11

## 2026-09-12

### 阶段 13：assignment 文档与 benchmark 同步
- **状态：** complete
- 同步 `docs/assignment_and_implementation.md`：源 Parquet → Delta 摄取定义、2026-09-12 benchmark 数字、schema/元数据/quarantine 代码修复、19 tests、剩余限制（架构图 PNG 待导出）。

### 阶段 12：设计报告扩充与文档一致性
- **状态：** complete
- 扩充 `docs/design_report.md`：四类数据 catalog 六问、Task 3 六问、完整转换清单、存储/集成/benchmark 工程决策。
- 同步 `docs/assignment_and_implementation.md`：Windows 路径与命令、benchmark 结论（查询持平、默认未分区）。
- 同步 `findings.md` / `progress.md`：Windows 环境、2026-09-12 benchmark 数字，移除过时 WSL 主路径表述。

### 阶段 11：独立复审与任务符合性测试
- **状态：** complete
- 用户要求对项目做 review 和测试，并严格对照 `docs/assignment_and_implementation.md` 分析完成度。
- 4 张附件 PNG 存在但当前通道无法渲染，且本机/WSL 无 OCR；仅记录该限制，不据截图扩大或改变任务范围。
- 提取任务 1–6、交付物和建议验收标准，复审源代码、报告与 artifact 一致性；判定项目功能上大幅完成，但不是严格 100% 符合。
- 发现 2 个 P1：supplied schema 读取使 schema 校验变成同义反复；Delta overwrite 后统计全部物理文件使 benchmark 存储/file count 不可重复。
- 发现 5 个 P2/P3 主要偏差：空气质量候选键未强制、失败/跳过运行无 metadata、benchmark 写入时间定义不符、天气时区政策内部冲突、Gold 删除观测时间且无 lag 字段；另有 stale quarantine、README 冷启动和测试覆盖不足。
- 复跑基准后 WSL 环境 JSON 查询中位数为未分区 1,727 / 1,838 / 1,679 ms、分区 3,617 / 3,465 / 3,396 ms；后在 Windows 本机重跑，查询几乎持平（见 `artifacts/benchmark_report.json` 2026-09-12）。`docs/benchmark_report.md` 与 JSON 已对齐。
- 反向测试确认错误 CSV/Parquet 物理 schema 可通过校验、空气质量合成重复候选键不进 quarantine、Gold 四个审计字段不存在。

### 阶段 7：运行环境与项目骨架
- **状态：** complete
- 锁定 WSL 运行栈：OpenJDK 17、Python 3.12（`.venv-wsl`）、PySpark 3.5.7、delta-spark 3.3.2。
- `scripts/smoke_delta.py` 写读 Delta 冒烟通过。
- 新增 `config/datasets.yml`、`src/urban_data/` 包与 `tests/test_config.py`。
- `taxi_2024_01` 完成 Bronze/Silver/Quarantine 摄取：读入 2,964,624 行，接受 2,963,754，隔离 870，告警 86,241；重复运行按源哈希跳过。

### 阶段 8：通用摄取与四类数据
- **状态：** complete
- 继续上次中断前留下的通用摄取实现；工作区已有 `src/urban_data/ingest.py` 的 quarantine 按源替换修正，本轮保留并核验。
- 现有两个配置测试以直接函数调用执行，2/2 通过；`pytest` 模块本身缺失，已记录。
- 3 月出租车摄取确认成功：读入 3,582,628，接受 3,581,500，隔离 1,128。
- 空气质量重复命令按同哈希跳过，元数据和表存在性将在统一验收脚本中确认。
- 修复共享出租车 Silver 的分区覆盖缺陷：新增稳定 `source_file_month` 覆盖分区，保留业务 `pickup_month` 普通列；三个月强制重建后总行数恢复正确。
- `scripts/verify_ingestion.py` 验收通过，机器可读结果写入 `artifacts/ingestion_verification.json`：Bronze 六项、Silver 四表、taxi quarantine 2,801 行、区域/天气/空气质量唯一键全部匹配。

### 阶段 9：集成流水线
- **状态：** complete
- 发现并修复 Spark 时间语义缺陷：同时固定进程 `TZ`、SQL session timezone 和 Driver JVM timezone 为 UTC 后，出租车/天气/空气质量边界时间与源画像一致。
- 已按 UTC 语义强制重建三个月出租车、天气和空气质量 Silver；行数与隔离数保持不变，出租车告警数修正为 37,637 / 40,816 / 58,740。
- 第一次 Gold 写入在 `local[*]` 与 Driver 默认 1 GB 堆下 OOM；连接行数检查尚未报错。已改为 `local[8]` 和 4 GB Driver 堆后重跑。
- `integrate` 成功写出 `lakehouse/gold/integrated_taxi_trips/`：Silver 与 Gold 均为 9,551,977 行；四次连接后行数均不变。
- `scripts/verify_integration.py` 从磁盘重读 Gold 并通过：三个 source month 分区计数对账，区域匹配 9,551,977/9,551,977，天气与空气质量各 19 条 missing，1 个重复行程指纹仅报告。

### 阶段 10：基准与最终交付
- **状态：** complete
- 基准命令成功比较未分区与按 `pickup_month` 分区两版 9,551,977 行 Delta 表，使用相同输入、Snappy 压缩、1 次预热和 3 次测量。
- 未分区表：写入 67.166 s、827.74 MiB、20 个数据文件；分区表：写入 54.622 s、833.02 MiB、46 个数据文件。
- 三条查询中位数：未分区 1,670.160 / 1,468.995 / 1,639.759 ms；分区 3,255.404 / 3,608.642 / 3,391.770 ms；两版结果 SHA-256 一致。
- 新增 `src/urban_data/benchmark.py`、`docs/benchmark_report.md`、`docs/design_report.md` 和 `artifacts/benchmark_report.json`、`artifacts/benchmark_physical_plans.json`。
- README 与 `docs/assignment_and_implementation.md` 已同步最终状态、实际 WSL 命令和剩余限制。

### 阶段 1：项目与数据盘点
- **状态：** complete
- 枚举项目根目录与全部文件。
- 确认没有既有 Markdown、规则文件、代码或 Git 仓库。
- 确认空气质量同时存在 ZIP 和解压 CSV。
- 未移动、覆盖或删除任何现有文件。

### 阶段 2：数据完整性与内容核验
- **状态：** complete
- 验证现有 SHA-256 清单中的 6 个源文件，全部一致。
- 比较 ZIP 条目与解压 CSV 的长度和 SHA-256，确认逐字节一致。
- 新增并运行 `scripts/profile_data.py`，生成 `artifacts/data_profile.json`。
- 完成四类数据的行数、schema、时间范围、空值、候选键和异常值画像。

### 阶段 3：Assignment 与实现路径
- **状态：** complete
- 创建 `docs/assignment_and_implementation.md`。
- 整理完整中文 Assignment、数据目录、存储架构、Mermaid 架构图、摄取与集成策略、基准方法和分阶段实现路线。

### 阶段 4：项目入口与规则
- **状态：** complete
- 创建 `README.md`，只列当前可运行命令和真实状态。
- 创建 46 行 `AGENTS.md`，规定源数据保护、目录、技术路线和下一步。

### 阶段 5：验证与交接
- **状态：** complete
- Python 语法检查和画像关键断言通过。
- 全部 Markdown 本地链接通过。
- 实现文档关键统计与架构图标记检查通过。
- 发现一个由语法检查生成的 `__pycache__`，保留为待确认清理候选。

### 阶段 6：目录迁移收尾与方案深化
- **状态：** complete
- 用户确认同时保留 `ID2221/datasets` 与 `ID2221/Week1/datasets`。
- 修复 `scripts/profile_data.py`，所有输入改从 `Week1/datasets` 读取。
- 新增 `.gitignore`，避免未来误跟踪大型数据、Delta 产物和 Python/Spark 缓存。
- 在迁移后重新运行完整画像成功，关键数据规模保持一致。
- 核对本机 CPU、内存、磁盘、Java、Spark 和 Python 包状态，为可行性分析提供证据。
- 同步 README、AGENTS、findings、task_plan 和实现文档中的项目路径与当前状态。
- 在实现文档中加入可行性矩阵、双目录职责、质量分级、七阶段实践路线和验收门。
- 验证 Markdown 本地链接、实现文档关键章节、AGENTS.md 体量和迁移后画像断言。
- 静态核对 `.gitignore` 的 11 个必需模式；Git 行为验证留到仓库初始化后。

## 验证结果
| 检查 | 预期 | 实际 | 状态 |
| --- | --- | --- | --- |
| 项目文件枚举 | 获取全部现有文件 | 8 个数据相关文件 | 通过 |
| 规划文件恢复 | 读取既有计划或确认不存在 | 不存在，已初始化 | 通过 |
| Git 状态 | 明确是否已有仓库 | 不是 Git 仓库 | 通过 |
| 源文件 SHA-256 | 6 个条目全部匹配 | 6/6 匹配 | 通过 |
| 空气质量解压文件 | 与 ZIP 唯一条目逐字节一致 | 大小、SHA-256 均一致 | 通过 |
| 画像脚本 | 语法有效且关键断言成立 | 通过 | 通过 |
| Markdown 链接 | 所有本地目标存在 | 通过 | 通过 |
| 项目规则体量 | AGENTS.md 不超过 60 行 | 46 行 | 通过 |
| 实现文档关键事实 | 核心行数、异常数和架构图齐全 | 通过 | 通过 |
| 迁移后画像命令 | 从 `Week1/datasets` 读取并更新根目录 artifacts | 成功完成 | 通过 |
| 本机资源 | 足以开展课程级本地 Spark 验证 | 16 核/32 线程、31.2 GiB 内存、563.2 GiB C 盘可用 | 通过 |
| Spark 运行环境 | Java、Spark、PySpark、Delta 可用 | WSL：JDK 17、PySpark 3.5.7、delta-spark 3.3.2；冒烟与 2024-01 摄取通过 | 通过 |
| Markdown 与方案文档 | 路径有效、可行性章节完整 | 通过 | 通过 |
| 项目规则体量 | AGENTS.md 不超过 60 行 | 49 行 | 通过 |
| `.gitignore` 内容 | 11 个必需模式完整 | 11/11 | 通过 |
| `.gitignore` Git 行为 | 在 Git 仓库中验证忽略与例外 | 源数据、虚拟环境、lakehouse 被忽略；证据 JSON 未被忽略 | 通过 |
| 双数据目录 | 两个目录均保留且职责明确 | 各 8 个文件、2,593,502,350 字节 | 通过 |
| 最终配置测试 | 标准 `unittest` 命令通过 | 2 tests, OK | 通过 |
| 最终语法编译 | 源代码、测试和验证脚本有效 | compileall 无错误 | 通过 |
| Markdown 本地链接 | 8 个项目 Markdown 的本地目标存在 | 通过 | 通过 |
| 证据 JSON | 5 份机器可读结果可解析 | profile、ingestion、integration、benchmark、plans 均有效 | 通过 |
| 基准报告对账 | 报告数值来自 JSON | 6/6 关键数值一致 | 通过 |
| NYC 空气质量筛选 | 五个纽约市县名全部保留，非 NYC 县排除 | 5/5 expected counties retained | 通过 |
| 阶段 11 单元测试 | 标准 unittest 命令通过 | 2 tests, OK | 通过 |
| 阶段 11 语法编译 | 源代码、测试和验证脚本可编译 | compileall 无错误 | 通过 |
| 阶段 11 证据状态 | ingestion/integration artifact 为 passed | 两份 JSON 均 passed；Gold/Silver 均 9,551,977 行 | 通过 |
| 阶段 11 schema 反向检查 | 物理列名不匹配应拒绝 | CSV/Parquet 反例均通过，校验无效 | 失败 |
| 阶段 11 空气键反向检查 | 重复候选键应 quarantine | 1 小时行 / 0 quarantine / 2 站点观测 | 失败 |
| 阶段 11 benchmark 对账 | 报告与最新 JSON 一致 | Windows 2026-09-12 JSON 与 `docs/benchmark_report.md` 一致 | 通过 |
| 设计报告篇幅 | catalog/摄取/转换清单并入正式报告 | `docs/design_report.md` 扩充至 8 节 | 通过 |

## 错误日志
| 时间 | 错误 | 处理 |
| --- | --- | --- |
| 2026-09-11 | `findings.md` 一行混入校对备注 | 读取当前文件并用精确补丁更正，数据未受影响 |
| 2026-09-11 | 大型文档补丁脚本语法错误 | 工具在写入前拒绝执行；改用小补丁完成 |
| 2026-09-11 | 实现文档首次打开请求参数无效 | 使用正确参数重试，已排队打开 |
| 2026-09-11 | `.gitignore` 首次验证命令含空管道 | 改为结果数组；未修改项目文件 |
| 2026-09-11 | `git check-ignore --no-index` 在非仓库中返回 128 | 改做规则内容检查，行为验证留到初始化 Git 后 |
| 2026-09-11 | 使用了 PowerShell 不支持的 Bash here-string | 停止该写法并改用原生 PowerShell |
| 2026-09-11 | pip 的 PySpark 下载临时文件持续为 0 字节 | 中止原会话，改用 PyPI 固定 URL 与哈希进行本地缓存安装 |
| 2026-09-11 | 三次工具调用输入或补丁文本无效 | 调用均未改代码/数据；读取精确上下文并修正 findings |

| 2026-09-11 | `.venv-wsl` 中 `python -m pytest` 失败：No module named pytest | 改为直接导入并调用两个测试函数，均通过；未新增依赖 |
| 2026-09-11 | 两次嵌套 `python -c` 引号在 PowerShell/Bash 边界解析失败 | 改用 `wsl --cd ... env PYTHONPATH=...` 形式执行成功 |
| 2026-09-12 | 仅设置 `spark.sql.session.timeZone=UTC` 后时间仍偏移 8 小时 | 增加进程 `TZ=UTC` 与 Driver `-Duser.timezone=UTC`，时间函数冒烟和三类边界值验证通过 |
| 2026-09-12 | Gold 写入阶段 Java heap space OOM | 将本地 Spark 从 32 线程降为 8 线程，Driver 堆提升到 4 GB |
| 2026-09-12 | 集成验收脚本两处把 Spark Row 的 `count` 读成内建方法 | 改用 `row["count"]` 后验收通过；数据未受影响 |
| 2026-09-12 | 首轮 Gold 基准后实现文档仍保留“未实现”和旧分区建议 | 按最终实测结果更新状态、命令和剩余限制 |
| 2026-09-12 | Markdown 链接检查递归进入 `.venv-wsl/lib64` 符号链接失败 | 改用 `rg --files` 枚举 Git 工作区 Markdown 后通过 |
| 2026-09-12 | 五县过滤首次冒烟用例缺少 `parameter_code` 列导致 AnalysisException | 补齐生产 schema 中的列后重跑通过；被检代码无需修改 |
| 2026-09-12 | 复核版本时误用不存在的 `delta.__version__` | 改用 `importlib.metadata.version("delta-spark")`，得到 3.3.2 |
| 2026-09-12 | 附件 PNG 当前通道无法渲染且无 OCR | 不把截图当证据，以 `docs/assignment_and_implementation.md` 为复审依据 |

## 五问重启检查
| 问题 | 答案 |
| --- | --- |
| 我在哪里？ | 阶段 11 独立复审与任务符合性测试已完成 |
| 我要去哪里？ | 无剩余 review/testing 步骤；如继续，仅处理用户要求的修复、文档同步或提交 |
| 目标是什么？ | 在当前目录交付可运行的 ID2221 Spark + Delta 摄取、集成和基准项目 |
| 我学到了什么？ | 见 findings.md |
| 我做了什么？ | 见本文件上方记录 |
