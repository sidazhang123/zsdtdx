# zsdtdx 交接文档（工程维护）

更新时间：2026-09-22

## 1. 接管基线

1. 对外入口：`src/zsdtdx/simple_api.py` 的 `get_*` 系列函数。
2. 推荐调用方式：`with get_client() as client:` 中复用同一上下文。
3. 并行核心：`src/zsdtdx/parallel_fetcher.py`；父进程准入在 `src/zsdtdx/adaptive_scheduler.py`。
4. 数据封装核心：`src/zsdtdx/unified_client.py`。
5. 配置入口：`src/zsdtdx/config.yaml`。

## 2. 分层结构

1. API 层：`simple_api.py`
2. 并行层：`parallel_fetcher.py`、`adaptive_scheduler.py`
3. 封装层：`unified_client.py`
4. 协议层：`parser/*.py`
5. 网络层：`base_socket_client.py`

## 3. 术语定义

1. `task`：单个 K 线请求，股票任务字段 `{code,freq,start_time,end_time}`，指数任务字段 `{index_name,freq,start_time,end_time}`。
2. `chunk`：同 `code+freq` 的 task 集合，按 `start_time` 升序执行。
3. `bundle`：提交给单个进程池 future 的 chunk 批次。
4. `inproc 协程`：worker/主进程内的 chunk 协程并发。上限写死为 3，不读 YAML。

## 4. 关键调用链

1. 用户调用 `simple_api.get_stock_kline(task, mode=...)`。
2. `parallel_fetcher` 将 task 分组为 chunk，并构建 bundle。
3. async 模式下，父进程通过 `ProcessPoolExecutor.submit(_fetch_chunk_bundle, ...)` 派发 bundle。
4. worker 进程内通过 `asyncio` 协程 + `to_thread` 并发执行 chunk（`Semaphore` 上限写死为 3）。K 线进程按行情站拆池：进程创建时绑定自家站，该站的任务只进入这些进程；连接还在自家站上时不因下一批任务断开。
5. chunk 内通过 `unified_client.get_stock_kline_rows_for_chunk_tasks(...)` 拉取并复用缓存。
6. 主进程按 bundle 完成顺序归集结果并写入队列，最终追加 `event=done`。
7. 指数入口 `simple_api.get_index_kline(task, mode=...)` 现已接入 `ParallelFetcher`：sync 走主进程 inproc chunk，async 走进程池 bundle + worker chunk；chunk 内按 `(index_name, freq)` 分组后顺序调用 `unified_client.get_index_kline_rows_for_chunk_tasks(...)`。

## 5. sync/async 当前语义

1. sync：主进程内执行，不要求多进程，也不要求前置 `with get_client()`（task/chunk 路径自带 worker/主进程连接）；可启用主进程 chunk 协程并发（配置控制）。
2. sync：返回值为合并后的 `list[payload]`，队列可选。
3. async：后台任务立即返回 `StockKlineJob`。
4. async：队列产出粒度为“bundle 回收后逐 payload 写入”，不是 worker 内 chunk 完成即跨进程直推。
5. async：建议消费端按 `queue.get(timeout=20)` 持续读取，直到 `event=done`。
6. 指数 sync/async：与 stock_kline 共用日志、路由感知预热、按侧懒连接、自适应准入与进程池生命周期；事件结构保持一致。

## 6. 关键配置项（`config.yaml.parallel`）

1. `task_chunk_max_inflight_multiplier`（提交窗口；标准侧配额按倍率放大，扩展侧不放大）
2. `adaptive_processes_per_host_std`
3. `adaptive_processes_per_host_ex`
4. `adaptive_decrease_factor`
5. `adaptive_cooldown_seconds`
6. `chunk_timeout_seconds`
7. `chunk_retry_max_attempts`
8. `process_count_core_multiplier`
9. `index_kline.prefer_ex_markets`
10. `index_kline.aliases`

## 7. 文档联动约束

1. 修改 `simple_api` 参数或返回结构时，同步更新 `README.md` 示例。
2. 修改并行调度语义（chunk/bundle/future）时，同步更新本文档和 `README.md`。
3. 修改 `config.yaml` 键名或默认值时，同步更新两份文档说明。

## 8. 常见风险与排障

1. 连接风险：hosts 不可达会导致全链路失败，先检查 `hosts.*` 连通性与配置内容。
2. 超时风险：`get_future_kline` 的 DataFrame 批处理总超时固定 300 秒，单 future 超时固定 600 秒。预热超时固定 60 秒、最多 3 轮。这些都不是 YAML 项；用户文件里的同名键会被深合并丢弃。
3. 吞吐异常：优先检查可达 host 数与 `adaptive_*` 参数。
4. 期货 DataFrame 批处理路径不用于 task/chunk/bundle K 线。
5. 进程残留：测试脚本结束后应确认无残留 python 进程。

## 9. 建议回归清单

1. 离线单元/验收：`py -m pytest tests/ -q`（不收集 `tests/manual/`，不依赖真实行情网络）。
2. 手工脚本清单与命令见 `tests/manual/README.md`（冒烟、单点探测、chunk/弱网离线单测）。
3. 队列保障：async 消费端用 `timeout=20s` 验证连续返回与 done 收敛。

## 10. 发布流程（PyPI）

1. 更新版本号：`pyproject.toml` 与 `src/zsdtdx/__init__.py` 保持一致。
2. 清理产物目录：删除 `dist/` 与旧 `*.egg-info`。
3. 构建：`py -m build`。
4. 校验：`py -m twine check dist/*`。
5. 上传：`py -m twine upload dist/*`（使用用户目录下 `.pypirc` 的 token）。
