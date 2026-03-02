# zsdtdx 交接文档（工程维护）

更新时间：2026-03-02

## 1. 接管基线

1. 对外入口：`src/zsdtdx/simple_api.py` 的 `get_*` 系列函数。
2. 推荐调用方式：`with get_client() as client:` 中复用同一上下文。
3. 并行核心：`src/zsdtdx/parallel_fetcher.py`。
4. 数据封装核心：`src/zsdtdx/unified_client.py`。
5. 配置入口：`src/zsdtdx/config.yaml`。

## 2. 分层结构

1. API 层：`simple_api.py`
2. 并行层：`parallel_fetcher.py`
3. 封装层：`unified_client.py`
4. 协议层：`parser/*.py`
5. 网络层：`base_socket_client.py`

## 3. 术语定义

1. `task`：单个 K 线请求，字段 `{code,freq,start_time,end_time}`。
2. `chunk`：同 `code+freq` 的 task 集合，按 `start_time` 升序执行。
3. `bundle`：提交给单个进程池 future 的 chunk 批次。
4. `inproc future`：worker 进程内的 chunk 线程 future（上限由 `task_chunk_inproc_future_workers` 控制）。

## 4. 关键调用链

1. 用户调用 `simple_api.get_stock_kline(task, mode=...)`。
2. `parallel_fetcher` 将 task 分组为 chunk，并构建 bundle。
3. async 模式下，父进程通过 `ProcessPoolExecutor.submit(_fetch_chunk_bundle, ...)` 派发 bundle。
4. worker 进程内通过 `ThreadPoolExecutor` 并发执行 chunk。
5. chunk 内通过 `unified_client.get_stock_kline_rows_for_chunk_tasks(...)` 拉取并复用缓存。
6. 主进程按 bundle 完成顺序归集结果并写入队列，最终追加 `event=done`。

## 5. sync/async 当前语义

1. sync：主进程内执行，不要求多进程；可启用主进程 chunk 并发（配置控制）。
2. sync：返回值为合并后的 `list[payload]`，队列可选。
3. async：后台任务立即返回 `StockKlineJob`。
4. async：队列产出粒度为“bundle 回收后逐 payload 写入”，不是 worker 内 chunk 完成即跨进程直推。
5. async：建议消费端按 `queue.get(timeout=20)` 持续读取，直到 `event=done`。

## 6. 关键配置项（`config.yaml.parallel`）

1. `task_chunk_cache_min_tasks`
2. `task_chunk_inproc_future_workers`
3. `task_chunk_max_inflight_multiplier`
4. `chunk_reconnect_on_unavailable`
5. `chunk_reconnect_max_attempts`
6. `auto_prewarm_on_async`
7. `auto_prewarm_require_all_workers`
8. `auto_prewarm_timeout_seconds`
9. `auto_prewarm_max_rounds`
10. `auto_prewarm_spread_standard_hosts`

## 7. 文档联动约束

1. 修改 `simple_api` 参数或返回结构时，同步更新 `README.md` 示例。
2. 修改并行调度语义（chunk/bundle/future）时，同步更新本文档和 `README.md`。
3. 修改 `config.yaml` 键名或默认值时，同步更新两份文档说明。

## 8. 常见风险与排障

1. 连接风险：hosts 不可达会导致全链路失败，先检查 `hosts.*` 连通性与配置内容。
2. 超时风险：并行总超时触发后会回收未完成 future；确认 `parallel_total_timeout_seconds` 与任务规模匹配。
3. 吞吐异常：优先检查 `task_chunk_inproc_future_workers`、`task_chunk_max_inflight_multiplier`。
4. 进程残留：测试脚本结束后应确认无残留 python 进程。

## 9. 建议回归清单

1. 小样本：`10 code * 2 freq * 3 time` 验证功能正确性。
2. 中样本：`200 * 5 * 15` 比较 sync/async 总耗时与失败率。
3. 队列保障：async 消费端用 `timeout=20s` 验证连续返回与 done 收敛。

## 10. 发布流程（PyPI）

1. 更新版本号：`pyproject.toml` 与 `src/zsdtdx/__init__.py` 保持一致。
2. 清理产物目录：删除 `dist/` 与旧 `*.egg-info`。
3. 构建：`py -m build`。
4. 校验：`py -m twine check dist/*`。
5. 上传：`py -m twine upload dist/*`（使用用户目录下 `.pypirc` 的 token）。
