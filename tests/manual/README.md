# tests/manual — 手工可执行测试脚本

本目录脚本需用 `py tests/manual/<脚本名>` 直接运行，**不由** `pytest` 自动收集（见 `pyproject.toml` 中 `norecursedirs`）。

| 脚本 | 用途 | 网络 |
|------|------|------|
| `smoke_index_kline_sync_async_demo.py` | 指数 K 线 sync/async 小样本冒烟 | 是 |
| `probe_pytdx_standard_hosts.py` | 标准行情 host 连通性探测 | 是 |
| `probe_kline_pagination_direction.py` | K 线 `start` 分页方向实测 | 是 |
| `verify_zz2000_1315_bar.py` | 中证2000 单 bar（13:15）行为验证 | 是 |
| `weaknet_inject_retry.py` | 弱网注入与 chunk 重试路径（离线） | 否 |
| `run_stock_kline_async_full_compare.py` | 全量股票 async K 线单环境基准（workspace / site_packages） | 是 |

全量基准需主 Agent 分两次单独运行。**不要用「主进程是否退出」判断完成**；async 进程池不会自动关闭，脚本在收到队列 `event=done` 后会立刻调用 `destroy_parallel_fetcher()`，并写入 lifecycle 状态。

主 Agent 验收每一轮的条件（满足后再 `Stop-Process` 杀 py、进入下一轮）：

1. `artifacts/stock_kline_async_full_compare/<target>/lifecycle.json` 中 `lifecycle_status` 为 `completed`
2. 同目录 `run_meta.json` 含 `done` 与 `destroy_parallel_fetcher` 字段
3. （可选）确认无残留 worker：`Get-Process python*,py*`

```powershell
$env:ZSDTDX_BENCH_TARGET='workspace'; py tests/manual/run_stock_kline_async_full_compare.py
# 读 lifecycle.json 确认 completed 后，再杀 py
$env:ZSDTDX_BENCH_TARGET='site_packages'; py tests/manual/run_stock_kline_async_full_compare.py
```

参数见 `stock_kline_async_full_compare_config.yaml`（默认 5207 目标股数、freq=15/30/60/d/w、2026-05-18~22）。

```bash
py tests/manual/weaknet_inject_retry.py
py tests/manual/smoke_index_kline_sync_async_demo.py
py tests/manual/probe_pytdx_standard_hosts.py
py tests/manual/probe_kline_pagination_direction.py
py tests/manual/verify_zz2000_1315_bar.py
```

`probe_pytdx_standard_hosts.py` 结果默认写入同目录 `pytdx_standard_hosts_probe_result.json`。

离线 pytest 用例见上级 `tests/test_*.py`：`py -m pytest tests/ -q`。
