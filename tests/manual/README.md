# tests/manual — 手工可执行测试脚本

本目录脚本需用 `py tests/manual/<脚本名>` 直接运行，**不由** `pytest` 自动收集（见 `pyproject.toml` 中 `norecursedirs`）。

| 脚本 | 用途 | 网络 |
|------|------|------|
| `smoke_index_kline_sync_async_demo.py` | 指数 K 线 sync/async 小样本冒烟 | 是 |
| `probe_pytdx_standard_hosts.py` | 标准行情 host 连通性探测 | 是 |
| `probe_kline_pagination_direction.py` | K 线 `start` 分页方向实测 | 是 |
| `probe_pagination_termination.py` | 各分页协议末短页及后继空页实测 | 是 |
| `probe_ex_instrument_pagination.py` | 扩展码表可变页长与空页终止实测 | 是 |
| `verify_zz2000_1315_bar.py` | 中证2000 单 bar（13:15）行为验证 | 是 |
| `run_stock_kline_async_full_compare.py` | 全量股票 async K 线单环境基准（workspace / site_packages） | 是 |
| `run_stock_kline_async_week.py` | 默认股票清单指定日期区间 15/30/60/d/w async 抽样 | 是 |
| `compare_std_host_bars.py` | 钉死标准行情两个能力组，对比同一已收盘 15 分钟 K 线跨站是否一致 | 是 |
| `live_full_api/run_one.py` | 分轮现场验证对外 `get_*` API | 是 |
| `run_kline_universe_week_validate.py` | 全市场周区间多周期 K 线质量校验 | 是 |
| `run_zsdtdx_weekly_daily_bench.py` | zsdtdx 周/日线基准 | 是 |
| `run_tdxquant_weekly_daily_bench.py` | tdxquant 周/日线对照基准 | 是 |
| `analyze_tdxquant_vs_zsdtdx_weekly.py` | tdxquant vs zsdtdx 周线差异分析 | 否 |
| `check_boc_ohlcv_quality.py` | 单标的 OHLCV 质量抽检 | 是 |
| `_analyze_kline_errors.py` | K 线错误日志辅助分析 | 否 |
| `run_cache_lifecycle_acceptance.py` | 真实行情缓存冷建、命中、过期重建与跨接口共享验收 | 是 |

全量基准需主 Agent 分两次单独运行。**不要用「主进程是否退出」判断完成**；常驻线程执行器需显式回收，脚本在收到队列 `event=done` 后会立刻调用 `destroy_parallel_fetcher()`，并写入 lifecycle 状态。

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
py tests/manual/smoke_index_kline_sync_async_demo.py
py tests/manual/probe_pytdx_standard_hosts.py
py tests/manual/probe_kline_pagination_direction.py
py tests/manual/verify_zz2000_1315_bar.py
py tests/manual/run_cache_lifecycle_acceptance.py
```

`probe_pytdx_standard_hosts.py` 结果默认写入同目录 `pytdx_standard_hosts_probe_result.json`。

离线 pytest 用例见上级 `tests/test_*.py`：`py -m pytest tests/ -q`。

说明：旧版 `run_simple_api_full_acceptance.py`、`run_simple_api_anomaly_acceptance.py`
和 `smoke_yinhe_simple_api.py` 依赖已经废弃的英文股票统计列名，已移除。当前对外 API
分轮实测使用 `live_full_api/run_one.py`，K 线组合覆盖使用
`run_simple_api_market_freq_matrix.py`，缓存生命周期使用
`run_cache_lifecycle_acceptance.py`。


线程列式重构验收：

- `run_thread_columnar_benchmark.py`：读取同名 YAML，在独立解释器交替比较基线与线程快照，按本次进程树采样，输出五轮冷/热结果和任务完整性摘要。需先准备基线归档与固定任务文件，路径在 YAML 中配置。
- `run_thread_contract_compare.py`：读取同名 YAML，复用基准产生的快照，比较复权/不复权、港股、指数、板块、期货和公司信息；双方同时空结果不算通过。
- `run_thread_fault_injection.py`：读取同名 YAML，在真实一万任务执行中三轮关闭部分连接，比较完整任务指纹并检查销毁后的线程回收。
- 不同 host 的历史分钟线可能存在切分或成交量差异；跨 API 对拍及混合周期基准固定同一 host，避免将上游差异归因于解析器。日线大批基准使用默认多 host。
- 同地址也观察到旧版历史分钟线波动；`freeze_kline_responses` 使用受环境清单控制的 `kline_capture_hook/sitecustomize.py`，使旧版 spawn worker 和新版线程按完整请求复用同一份真实解压回包。该工具只作用于验收子进程，不进入发布包。公司信息仍实时拉取。
- `baseline_results_dir` 非空时只重测线程快照，旧版指标引用该目录已有记录；报告必须注明引用关系，不能称为同轮交替测量。设置为空可重新执行旧新交替对照。
# 六类板块完整目录与K线覆盖

执行 `py tests/manual/verify_six_category_block_kline.py`，默认对2026-10-01至2026-10-30的全部六类板块验证15/30/60分钟、日线、周线。窗口和周期在同目录 `verify_six_category_block_kline.yaml` 中调整。

脚本同时检查统一映射6小时缓存的跨客户端磁盘重载、分类同名路由隔离，以及公开同步/异步接口。报告分别统计有行情、无代码空返回、服务端/区间为空和真实请求失败；产物保存在 `artifacts/six_category_block_kline/<运行时间>/`。
