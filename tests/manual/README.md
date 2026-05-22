# tests/manual — 手工可执行测试脚本

本目录脚本需用 `py tests/manual/<脚本名>` 直接运行，**不由** `pytest` 自动收集（见 `pyproject.toml` 中 `norecursedirs`）。

| 脚本 | 用途 | 网络 |
|------|------|------|
| `smoke_index_kline_sync_async_demo.py` | 指数 K 线 sync/async 小样本冒烟 | 是 |
| `probe_pytdx_standard_hosts.py` | 标准行情 host 连通性探测 | 是 |
| `probe_kline_pagination_direction.py` | K 线 `start` 分页方向实测 | 是 |
| `verify_zz2000_1315_bar.py` | 中证2000 单 bar（13:15）行为验证 | 是 |
| `test_chunk_timeout_retry.py` | chunk 超时/重试（monkey-patch 离线） | 否 |
| `weaknet_inject_retry.py` | 弱网注入与 chunk 重试路径（离线） | 否 |

```bash
py tests/manual/test_chunk_timeout_retry.py
py tests/manual/weaknet_inject_retry.py
py tests/manual/smoke_index_kline_sync_async_demo.py
py tests/manual/probe_pytdx_standard_hosts.py
py tests/manual/probe_kline_pagination_direction.py
py tests/manual/verify_zz2000_1315_bar.py
```

`probe_pytdx_standard_hosts.py` 结果默认写入同目录 `pytdx_standard_hosts_probe_result.json`。

离线 pytest 用例见上级 `tests/test_*.py`：`py -m pytest tests/ -q`。
