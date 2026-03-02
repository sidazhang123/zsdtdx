# zsdtdx

`zsdtdx` 是面向 A 股/期货行情场景的 Python 封装库，基于 pytdx 生态提供统一 API、连接池、重试和并行抓取能力。

## 安装

```bash
pip install zsdtdx
```

## simple_api 快速开始

### 1) sync 模式（阻塞返回）

```python
import queue as py_queue
from zsdtdx import StockKlineTask, get_client, get_stock_kline

with get_client() as client:
    q = py_queue.Queue()
    payloads = get_stock_kline(
        task=[
            StockKlineTask(code="600000", freq="d", start_time="2026-02-01", end_time="2026-02-14"),
            {"code": "000001", "freq": "60", "start_time": "2026-02-01", "end_time": "2026-02-14"},
        ],
        queue=q,
        mode="sync",
    )
    print(f"sync payload count: {len(payloads)}")
```

### 2) async 模式（队列实时消费）

```python
from zsdtdx import get_client, get_stock_kline

with get_client() as client:
    job = get_stock_kline(
        task=[{"code": "600000", "freq": "d", "start_time": "2026-02-01", "end_time": "2026-02-14"}],
        mode="async",
    )

    while True:
        event = job.queue.get(timeout=20)
        if event.get("event") == "done":
            break
        # event="data" 时：按 task/rows/error 实时处理
        print(event.get("task"), event.get("error"))

    # 等待后台任务结束并传播异常
    job.result()
```

## 关键行为说明

- 时间窗口按闭区间 `[start_time, end_time]` 处理。
- 股票 task 日期输入会自动补全为：
  - `start_time`: `09:30:00`
  - `end_time`: `16:00:00`
- 期货时间窗口日期输入会自动补全为：
  - `start_time`: `09:00:00`
  - `end_time`: `15:00:00`
- `get_stock_kline` 仅接受 `task` 列表（元素为 `StockKlineTask` 或 `dict`）。
- 队列事件结构：
  - 数据事件：`{"event":"data","task":{...},"rows":[...],"error":str|None,"worker_pid":int}`
  - 完成事件：`{"event":"done","total_tasks":...,"success_tasks":...,"failed_tasks":...}`

## 并行模型（task 链路）

- `chunk`：同 `code+freq` 的任务集合，内部按 `start_time` 升序执行。
- `bundle`：提交给单个进程池 future 的 chunk 批次。
- async（多进程）：
  - 父进程将 bundle 提交给 `ProcessPoolExecutor`。
  - worker 进程内再以 `ThreadPoolExecutor` 并发 chunk（上限由 `task_chunk_inproc_future_workers` 控制）。
  - 队列结果以“bundle 回收”为触发点写入（非 worker 内单 chunk 直接跨进程推送）。
- sync（单进程）：
  - 不启用多进程时，在主进程内直接并发 chunk future。

## 配置说明

- 默认读取包内 `config.yaml`。
- 可显式指定配置路径：

```python
from zsdtdx import get_client

with get_client(config_path=r"D:\\configs\\zsdtdx.yaml") as client:
    ...
```

- 常用并行配置位于 `config.yaml.parallel`：
  - `task_chunk_cache_min_tasks`
  - `task_chunk_inproc_future_workers`
  - `task_chunk_max_inflight_multiplier`
  - `chunk_reconnect_on_unavailable`
  - `chunk_reconnect_max_attempts`
  - `auto_prewarm_on_async`
  - `auto_prewarm_require_all_workers`
  - `auto_prewarm_timeout_seconds`
  - `auto_prewarm_max_rounds`
  - `auto_prewarm_spread_standard_hosts`

## API 概览

- `get_client`
- `get_supported_markets`
- `get_stock_code_name`
- `get_all_future_list`
- `get_stock_kline`
- `get_future_kline`
- `get_company_info`
- `get_stock_latest_price`
- `get_future_latest_price`
- `get_runtime_failures`
- `get_runtime_metadata`

## 运行环境与依赖

- Python: `>=3.10`
- 依赖：`pandas`、`PyYAML`、`six`、`psutil`

## 开发文档

- `HANDOFF.md`
- `PERFORMANCE_REPORT.md`
- `examples/`

## 许可证与来源

- 许可证：`MIT`（见 `LICENSE`）
- 第三方归属：见 `THIRD_PARTY_NOTICES.md`
