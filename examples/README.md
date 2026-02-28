# `get_stock_kline` 任务化示例

## 1. 同步模式（阻塞 + 实时队列）

```python
import queue as py_queue
from zsdtdx import StockKlineTask, get_client, get_stock_kline

with get_client() as client:
    q = py_queue.Queue()
    payloads = get_stock_kline(
        task=[
            StockKlineTask(code="600000", freq="d", start_time="2026-02-13", end_time="2026-02-14"),
            {"code": "000001", "freq": "60", "start_time": "2026-02-13", "end_time": "2026-02-14"},
        ],
        queue=q,
        mode="sync",
    )
    print(payloads)  # list[task_payload]
```

## 2. 异步模式（非阻塞 + 队列持续消费）

```python
import queue as py_queue
from zsdtdx import get_stock_kline

q = py_queue.Queue()
job = get_stock_kline(
    task=[
        {"code": "600000", "freq": "d", "start_time": "2026-02-13", "end_time": "2026-02-14"},
        {"code": "000001", "freq": "15", "start_time": "2026-02-13", "end_time": "2026-02-14"},
    ],
    queue=q,
    mode="async",
)

while True:
    item = q.get()
    if item.get("event") == "done":
        print("done:", item)
        break
    print("data:", item["task"], len(item.get("rows", [])), item.get("error"))

print("async result len:", len(job.result()))
```

## 3. 预处理函数示例

```python
def preprocess(payload: dict) -> dict | None:
    # 过滤空结果
    if payload.get("error") == "no_data":
        return None

    # 字段改名 + 值加工 + 删除字段
    payload["task_code"] = payload["task"]["code"]
    payload.pop("worker_pid", None)
    for row in payload.get("rows", []):
        row["turnover"] = row.pop("amount", None)
    return payload
```

## 4. 返回结构

1. 数据事件：  
`{"event":"data","task":{"code":"600000","freq":"d","start_time":"...","end_time":"..."},"rows":[...],"error":null,"worker_pid":12345}`

2. 结束事件：  
`{"event":"done","total_tasks":2,"success_tasks":2,"failed_tasks":0}`

## 5. chunk 并行说明

1. `get_stock_kline(task)` 会将同 `code+freq` 的任务归并为一个 chunk。  
2. chunk 内任务会按 `start_time` 从早到晚执行，以提升缓存命中率。  
3. 结果按“完成顺序”回收，不保证与输入任务顺序一致。  
4. 当 chunk 任务数达到 `parallel.task_chunk_cache_min_tasks`（默认 `3`）时，会启用 chunk 级轻量缓存。  

## 6. 兼容说明

`get_stock_kline` 已移除旧签名 `codes/freq/start_time/end_time`，请改用 `task` 参数。

## 7. 基准脚本

1. 先生成固定任务集（只需执行一次，后续 sync/async 复用同一份）：
`python examples/generate_benchmark_taskset_200x5x15.py`
2. 完整对比（sync + async）：
`python examples/benchmark_sync_async_200x5x15.py`
3. 仅跑 sync（读取固定任务集）：
`python examples/benchmark_sync_200x5x15.py`
4. 仅跑 async（读取固定任务集）：
`python examples/benchmark_async_200x5x15.py`
5. 一键完整对比并输出综合分析结论（调起以上 3 个脚本）：
`python examples/benchmark_full_compare_200x5x15.py`
