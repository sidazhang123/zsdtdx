"""
异步 K 线结果摘要测试。

职责：
1. 验证带队列时行情只进入队列，job.result() 返回计数摘要。
2. 验证队列取走行情后，句柄不再保住行情对象。
3. 验证未带队列时 result() 仍返回 payload 列表。
4. 验证异常终态只通知一次、保留原异常，并维持有界队列背压。

边界：
1. 通过 mock 迭代器避免网络和真实连接。
"""

from __future__ import annotations

import gc
import queue
import weakref
from unittest import mock

import pytest

from zsdtdx.engine.parallel_fetcher import ParallelKlineFetcher


class _Payload(dict):
    """输入字段同普通行情事件，输出可弱引用的字典。边界：只为回收测试提供引用追踪。"""


def _fetcher() -> ParallelKlineFetcher:
    """输入无，输出关闭预热后的抓取器。边界：不建立行情连接。"""
    fetcher = ParallelKlineFetcher()
    fetcher.num_threads = 1
    return fetcher


def _task() -> dict:
    """输入无，输出一只股票的日线任务。边界：只含抓取器归一化所需字段。"""
    return {
        "code": "600000",
        "freq": "d",
        "start_time": "2026-02-01",
        "end_time": "2026-02-14",
    }


def test_queued_async_returns_summary_and_releases_payload() -> None:
    """带队列完成一只股票后，摘要不含行情，且取走队列后原文可被回收。"""
    fetcher = _fetcher()
    held = _Payload(
        event="data",
        task={"code": "600000", "freq": "d"},
        rows=[{"close": 10.0}],
        error=None,
    )
    payload_ref = weakref.ref(held)
    box = {"item": held}
    del held

    def _iter(_tasks, **_kwargs):
        """输入任务，输出预先放入盒子的一条行情。边界：只产出一次。"""
        yield box.pop("item")

    result_queue = queue.Queue()
    with (
        mock.patch.object(fetcher, "_ensure_async_prewarm", return_value=None),
        mock.patch.object(
            fetcher, "_iter_task_payloads_parallel_chunked", side_effect=_iter
        ),
    ):
        job = fetcher.fetch_stock_tasks_async(tasks=[_task()], queue=result_queue)
        summary = job.result(timeout=2)

    assert summary == {"total_tasks": 1, "success_tasks": 1, "failed_tasks": 0}
    event = result_queue.get(timeout=1)
    done = result_queue.get(timeout=1)
    assert event["rows"][0]["close"] == 10.0
    assert done == {
        "event": "done",
        "total_tasks": 1,
        "success_tasks": 1,
        "failed_tasks": 0,
    }
    del event
    gc.collect()
    assert payload_ref() is None
    assert job.result(timeout=0) == summary


def test_queued_async_counts_failure_and_dropped_hook() -> None:
    """失败行情进入队列；钩子丢弃的成功行情不进入队列，计数仍保留。"""
    fetcher = _fetcher()
    payloads = [
        {
            "event": "data",
            "task": {"code": "600000", "freq": "d"},
            "rows": [],
            "error": "no_data",
        },
        {
            "event": "data",
            "task": {"code": "000001", "freq": "d"},
            "rows": [{"close": 2.0}],
            "error": None,
        },
    ]

    def _iter(_tasks, **_kwargs):
        """输入任务，按原顺序输出两条行情。边界：不复制 payload。"""
        yield from payloads

    result_queue = queue.Queue()
    with (
        mock.patch.object(fetcher, "_ensure_async_prewarm", return_value=None),
        mock.patch.object(
            fetcher, "_iter_task_payloads_parallel_chunked", side_effect=_iter
        ),
    ):
        job = fetcher.fetch_stock_tasks_async(
            tasks=[_task(), dict(_task(), code="000001")],
            queue=result_queue,
            preprocessor_operator=lambda item: None
            if item.get("error") is None
            else item,
        )
        summary = job.result(timeout=2)

    assert summary == {"total_tasks": 2, "success_tasks": 1, "failed_tasks": 1}
    event = result_queue.get(timeout=1)
    done = result_queue.get(timeout=1)
    assert result_queue.empty()
    assert event["error"] == "no_data"
    assert done["success_tasks"] == 1
    assert done["failed_tasks"] == 1
    assert job.result(timeout=0) == summary


def test_async_without_queue_still_returns_payload_list() -> None:
    """未提供队列时，result() 仍返回完整 payload 列表。"""
    fetcher = _fetcher()
    payload = {
        "event": "data",
        "task": {"code": "600000", "freq": "d"},
        "rows": [{"close": 3.0}],
        "error": None,
    }

    def _iter(_tasks, **_kwargs):
        """输入任务，输出唯一一条行情。边界：不额外构造事件。"""
        yield payload

    with (
        mock.patch.object(fetcher, "_ensure_async_prewarm", return_value=None),
        mock.patch.object(
            fetcher, "_iter_task_payloads_parallel_chunked", side_effect=_iter
        ),
    ):
        job = fetcher.fetch_stock_tasks_async(tasks=[_task()])
        assert job.queue is None
        assert job.result(timeout=2) == [payload]


def test_empty_queued_async_returns_zero_summary() -> None:
    """空任务带队列时只发送 done，result() 的三项计数都是 0。"""
    fetcher = _fetcher()
    result_queue = queue.Queue()
    with mock.patch.object(fetcher, "_ensure_async_prewarm", return_value=None):
        job = fetcher.fetch_stock_tasks_async(tasks=[], queue=result_queue)
        summary = job.result(timeout=2)
    assert summary == {"total_tasks": 0, "success_tasks": 0, "failed_tasks": 0}
    assert result_queue.get(timeout=1) == {"event": "done", **summary}
    assert result_queue.empty()


def _kind_task(kind: str, symbol: str) -> dict:
    """输入任务种类与标的，输出归一化所需任务；边界：仅供离线协调器测试。"""
    field = {"stock": "code", "index": "index_name", "block": "block_name"}[kind]
    task = _task()
    task.pop("code")
    return {**task, field: ("概念", symbol) if kind == "block" else symbol}


@pytest.mark.parametrize("kind", ["stock", "index", "block"])
def test_async_hook_failure_sends_done_and_preserves_exception(kind: str) -> None:
    """输入一次成功后钩子异常，输出唯一异常 done；剩余任务失败且原异常链可查。"""
    fetcher = _fetcher()
    tasks = [_kind_task(kind, symbol) for symbol in ("600000", "000001", "000002")]
    original = RuntimeError("写库钩子不可用")
    result_queue = queue.Queue()

    def _iter(_tasks, **_kwargs):
        """输入任务，逐项输出有效行情；边界：不访问网络。"""
        for task in _tasks:
            yield {"event": "data", "task": task, "rows": [{"close": 1}], "error": None}

    calls = 0

    def _hook(payload):
        """输入行情，首次原样输出、第二次抛错；边界：第三次不应执行。"""
        nonlocal calls
        calls += 1
        if calls == 2:
            raise original
        return payload

    with (
        mock.patch.object(fetcher, "_ensure_async_prewarm", return_value=None),
        mock.patch.object(
            fetcher, "_iter_task_payloads_parallel_chunked", side_effect=_iter
        ),
    ):
        job = fetcher._fetch_tasks_async(
            task_kind=kind, tasks=tasks, queue=result_queue, preprocessor_operator=_hook
        )
        with pytest.raises(ValueError, match="写库钩子不可用") as caught:
            job.result(timeout=2)

    assert caught.value.__cause__ is original
    assert job.exception(timeout=0) is caught.value
    assert calls == 2
    assert result_queue.get(timeout=1)["event"] == "data"
    done = result_queue.get(timeout=1)
    assert done == {
        "event": "done",
        "total_tasks": 3,
        "success_tasks": 1,
        "failed_tasks": 2,
        "error": str(caught.value),
    }
    assert result_queue.empty()


@pytest.mark.parametrize("kind", ["stock", "index", "block"])
@pytest.mark.parametrize("phase", ["create", "iterate"])
def test_async_iterator_failure_sends_done(kind: str, phase: str) -> None:
    """输入迭代器创建或迭代异常，输出异常 done；边界：已交付行情保持成功且不重复。"""
    fetcher = _fetcher()
    tasks = [_kind_task(kind, symbol) for symbol in ("600000", "000001")]
    original = RuntimeError("抓取迭代器中断")
    result_queue = queue.Queue()

    def _iter(_tasks, **_kwargs):
        """输入任务，输出一条行情后中断；边界：只用于迭代阶段异常。"""
        yield {"event": "data", "task": _tasks[0], "rows": [], "error": None}
        raise original

    with (
        mock.patch.object(fetcher, "_ensure_async_prewarm", return_value=None),
        mock.patch.object(
            fetcher,
            "_iter_task_payloads_parallel_chunked",
            side_effect=original if phase == "create" else _iter,
        ),
    ):
        job = fetcher._fetch_tasks_async(
            task_kind=kind, tasks=tasks, queue=result_queue
        )
        with pytest.raises(RuntimeError) as caught:
            job.result(timeout=2)

    assert caught.value is original
    if phase == "iterate":
        assert result_queue.get(timeout=1)["event"] == "data"
    success = int(phase == "iterate")
    assert result_queue.get(timeout=1) == {
        "event": "done",
        "total_tasks": 2,
        "success_tasks": success,
        "failed_tasks": 2 - success,
        "error": str(original),
    }
    assert result_queue.empty()


@pytest.mark.parametrize(
    "fail_data,fail_done", [(True, False), (True, True), (False, True)]
)
def test_async_queue_failure_preserves_first_error(
    fail_data: bool, fail_done: bool
) -> None:
    """输入数据或结束投递异常，输出首次异常；边界：结束事件只尝试一次且不掩盖数据投递失败。"""
    fetcher = _fetcher()
    data_error = RuntimeError("数据队列不可写")
    done_error = RuntimeError("结束队列不可写")
    events = []
    attempts = []

    class _Queue:
        """输入事件并记录投递，输出无；边界：指定事件类型抛出固定异常。"""

        def put(self, payload):
            """输入行情或结束事件，输出无；失败不保存事件，但保留投递次数。"""
            event = payload["event"]
            attempts.append(event)
            if event == "data" and fail_data:
                raise data_error
            if event == "done" and fail_done:
                raise done_error
            events.append(payload)

    payload = {"event": "data", "task": _task(), "rows": [], "error": None}
    with (
        mock.patch.object(fetcher, "_ensure_async_prewarm", return_value=None),
        mock.patch.object(
            fetcher,
            "_iter_task_payloads_parallel_chunked",
            return_value=iter([payload]),
        ),
    ):
        job = fetcher.fetch_stock_tasks_async(tasks=[_task()], queue=_Queue())
        with pytest.raises(RuntimeError) as caught:
            job.result(timeout=2)

    assert caught.value is (data_error if fail_data else done_error)
    assert attempts == ["data", "done"]
    if fail_data and not fail_done:
        assert events == [
            {
                "event": "done",
                "total_tasks": 1,
                "success_tasks": 0,
                "failed_tasks": 1,
                "error": str(data_error),
            }
        ]


def test_async_bounded_queue_delivers_all_data_before_done() -> None:
    """输入容量一的队列，持续消费后输出全部行情与唯一 done；边界：不绕过背压丢弃事件。"""
    fetcher = _fetcher()
    tasks = [_task(), dict(_task(), code="000001")]
    result_queue = queue.Queue(maxsize=1)

    def _iter(_tasks, **_kwargs):
        """输入任务，逐项输出行情；边界：队列容量不改变任务顺序。"""
        for task in _tasks:
            yield {"event": "data", "task": task, "rows": [], "error": None}

    with (
        mock.patch.object(fetcher, "_ensure_async_prewarm", return_value=None),
        mock.patch.object(
            fetcher, "_iter_task_payloads_parallel_chunked", side_effect=_iter
        ),
    ):
        job = fetcher.fetch_stock_tasks_async(tasks=tasks, queue=result_queue)
        events = [result_queue.get(timeout=2) for _ in range(3)]
        summary = job.result(timeout=2)

    assert [item["event"] for item in events] == ["data", "data", "done"]
    assert [item["task"]["code"] for item in events[:2]] == ["600000", "000001"]
    assert summary == {"total_tasks": 2, "success_tasks": 2, "failed_tasks": 0}
    assert events[-1] == {"event": "done", **summary}
    assert result_queue.empty()
