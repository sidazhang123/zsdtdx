"""
异步公司信息结果摘要测试。

职责：
1. 验证带队列的异步公司信息只把正文放进队列，job.result() 返回计数。
2. 验证前一只股票被消费后，任务尚未结束时库内不再保住该正文。
3. 验证同步并行调用仍返回全文。
4. 验证 result() 的超时和协调异常仍向外抛出。

边界：
1. 用替身迭代器避免网络和真实连接。
"""

from __future__ import annotations

import gc
import queue
import threading
import weakref
from concurrent.futures import TimeoutError as FuturesTimeoutError

import pytest

from zsdtdx.engine import parallel_fetcher as pf
from zsdtdx.util.company_info_state import company_info_outcome


class _Row(dict):
    """输入字段同公司信息行，输出可弱引用的字典。边界：只为回收测试提供引用追踪。"""


def _patch_idle(monkeypatch) -> None:
    """输入补丁器，输出无。边界：不碰真实客户端和运行时失败记录。"""
    monkeypatch.setattr(pf, "_shared_client_context", None)


def test_queued_async_releases_body_while_job_still_running(monkeypatch) -> None:
    """第一只股票进入队列并被取走后，第二只仍在等待时原文可被回收。"""
    row = _Row(code="600000", category="公司概况", content="正文")
    row_ref = weakref.ref(row)
    box = {"row": row}
    del row
    second_started = threading.Event()
    release_second = threading.Event()
    consumed = threading.Event()

    def fake(**_kwargs):
        """输入调用参数，先后输出两只股票。边界：第一只交出后不再保留其正文。"""
        payload = {
            "outcomes": [company_info_outcome("600000", rows=[box.pop("row")])],
            "requested_codes": ["600000"],
        }
        yield payload
        payload = None
        second_started.set()
        assert release_second.wait(3)
        yield {
            "outcomes": [
                company_info_outcome(
                    "000001",
                    rows=[
                        {
                            "code": "000001",
                            "category": "公司概况",
                            "content": "后文",
                        }
                    ],
                )
            ],
            "requested_codes": ["000001"],
        }

    _patch_idle(monkeypatch)
    monkeypatch.setattr(pf, "_iter_company_info_chunk_results", fake)
    result_queue = queue.Queue()

    def consume() -> None:
        """输入无，输出无。边界：取走第一只后立即放开事件。"""
        event = result_queue.get(timeout=3)
        assert event["event"] == "data"
        assert event["code"] == "600000"
        assert event["rows"][0]["category"] == "公司概况"
        assert event["rows"][0]["content"] == "正文"
        del event
        consumed.set()

    threading.Thread(target=consume, daemon=True).start()
    job = pf.fetch_company_info_async(["600000", "000001"], queue=result_queue)
    assert consumed.wait(3)
    assert second_started.wait(3)
    gc.collect()
    assert row_ref() is None
    release_second.set()
    summary = job.result(timeout=3)
    assert summary == {"total_codes": 2, "success_codes": 2, "failed_codes": 0}
    second = result_queue.get(timeout=1)
    done = result_queue.get(timeout=1)
    assert second["rows"][0]["content"] == "后文"
    assert done == {"event": "done", **summary}
    assert result_queue.empty()
    assert job.result(timeout=0) == summary


def test_sync_parallel_with_queue_still_returns_rows(monkeypatch) -> None:
    """同步并行即使写队列，返回值仍是全文行。"""
    payload = {
        "outcomes": [
            company_info_outcome(
                "600000",
                rows=[{"code": "600000", "category": "公司概况", "content": "正文"}],
            )
        ],
        "requested_codes": ["600000"],
    }

    def fake(**_kwargs):
        """输入调用参数，输出唯一分片。边界：只产出一次。"""
        yield payload

    _patch_idle(monkeypatch)
    monkeypatch.setattr(pf, "_iter_company_info_chunk_results", fake)
    result_queue = queue.Queue()
    rows = pf.fetch_company_info_parallel(["600000"], queue=result_queue)
    assert rows == payload["outcomes"][0]["rows"]
    event = result_queue.get(timeout=1)
    assert event["rows"][0]["content"] == "正文"
    assert result_queue.get(timeout=1)["success_codes"] == 1


def test_async_result_timeout_then_summary(monkeypatch) -> None:
    """协调器尚未结束时 result 超时；放开后返回失败计数。"""
    started = threading.Event()
    release = threading.Event()

    def blocked(**_kwargs):
        """输入调用参数，输出空迭代。边界：第一次拉取前阻塞。"""
        started.set()
        assert release.wait(3)
        if False:
            yield None

    _patch_idle(monkeypatch)
    monkeypatch.setattr(pf, "_iter_company_info_chunk_results", blocked)
    job = pf.fetch_company_info_async(["600000"], queue=queue.Queue())
    assert started.wait(3)
    try:
        with pytest.raises(FuturesTimeoutError):
            job.result(timeout=0.01)
    finally:
        release.set()
    assert job.result(timeout=3) == {
        "total_codes": 1,
        "success_codes": 0,
        "failed_codes": 1,
    }


def test_async_result_raises_original_coordinator_error(monkeypatch) -> None:
    """结束通知写入失败时，result() 抛出原来的异常。"""

    class DoneBoom(queue.Queue):
        """输入普通队列操作，输出无。边界：只在 done 事件上失败。"""

        def put(self, item, block=True, timeout=None):
            """输入事件，输出无。边界：data 正常入队，done 抛出原错误。"""
            if isinstance(item, dict) and item.get("event") == "done":
                raise RuntimeError("done failed")
            return super().put(item, block=block, timeout=timeout)

    payload = {
        "outcomes": [
            company_info_outcome(
                "600000",
                rows=[{"code": "600000", "category": "公司概况", "content": "正文"}],
            )
        ],
        "requested_codes": ["600000"],
    }

    def fake(**_kwargs):
        """输入调用参数，输出一只成功股票。边界：只产出一次。"""
        yield payload

    _patch_idle(monkeypatch)
    monkeypatch.setattr(pf, "_iter_company_info_chunk_results", fake)
    job = pf.fetch_company_info_async(["600000"], queue=DoneBoom())
    with pytest.raises(RuntimeError, match="done failed"):
        job.result(timeout=3)
