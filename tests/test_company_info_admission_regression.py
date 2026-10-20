"""公司信息准入、同站正文和许可回收的离线回归。

输入：实际连接池、调度器与纯内存 API 替身。
输出：故障语义、能力约束、预算和并发正文完整性断言。
边界：不建立 TCP 连接，不读取行情，不修改价格解析、缓存或已安装包。
"""

from __future__ import annotations

import threading
import time

import pytest

from zsdtdx.engine.adaptive_scheduler import AdaptiveConcurrencyController
from zsdtdx.engine.thread_runtime import LeasedFailoverPool, attempt_deadline
from zsdtdx.engine.unified_client import (
    AttemptSocketCloser,
    PersistentFailoverPool,
    UnifiedTdxClient,
    _attempt_socket_closer,
)
from zsdtdx.util.errors import CompanyInfoFetchError


HOST_A = ("offline-a", 7709)
HOST_B = ("offline-b", 7709)


def make_client(*, leased=True, hosts=None, mode="ok", bad_connect=(), categories=1):
    """输入替身故障参数，输出真实客户端空壳、池、配额和调用记录；不联网。"""
    hosts = list(hosts or [HOST_A, HOST_B])
    calls = []

    class Api:
        """内存行情站；记录目录和正文身份，只返回指定测试字节。"""

        client = None

        def connect(self, host, port, time_out):
            """输入站名与预算，输出建连结果；只保存站名，不创建 socket。"""
            self.host = (host, port)
            calls.append(("connect", self.host))
            if mode == "fail_body_connect_first" and self.host == HOST_A:
                return sum(item == ("connect", HOST_A) for item in calls) == 1
            return self.host not in bad_connect

        def disconnect(self):
            """输入无，输出无；内存 API 无系统资源。"""

        def get_company_info_category(self, market, code):
            """输入协议身份，输出测试目录；空目录模式不发送正文请求。"""
            calls.append(("directory", self.host, market, code))
            if mode == "empty_directory":
                return []
            return [
                {
                    "index": index,
                    "name": f"栏目{index}",
                    "filename": f"{code}.txt",
                    "start": index * 100,
                    "length": 2,
                }
                for index in range(categories)
            ]

        def get_company_info_content(
            self, market, code, filename, start, length, index
        ):
            """输入同站目录偏移，输出完整、短页或坏 GBK；保留请求身份记录。"""
            calls.append(("content", self.host, market, code, start, index))
            if mode == "short":
                return b"x"
            if mode == "bad_gbk":
                return b"x\xff"
            if mode == "cancel_body":
                closer = _attempt_socket_closer.get()
                assert closer is not None
                closer.close()
                return b"OK"
            return b"OK"

    original = PersistentFailoverPool("std", Api, hosts, 0.1)
    controller = AdaptiveConcurrencyController(
        max_threads=4, chunks_per_lease=1, threads_per_host_std=1
    )
    controller.update_hosts("std", hosts)
    pool = LeasedFailoverPool(original, controller, "std") if leased else original
    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    client.std_pool = pool
    client.pagination = {"company_info_chunk_size": 30720}
    client.market_rules = {"include_beijing_prefixes": ["92"]}
    client._stock_route = {
        "600000": {
            "code": "600000",
            "name": "测试",
            "market": 1,
            "source": "std",
            "asset_type": "stock",
        },
        "920001": {
            "code": "920001",
            "name": "北交测试",
            "market": 2,
            "source": "std",
            "asset_type": "stock",
        },
    }
    client._runtime_failures = []
    client.output = {"return_df_default": False}
    return client, pool, controller, calls


def release_pool(pool):
    """输入连接池，输出无；清理本测试线程许可后关闭连接，普通池直接关闭。"""
    if isinstance(pool, LeasedFailoverPool):
        pool.release_thread()
    pool.close()


@pytest.mark.parametrize("leased", [False, True])
def test_all_connections_failed_are_explicit(leased):
    """输入所有站建连失败，输出 connect_failed；禁止未发目录的空成功。"""
    client, pool, controller, calls = make_client(
        leased=leased, bad_connect=(HOST_A, HOST_B)
    )
    try:
        with pytest.raises(CompanyInfoFetchError) as caught:
            client.get_company_info_content("600000", return_df=False)
        assert caught.value.reason == "connect_failed"
        assert caught.value.retryable
        assert not any(item[0] == "directory" for item in calls)
        assert controller.snapshot()["inflight_threads"] == 0
        assert client._runtime_failures == []
    finally:
        release_pool(pool)


def test_available_second_host_is_used_without_waiting_for_first():
    """输入 A 满 B 空，输出 B 站完整正文；目录和正文保持 B，不等待 A。"""
    client, pool, controller, calls = make_client()
    held = controller.acquire("std", max_units=1, timeout=0, allowed_hosts={HOST_A})
    try:
        rows = client.get_company_info_content("600000", return_df=False)
        assert rows[0]["content"] == "OK"
        assert {item[1] for item in calls if item[0] in {"directory", "content"}} == {
            HOST_B
        }
    finally:
        controller.release(held, success=True)
        release_pool(pool)
    assert controller.snapshot()["inflight_threads"] == 0


def test_single_host_waits_for_delayed_quota_release():
    """输入单站配额在 0.3 秒释放，输出成功；不再以 0.25 秒空返回。"""
    client, pool, controller, _ = make_client(hosts=[HOST_A])
    held = controller.acquire("std", max_units=1, timeout=0, allowed_hosts={HOST_A})
    timer = threading.Timer(
        0.3, controller.release, args=(held,), kwargs={"success": True}
    )
    token = attempt_deadline.set(time.monotonic() + 2)
    timer.start()
    try:
        assert (
            client.get_company_info_content("600000", return_df=False)[0]["content"]
            == "OK"
        )
    finally:
        timer.join(2)
        controller.release(held, success=True)
        attempt_deadline.reset(token)
        release_pool(pool)
    assert controller.snapshot()["inflight_threads"] == 0


def test_quota_budget_expiry_leaves_no_waiter_or_extra_permit():
    """输入单站占满且预算耗尽，输出 attempt_timeout；等待票和额外许可为零。"""
    client, pool, controller, calls = make_client(hosts=[HOST_A])
    held = controller.acquire("std", max_units=1, timeout=0, allowed_hosts={HOST_A})
    token = attempt_deadline.set(time.monotonic() + 0.05)
    try:
        with pytest.raises(CompanyInfoFetchError) as caught:
            client.get_company_info_content("600000", return_df=False)
        assert caught.value.reason == "attempt_timeout"
        assert caught.value.retryable
        assert not calls
        assert controller.snapshot()["inflight_threads"] == 1
        assert controller._waiting_tickets == {}
    finally:
        controller.release(held, success=True)
        attempt_deadline.reset(token)
        release_pool(pool)


def test_quota_cancellation_cleans_waiting_ticket():
    """输入等待中取消，输出不可重试 cancelled；不建连、不泄漏队列票据。"""
    client, pool, controller, calls = make_client(hosts=[HOST_A])
    held = controller.acquire("std", max_units=1, timeout=0, allowed_hosts={HOST_A})
    closer = AttemptSocketCloser()
    token = _attempt_socket_closer.set(closer)
    timer = threading.Timer(0.02, closer.close)
    timer.start()
    try:
        with pytest.raises(CompanyInfoFetchError) as caught:
            client.get_company_info_content("600000", return_df=False)
        assert caught.value.reason == "cancelled"
        assert not caught.value.retryable
        assert not calls and controller._waiting_tickets == {}
    finally:
        timer.join(1)
        _attempt_socket_closer.reset(token)
        controller.release(held, success=True)
        release_pool(pool)


def test_beijing_does_not_take_available_szsh_only_host():
    """输入北交站满、深沪站空，输出预算失败；禁止越过能力约束访问深沪站。"""
    client, pool, controller, calls = make_client()
    pool.configure_host_groups({"szshbj": [HOST_A], "szsh": [HOST_A, HOST_B]})
    held = controller.acquire("std", max_units=1, timeout=0, allowed_hosts={HOST_A})
    token = attempt_deadline.set(time.monotonic() + 0.05)
    try:
        with pytest.raises(CompanyInfoFetchError) as caught:
            client.get_company_info_content("920001", return_df=False)
        assert caught.value.reason == "attempt_timeout"
        assert not calls
    finally:
        controller.release(held, success=True)
        attempt_deadline.reset(token)
        release_pool(pool)


def test_partial_text_is_not_complete_success():
    """输入所有站短页，输出明确失败；不暴露截断正文行。"""
    client, pool, controller, calls = make_client(mode="short")
    try:
        with pytest.raises(CompanyInfoFetchError) as caught:
            client.get_company_info_content("600000", return_df=False)
        assert caught.value.reason == "content_failed"
        assert len([item for item in calls if item[0] == "directory"]) == 2
        assert client._runtime_failures == []
        assert controller.snapshot()["inflight_threads"] == 0
    finally:
        release_pool(pool)


def test_lossy_gbk_text_is_accepted_without_retry():
    """输入长度完整但含非法 GBK 的正文，输出 ignore 后文本且不换站。"""
    client, pool, controller, calls = make_client(mode="bad_gbk")
    try:
        assert client.get_company_info_content("600000", return_df=False) == [
            {"code": "600000", "category": "栏目0", "content": "x"}
        ]
        assert len([item for item in calls if item[0] == "directory"]) == 1
        assert client._runtime_failures == []
        assert controller.snapshot()["inflight_threads"] == 0
    finally:
        release_pool(pool)


@pytest.mark.parametrize(
    "mode, category", [("empty_directory", None), ("ok", ["不存在栏目"])]
)
def test_real_empty_directory_or_absent_candidates_remain_empty(mode, category):
    """输入真实空目录或候选栏目不存在，输出合法空列表；不强制栏目数量。"""
    client, pool, _, calls = make_client(mode=mode)
    try:
        assert (
            client.get_company_info_content(
                "600000", category=category, return_df=False
            )
            == []
        )
        assert len([item for item in calls if item[0] == "directory"]) == 1
        assert not any(item[0] == "content" for item in calls)
        assert client._runtime_failures == []
    finally:
        release_pool(pool)


def test_concurrent_categories_with_single_permit_complete_and_release():
    """输入同站两栏目并发但许可为一，输出全部正文；目录先让许可，子线程归还。"""
    client, pool, controller, calls = make_client(hosts=[HOST_A], categories=2)
    token = attempt_deadline.set(time.monotonic() + 1)
    try:
        rows = client.get_company_info_content(
            "600000", category_workers=2, return_df=False
        )
        assert [row["content"] for row in rows] == ["OK", "OK"]
        assert {item[1] for item in calls if item[0] in {"directory", "content"}} == {
            HOST_A
        }
        assert controller.snapshot()["inflight_threads"] == 0
    finally:
        attempt_deadline.reset(token)
        release_pool(pool)


def test_concurrent_categories_inherit_cancellation_and_release():
    """输入正文线程触发共享取消，输出 cancelled；子线程继承上下文且归还许可。"""
    client, pool, controller, _ = make_client(
        hosts=[HOST_A], categories=2, mode="cancel_body"
    )
    closer = AttemptSocketCloser()
    token = _attempt_socket_closer.set(closer)
    clock_token = attempt_deadline.set(time.monotonic() + 1)
    try:
        with pytest.raises(CompanyInfoFetchError) as caught:
            client.get_company_info_content(
                "600000", category_workers=2, return_df=False
            )
        assert caught.value.reason == "cancelled"
        assert controller.snapshot()["inflight_threads"] == 0
        assert controller._waiting_tickets == {}
    finally:
        attempt_deadline.reset(clock_token)
        _attempt_socket_closer.reset(token)
        release_pool(pool)


def test_body_reconnect_failure_reloads_directory_on_next_host(monkeypatch):
    """输入首站目录成功但正文重连失败，输出次站重取目录的全文；预算取消不在此模拟。"""
    client, pool, controller, calls = make_client(
        categories=2, mode="fail_body_connect_first"
    )

    def discard_directory_connection(*, success=True):
        """输入回收结果，输出无；模拟目录连接已不可复用，让正文线程必须重新建连。"""
        pool.release_thread(success=False, discard=True)

    monkeypatch.setattr(
        pool, "release_company_info_thread", discard_directory_connection
    )
    token = attempt_deadline.set(time.monotonic() + 1)
    try:
        rows = client.get_company_info_content(
            "600000", category_workers=2, return_df=False
        )
        assert [row["content"] for row in rows] == ["OK", "OK"]
        assert [item[1] for item in calls if item[0] == "directory"] == [HOST_A, HOST_B]
        assert {item[1] for item in calls if item[0] == "content"} == {HOST_B}
        assert controller.snapshot()["inflight_threads"] == 0
    finally:
        attempt_deadline.reset(token)
        release_pool(pool)
