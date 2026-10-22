"""公司资料冷启动和错位正文回归；使用真实调度和内存行情站，不访问网络。"""

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from zsdtdx.engine.unified_client import UnifiedTdxClient
from zsdtdx.util.errors import CompanyInfoFetchError
from tests.test_company_info_admission_regression import make_client, release_pool
from tests.test_company_info_accounting_regression import (
    FakeClient,
    events,
    assert_accounted,
    install_parallel as _install_parallel_fixture,
)
from zsdtdx.engine import parallel_fetcher as pf
from zsdtdx.engine.thread_runtime import attempt_deadline
import queue


@pytest.fixture
def install_parallel(monkeypatch):
    """复用记账回归的真实线程夹具，独立注册以支持单文件及全量收集。"""
    yield from _install_parallel_fixture.__wrapped__(monkeypatch)


@pytest.mark.parametrize(
    "text, valid",
    [
        ("☆财务分析☆ ◇601718 ST际华 更新日期：2026-10-21◇\r\n正文", True),
        ("\ufeff\r\n ☆ 财务分析 ☆ ◇601718 测试◇\n正文", True),
        ("☆财务分析☆ ◇601319 中国人保◇\n正文", False),
        ("☆经营分析☆ ◇601718 ST际华◇\n正文", False),
        ("经营分析中段 601718 财务分析\n正文", False),
        ("中段正文\n☆财务分析☆ ◇601718 ST际华◇", False),
        ("☆财务分析☆ ◇6017180 测试◇", False),
    ],
)
def test_identity_is_the_leading_title(text, valid):
    """标题必须在正文起点且代码、栏目同时匹配，不能靠全文包含关键字蒙混通过。"""
    assert (
        UnifiedTdxClient._company_info_identity_matches(text, "601718", "财务分析")
        is valid
    )


def test_sixty_cold_callers_share_one_route_load():
    """60 个同时冷启动的单股入口只加载一次，绝不使用 refresh=True 绕过磁盘缓存。"""
    client, pool, _, _ = make_client(leased=False)
    client._stock_route = {}
    calls = []
    barrier = threading.Barrier(60)

    def load(refresh=False, route_source=None):
        calls.append((refresh, route_source))
        time.sleep(0.03)
        client._stock_route = {"600000": {"code": "600000", "source": "std"}}

    client._ensure_stock_route_cache_no_df = load

    def resolve(_):
        barrier.wait(timeout=5)
        return client._company_info_route("600000")

    try:
        with ThreadPoolExecutor(max_workers=60) as executor:
            routes = list(executor.map(resolve, range(60)))
        assert len(routes) == 60 and all(route["code"] == "600000" for route in routes)
        assert calls == [(False, "std")]
    finally:
        release_pool(pool)


def test_route_wait_obeys_attempt_budget():
    """初始化在其它线程进行时，单股等待预算耗尽必须退出，不能无期限等锁。"""
    client, pool, _, _ = make_client(leased=False)
    client._stock_route = {}
    lock = client._company_info_route_lock
    lock.acquire()
    token = attempt_deadline.set(time.monotonic() + 0.01)
    try:
        with pytest.raises(CompanyInfoFetchError) as caught:
            client._company_info_route("600000")
        assert caught.value.reason == "attempt_timeout"
    finally:
        lock.release()
        attempt_deadline.reset(token)
        release_pool(pool)


@pytest.mark.parametrize(
    "lost_before", ["get_company_info_category", "get_company_info_content"]
)
def test_lost_bound_connection_cannot_reacquire_another_host(monkeypatch, lost_before):
    """目录或正文 IO 前失去连接时，自动取租约仍必须钉在已选站，禁止静默跳站。"""
    client, pool, _, calls = make_client()
    original_call = pool.call_current_host
    lost = False

    def interrupted_call(method, *args, **kwargs):
        nonlocal lost
        if method == lost_before and not lost:
            lost = True
            pool.release_thread(success=False, discard=True)
        return original_call(method, *args, **kwargs)

    monkeypatch.setattr(pool, "call_current_host", interrupted_call)
    try:
        assert len(client.get_company_info_content("600000", return_df=False)) == 1
        actual_hosts = [row[1] for row in calls if row[0] in {"directory", "content"}]
        assert actual_hosts == [("offline-a", 7709), ("offline-a", 7709)]
    finally:
        release_pool(pool)


@pytest.mark.parametrize("reason", ["connect_failed", "cancelled"])
def test_identity_retry_connection_failure_or_cancellation(monkeypatch, reason):
    """同站重取无法建连时继续下一站；取消则保留取消身份，不能改报正文失败。"""
    client, pool, controller, _ = make_client()
    first = ("offline-a", 7709)
    original_connect = pool.connect_company_info_host

    def connect(candidates):
        if list(candidates) == [first]:
            raise CompanyInfoFetchError(
                "同站重取失败", reason=reason, retryable=reason != "cancelled"
            )
        return original_connect(candidates)

    def content(**kwargs):
        if kwargs["bind_host"] == first:
            return "中段正文", "success"
        return "☆栏目0☆ ◇600000 测试◇\r\n恢复", "success"

    monkeypatch.setattr(pool, "connect_company_info_host", connect)
    monkeypatch.setattr(client, "_fetch_company_content", content)
    try:
        if reason == "cancelled":
            with pytest.raises(CompanyInfoFetchError) as caught:
                client.get_company_info_content("600000", return_df=False)
            assert caught.value.reason == "cancelled"
        else:
            rows = client.get_company_info_content("600000", return_df=False)
            assert rows[0]["content"].endswith("恢复")
        assert controller.snapshot()["inflight_threads"] == 0
    finally:
        release_pool(pool)


@pytest.mark.parametrize("fail_prepare", [False, True])
def test_public_batch_prepares_before_submitting_any_stock(
    install_parallel,
    fail_prepare,
):
    """真实线程批路径在提交前准备一次；失败时无单股请求且全部代码都有失败终态。"""

    class Client(FakeClient):
        prepared = False
        prepares = 0

        def prepare_company_info_routes(self):
            self.prepares += 1
            if fail_prepare:
                raise RuntimeError("目录初始化失败")
            self.prepared = True

        def get_company_info_content(self, code, **kwargs):
            assert self.prepared
            return super().get_company_info_content(code, **kwargs)

    client = Client({})
    install_parallel(client)
    result_queue = queue.Queue()
    rows = pf.fetch_company_info_parallel(["600000", "000001"], queue=result_queue)
    assert client.prepares == 1
    assert_accounted(events(result_queue), 2, 0 if fail_prepare else 2)
    assert len(rows) == (0 if fail_prepare else 2)
    assert len(client.calls) == (0 if fail_prepare else 2)


@pytest.mark.parametrize("recover", ["same_host", "next_host", "never"])
@pytest.mark.parametrize("workers", [1, 2])
def test_misaligned_body_reloads_directory_then_fails_over(recover, workers):
    """错位全文不得交付；同站新目录恢复优先，其后换站，耗尽必须明确失败。"""
    client, pool, controller, calls = make_client(categories=2)
    directory_reads = {}
    original_factory = pool.api_cls

    def factory(*args, **kwargs):
        api = original_factory(*args, **kwargs)
        original_directory = api.get_company_info_category

        def directory(market, code):
            directory_reads[api.host] = directory_reads.get(api.host, 0) + 1
            rows = original_directory(market, code)
            for row in rows:
                row["start"] = 1000 * directory_reads[api.host] + row["index"] * 100
                row["length"] = len(
                    f"☆栏目{row['index']}☆ ◇{code} 测试◇\r\n正文".encode("gbk")
                )
            return rows

        def content(market, code, filename, start, length, index):
            calls.append(("identity_content", api.host, start))
            good = (recover == "same_host" and directory_reads[api.host] >= 2) or (
                recover == "next_host" and api.host[0] == "offline-b"
            )
            if good:
                return f"☆栏目{index}☆ ◇{code} 测试◇\r\n正文".encode("gbk")
            return b"x" * length

        api.get_company_info_category = directory
        api.get_company_info_content = content
        return api

    pool.api_cls = factory
    try:
        if recover == "never":
            with pytest.raises(CompanyInfoFetchError) as caught:
                client.get_company_info_content(
                    "600000", category_workers=workers, return_df=False
                )
            assert caught.value.reason == "content_failed"
            assert "identity_mismatch" in caught.value.detail["statuses"]
            assert sorted(directory_reads.values()) == [2, 2]
        else:
            rows = client.get_company_info_content(
                "600000", category_workers=workers, return_df=False
            )
            assert len(rows) == 2 and all("◇600000" in row["content"] for row in rows)
            assert directory_reads[("offline-a", 7709)] == 2
            if recover == "same_host":
                assert len(directory_reads) == 1
        assert controller.snapshot()["inflight_threads"] == 0
    finally:
        release_pool(pool)
