"""用途：离线验证公司信息逐股重试、预算、取消及最终对账。

输入受控客户端和真实线程运行时；输出 pytest 断言结果。
边界：不创建行情 socket，不修改证券缓存；主连接与并行路径均覆盖。
"""

import json
import queue
import threading
import time
from collections import Counter
from concurrent.futures import CancelledError
from types import SimpleNamespace

import pytest

from zsdtdx.biz import company_info as biz
from zsdtdx.engine import parallel_fetcher as pf
from zsdtdx.engine import unified_client as uc
from zsdtdx.engine.thread_runtime import ThreadRuntime, attempt_deadline
from zsdtdx.util.errors import CompanyInfoFetchError


def body(code):
    """输入代码，输出一条完整正文；用于断言行结构保持不变，无网络访问。"""
    return [{"code": code, "category": "公司概况", "content": "正文"}]


class FakePool:
    """输入无，提供主池和租约池替身；只记录调用，不接触网络或全局上下文。"""

    def __init__(self):
        """输入无，输出记录容器；调用记录用于核验回收、读超时还原。"""
        self.begins = 0
        self.releases = []
        self.timeouts = []
        self.restores = 0
        self.resets = 0
        self.data = {
            "last_lease_at": 0,
            "socket_read_deadline": None,
            "socket_read_timeout_override": None,
        }

    def begin_attempt(self):
        """输入无，输出无；记录租约尝试次数，不创建连接。"""
        self.begins += 1

    def _get_thread_data(self):
        """输入无，输出固定最后租约时刻；只满足回收选择，不持有 socket。"""
        return self.data

    def release_thread(self, **kwargs):
        """输入回收标志，输出无；保存标志供断言，始终无真实资源。"""
        self.releases.append(kwargs)

    def set_thread_socket_read_timeout(self, timeout):
        """输入预算，输出无；记录主池尝试预算，不修改系统 socket。"""
        self.timeouts.append(timeout)
        self.data["socket_read_deadline"] = time.monotonic() + timeout
        self.data["socket_read_timeout_override"] = timeout

    def restore_thread_socket_read_timeout(self):
        """输入无，输出无；统计主池预算还原次数，便于检查 finally。"""
        self.restores += 1
        self.data["socket_read_deadline"] = None
        self.data["socket_read_timeout_override"] = None

    def reset_thread_connection(self, reconnect=False):
        """输入是否重连，输出无；异常只清当前连接，禁止这里直接重连。"""
        assert reconnect is False
        self.resets += 1


class FakeClient:
    """输入逐股动作列表，输出可重复取数的客户端替身；动作耗尽后重复最后动作。"""

    def __init__(self, actions, retries=2, timeout=0.2):
        """输入动作及预算，输出替身客户端；仅内存计数，无网络或数据落盘。"""
        self.actions = actions
        self.calls = Counter()
        self.lock = threading.Lock()
        self.std_pool = FakePool()
        self.ex_pool = FakePool()
        self.failures = []
        self.config = {
            "parallel": {
                "chunk_retry_max_attempts": retries,
                "chunk_timeout_seconds": timeout,
            }
        }

    def prepare_company_info_routes(self):
        """本替身不解析路由；批次准备不应消费单股重试或占用工作线程。"""

    def get_company_info_content(self, code, **kwargs):
        """输入代码和栏目，输出受控正文或抛错；动作调用始终在当前工作线程。"""
        with self.lock:
            index = self.calls[code]
            self.calls[code] += 1
        actions = self.actions.get(code, [body(code)])
        action = actions[min(index, len(actions) - 1)]
        if isinstance(action, BaseException):
            raise action
        return action() if callable(action) else action

    def record_runtime_failure(self, task, code, reason, detail):
        """输入最终失败，输出无；只保存摘要供断言，不参与是否重试的决策。"""
        self.failures.append(
            {"task": task, "code": code, "reason": reason, "detail": detail}
        )

    def _default_return_df(self, value):
        """输入返回开关，输出布尔值；None 默认为列表，避免测试无关配置。"""
        return bool(value)


@pytest.fixture
def install_parallel(monkeypatch):
    """输入 monkeypatch，输出替身安装函数；结束时关闭所有真实线程运行时。"""
    runtimes = []

    def install(client, chunk_size=2):
        """输入客户端和分片大小，输出真实运行时；探测及预热均替换为空操作。"""
        runtime = ThreadRuntime(2, 2)
        runtimes.append(runtime)
        config = client.config["parallel"]
        fetcher = SimpleNamespace(
            chunk_retry_max_attempts=config["chunk_retry_max_attempts"],
            chunk_timeout_seconds=config["chunk_timeout_seconds"],
            auto_prewarm_on_async=False,
            company_info_codes_per_chunk=chunk_size,
            company_info_max_inflight_multiplier=2,
            num_threads=2,
        )
        monkeypatch.setattr(pf, "get_fetcher", lambda: fetcher)
        monkeypatch.setattr(pf, "_ensure_shared_client_context", lambda: client)
        monkeypatch.setattr(pf, "_shared_client_context", client)
        monkeypatch.setattr(pf, "_get_global_thread_runtime", lambda n: runtime)
        monkeypatch.setattr(pf, "_refresh_thread_hosts", lambda sources: None)
        monkeypatch.setattr(
            uc, "_ensure_availability_hosts_cache", lambda **kwargs: None
        )
        return runtime

    yield install
    for runtime in runtimes:
        assert runtime.stop(timeout=2.0) == 0


def events(result_queue):
    """输入已完成队列，输出事件列表；只非阻塞消费，不等待额外消息。"""
    output = []
    while not result_queue.empty():
        output.append(result_queue.get_nowait())
    return output


def assert_accounted(messages, total, success):
    """输入终态事件和预期数，输出无；逐股唯一且 done 与 data 同一口径。"""
    data = [item for item in messages if item["event"] == "data"]
    done = messages[-1]
    assert len(data) == total == len({item["code"] for item in data})
    assert done == {
        "event": "done",
        "total_codes": total,
        "success_codes": success,
        "failed_codes": total - success,
    }
    assert sum(item["error"] is None for item in data) == success


def test_retry_only_failed_stock_and_normalized_unique_result(install_parallel):
    """输入同分片一成功一暂时失败及重复代码，输出每股一次结果且只失败股重试。"""
    client = FakeClient(
        {
            "600000": [body("600000")],
            "000001": [
                CompanyInfoFetchError("配额暂满", reason="quota_timeout"),
                body("000001"),
            ],
        }
    )
    install_parallel(client)
    result_queue = queue.Queue()
    rows = pf.fetch_company_info_parallel(
        [" sh.600000 ", "600000", "000001", "sz.000001"], queue=result_queue
    )
    assert client.calls == {"600000": 1, "000001": 2}
    assert rows == body("600000") + body("000001")
    messages = events(result_queue)
    assert_accounted(messages, 2, 2)
    assert all(
        set(item) == {"event", "code", "rows", "error"} for item in messages[:-1]
    )
    assert client.failures == []
    assert len(client.std_pool.releases) == 3
    assert sum(item["discard"] for item in client.std_pool.releases) == 1


@pytest.mark.parametrize("as_job", [False, True])
def test_empty_and_final_failure_have_one_terminal_event(install_parallel, as_job):
    """输入空目录、成功和持续失败，输出同一队列口径；异步 result 为计数，同步仍返回正文。"""
    client = FakeClient(
        {
            "600000": [[]],
            "000001": [
                CompanyInfoFetchError(
                    "正文未读完",
                    reason="content_failed",
                    detail={"statuses": ["short_page_terminated"]},
                )
            ],
        }
    )
    install_parallel(client)
    result_queue = queue.Queue()
    fn = pf.fetch_company_info_async if as_job else pf.fetch_company_info_parallel
    result = fn(["600000", "000001", "920128"], queue=result_queue)
    if as_job:
        assert result.result(timeout=3) == {
            "total_codes": 3,
            "success_codes": 1,
            "failed_codes": 2,
        }
    else:
        assert result == body("920128")
    assert client.calls == {"600000": 1, "000001": 3, "920128": 1}
    messages = events(result_queue)
    assert_accounted(messages, 3, 1)
    by_code = {item["code"]: item for item in messages[:-1]}
    assert by_code["920128"]["rows"] == body("920128")
    assert by_code["600000"]["status"] == "no_data"
    assert by_code["600000"]["reason"] == "no_data"
    assert by_code["000001"]["reason"] == "content_failed"
    assert by_code["000001"]["detail"] == {"statuses": ["short_page_terminated"]}
    assert Counter(item["reason"] for item in client.failures) == {
        "no_data": 1,
        "content_failed": 1,
    }
    failed_record = next(
        item for item in client.failures if item["reason"] == "content_failed"
    )
    assert json.loads(failed_record["detail"])["detail"] == {
        "statuses": ["short_page_terminated"]
    }


def test_sync_snapshot_failure_preserves_callers_deadline():
    """输入读取主池状态就失败的替身，输出异常且调用前预算不泄漏；无协议请求。"""
    client = FakeClient({})

    def broken_snapshot():
        """输入无，输出异常；模拟池状态读取故障，不修改连接或全局状态。"""
        raise RuntimeError("snapshot unavailable")

    client.std_pool._get_thread_data = broken_snapshot
    token = attempt_deadline.set(time.monotonic() + 60)
    previous = attempt_deadline.get()
    try:
        with pytest.raises(RuntimeError, match="snapshot unavailable"):
            biz._sync_company_info_attempt(client, "600000", None, 0.2)
        assert attempt_deadline.get() == previous
        assert client.calls == {}
    finally:
        attempt_deadline.reset(token)


@pytest.mark.parametrize(
    "error",
    [
        CompanyInfoFetchError("不可重试", reason="unsupported", retryable=False),
        CompanyInfoFetchError("已经取消", reason="cancelled", retryable=False),
        ValueError("股票不存在"),
        CancelledError(),
    ],
)
def test_permanent_and_cancelled_errors_do_not_retry(install_parallel, error):
    """输入不可重试或取消，输出一次调用即终态；取消不会耗尽全部重试次数。"""
    client = FakeClient({"600000": [error]})
    install_parallel(client)
    result_queue = queue.Queue()
    assert pf.fetch_company_info_parallel(["600000"], queue=result_queue) == []
    assert client.calls["600000"] == 1
    messages = events(result_queue)
    assert_accounted(messages, 1, 0)
    if (
        isinstance(error, CancelledError)
        or getattr(error, "reason", None) == "cancelled"
    ):
        assert messages[0]["status"] == "cancelled"


def test_runtime_stop_marks_remaining_codes_cancelled_without_requests(
    install_parallel,
):
    """输入执行中停机，输出剩余股取消终态，已停运行时不再启动协议请求。"""
    client = FakeClient({})
    runtime = install_parallel(client)

    def stop():
        """输入无，输出正文并设置停止；模拟正在取数时收到取消，不创建 socket。"""
        runtime.stopped.set()
        return body("600000")

    client.actions["600000"] = [stop]
    result_queue = queue.Queue()
    assert (
        pf.fetch_company_info_parallel(["600000", "000001"], queue=result_queue) == []
    )
    assert client.calls == {"600000": 1}
    messages = events(result_queue)
    assert_accounted(messages, 2, 0)
    assert {item["status"] for item in messages[:-1]} == {"cancelled"}
    assert len(client.std_pool.releases) == 1


def test_runtime_stop_socket_timeout_is_reported_as_cancelled(install_parallel):
    """输入停机关闭 socket 后冒出的超时，输出取消终态而非网络超时。"""
    client = FakeClient({"600000": [body("600000")]})
    runtime = install_parallel(client)

    def stopped_timeout(_fn, _timeout, _payload):
        """设置停机并模拟被关闭 socket 抛出的 watchdog 超时。"""
        runtime.stopped.set()
        raise TimeoutError("chunk attempt timeout")

    runtime.attempt = stopped_timeout
    result_queue = queue.Queue()
    assert pf.fetch_company_info_parallel(["600000"], queue=result_queue) == []
    messages = events(result_queue)
    assert_accounted(messages, 1, 0)
    assert messages[0]["status"] == "cancelled"
    assert messages[0]["reason"] == "cancelled"


def test_each_stock_retry_receives_fresh_deadline(install_parallel):
    """输入每次都超预算的受控正文，输出三次独立预算和明确超时失败。"""
    deadlines = []

    def slow():
        """输入无，输出迟到正文；记录实际预算，睡眠仅用于触发真实 watchdog。"""
        deadlines.append(attempt_deadline.get())
        time.sleep(0.02)
        return body("600000")

    client = FakeClient({"600000": [slow]}, timeout=0.005)
    install_parallel(client)
    result_queue = queue.Queue()
    assert pf.fetch_company_info_parallel(["600000"], queue=result_queue) == []
    assert client.calls["600000"] == len(deadlines) == 3
    assert deadlines[0] < deadlines[1] < deadlines[2]
    messages = events(result_queue)
    assert_accounted(messages, 1, 0)
    assert messages[0]["reason"] == "timeout"
    assert all(item["discard"] for item in client.std_pool.releases)


def test_aggregate_missing_and_duplicate_results_are_accounted(monkeypatch):
    """输入遗漏或重复的内部结果，输出唯一终态且遗漏被明确计失败，禁止静默少报。"""
    result_queue = queue.Queue()
    result = {"outcomes": [biz.company_info_outcome("600000", rows=body("600000"))]}
    monkeypatch.setattr(
        pf, "_iter_company_info_chunk_results", lambda **kwargs: iter([result, result])
    )
    monkeypatch.setattr(pf, "_shared_client_context", None)
    rows = pf.fetch_company_info_parallel(["600000", "000001"], queue=result_queue)
    assert rows == body("600000")
    messages = events(result_queue)
    assert_accounted(messages, 2, 1)
    assert messages[1]["reason"] == "missing_result"


@pytest.mark.parametrize("return_df", [False, True])
def test_sync_main_context_retries_and_restores_budget(monkeypatch, return_df):
    """输入主客户端及暂时失败/空结果，输出对账与原返回类型，预算和上下文均还原。"""
    client = FakeClient(
        {
            "600000": [
                CompanyInfoFetchError("配额暂满", reason="quota_timeout"),
                body("600000"),
            ],
            "000001": [[]],
        }
    )
    monkeypatch.setattr(
        uc.UnifiedTdxClient, "get_active_context_client", staticmethod(lambda: client)
    )
    token = attempt_deadline.set(time.monotonic() + 60)
    previous = attempt_deadline.get()
    client.std_pool.data.update(
        {"socket_read_deadline": previous, "socket_read_timeout_override": 60.0}
    )
    prior_pool_data = dict(client.std_pool.data)
    result_queue = queue.Queue()
    try:
        rows = biz.fetch_company_info(
            ["sh.600000", "600000", "000001"],
            mode="sync",
            queue=result_queue,
            return_df=return_df,
        )
        assert (rows.to_dict("records") if return_df else rows) == body("600000")
        assert attempt_deadline.get() == previous
        assert client.calls == {"600000": 2, "000001": 1}
        assert client.std_pool.restores == len(client.std_pool.timeouts) == 3
        assert client.std_pool.resets == 1
        assert client.std_pool.data == prior_pool_data
        assert_accounted(events(result_queue), 2, 1)
        assert biz.fetch_company_info(["600000"], mode="sync", return_df=False) == body(
            "600000"
        )
        assert uc.UnifiedTdxClient.get_active_context_client() is client
    finally:
        attempt_deadline.reset(token)


def test_sync_timeout_and_cancellation_are_bounded(monkeypatch):
    """输入主池超时及取消，输出有限重试、取消不重试，finally 清预算且后续仍可调用。"""

    def slow():
        """输入无，输出迟到正文；仅睡眠触发同步协作预算，不创建线程。"""
        time.sleep(0.03)
        return body("600000")

    client = FakeClient(
        {
            "600000": [slow],
            "000001": [
                CompanyInfoFetchError("已取消", reason="cancelled", retryable=False)
            ],
        },
        timeout=0.005,
    )
    monkeypatch.setattr(
        uc.UnifiedTdxClient, "get_active_context_client", staticmethod(lambda: client)
    )
    previous = attempt_deadline.get()
    result_queue = queue.Queue()
    assert (
        biz.fetch_company_info(["600000", "000001"], mode="sync", queue=result_queue)
        == []
    )
    assert client.calls == {"600000": 3, "000001": 1}
    assert client.std_pool.restores == client.std_pool.resets == 4
    assert attempt_deadline.get() == previous
    messages = events(result_queue)
    assert_accounted(messages, 2, 0)
    assert messages[0]["reason"] == "timeout"
    assert messages[1]["status"] == "cancelled"
