"""线程与列式链路的独立验收：数值、任务完整性、配额、取消和资源归还。"""

import datetime as dt
import socket
import struct
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pytest

from zsdtdx.engine.adaptive_scheduler import AdaptiveConcurrencyController
from zsdtdx.engine.thread_runtime import LeasedFailoverPool, ThreadRuntime
from zsdtdx.engine.unified_client import PersistentFailoverPool, _bind_attempt_socket
from zsdtdx.parser.kline_columns import (
    KlineColumns,
    clock_text,
    merge_pages,
    parse_extended_columns,
    parse_standard_columns,
    timestamps_for,
)
from zsdtdx.util.helper import get_volume


def encode_price(value):
    """输入有符号价差，输出标准变长整数；用于独立回包夹具。"""
    negative, value = value < 0, abs(value)
    first, value = value & 63, value >> 6
    output = bytearray([first | (64 if negative else 0) | (128 if value else 0)])
    while value:
        part, value = value & 127, value >> 7
        output.append(part | (128 if value else 0))
    return bytes(output)


def standard_body(category=4, counts=False):
    """输入周期和家数标志，输出含正负价差及量能边界的固定页。"""
    minute = category < 4 or category in (7, 8)
    body = bytearray(struct.pack("<H", 3))
    for i in range(3):
        day = ((2023 - 2004) << 11) + 100 + 3 + i
        body += (
            struct.pack("<HH", day, 570 + i)
            if minute
            else struct.pack("<I", 20230103 + i)
        )
        for price in (10001 if i == 0 else -110, 24, 53, -19):
            body += encode_price(price)
        body += struct.pack("<II", [0, 0x40800000, 0x50812345][i], 0x50112233)
        if counts:
            body += struct.pack("<HH", 19 + i, 7 + i)
    return bytes(body)


@pytest.mark.parametrize("category", range(12))
@pytest.mark.parametrize(
    "counts,absolute", [(False, False), (True, False), (True, True)]
)
def test_standard_decoder_matches_reference(category, counts, absolute):
    """所有周期、价格分支、量能及时间与独立 Python 计算按位一致。"""
    page = parse_standard_columns(
        standard_body(category, counts),
        category,
        with_index_counts=counts,
        absolute_ohlc=absolute,
    )
    rows, previous = page.raw_rows(), 0
    for i, row in enumerate(rows):
        po, pc, ph, pl = 10001 if i == 0 else -110, 24, 53, -19
        base = previous + po
        expected = (
            [po / 1000, pc / 1000, ph / 1000, pl / 1000]
            if absolute
            else [
                base / 1000,
                (base + pc) / 1000,
                (base + ph) / 1000,
                (base + pl) / 1000,
            ]
        )
        previous = base + pc
        for name, value in zip(("open", "close", "high", "low"), np.round(expected, 2)):
            assert struct.pack("d", row[name]) == struct.pack("d", value)
        assert row["vol"] == int(np.rint(get_volume([0, 0x40800000, 0x50812345][i])))
        assert row["amount"] == int(np.rint(get_volume(0x50112233)))
        expected_time = (
            dt.datetime(2023, 1, 3 + i, 9, 30 + i)
            if category < 4 or category in (7, 8)
            else dt.datetime(2023, 1, 3 + i, 15)
        )
        assert row["datetime"] == expected_time.strftime("%Y-%m-%d %H:%M:%S")
        assert row["_ts"] == int(expected_time.timestamp())
        if counts:
            assert (row["up_count"], row["down_count"]) == (19 + i, 7 + i)


def test_numba_volume_matches_ieee_float32_random():
    """0x052D 量额按 IEEE float32 位型解码，覆盖普通数、零与次正规数。"""
    from zsdtdx.parser._kline_numba import volume_value

    rng = np.random.default_rng(42)
    values = list(rng.integers(0, 0x7F800000, 10003, dtype=np.uint64))
    values += [0, 1, 0x3F800000, 0x41400000, 0x42480000, 0x426C0000]
    for value in values:
        raw = int(value).to_bytes(4, "little")
        expected = struct.unpack("<f", raw)[0]
        assert struct.pack("d", volume_value(int(value))) == struct.pack("d", expected)


@pytest.mark.parametrize(
    "raw_hex, expected, pytdx_style_wrong",
    [
        ("00004041", 12.0, 1032.0),
        ("00004842", 50.0, 320.0),
        ("00006c42", 59.0, 464.0),
    ],
)
def test_standard_decoder_fixes_real_low_weekly_volume(
    raw_hex, expected, pytdx_style_wrong
):
    """真实低量周线原码必须按 float32 解码，不能进入 pytdx 负指数旧分支。"""
    from zsdtdx.parser._kline_numba import volume_value

    raw = int.from_bytes(bytes.fromhex(raw_hex), "little")
    assert volume_value(raw) == expected
    assert get_volume(raw) == pytdx_style_wrong


def test_standard_decoder_fixes_real_hleax_0x80_volume_boundary():
    """输入 511920.SH 实盘量能原码 0000803f；输出 1，避免旧算法误解成 32768。"""
    from zsdtdx.parser._kline_numba import volume_value

    raw = int.from_bytes(bytes.fromhex("0000803f"), "little")
    assert raw == 0x3F800000
    assert get_volume(raw) == 1.0
    assert volume_value(raw) == 1.0
    body = bytearray(struct.pack("<HI", 1, 20261014))
    for price in (99998, 0, 0, 0):
        body += encode_price(price)
    body += struct.pack("<II", raw, 0x42C80000)
    row = parse_standard_columns(bytes(body), 4).raw_rows()[0]
    assert row["vol"] == 1


@pytest.mark.parametrize(
    "body", [b"", b"\x01", b"\x01\x00", b"\xff\xff", b"\x01\x00" + b"\x80" * 100]
)
def test_malformed_standard_fails_bounded(body):
    """坏记录不得越界、静默产生数据或无界循环。"""
    with pytest.raises(ValueError):
        parse_standard_columns(body, 4)


def test_extended_layout_and_truncation():
    """扩展记录的成交额、持仓、结算价与原布局一致，截断明确失败。"""
    prefix = bytearray(42)
    struct.pack_into("<H", prefix, 40, 1)
    record = struct.pack(
        "<IfffffIf", 20230103, 5.325, 5.39, 5.30, 5.34, -5138.0, 40, 5.31
    )
    page = parse_extended_columns(bytes(prefix) + record, 4)
    row = page.raw_rows()[0]
    assert row["trade"] == 40 and row["amount"] == -5138
    assert row["open"] == float(np.round(float(np.float32(5.325)), 2))
    assert row["price"] == float(np.round(float(np.float32(5.31)), 2))
    with pytest.raises(ValueError):
        parse_extended_columns(bytes(prefix) + record[:-1], 4)


def test_readonly_slice_outlives_cache_and_dedup_prefers_newest():
    """窗口引用在原页释放后有效，跨页重复保留先抓取的最新页。"""
    page = parse_standard_columns(standard_body(), 4)
    old = KlineColumns(
        {k: (v - 1 if k == "close" else v) for k, v in page.columns.items()}
    )
    merged = merge_pages([page, old])
    selected = merged.take(slice(1, 3))
    expected = page.columns["close"][1:].copy()
    del page, old, merged
    np.testing.assert_array_equal(selected.columns["close"], expected)
    with pytest.raises(ValueError):
        selected.columns["close"][0] = 0


@pytest.mark.parametrize(
    "clock", [198805011500, 199109151500, 200002291500, 202301030930]
)
def test_timezone_matches_datetime_timestamp(clock):
    """时间轴保持本机历史时区规则，不写死 UTC+8。"""
    value = dt.datetime.strptime(clock_text(clock), "%Y-%m-%d %H:%M:%S")
    assert timestamps_for(np.array([clock], np.int64))[0] == int(value.timestamp())


def test_submit_failure_returns_window(monkeypatch):
    """执行器拒绝提交时归还窗口，下一次提交仍可正常完成。"""
    runtime = ThreadRuntime(1, 1)
    original = runtime.executor.submit

    def broken(*args):
        raise RuntimeError("pool is broken")

    monkeypatch.setattr(runtime.executor, "submit", broken)
    with pytest.raises(RuntimeError, match="pool is broken"):
        runtime.submit(lambda: None)
    monkeypatch.setattr(runtime.executor, "submit", original)
    assert runtime.submit(lambda: 17).result(timeout=1) == 17
    assert runtime.stop() == 0


def test_host_quota_survives_contention():
    """多个调用者争抢单站时不超配，全部完成且全部归还。"""
    controller = AdaptiveConcurrencyController(
        max_threads=4, chunks_per_lease=1, threads_per_host_std=1
    )
    controller.update_hosts("std", [("host", 7709)])
    current, peak = 0, 0
    lock = threading.Lock()

    def job():
        nonlocal current, peak
        permit = controller.acquire("std", max_units=1, timeout=2)
        with lock:
            current += 1
            peak = max(current, peak)
        time.sleep(0.005)
        with lock:
            current -= 1
        controller.release(permit, success=True)

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(lambda _: job(), range(16)))
    assert peak == 1
    assert controller.snapshot()["inflight_threads"] == 0


def test_timeout_closes_socket_and_prevents_new_io():
    """真实阻塞 TCP 读在尝试超时后退出，取消状态阻止继续建连。"""
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    accepted = []

    def accept():
        conn, _ = listener.accept()
        accepted.append(conn)

    thread = threading.Thread(target=accept, daemon=True)
    thread.start()
    runtime = ThreadRuntime(1)

    def read():
        sock = socket.socket()
        _bind_attempt_socket(sock)
        sock.connect(listener.getsockname())
        try:
            sock.recv(8)
        finally:
            sock.close()

    started = time.monotonic()
    future = runtime.submit(runtime.attempt, read, 0.1)
    with pytest.raises((OSError, TimeoutError)):
        future.result(timeout=1)
    assert time.monotonic() - started < 1
    assert runtime.stop() == 0
    listener.close()
    for conn in accepted:
        conn.close()


def test_failover_success_does_not_reduce_host_capacity():
    """首站建连失败而后备成功时不降配额，连接在批次之间复用。"""
    calls = []

    class Api:
        client = None

        def connect(self, host, port, time_out):
            calls.append(host)
            return host == "good"

        def disconnect(self):
            pass

    controller = AdaptiveConcurrencyController(max_threads=2, chunks_per_lease=1)
    hosts = [("bad", 7709), ("good", 7709)]
    controller.update_hosts("std", hosts)
    original = PersistentFailoverPool("std", Api, hosts, 1)
    pool = LeasedFailoverPool(original, controller, "std")
    assert pool._ensure_connected()
    pool.release_thread()
    assert controller.snapshot()["sides"]["std"]["bad:7709"]["thread_cap"] == 4
    assert controller.snapshot()["sides"]["std"]["good:7709"]["thread_cap"] == 4
    pool.close()


def test_allowed_hosts_share_physical_quota_without_cross_group_routing():
    """能力过滤只限制候选站，物理站配额仍由同一个控制器状态统一计数。"""
    controller = AdaptiveConcurrencyController(
        max_threads=2, chunks_per_lease=1, threads_per_host_std=1
    )
    bj_host = ("bj", 7709)
    szsh_only_host = ("szsh", 7709)
    controller.update_hosts("std", [bj_host, szsh_only_host])
    permit = controller.acquire("std", max_units=1, timeout=0, allowed_hosts={bj_host})
    assert permit.host == bj_host
    with pytest.raises(TimeoutError):
        controller.acquire("std", max_units=1, timeout=0, allowed_hosts={bj_host})
    snapshot = controller.snapshot()["sides"]["std"]
    assert set(snapshot) == {"bj:7709", "szsh:7709"}
    controller.release(permit, success=True)


def test_beijing_waiter_does_not_block_available_szsh_only_host():
    """北交候选站满载时，等待队列不会阻塞可使用深沪专用站的后续任务。"""
    controller = AdaptiveConcurrencyController(
        max_threads=2, chunks_per_lease=1, threads_per_host_std=1
    )
    bj_host = ("bj", 7709)
    szsh_only_host = ("szsh", 7709)
    controller.update_hosts("std", [bj_host, szsh_only_host])
    occupied = controller.acquire(
        "std", max_units=1, timeout=0, allowed_hosts={bj_host}
    )
    waiting_started = threading.Event()
    waiting_result = {}

    def wait_for_beijing():
        """输入无，输出无；先登记等待，再在北交配额释放后保存凭证。"""
        waiting_started.set()
        waiting_result["permit"] = controller.acquire(
            "std", max_units=1, timeout=2, allowed_hosts={bj_host}
        )

    waiter = threading.Thread(target=wait_for_beijing)
    waiter.start()
    assert waiting_started.wait(timeout=1)
    szsh = controller.acquire(
        "std", max_units=1, timeout=0.5, allowed_hosts={bj_host, szsh_only_host}
    )
    assert szsh.host == szsh_only_host
    controller.release(szsh, success=True)
    controller.release(occupied, success=True)
    waiter.join(timeout=2)
    assert waiting_result["permit"].host == bj_host
    controller.release(waiting_result["permit"], success=True)


def iterator_fixture(monkeypatch, execute, workers=2, timeout=0.1):
    """输入可控执行体，输出真实线程运行时及抓取器；禁用公网探测。"""
    from zsdtdx.engine import parallel_fetcher as pf

    monkeypatch.setattr(
        "zsdtdx.engine.unified_client._ensure_availability_hosts_cache",
        lambda **kwargs: {},
    )
    monkeypatch.setattr(
        "zsdtdx.parser.kline_columns.warmup_kline_decoders", lambda: None
    )
    runtime = ThreadRuntime(workers)
    monkeypatch.setattr(pf, "_get_global_thread_runtime", lambda _: runtime)
    monkeypatch.setattr(pf, "_run_thread_chunk", execute)
    fetcher = pf.ParallelKlineFetcher()
    fetcher.num_threads, fetcher.chunk_timeout_seconds = workers, timeout
    fetcher.chunk_retry_max_attempts, fetcher.chunk_watchdog_grace_seconds = 0, 0
    tasks = [
        dict(code=code, freq="d", start_time="2023-01-01", end_time="2023-01-02")
        for code in ("000001", "600000", "000001")
    ]
    return pf, runtime, fetcher, tasks


def test_watchdog_preserves_siblings_and_task_cardinality(monkeypatch):
    """单桶超时不终止其它桶，重复输入仍逐项交付，实际停止前不回收工作窗口。"""
    released = threading.Event()

    def execute(runtime, payload):
        if payload["code"] == "000001":
            released.wait(timeout=2)
        return {
            "payloads": [
                dict(event="data", task=t, rows=[], error=None)
                for t in payload["tasks"]
            ]
        }

    pf, runtime, fetcher, tasks = iterator_fixture(monkeypatch, execute)
    try:
        rows = list(fetcher._iter_task_payloads_parallel_chunked(tasks))
        assert len(rows) == 3
        assert len([r for r in rows if r["error"] == "chunk watchdog timeout"]) == 2
        assert any(r["task"]["code"] == "600000" and r["error"] is None for r in rows)
        assert runtime._futures
    finally:
        released.set()
        assert runtime.stop() == 0


def test_stopped_runtime_rejects_new_work():
    """已销毁代拒绝新工作，不把任务提交给失效连接。"""
    runtime = ThreadRuntime(1)
    runtime.stop()
    with pytest.raises(RuntimeError, match="restarted or destroyed"):
        runtime.submit(lambda: None)


def test_concurrent_jobs_keep_adjustment_and_duplicate_results_isolated(monkeypatch):
    """同代码周期的并发复权作业不共享可变缓存，重复输入完整交付且各有一个 done。"""
    import queue
    from zsdtdx.engine import parallel_fetcher as pf
    from zsdtdx.engine.unified_client import UnifiedTdxClient

    class Sock:
        def settimeout(self, value):
            pass

        def shutdown(self, how):
            pass

        def close(self):
            pass

    class Api:
        def connect(self, *args, **kwargs):
            self.client = Sock()
            return True

        def disconnect(self):
            pass

        def get_security_bars(self, *args, qfq):
            time.sleep(0.01)
            page = parse_standard_columns(standard_body(), 4)
            if not qfq:
                page = KlineColumns(
                    {
                        k: v + 1 if k in {"open", "close", "high", "low"} else v
                        for k, v in page.columns.items()
                    }
                )
            return page

    controller = AdaptiveConcurrencyController(
        max_threads=2, chunks_per_lease=1, threads_per_host_std=1
    )
    controller.update_hosts("std", [("s", 7709)])
    controller.update_hosts("ex", [("e", 7720)])
    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    client.output_cfg = {"filter_suspended_placeholder_bar": True}
    client._resolve_stock_route_no_df = lambda code, freq, source: (
        code,
        {"source": "std", "market": 0},
    )
    client.std_pool = LeasedFailoverPool(
        PersistentFailoverPool("std", Api, [("s", 7709)], 1), controller, "std"
    )
    client.ex_pool = LeasedFailoverPool(
        PersistentFailoverPool("ex", Api, [("e", 7720)], 1), controller, "ex"
    )
    monkeypatch.setattr(pf, "_ensure_shared_client_context", lambda: client)
    monkeypatch.setattr(
        "zsdtdx.engine.unified_client._ensure_availability_hosts_cache",
        lambda **kwargs: {},
    )
    runtime = ThreadRuntime(2)
    monkeypatch.setattr(pf, "_get_global_thread_runtime", lambda _: runtime)
    fetcher = pf.ParallelKlineFetcher()
    fetcher.num_threads = 2
    task = dict(code="000001", freq="d", start_time="2023-01-01", end_time="2023-01-06")
    a_queue, b_queue = queue.Queue(), queue.Queue()
    try:
        a = fetcher.fetch_stock_tasks_async(tasks=[task, task], queue=a_queue, qfq=True)
        b = fetcher.fetch_stock_tasks_async(tasks=[task], queue=b_queue, qfq=False)
        assert a.result(timeout=3) == {
            "total_tasks": 2,
            "success_tasks": 2,
            "failed_tasks": 0,
        }
        assert b.result(timeout=3) == {
            "total_tasks": 1,
            "success_tasks": 1,
            "failed_tasks": 0,
        }

        def _take(stream):
            """输入队列，输出全部已到达事件。边界：不阻塞等待尚未入队的事件。"""
            events = []
            while not stream.empty():
                events.append(stream.get_nowait())
            return events

        a_events, b_events = _take(a_queue), _take(b_queue)
        a_data = [event for event in a_events if event.get("event") == "data"]
        b_data = [event for event in b_events if event.get("event") == "data"]
        assert len(a_data) == 2 and len(b_data) == 1
        assert all(item["error"] is None for item in a_data + b_data)
        assert a_data[0]["rows"][0]["open"] + 1 == b_data[0]["rows"][0]["open"]
        a_data[0]["rows"][0]["open"] = -1
        assert a_data[1]["rows"][0]["open"] != -1
        assert [event["event"] for event in a_events] == ["data", "data", "done"]
        assert [event["event"] for event in b_events] == ["data", "done"]
        assert a_events[-1]["success_tasks"] == 2
        assert b_events[-1]["success_tasks"] == 1
        assert controller.snapshot()["inflight_threads"] == 0
    finally:
        assert runtime.stop() == 0
        client.std_pool.close()
        client.ex_pool.close()


def test_stopping_job_suppresses_late_success_and_emits_done(monkeypatch):
    """销毁时未交付的任务失败归队，迟到成功不得越过最终 done。"""
    import queue

    started, unblock = threading.Event(), threading.Event()

    def execute(runtime, payload):
        started.set()
        unblock.wait(timeout=1)
        return {
            "payloads": [
                dict(event="data", task=t, rows=[{"stale": True}], error=None)
                for t in payload["tasks"]
            ]
        }

    pf, runtime, fetcher, tasks = iterator_fixture(
        monkeypatch, execute, workers=1, timeout=0.1
    )
    stream = queue.Queue()
    try:
        job = fetcher.fetch_stock_tasks_async(tasks=tasks, queue=stream)
        assert started.wait(timeout=1)
        assert runtime.stop(timeout=0.01) == 1
        summary = job.result(timeout=1)
        assert summary == {
            "total_tasks": len(tasks),
            "success_tasks": 0,
            "failed_tasks": len(tasks),
        }
        events = []
        while not stream.empty():
            events.append(stream.get_nowait())
        data_events = [event for event in events if event.get("event") == "data"]
        assert len(data_events) == len(tasks)
        assert all(item["error"] and item["rows"] == [] for item in data_events)
        assert events[-1]["event"] == "done"
        assert events[-1]["failed_tasks"] == len(tasks)
        unblock.set()
        assert runtime.stop() == 0
        assert stream.empty()
    finally:
        unblock.set()
        runtime.stop()


def test_completed_attempt_cannot_close_reused_socket():
    """旧定时器晚到时不能关闭已完成尝试的连接，关闭先发生时不能解除取消。"""
    from zsdtdx.engine.unified_client import AttemptSocketCloser
    from unittest.mock import Mock

    sock = Mock()
    closer = AttemptSocketCloser()
    closer.bind(sock)
    assert closer.disarm()
    closer.close()
    sock.close.assert_not_called()
    detached = AttemptSocketCloser()
    detached.bind(sock)
    assert detached.detach(sock)
    detached.close()
    sock.close.assert_not_called()
    cancelled = AttemptSocketCloser()
    cancelled.close()
    assert not cancelled.disarm()
    assert not cancelled.detach(sock)
    cancelled.bind(sock)
    sock.close.assert_called_once()


def test_one_watchdog_handles_many_attempts():
    """大量短尝试只保留一个看门狗，正常任务不会产生每次尝试一个 Timer。"""
    runtime = ThreadRuntime(2)
    try:
        for _ in range(200):
            assert runtime.submit(runtime.attempt, lambda: 7, 1).result(timeout=1) == 7
        assert runtime._watchdog.is_alive()
        assert not runtime._closers
        assert len(runtime.executor._threads) <= 2
    finally:
        assert runtime.stop() == 0


def test_reused_connection_registers_new_attempt_deadline():
    """第二次租用空闲长连接也必须绑定新预算，不能沿用旧尝试的 socket 超时。"""
    from zsdtdx.net.base_socket_client import TrafficStatSocket

    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    accepted = []
    server = threading.Thread(
        target=lambda: accepted.append(listener.accept()[0]), daemon=True
    )
    server.start()

    class Api:
        def connect(self, *args, **kwargs):
            """输入站点，输出建连结果；使用真实 TCP 以验证租约复用。"""
            self.client = TrafficStatSocket(socket.AF_INET, socket.SOCK_STREAM)
            _bind_attempt_socket(self.client)
            self.client.connect(listener.getsockname())
            return True

        def disconnect(self):
            """输入无，输出无；重复关闭安全。"""
            self.client.close()

        def request(self, block=False):
            """输入阻塞标志，输出结果；第二次请求故意等待没有数据的连接。"""
            return self.client.recv(1) if block else 7

    host = ("local", 7709)
    controller = AdaptiveConcurrencyController(max_threads=1, chunks_per_lease=1)
    controller.update_hosts("std", [host])
    pool = LeasedFailoverPool(
        PersistentFailoverPool("std", Api, [host], 1), controller, "std"
    )
    runtime = ThreadRuntime(1)

    def call(block, budget):
        """输入请求方式和预算，输出结果；结束才归还实际租约。"""
        try:
            return runtime.attempt(lambda: pool.call("request", block), budget)
        finally:
            pool.release_thread(discard=block)

    try:
        assert runtime.submit(call, False, 5).result(timeout=1) == 7
        started = time.monotonic()
        with pytest.raises((RuntimeError, TimeoutError, OSError)):
            runtime.submit(call, True, 0.1).result(timeout=0.8)
        assert time.monotonic() - started < 0.7
        assert controller.snapshot()["inflight_threads"] == 0
    finally:
        assert runtime.stop() == 0
        pool.close()
        listener.close()
        server.join(timeout=1)
        for sock in accepted:
            sock.close()


def test_mixed_sources_alternate_and_bound_extended_queue(monkeypatch):
    """扩展站只有一个槽时仅提交一个扩展任务，标准任务继续派发且不丢任务。"""

    release_ex = threading.Event()
    dispatched, lock = [], threading.Lock()

    def execute(runtime, payload):
        with lock:
            dispatched.append(payload["route_source"])
        if payload["route_source"] == "ex":
            release_ex.wait(timeout=2)
        else:
            time.sleep(0.01)
        return {
            "payloads": [
                dict(event="data", task=t, rows=[], error=None)
                for t in payload["tasks"]
            ]
        }

    pf, runtime, fetcher, _ = iterator_fixture(
        monkeypatch, execute, workers=3, timeout=3
    )
    fetcher.threads_per_host_ex = 1
    monkeypatch.setattr(
        "zsdtdx.engine.unified_client.get_probe_result_cache",
        lambda: {"extended": [("ex", 7720)]},
    )
    tasks = [
        dict(
            code=str(i),
            freq="d",
            start_time="2023-01-01",
            end_time="2023-01-02",
            _route_source=source,
        )
        for source in ("ex", "std")
        for i in range(4)
    ]
    output = []
    job = threading.Thread(
        target=lambda: output.extend(
            fetcher._iter_task_payloads_parallel_chunked(tasks)
        )
    )
    try:
        job.start()
        deadline = time.monotonic() + 1
        while dispatched.count("std") < 4 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert dispatched[:2] == ["std", "ex"]
        assert dispatched.count("ex") == 1
        assert dispatched.count("std") == 4
    finally:
        release_ex.set()
        job.join(timeout=3)
        assert runtime.stop() == 0
    assert len(output) == len(tasks)


def test_drip_feed_uses_cumulative_deadline():
    """持续少量回包不会不断刷新预算；真实 TCP 收包到累计期限即退出。"""
    from zsdtdx.net.base_socket_client import TrafficStatSocket
    from zsdtdx.parser.base import _recv_exact

    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    finished = threading.Event()

    def serve():
        conn, _ = listener.accept()
        try:
            while not finished.wait(0.02):
                conn.sendall(b"x")
        except OSError:
            pass
        finally:
            conn.close()

    server = threading.Thread(target=serve, daemon=True)
    runtime = ThreadRuntime(1)
    server.start()

    def read():
        with TrafficStatSocket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            _bind_attempt_socket(sock)
            sock.connect(listener.getsockname())
            return _recv_exact(sock, 100)

    started = time.monotonic()
    try:
        with pytest.raises((OSError, TimeoutError)):
            runtime.submit(runtime.attempt, read, 0.12).result(timeout=1)
        assert time.monotonic() - started < 0.8
    finally:
        finished.set()
        assert runtime.stop() == 0
        server.join(timeout=1)
        listener.close()


@pytest.mark.parametrize(
    "start,expected", [("2023-01-03 15:00:00.500000", 2), ("2023-01-03 09:00:00", 3)]
)
def test_fractional_future_start_excludes_previous_bar(monkeypatch, start, expected):
    """期货亚秒起点必须排除更早的整秒 bar，不能向下取整起点。"""
    from zsdtdx.engine import columnar_kline as ck
    from zsdtdx.engine.unified_client import UnifiedTdxClient

    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    client.output_cfg = {}
    page = parse_standard_columns(standard_body(), 4)
    monkeypatch.setattr(
        ck, "_resolve_route", lambda *args: ("CU", {"source": "ex", "market": 30})
    )
    monkeypatch.setattr(ck, "_page", lambda *args: (page, 800))
    task = dict(
        code="CU",
        freq="d",
        start_time=start,
        end_time="2023-01-05 16:00:00",
    )
    result = ck.fetch_chunk_columns(client, [task], "future")
    assert len(result["results"][0]["columns"]) == expected


@pytest.mark.parametrize("silent", [False, True])
def test_partial_frame_is_disconnected_before_retry(monkeypatch, silent):
    """半包异常或底层吞错返回 None 时，下一请求必须换干净连接，不能残包续读。"""
    from types import SimpleNamespace
    from zsdtdx.parser.base import ResponseRecvFails

    calls = []

    class Api:
        def __init__(self, broken):
            """输入故障标志，输出夹具；模拟尚未完成的协议帧。"""
            self.broken = broken
            self.client = SimpleNamespace(_zsdtdx_frame_pending=broken)

        def request(self):
            """输入无，输出合法空页或失败；记录请求顺序。"""
            calls.append("broken_request" if self.broken else "clean_request")
            if not self.broken:
                return []
            if silent:
                return None
            raise ResponseRecvFails("partial response")

        def disconnect(self):
            """输入无，输出无；记录丢弃必须早于下一次发送。"""
            calls.append("drop_broken" if self.broken else "drop_clean")

    pool = PersistentFailoverPool("test", Api, [("host", 7709)], 1)
    data = pool._get_thread_data()
    data.update(api=Api(True), active_index=0)

    def reconnect():
        """输入无，输出恢复结果；只安装全新连接，保留单 host 恢复阶梯。"""
        data["api"] = Api(False)
        return True

    monkeypatch.setattr(pool, "_reconnect_active_host", reconnect)
    try:
        assert pool.call("request", allow_none=True) == []
        assert calls == ["broken_request", "drop_broken", "clean_request"]
    finally:
        pool.close()


@pytest.mark.parametrize("tail", [False, True])
def test_compressed_frame_rejects_size_mismatch_and_tail(tail):
    """压缩头长度不符或流后有残留数据时拒绝交付，连接保留未完成标志。"""
    import zlib
    from tests.test_parser_frame_io import _EchoParser, _FragSocket, _frame
    from zsdtdx.parser.base import ResponseRecvFails

    plain = b"a" * 64
    packet = zlib.compress(plain) + (b"unexpected" if tail else b"")
    sock = _FragSocket(_frame(packet, unzipsize=64 if tail else 2))
    parser = _EchoParser(sock)
    parser.send_pkg = b"request"
    with pytest.raises(ResponseRecvFails):
        parser.call_api()
    assert sock._zsdtdx_frame_pending


def test_future_dedup_matches_existing_dataframe_contract(monkeypatch):
    """期货窗口内重复时间按原 DataFrame 排序后保留末条，不能套用股票首条规则。"""
    import pandas as pd
    from zsdtdx.engine import columnar_kline as ck
    from zsdtdx.engine.unified_client import UnifiedTdxClient

    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    client.output_cfg = {}
    first = parse_standard_columns(standard_body(), 4)
    old = KlineColumns(
        {k: v - 1 if k == "close" else v for k, v in first.columns.items()}
    )
    empty = first.take(slice(0, 0))
    pages = iter([first, old, empty])
    monkeypatch.setattr(
        ck, "_resolve_route", lambda *args: ("CU", {"source": "ex", "market": 30})
    )
    monkeypatch.setattr(ck, "_page", lambda *args: (next(pages), 3))
    task = dict(
        code="CU",
        freq="d",
        start_time="2023-01-03 09:00:00",
        end_time="2023-01-05 16:00:00",
    )
    result = ck.fetch_chunk_columns(client, [task], "future")["results"][0]["columns"]
    reference = (
        pd.DataFrame(first.raw_rows() + old.raw_rows())
        .sort_values("datetime")
        .drop_duplicates("datetime", keep="last")
    )
    np.testing.assert_array_equal(
        result.columns["close"], reference["close"].to_numpy()
    )
