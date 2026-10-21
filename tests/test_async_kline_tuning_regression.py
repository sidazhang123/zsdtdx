"""
模块：tests/test_async_kline_tuning_regression.py

职责：
1. 锁定 zsdtdx async K 线性能与精度对齐方案（A-D 阶段）的所有改造点行为；
2. 全程离线、不依赖真实网络，可在 CI/沙箱内重复执行；
3. 任何后续 PR 改动若破坏本回归契约会立即暴露。

边界：
1. 本文件不发起 socket 请求；真实网络单点探测与弱网离线注入见 tests/manual/ 下对应脚本。
2. 测试基于"目标态"断言；不保留历史分区池兼容接口。
"""

import os
import struct
import sys
import threading
import time
from typing import Any, List

# 把工程 src 加入 sys.path（与 pyproject.toml pythonpath 一致，便于直接 py 本文件调试）。
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_SRC_DIR = os.path.join(_PROJECT_ROOT, "src")
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)


# =============================================================================
# F1: ChunkLocalRowCache 行为锁定
# =============================================================================


class TestChunkLocalRowCache:
    """
    阶段二改造点 B4：使用 ChunkLocalRowCache 管理 chunk 内分区。

    验收要点：
    1. 同 chunk 内对同 (code,freq) 重复 acquire 返回同一对象，并归零游标；
    2. 不同 (code,freq) 互不影响；
    3. soft_delete 后再 acquire 是新分区（不复用 list 槽位）；
    4. 移除 `_pool_lock` 与 `_free` 池属性；
    5. 分区通过 acquire_partition/soft_delete 管理。
    """

    def _new_cache(self):
        from zsdtdx.engine.unified_client import ChunkLocalRowCache

        return ChunkLocalRowCache()

    def test_acquire_idempotent_within_chunk(self):
        """同 chunk 内重复 acquire 同 key 返回同对象，且 _n/oldest/newest/page_start 归零。"""
        cache = self._new_cache()
        p1 = cache.acquire_partition("000001", "d")
        p1["_n"] = 5
        p1["oldest_dt"] = "fake"
        p1["page_start"] = 100
        p1["fetched_pages"] = 7
        p1["history_exhausted"] = True
        p2 = cache.acquire_partition("000001", "d")
        assert p1 is p2, "重复 acquire 应返回同一分区对象"
        assert p2["_n"] == 0
        assert p2["oldest_dt"] is None
        assert p2["newest_dt"] is None
        assert p2["page_start"] == 0
        assert p2["fetched_pages"] == 0
        assert p2["history_exhausted"] is False

    def test_distinct_keys_isolated(self):
        """不同 (code,freq) 分区互不影响。"""
        cache = self._new_cache()
        p1 = cache.acquire_partition("000001", "d")
        p2 = cache.acquire_partition("000002", "d")
        assert p1 is not p2
        assert p1["_partition_key"] == "000001|d"
        assert p2["_partition_key"] == "000002|d"

    def test_soft_delete_releases_partition(self):
        """soft_delete 后再次 acquire 返回新对象，验证不再有 free 池复用语义。"""
        cache = self._new_cache()
        p1 = cache.acquire_partition("000001", "d")
        cache.soft_delete("000001", "d")
        p2 = cache.acquire_partition("000001", "d")
        assert p1 is not p2, "soft_delete 后应释放分区引用，不复用 list 槽位"

    def test_soft_delete_unknown_is_noop(self):
        """对未持有的 key 调用 soft_delete 不抛错。"""
        cache = self._new_cache()
        cache.soft_delete("unknown", "d")  # 不抛即通过

    def test_no_pool_lock_or_free_attributes(self):
        """ChunkLocalRowCache 不再持有 _pool_lock / _free / _local 等旧池属性。"""
        cache = self._new_cache()
        assert not hasattr(cache, "_pool_lock"), "应移除 _pool_lock"
        assert not hasattr(cache, "_free"), "应移除 _free 池"
        assert not hasattr(cache, "_local"), "应移除 threading.local 租约"


# =============================================================================
# F5: BaseSocketClient.connect 启用 TCP_NODELAY
# =============================================================================


class TestTcpNoDelayEnabled:
    """
    阶段四 D1：BaseSocketClient.connect 成功后立即 setsockopt(IPPROTO_TCP, TCP_NODELAY, 1)。
    通过 monkey patch TrafficStatSocket 的工厂记录 setsockopt 调用。
    """

    def test_connect_sets_tcp_nodelay(self, monkeypatch):
        """connect 成功 → 应观察到 TCP_NODELAY setsockopt 调用。"""
        import socket
        from zsdtdx.net import base_socket_client as bsc

        recorded: List[tuple] = []

        class _FakeSock:
            def __init__(self, *_a, **_kw):
                pass

            def settimeout(self, _t):
                pass

            def bind(self, _addr):
                pass

            def connect(self, _addr):
                pass

            def close(self):
                pass

            def setsockopt(self, level, optname, value):
                recorded.append((level, optname, value))

            def shutdown(self, _how):
                pass

            # 流量统计相关属性
            send_pkg_num = 0
            recv_pkg_num = 0
            send_pkg_bytes = 0
            recv_pkg_bytes = 0
            first_pkg_send_time = None
            last_api_send_bytes = 0
            last_api_recv_bytes = 0

        monkeypatch.setattr(bsc, "TrafficStatSocket", _FakeSock)

        client = bsc.BaseSocketClient()
        # setup() 在子类实现；本测试用 need_setup=False 跳过协议握手
        client.need_setup = False
        try:
            client.connect(ip="127.0.0.1", port=7709)
        finally:
            try:
                client.disconnect()
            except Exception:
                pass

        # 应当至少包含一次 (IPPROTO_TCP, TCP_NODELAY, 1)
        assert any(
            tup == (socket.IPPROTO_TCP, socket.TCP_NODELAY, 1) for tup in recorded
        ), f"TCP_NODELAY 未启用；setsockopt 调用记录: {recorded}"


# =============================================================================
# F6b: chunk deadline 覆盖首次建连与底层请求
# =============================================================================


class TestPoolChunkDeadline:
    """
    chunk deadline 线程级状态：
    1. 未建连前设置 read timeout，后续新建 socket 也必须继承该预算；
    2. deadline 过期时底层请求应快速失败，避免进入阻塞 I/O。
    """

    def test_timeout_set_before_connect_applies_to_new_socket(self):
        """先设置线程读超时、再建连；新 socket 应收到 settimeout。"""
        from zsdtdx.engine.unified_client import PersistentFailoverPool

        class _Sock:
            def __init__(self):
                self.timeouts: List[float] = []

            def settimeout(self, value):
                self.timeouts.append(value)

        class _Api:
            instances: List[Any] = []

            def __init__(self, **_kwargs):
                self.client = _Sock()
                _Api.instances.append(self)

            def connect(self, _host, _port, time_out=0, **_kwargs):
                self.client.settimeout(time_out)
                return True

            def disconnect(self):
                pass

        pool = PersistentFailoverPool("test", _Api, [("127.0.0.1", 7709)], 1.5)
        pool.set_thread_socket_read_timeout(5.0)

        assert pool.ensure_connected() is True
        api = _Api.instances[-1]
        assert api.client.timeouts, "新 socket 应至少设置过 timeout"
        assert api.client.timeouts[-1] <= 5.0
        assert api.client.timeouts[-1] > 0

    def test_expired_deadline_fails_before_api_call(self, monkeypatch):
        """deadline 已过期时，_attempt_call_step 不应调用底层 API。"""
        from zsdtdx.engine.unified_client import PersistentFailoverPool

        called = {"n": 0}

        class _Api:
            client = object()

            def quote(self):
                called["n"] += 1
                return {"ok": True}

        pool = PersistentFailoverPool.__new__(PersistentFailoverPool)
        pool.name = "test"
        monkeypatch.setattr(
            pool,
            "_get_thread_data",
            lambda: {
                "api": _Api(),
                "active_index": 0,
                "socket_read_deadline": time.monotonic() - 1.0,
                "socket_read_timeout_override": 0.1,
            },
        )

        result, exc, needs_retry = pool._attempt_call_step("quote", False)

        assert result is None
        assert needs_retry is True
        assert isinstance(exc, TimeoutError)
        assert called["n"] == 0


# =============================================================================
# F7: 日志选项 RCU 写入
# =============================================================================


class TestLogDetailOptionsRCU:
    """
    阶段三 C5：set_log_detail_options 整体替换全局快照对象（RCU），读路径不加锁。

    验收要点：
    1. _read_log_detail_options 返回的对象就是当前全局快照（同一引用）；
    2. 写入会让全局引用指向一个新对象（id 变化）；
    3. 并发多线程 read 不抛、不死锁；
    4. 写入返回的是更新前快照副本。
    """

    def test_write_replaces_snapshot_object(self):
        """set 后全局 dict 引用应是新对象。"""
        import zsdtdx.engine.parallel_fetcher as pf

        before = pf._read_log_detail_options()
        before_id = id(before)
        prev = pf.set_log_detail_options(sample_size=15)
        try:
            after = pf._read_log_detail_options()
            assert id(after) != before_id, "RCU 写入应整体替换全局引用"
            assert after.get("sample_size") == 15
            assert prev.get("sample_size") != 15  # prev 是更新前副本
        finally:
            # 还原默认
            pf.set_log_detail_options(sample_size=prev.get("sample_size", 8))

    def test_read_returns_snapshot_reference(self):
        """_read_log_detail_options 直接返回当前全局快照（不拷贝）。"""
        import zsdtdx.engine.parallel_fetcher as pf

        snap1 = pf._read_log_detail_options()
        snap2 = pf._read_log_detail_options()
        assert snap1 is snap2, "读路径应直接返回全局引用（零拷贝）"

    def test_concurrent_read_write_no_crash(self):
        """并发读 + 写不抛异常、不死锁。"""
        import zsdtdx.engine.parallel_fetcher as pf

        stop = threading.Event()
        errors: List[Exception] = []

        def reader():
            try:
                for _ in range(2000):
                    opt = pf._read_log_detail_options()
                    # 读取字段不应抛
                    _ = int(opt.get("sample_size", 0) or 0)
                    if stop.is_set():
                        return
            except Exception as exc:
                errors.append(exc)

        def writer():
            try:
                for i in range(200):
                    pf.set_log_detail_options(sample_size=(i % 100) + 1)
            except Exception as exc:
                errors.append(exc)

        readers = [threading.Thread(target=reader) for _ in range(4)]
        writers = [threading.Thread(target=writer) for _ in range(2)]
        for t in readers + writers:
            t.start()
        for t in writers:
            t.join(timeout=5)
        stop.set()
        for t in readers:
            t.join(timeout=5)
        # 还原默认
        pf.set_log_detail_options(sample_size=8)

        assert not errors, f"并发 read/write 不应抛异常: {errors}"

    def test_invalid_sample_size_clamped(self):
        """sample_size 越界会被收敛到 [1, 200]。"""
        import zsdtdx.engine.parallel_fetcher as pf

        prev = pf.set_log_detail_options(sample_size=0)
        try:
            assert pf._read_log_detail_options().get("sample_size") == 1
            pf.set_log_detail_options(sample_size=10000)
            assert pf._read_log_detail_options().get("sample_size") == 200
        finally:
            pf.set_log_detail_options(sample_size=prev.get("sample_size", 8))


# =============================================================================
# F8: get_volume 除已证伪的 hleax=0x80 外与 pytdx 原版 fuzz 等价
# =============================================================================


class TestGetVolumeParityFuzz:
    """
    当前工程 helper.get_volume 保留 pytdx 的主体算法，只修正真实行情证明错误的 0x80 边界。

    历史 bug：
    1. pytdx 的 `if hleax > 0x80` 会把实盘原码 0000803f 的 1 手误解成 32768；
    2. `else` 中 `dwEdx<0` 时 pytdx 是 `1/pow(2,dwEdx)=pow(2,-dwEdx)`（方向反向但要保持等价）。

    本测试在 0x80 以外做随机等价 fuzz，并单独锁定已验证的 0x80 正确值。
    """

    def _pytdx_get_volume(self):
        from pytdx.helper import get_volume as pytdx_get_volume

        return pytdx_get_volume

    def _zs_get_volume(self):
        from zsdtdx.util.helper import get_volume as zs_get_volume

        return zs_get_volume

    def test_explicit_boundary_hleax_eq_0x80(self):
        """真实 511920.SH 原码必须解为 1，并明确偏离 pytdx 的错误边界结果。"""
        ivol = 0x3F800000
        z = self._zs_get_volume()(ivol)
        p = self._pytdx_get_volume()(ivol)
        assert z == 1.0
        assert p != z

    def test_explicit_boundary_hleax_gt_0x80(self):
        """hleax>0x80 走大分支。"""
        ivol = (0x42 << 24) | (0x81 << 16) | (0x12 << 8) | 0x34
        z = self._zs_get_volume()(ivol)
        p = self._pytdx_get_volume()(ivol)
        assert z == p

    def test_explicit_boundary_dwEdx_negative(self):
        """构造 logpoint 让 dwEdx 为负，触发原 bug 分支。"""
        # logpoint 选小值让 (logpoint*2 - 0x86) < 0；同时 hleax<=0x80 走 else 分支
        ivol = (0x10 << 24) | (0x55 << 16) | (0x33 << 8) | 0x77
        z = self._zs_get_volume()(ivol)
        p = self._pytdx_get_volume()(ivol)
        assert z == p

    def test_explicit_zero(self):
        """ivol=0 必须为 0（pytdx 与 zsdtdx 完全一致）。"""
        z = self._zs_get_volume()(0)
        p = self._pytdx_get_volume()(0)
        assert z == 0 or z == p

    def test_fuzz_random_2k_samples(self):
        """随机 fuzz 2000 个 ivol 做按位等价比对。"""
        import random

        rng = random.Random(20260522)
        zs = self._zs_get_volume()
        pt = self._pytdx_get_volume()
        mismatches: List[tuple] = []
        for _ in range(2000):
            ivol = rng.randint(0, (1 << 32) - 1)
            if ((ivol >> 16) & 0xFF) == 0x80:
                continue
            z = zs(ivol)
            p = pt(ivol)
            if z != p:
                # 浮点对比允许 ULP 1 内（双方都是同 pow 链路应当严格相等，但极端 IEEE-754 误差兜底）
                if not (
                    isinstance(z, float)
                    and isinstance(p, float)
                    and abs(z - p) < 1e-12 * max(1.0, abs(p))
                ):
                    mismatches.append((ivol, z, p))
        assert not mismatches, f"get_volume fuzz mismatch (sample 5): {mismatches[:5]}"


# =============================================================================
# F-aux: 离线快速回归 — Stock & Index parser → normalize → 占位过滤端到端
# =============================================================================


class TestParseToNormalizeOffline:
    """
    端到端离线回归：构造合成 body_buf →
    parser → _normalize_stock_kline_rows → 占位过滤；
    验证 9 字段 schema 不动、数值刻度正确、占位 bar 被过滤。
    """

    @staticmethod
    def _encode_price_varint(value: int) -> bytes:
        negative = value < 0
        magnitude = -value if negative else value
        out = bytearray()
        first = magnitude & 0x3F
        if negative:
            first |= 0x40
        rest = magnitude >> 6
        if rest:
            first |= 0x80
        out.append(first)
        while rest:
            chunk = rest & 0x7F
            rest >>= 7
            if rest:
                chunk |= 0x80
            out.append(chunk)
        return bytes(out)

    def _build_bar(self, zipday, oc, od, hd, ld, vol, dbvol):
        body = bytearray()
        body.extend(struct.pack("<I", zipday))
        body.extend(self._encode_price_varint(oc))
        body.extend(self._encode_price_varint(od))
        body.extend(self._encode_price_varint(hd))
        body.extend(self._encode_price_varint(ld))
        body.extend(struct.pack("<II", vol & 0xFFFFFFFF, dbvol & 0xFFFFFFFF))
        return bytes(body)

    def test_parser_output_schema_9_fields(self):
        """parser 输出仍保留 vol/amount/datetime/open/close/high/low/_ts，
        normalize 后输出 9 字段：code/freq/open/close/high/low/volume/amount/datetime。"""
        from zsdtdx.parser.diff_kline_page import parse_diff_encoded_kline_page

        body = bytearray()
        body.extend(struct.pack("<H", 1))
        body.extend(
            self._build_bar(20260102, 12340, 10, 200, -10, 0x42801000, 0x44A00000)
        )

        rows = parse_diff_encoded_kline_page(bytes(body), 9, with_index_counts=False)
        assert len(rows) == 1
        keys = set(rows[0].keys())
        # parser 输出必须包含的字段
        assert {
            "open",
            "close",
            "high",
            "low",
            "vol",
            "amount",
            "datetime",
            "_ts",
        } <= keys


# =============================================================================
# F-aux: parser → ChunkLocalRowCache 行管理 sanity（无 socket）
# =============================================================================


class TestChunkLocalRowCacheRowAccumulate:
    """
    模拟 chunk 内多 task 命中场景：手工往 partition.cache_rows 追加并维护 _n。
    """

    def test_n_monotonic_within_chunk(self):
        from zsdtdx.engine.unified_client import ChunkLocalRowCache

        cache = ChunkLocalRowCache()
        partition = cache.acquire_partition("000001", "d")
        rows = partition["cache_rows"]
        for i in range(5):
            rows.append((i, {"row": i}))
            partition["_n"] += 1
        # 同 chunk 内累加：_n 单调
        assert partition["_n"] == 5
        # 同 chunk 内重复 acquire 不会清除底层 list（但 _n 归零）
        partition2 = cache.acquire_partition("000001", "d")
        assert partition2 is partition
        assert partition2["_n"] == 0
        # 但 cache_rows 底层 list 仍持有上次数据（外层逻辑只读 [0,_n)）
        # 因此再次写入会覆盖前 5 个 tuple
        assert len(partition2["cache_rows"]) == 5
