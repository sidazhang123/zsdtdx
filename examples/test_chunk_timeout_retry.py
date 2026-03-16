# -*- coding: utf-8 -*-
"""
针对 chunk 级超时 + 重试改造的定向测试。

测试目标（全部通过 monkey-patch 注入故障，不依赖真实行情服务器）：
1. chunk 超时：模拟卡死的 chunk，验证 chunk_timeout_seconds 能真正截断，不阻塞。
2. chunk 重试成功：模拟首次失败、第二次成功，验证重试循环生效。
3. chunk 重试耗尽：模拟连续失败超过 retry_max，验证所有 tasks 标记 error 并正常返回。
4. shutdown(wait=False)：验证超时后调用方不被 __exit__ 阻塞。
5. 正常路径：模拟成功场景，验证不触发重试。

运行方式：
    cd examples
    python test_chunk_timeout_retry.py
"""

from __future__ import annotations

import os
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List
from unittest.mock import MagicMock

# 把 src 加到 path
sys.path.insert(0, str(os.path.join(os.path.dirname(__file__), "..", "src")))

from zsdtdx.parallel_fetcher import (
    _fetch_one_task_chunk,
    _build_task_payload,
    _emit_log,
)

# ─── 辅助 ───────────────────────────────────────────────

_FAKE_TASKS = [
    {"code": "000001", "freq": "15", "start_time": "2025-01-01", "end_time": "2025-01-31"},
    {"code": "000001", "freq": "15", "start_time": "2025-02-01", "end_time": "2025-02-28"},
]

_GOOD_RESULT = {
    "results": [
        {"task": t, "rows": [{"close": 10.0}], "error": None}
        for t in _FAKE_TASKS
    ],
    "chunk_hit_tasks": 2,
    "chunk_network_page_calls": 1,
}


def _make_chunk_payload(
    *,
    timeout: float = 3.0,
    retry_max: int = 2,
    reconnect: bool = False,
) -> Dict[str, Any]:
    return {
        "chunk_id": "test_chunk_001",
        "code": "000001",
        "freq": "15",
        "tasks": list(_FAKE_TASKS),
        "enable_cache": False,
        "reconnect_on_unavailable": reconnect,
        "chunk_timeout_seconds": timeout,
        "chunk_retry_max_attempts": retry_max,
    }


def _patch_worker_context(module, behavior_fn):
    """
    Monkey-patch _ensure_worker_client_context 使其返回一个
    带有 get_stock_kline_rows_for_chunk_tasks = behavior_fn 的 mock。
    """
    mock_ctx = MagicMock()
    mock_ctx.get_stock_kline_rows_for_chunk_tasks = behavior_fn
    original = module._ensure_worker_client_context

    def _fake_ensure():
        return mock_ctx

    module._ensure_worker_client_context = _fake_ensure
    return original


# ─── 测试用例 ────────────────────────────────────────────

def test_1_timeout_not_blocking():
    """
    测试 1: chunk 超时后不阻塞。
    模拟 chunk 执行 sleep(60)，设 timeout=2s。
    验证 _fetch_one_task_chunk 在 ~2s 内返回，而非等待 60s。
    """
    import zsdtdx.parallel_fetcher as mod

    call_count = 0

    def _hang(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        time.sleep(60)  # 模拟卡死
        return _GOOD_RESULT

    original = _patch_worker_context(mod, _hang)
    try:
        payload = _make_chunk_payload(timeout=2.0, retry_max=0)  # 不重试，只测超时
        t0 = time.monotonic()
        report = _fetch_one_task_chunk(payload)
        elapsed = time.monotonic() - t0

        # 验证
        assert elapsed < 5.0, f"耗时 {elapsed:.1f}s，超时应在 ~2s 内返回（shutdown 不应阻塞）"
        assert isinstance(report, dict), "应返回 dict"
        assert len(report["payloads"]) == 2, f"应有 2 个 payload，实际 {len(report['payloads'])}"
        for p in report["payloads"]:
            assert p["error"] is not None, "超时 chunk 的 tasks 应标记 error"
        assert call_count == 1, f"retry_max=0 应只调用 1 次，实际 {call_count}"
        print(f"  [PASS] 超时在 {elapsed:.2f}s 返回，payload error 正确")
    finally:
        mod._ensure_worker_client_context = original


def test_2_retry_success():
    """
    测试 2: 首次失败 → 重试成功。
    模拟第 1 次抛异常，第 2 次返回正常结果。
    """
    import zsdtdx.parallel_fetcher as mod

    call_count = 0

    def _fail_then_ok(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise ConnectionError("模拟连接断开")
        return _GOOD_RESULT

    original = _patch_worker_context(mod, _fail_then_ok)
    try:
        payload = _make_chunk_payload(timeout=5.0, retry_max=2)
        report = _fetch_one_task_chunk(payload)

        assert call_count == 2, f"应调用 2 次（1次失败+1次重试成功），实际 {call_count}"
        error_payloads = [p for p in report["payloads"] if p.get("error")]
        assert len(error_payloads) == 0, f"重试成功后不应有 error payload，实际 {len(error_payloads)}"
        assert len(report["payloads"]) == 2
        assert report["chunk_hit_tasks"] == 2
        print(f"  [PASS] 第 1 次失败 → 第 2 次成功，{call_count} 次调用")
    finally:
        mod._ensure_worker_client_context = original


def test_3_retry_exhausted():
    """
    测试 3: 重试耗尽仍失败。
    模拟每次都抛异常，retry_max=2 → 共 3 次调用（1+2），全部失败。
    """
    import zsdtdx.parallel_fetcher as mod

    call_count = 0

    def _always_fail(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        raise RuntimeError(f"永久故障 #{call_count}")

    original = _patch_worker_context(mod, _always_fail)
    try:
        payload = _make_chunk_payload(timeout=5.0, retry_max=2)
        report = _fetch_one_task_chunk(payload)

        assert call_count == 3, f"应调用 3 次（1+2 重试），实际 {call_count}"
        assert len(report["payloads"]) == 2
        for p in report["payloads"]:
            assert p["error"] is not None, "重试耗尽后所有 tasks 应标记 error"
            assert "永久故障" in p["error"]
        assert len(report["failures"]) == 2
        print(f"  [PASS] 重试耗尽，{call_count} 次调用，所有 tasks 标记失败")
    finally:
        mod._ensure_worker_client_context = original


def test_4_timeout_with_retry():
    """
    测试 4: 超时 + 重试组合。
    模拟第 1 次卡死(超时)，第 2 次正常返回。
    验证超时触发重试、重试成功后正常返回、总耗时合理。
    """
    import zsdtdx.parallel_fetcher as mod

    call_count = 0

    def _timeout_then_ok(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            time.sleep(60)  # 第 1 次卡死
        return _GOOD_RESULT

    original = _patch_worker_context(mod, _timeout_then_ok)
    try:
        payload = _make_chunk_payload(timeout=2.0, retry_max=2)
        t0 = time.monotonic()
        report = _fetch_one_task_chunk(payload)
        elapsed = time.monotonic() - t0

        assert elapsed < 6.0, f"耗时 {elapsed:.1f}s，应在 ~2s(超时) + 即时(重试成功) 内返回"
        assert call_count == 2, f"应调用 2 次，实际 {call_count}"
        error_payloads = [p for p in report["payloads"] if p.get("error")]
        assert len(error_payloads) == 0, "重试成功后不应有 error payload"
        assert report["chunk_hit_tasks"] == 2
        print(f"  [PASS] 第 1 次超时 → 第 2 次成功，耗时 {elapsed:.2f}s")
    finally:
        mod._ensure_worker_client_context = original


def test_5_normal_no_retry():
    """
    测试 5: 正常路径不触发重试。
    模拟一次性成功，验证只调用 1 次。
    """
    import zsdtdx.parallel_fetcher as mod

    call_count = 0

    def _always_ok(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        return _GOOD_RESULT

    original = _patch_worker_context(mod, _always_ok)
    try:
        payload = _make_chunk_payload(timeout=5.0, retry_max=2)
        report = _fetch_one_task_chunk(payload)

        assert call_count == 1, f"成功路径应只调用 1 次，实际 {call_count}"
        assert len(report["payloads"]) == 2
        for p in report["payloads"]:
            assert p["error"] is None, f"成功路径不应有 error: {p['error']}"
        assert report["chunk_hit_tasks"] == 2
        print(f"  [PASS] 一次成功，无重试")
    finally:
        mod._ensure_worker_client_context = original


def test_6_never_raises():
    """
    测试 6: _fetch_one_task_chunk 永远不向上抛异常。
    模拟各种异常类型，验证函数总是正常返回 dict。
    """
    import zsdtdx.parallel_fetcher as mod

    exceptions = [
        TimeoutError("模拟超时"),
        ConnectionError("模拟连接断开"),
        OSError("模拟 IO 错误"),
        RuntimeError("模拟运行时错误"),
        ValueError("模拟值错误"),
    ]

    for exc in exceptions:
        def _raise_exc(*args, **kwargs):
            raise exc

        original = _patch_worker_context(mod, _raise_exc)
        try:
            payload = _make_chunk_payload(timeout=2.0, retry_max=0)
            report = _fetch_one_task_chunk(payload)
            assert isinstance(report, dict), f"{type(exc).__name__}: 应返回 dict"
            assert len(report["payloads"]) == 2
        finally:
            mod._ensure_worker_client_context = original

    print(f"  [PASS] 测试了 {len(exceptions)} 种异常，全部正常返回")


def test_7_abandoned_thread_does_not_block():
    """
    测试 7: 并发验证超时后孤立线程不阻塞后续调用。
    连续提交 3 个"卡死+重试成功"场景，验证它们互不阻塞。
    """
    import zsdtdx.parallel_fetcher as mod

    lock = threading.Lock()
    total_calls = 0

    def _hang_then_ok(*args, **kwargs):
        nonlocal total_calls
        with lock:
            total_calls += 1
            n = total_calls
        if n % 2 == 1:  # 奇数次卡死，偶数次成功
            time.sleep(60)
        return _GOOD_RESULT

    original = _patch_worker_context(mod, _hang_then_ok)
    try:
        t0 = time.monotonic()
        results = []

        def _run_one():
            payload = _make_chunk_payload(timeout=2.0, retry_max=1)
            return _fetch_one_task_chunk(payload)

        # 串行跑 3 次（模拟真实场景中 chunk 是串行或有限并发）
        for i in range(3):
            r = _run_one()
            results.append(r)

        elapsed = time.monotonic() - t0
        # 每次: ~2s 超时 + 即时成功 ≈ 2s，3 次 ≈ 6s，给余量 12s
        assert elapsed < 15.0, f"3 轮耗时 {elapsed:.1f}s，孤立线程不应阻塞后续调用"
        for r in results:
            error_count = sum(1 for p in r["payloads"] if p.get("error"))
            assert error_count == 0, "每轮重试后应成功"
        print(f"  [PASS] 3 轮超时+重试，总耗时 {elapsed:.2f}s，无阻塞")
    finally:
        mod._ensure_worker_client_context = original


# ─── 主入口 ──────────────────────────────────────────────

ALL_TESTS = [
    ("1. chunk 超时不阻塞",                test_1_timeout_not_blocking),
    ("2. 首次失败 → 重试成功",              test_2_retry_success),
    ("3. 重试耗尽 → 全部标记失败",          test_3_retry_exhausted),
    ("4. 超时 + 重试组合",                  test_4_timeout_with_retry),
    ("5. 正常路径不触发重试",               test_5_normal_no_retry),
    ("6. 任何异常都不向上抛",               test_6_never_raises),
    ("7. 孤立线程不阻塞后续调用",           test_7_abandoned_thread_does_not_block),
]


def main():
    print("=" * 60)
    print("chunk 级超时 + 重试改造 定向测试")
    print("=" * 60)

    passed = 0
    failed = 0
    errors = []

    for name, fn in ALL_TESTS:
        print(f"\n[TEST] {name}")
        try:
            fn()
            passed += 1
        except Exception as exc:
            failed += 1
            errors.append((name, str(exc)))
            print(f"  [FAIL] {exc}")

    print("\n" + "=" * 60)
    print(f"结果: {passed} passed, {failed} failed, {passed + failed} total")
    if errors:
        print("\n失败详情:")
        for name, err in errors:
            print(f"  - {name}: {err}")
    print("=" * 60)

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
