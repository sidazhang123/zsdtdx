"""
模块：tests/manual/weaknet_inject_retry.py

职责：
1. 离线注入"socket 延迟 + RST"故障，验证 chunk attempt timeout 路径下：
   - 重置目标按 chunk 路由（stock=std-only / index=按 _index_route_source）；
   - _log_chunk_retry 不再二次触发 recover；
2. 与压测脚本分离，便于在沙箱内重复执行。

使用：
    py tests/manual/weaknet_inject_retry.py
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from typing import Any, Dict, List
from unittest import mock

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
_SRC_DIR = os.path.join(_PROJECT_ROOT, "src")
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)


# ---------------------------------------------------------------------------
# Scenario 1: stock chunk 全程超时 → 仅 std 边被重置
# ---------------------------------------------------------------------------

def _make_minimal_prep(task_kind: str, normalized_tasks: List[Dict[str, Any]]) -> Dict[str, Any]:
    """构造一个绕过 _prepare_one_task_chunk 的最小 prep 字典，让 _fetch_one_task_chunk_async 直接进入主流程。"""
    return {
        "task_kind": task_kind,
        "profile": {"kind": task_kind, "symbol_field": "code", "build_payload": lambda **kw: {}, "log_tag": "chunk"},
        "symbol_field": "code",
        "build_payload": lambda **kw: {"event": "data", "task": kw.get("task", {}), "rows": [], "error": kw.get("error")},
        "log_tag": "chunk",
        "worker_pid": 99999,
        "chunk_id": "wn_synthetic",
        "raw_tasks": [{}],
        "enable_cache": False,
        "task_detail": [],
        "normalized_tasks": normalized_tasks,
        "payloads": [],
        "failures": [],
        "reconnect_on_unavailable": True,
        "chunk_timeout": 0.05,
        "chunk_retry_max": 1,
    }


def scenario_stock_chunk_timeout_resets_std_only() -> Dict[str, Any]:
    """模拟 stock chunk 每次尝试都超时，验证 _recover_worker_pools_current_thread 仅以 target='std' 被调用。"""
    import zsdtdx.parallel_fetcher as pf

    recover_calls: List[Dict[str, Any]] = []

    def _fake_recover(reason: str = "", target: str = "both"):
        recover_calls.append({"reason": reason, "target": target})
        return {"std": {"ok": True}, "ex": {"ok": False, "error": "skipped"}, "target": target}

    def _hang_attempt(prep):
        import time
        # 比 chunk_timeout 慢很多，强制 asyncio.wait_for 触发 TimeoutError
        time.sleep(0.5)
        return {"results": []}

    prep = _make_minimal_prep("stock", [{"code": "000001", "freq": "d"}])

    async def _run():
        with mock.patch.object(pf, "_prepare_one_task_chunk", return_value=prep), \
             mock.patch.object(pf, "_fetch_one_chunk_fetch_attempt", side_effect=_hang_attempt), \
             mock.patch.object(pf, "_recover_worker_pools_current_thread", side_effect=_fake_recover), \
             mock.patch.object(pf, "_emit_log", new=lambda *a, **kw: None):
            return await pf._fetch_one_task_chunk_async({"chunk_id": "wn_stock"})

    asyncio.run(_run())

    return {
        "scenario": "stock_chunk_timeout",
        "recover_calls": recover_calls,
        "ok_at_least_one_recover": len(recover_calls) >= 1,
        "ok_target_only_std": all(c["target"] == "std" for c in recover_calls) if recover_calls else False,
        "ok_no_ex_reset": not any(c["target"] in {"ex", "both"} for c in recover_calls),
    }


# ---------------------------------------------------------------------------
# Scenario 2: index chunk (源=ex) 全程超时 → 仅 ex 边被重置
# ---------------------------------------------------------------------------

def scenario_index_chunk_ex_source_resets_ex_only() -> Dict[str, Any]:
    """模拟 index chunk (_index_route_source='ex') 超时，验证 target='ex'。"""
    import zsdtdx.parallel_fetcher as pf

    recover_calls: List[Dict[str, Any]] = []

    def _fake_recover(reason: str = "", target: str = "both"):
        recover_calls.append({"reason": reason, "target": target})
        return {"std": {"ok": False, "error": "skipped"}, "ex": {"ok": True}, "target": target}

    def _hang_attempt(prep):
        import time
        time.sleep(0.5)
        return {"results": []}

    prep = _make_minimal_prep(
        "index",
        [{"index_name": "BTC指数", "freq": "d", "_index_route_source": "ex"}],
    )

    async def _run():
        with mock.patch.object(pf, "_prepare_one_task_chunk", return_value=prep), \
             mock.patch.object(pf, "_fetch_one_chunk_fetch_attempt", side_effect=_hang_attempt), \
             mock.patch.object(pf, "_recover_worker_pools_current_thread", side_effect=_fake_recover), \
             mock.patch.object(pf, "_emit_log", new=lambda *a, **kw: None):
            return await pf._fetch_one_task_chunk_async({"chunk_id": "wn_index_ex"})

    asyncio.run(_run())
    return {
        "scenario": "index_chunk_ex_source_timeout",
        "recover_calls": recover_calls,
        "ok_at_least_one_recover": len(recover_calls) >= 1,
        "ok_target_only_ex": all(c["target"] == "ex" for c in recover_calls) if recover_calls else False,
        "ok_no_std_reset": not any(c["target"] in {"std", "both"} for c in recover_calls),
    }


# ---------------------------------------------------------------------------
# Scenario 3: 连接不可用错误 → _log_chunk_retry 不再二次 recover
# ---------------------------------------------------------------------------

def scenario_connection_unavailable_no_second_recover() -> Dict[str, Any]:
    """模拟 _log_chunk_retry 收到连接不可用错误时不再二次触发 _recover_*。"""
    import zsdtdx.parallel_fetcher as pf

    recover_calls: List[Dict[str, Any]] = []
    emit_calls: List[Dict[str, Any]] = []

    def _fake_recover(reason: str = "", target: str = "both"):
        recover_calls.append({"reason": reason, "target": target})
        return {}

    def _fake_emit(level: str, msg: str, detail: Dict[str, Any] = None):  # type: ignore
        emit_calls.append({"level": level, "msg": msg, "detail": detail})

    with mock.patch.object(pf, "_recover_worker_pools_current_thread", side_effect=_fake_recover), \
         mock.patch.object(pf, "_emit_log", side_effect=_fake_emit), \
         mock.patch.object(pf, "_is_connection_unavailable_error", new=lambda txt: True):
        pf._log_chunk_retry(
            {
                "log_tag": "chunk",
                "chunk_id": "wn_log",
                "raw_tasks": [{}, {}],
                "chunk_retry_max": 2,
                "reconnect_on_unavailable": True,
            },
            retry_count=1,
            error_text="connection lost",
        )

    return {
        "scenario": "connection_unavailable_no_second_recover",
        "recover_calls": recover_calls,
        "emit_call_stage": (emit_calls[0]["detail"]["stage"] if emit_calls else None),
        "ok_zero_recover": len(recover_calls) == 0,
        "ok_only_log": len(emit_calls) == 1,
    }


# ---------------------------------------------------------------------------
# 入口
# ---------------------------------------------------------------------------

def main() -> int:
    """运行三种弱网注入场景，输出 JSON 摘要；任一 sanity 失败返回非零退出码。"""
    scenarios = [
        scenario_stock_chunk_timeout_resets_std_only,
        scenario_index_chunk_ex_source_resets_ex_only,
        scenario_connection_unavailable_no_second_recover,
    ]
    results = []
    all_ok = True
    for fn in scenarios:
        try:
            outcome = fn()
        except Exception as exc:  # pragma: no cover - 防御性兜底
            outcome = {"scenario": fn.__name__, "error": str(exc), "ok": False}
            all_ok = False
        results.append(outcome)
        # 检查所有 ok_* 字段
        for key, value in outcome.items():
            if key.startswith("ok_") and not value:
                all_ok = False
                outcome["_failure_keys"] = outcome.get("_failure_keys", []) + [key]

    print(json.dumps({"summary_ok": all_ok, "scenarios": results}, ensure_ascii=False, indent=2))
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
