"""模块：`util/company_info_state.py`。

职责：规范化公司信息请求代码并统一逐股终态、事件及失败记录。
输入：代码序列、正文或最终异常；输出：规范代码及可追踪终态。
边界：不发网络请求、不执行重试，不判断证券资格，不依赖业务或引擎模块。
"""

import json
from concurrent.futures import CancelledError
from typing import Any, Dict, List


def normalize_company_info_codes(codes) -> List[str]:
    """输入代码序列，输出去前缀、去空白且保序去重代码；边界：空项跳过。"""
    normalized = []
    seen = set()
    for item in codes or []:
        code = str(item).strip()
        if "." in code:
            code = code.split(".", 1)[1].strip()
        if code and code not in seen:
            normalized.append(code)
            seen.add(code)
    return normalized


def company_info_outcome(code, rows=None, error=None) -> Dict[str, Any]:
    """输入单股正文或最终异常，输出唯一终态；空结果仅表示本次无数据。"""
    part_rows = list(rows or [])
    if error is None and part_rows:
        return {"code": code, "rows": part_rows, "error": None, "status": "success"}
    if error is None:
        return {
            "code": code,
            "rows": [],
            "error": "no_data: 本次目录为空或无匹配栏目",
            "status": "no_data",
            "reason": "no_data",
        }
    cancelled = (
        isinstance(error, CancelledError)
        or getattr(error, "reason", None) == "cancelled"
        or str(error) == "fetcher restarted or destroyed"
    )
    reason = getattr(error, "reason", None) or (
        "cancelled"
        if cancelled
        else "timeout"
        if isinstance(error, TimeoutError)
        else "fetch_failed"
    )
    result = {
        "code": code,
        "rows": [],
        "error": str(error) or type(error).__name__,
        "status": "cancelled" if cancelled else "failed",
        "reason": reason,
    }
    detail = getattr(error, "detail", None)
    if detail is not None:
        result["detail"] = detail
    return result


def publish_company_info_outcome(outcome, queue) -> None:
    """输入终态和可选队列，输出无；成功事件保持原字段，失败附状态与原因。"""
    if queue is None:
        return
    event = {
        "event": "data",
        "code": outcome["code"],
        "rows": outcome["rows"],
        "error": outcome["error"],
    }
    if outcome["status"] != "success":
        event.update(
            {
                key: value
                for key, value in outcome.items()
                if key in {"status", "reason", "detail"}
            }
        )
    queue.put(event)


def record_company_info_outcome(client, outcome) -> None:
    """输入客户端与最终状态，输出无；仅最终失败写运行态，恢复成功不写失败。"""
    recorder = getattr(client, "record_runtime_failure", None)
    if outcome["status"] != "success" and callable(recorder):
        detail = outcome["error"]
        if "detail" in outcome:
            detail = json.dumps(
                {"error": outcome["error"], "detail": outcome["detail"]},
                ensure_ascii=False,
                default=str,
            )
        recorder("company_info", outcome["code"], outcome["reason"], detail)
