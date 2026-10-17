"""模块：`engine/columnar_kline.py`。

职责：列式分页、占位过滤、去重和多任务窗口复用，交付适配单独实现。
边界：复用统一客户端的市场路由与协议调用，不创建线程或公开输出模式。
"""

from __future__ import annotations

import threading

import numpy as np

from zsdtdx.engine.thread_runtime import check_deadline
from zsdtdx.params import TDXParams
from zsdtdx.parser.kline_columns import (
    KlineColumns,
    clock_text,
    columnar_decode,
    merge_pages,
)

_metadata_lock = threading.RLock()


def _resolve_route(client, task, kind):
    """输入客户端与任务，输出独立路由快照；只在目录访问阶段串行。"""
    with _metadata_lock:
        check_deadline()
        if kind == "future":
            client.get_all_future_list(return_df=True)
            code = client._normalize_future_query_code(task["code"])
            route = client._future_route.get(code)
            if route is None:
                client._refresh_future_catalog_view()
                route = client._future_route.get(code)
            if route is None:
                raise ValueError(f"期货代码未找到: {code}")
            return str(route.get("code") or code), {**route, "source": "ex"}
        if kind == "stock":
            code, route = client._resolve_stock_route_no_df(
                task["code"], task["freq"], task.get("_route_source")
            )
            return code, dict(route)
        field = "block_name" if kind == "block" else "index_name"
        if task.get("_index_route_code"):
            route = {
                "source": task["_index_route_source"],
                "market": int(task["_index_route_market"]),
                "code": task["_index_route_code"],
                "name": task.get("_index_route_name", task[field]),
            }
        else:
            route = (
                client.resolve_block_index_route(task[field])
                if kind == "block"
                else client.resolve_index_name(task[field])
            )
        return str(route["code"]), dict(route)


def _page(client, route, code, category, start, kind, qfq):
    """输入已固化路由和偏移，输出列式页；错误保留给上层重试。"""
    check_deadline()
    source = route["source"]
    pool = client.std_pool if source == "std" else client.ex_pool
    method = (
        (
            "get_security_bars"
            if kind == "stock"
            else "get_block_bars"
            if kind == "block"
            else client._std_bars_api_name(route)
        )
        if source == "std"
        else "get_instrument_bars"
    )
    size = client._kline_page_size(source)
    kwargs = {"qfq": bool(qfq)} if kind == "stock" else {}
    token = columnar_decode.set(True)
    try:
        host_group = (
            "szshbj"
            if source == "std" and int(route["market"]) == 2
            else "szsh"
            if source == "std"
            else "all"
        )
        with pool.host_scope(host_group):
            result = pool.call(
                method,
                category,
                int(route["market"]),
                code,
                start,
                size,
                allow_none=True,
                **kwargs,
            )
        if result is None:
            raise ConnectionError(
                f"{pool.name} 无可用连接：K线页请求重试耗尽，未取得合法回包"
            )
        if not isinstance(result, KlineColumns):
            raise TypeError("K线协议未返回列式页")
        return result, size
    finally:
        columnar_decode.reset(token)


def fetch_chunk_columns(client, tasks, kind="stock", qfq=True):
    """输入同一标的周期的任务，输出列式逐任务结果；不在热路径创建行对象。"""
    if not tasks:
        return {"results": [], "chunk_hit_tasks": 0, "chunk_network_page_calls": 0}
    code, route = _resolve_route(client, tasks[0], kind)
    category = client._freq_to_category(tasks[0]["freq"])
    windows = []
    for task in tasks:
        try:
            if kind == "future":
                start, end = client._normalize_future_time_window(
                    task["start_time"], task["end_time"]
                )
                # pandas 的无时区 Timestamp.timestamp 按 UTC 解释，页时间轴按本地时间。
                start, end = start.to_pydatetime(), end.to_pydatetime()
            else:
                start, end = (
                    client._to_datetime_no_df(task["start_time"]),
                    client._to_datetime_no_df(task["end_time"]),
                )
            if start > end:
                raise ValueError("start_time 不能晚于 end_time")
            windows.append((task, start.timestamp(), end.timestamp(), None))
        except (TypeError, ValueError, OverflowError) as exc:
            windows.append((task, 0, 0, str(exc)))
    valid = [w for w in windows if w[3] is None]
    pages, calls, offset = [], 0, 0
    if valid:
        boundary = min(w[1] for w in valid)
        for _ in range(TDXParams.MAX_KLINE_PAGES):
            page, size = _page(client, route, code, category, offset, kind, qfq)
            calls += 1
            if not len(page):
                break
            raw_count = len(page)
            oldest = int(np.min(page.columns["_ts"]))
            if client.output_cfg.get("filter_suspended_placeholder_bar", True):
                d = page.columns
                volume = d.get("vol", d.get("trade"))
                placeholder = (
                    (d["open"] == d["close"])
                    & (d["open"] == d["high"])
                    & (d["open"] == d["low"])
                    & (np.abs(volume) <= 1e-8)
                    & (np.abs(d["amount"]) <= 1e-8)
                )
                page = page.take(~placeholder)
            if len(page):
                pages.append(page)
            offset += raw_count
            if oldest <= boundary or raw_count < size:
                break
    # 期货公开入口遵循 DataFrame 的窗口内 quicksort + keep=last；股票保留首条。
    combined = (
        KlineColumns(
            {
                k: np.concatenate([page.columns[k] for page in pages])
                for k in pages[0].columns
            }
        )
        if kind == "future" and pages
        else merge_pages(pages)
    )
    results = []
    for task, start, end, error in windows:
        check_deadline()
        selected = None
        if combined is not None and error is None:
            time_axis = combined.columns["_ts"]
            if kind == "future":
                index = np.flatnonzero((time_axis >= start) & (time_axis <= end))
                index = index[np.argsort(time_axis[index], kind="quicksort")]
                if len(index):
                    axis = time_axis[index]
                    index = index[np.r_[axis[1:] != axis[:-1], True]]
                selected = combined.take(index)
            else:
                lo = int(np.searchsorted(time_axis, start, side="left"))
                hi = int(np.searchsorted(time_axis, end, side="right"))
                selected = combined.take(slice(lo, hi))
        results.append(
            {
                "task": task,
                "columns": selected,
                "route": route,
                "code": code,
                "kind": kind,
                "error": error
                or (None if selected is not None and len(selected) else "no_data"),
            }
        )
    return {
        "results": results,
        "chunk_hit_tasks": max(0, len(valid) - 1),
        "chunk_network_page_calls": calls,
    }


def materialize_task_columns(client, item):
    """输入列式任务结果，输出既有 rows；仅在交付边缘生成字典和时间文本。"""
    page = item.get("columns")
    if page is None:
        return []
    task, route, kind = item["task"], item["route"], item["kind"]
    data, result = page.columns, []
    if kind == "future":
        import pandas as pd

        return pd.DataFrame(
            {
                "code": str(item["code"]).upper(),
                "freq": client._normalize_freq(task["freq"]),
                **{name: data[name] for name in ("open", "close", "high", "low")},
                "settlement_price": data["price"],
                "volume": data["trade"].astype(np.int64),
                "datetime": pd.to_datetime([clock_text(v) for v in data["_clock"]]),
            }
        )
    if kind == "stock":
        identity = {
            "code": client._stock_code_with_prefix_no_df(
                route["source"], int(route["market"]), item["code"]
            )
        }
    else:
        field = "block_name" if kind == "block" else "index_name"
        identity = {field: str(route.get("name") or task[field])}
        if kind != "block":
            identity["code"] = item["code"]
    frequency = client._normalize_freq(task["freq"])
    opens, closes, highs, lows = (
        data[name] for name in ("open", "close", "high", "low")
    )
    volumes, amounts, clocks = (
        data.get("vol", data.get("trade")),
        data["amount"],
        data["_clock"],
    )
    clamp_amount = kind == "stock" and route["source"] == "ex"
    for i in range(len(page)):
        amount = int(amounts[i])
        result.append(
            {
                **identity,
                "freq": frequency,
                "open": float(opens[i]),
                "close": float(closes[i]),
                "high": float(highs[i]),
                "low": float(lows[i]),
                "volume": int(volumes[i]),
                "amount": max(0, amount) if clamp_amount else amount,
                "datetime": clock_text(clocks[i]),
            }
        )
    return result
