"""真实探测各分页协议的满页、末短页与后继空页行为。"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Callable

from zsdtdx import get_client
from zsdtdx.params import TDXParams


def _probe_until_terminal(
    fetch_page: Callable[[int, int], Any],
    *,
    page_size: int,
    max_pages: int,
) -> dict[str, Any]:
    """连续翻页；遇短页时额外请求下一页，确认短页后是否确实为空。"""
    start = 0
    lengths: list[int] = []
    short_page_start: int | None = None
    page_after_short_length: int | None = None
    for _ in range(max_pages):
        page = fetch_page(start, page_size)
        if page is None:
            raise RuntimeError(f"分页请求失败: start={start}, count={page_size}")
        length = len(page)
        lengths.append(length)
        if length == 0:
            break
        if length < page_size:
            short_page_start = start
            after = fetch_page(start + length, page_size)
            if after is None:
                raise RuntimeError(
                    f"短页后的验证请求失败: start={start + length}, count={page_size}"
                )
            page_after_short_length = len(after)
            break
        start += length
    else:
        raise RuntimeError(f"超过 {max_pages} 页仍未到达边界")

    nonempty = [length for length in lengths if length > 0]
    return {
        "page_size": page_size,
        "page_lengths": lengths,
        "downloaded_before_terminal_check": sum(nonempty),
        "short_page_start": short_page_start,
        "page_after_short_length": page_after_short_length,
        "short_page_proven_terminal": (
            short_page_start is not None and page_after_short_length == 0
        ),
        "ended_by_empty_page": bool(lengths and lengths[-1] == 0),
    }


def _pick_future_route(client: Any) -> dict[str, Any]:
    """从真实扩展码表选择有长期历史的商品期货连续合约。"""
    records = client.get_all_future_list(return_df=False)
    preferred = ("CUL8", "AUL8", "RBL8", "MAL8")
    by_code = {str(item.get("code", "")).upper(): item for item in records}
    for code in preferred:
        if code in by_code:
            return dict(by_code[code])
    for item in records:
        code = str(item.get("code", "")).upper()
        if code.endswith("L8"):
            return dict(item)
    if not records:
        raise RuntimeError("真实期货码表为空")
    return dict(records[0])


def main() -> None:
    """探测标准码表、三类标准 K 线、扩展 K 线与统计版面。"""
    results: dict[str, Any] = {}
    with get_client() as client:
        security_page = int(TDXParams.MAX_SECURITY_LIST_COUNT)
        for market in (0, 1, 2):
            results[f"std_security_list_market_{market}"] = _probe_until_terminal(
                lambda start, count, market=market: client.std_pool.call(
                    "get_security_list",
                    market,
                    start,
                    count,
                    allow_none=True,
                ),
                page_size=security_page,
                max_pages=20,
            )

        category = int(TDXParams.KLINE_TYPE_DAILY)
        std_kline_page = int(TDXParams.MAX_KLINE_COUNT)
        results["std_stock_daily_600000"] = _probe_until_terminal(
            lambda start, count: client.std_pool.call(
                "get_security_bars",
                category,
                1,
                "600000",
                start,
                count,
                allow_none=True,
            ),
            page_size=std_kline_page,
            max_pages=30,
        )
        results["std_stock_daily_688981"] = _probe_until_terminal(
            lambda start, count: client.std_pool.call(
                "get_security_bars",
                category,
                1,
                "688981",
                start,
                count,
                allow_none=True,
            ),
            page_size=std_kline_page,
            max_pages=30,
        )
        results["std_index_daily_399001"] = _probe_until_terminal(
            lambda start, count: client.std_pool.call(
                "get_index_bars",
                category,
                0,
                "399001",
                start,
                count,
                allow_none=True,
            ),
            page_size=std_kline_page,
            max_pages=30,
        )

        block_name = client.get_block_names()[0]
        block_route = client.resolve_block_index_route(block_name)
        block_api = client._std_bars_api_name(block_route)
        results[f"std_block_daily_{block_route['code']}"] = _probe_until_terminal(
            lambda start, count: client.std_pool.call(
                block_api,
                category,
                int(block_route["market"]),
                str(block_route["code"]),
                start,
                count,
                allow_none=True,
            ),
            page_size=std_kline_page,
            max_pages=30,
        )

        ex_kline_page = int(TDXParams.MAX_EXTENDED_KLINE_COUNT)
        index_route = client.resolve_index_name("中证2000")
        if str(index_route.get("source")) != "ex":
            raise RuntimeError(f"中证2000未路由到扩展行情: {index_route}")
        results[f"ex_index_daily_{index_route['code']}"] = _probe_until_terminal(
            lambda start, count: client.ex_pool.call(
                "get_instrument_bars",
                category,
                int(index_route["market"]),
                str(index_route["code"]),
                start,
                count,
                allow_none=True,
            ),
            page_size=ex_kline_page,
            max_pages=30,
        )

        future_route = _pick_future_route(client)
        results[f"ex_future_daily_{future_route['code']}"] = _probe_until_terminal(
            lambda start, count: client.ex_pool.call(
                "get_instrument_bars",
                category,
                int(future_route["market"]),
                str(future_route["code"]),
                start,
                count,
                allow_none=True,
            ),
            page_size=ex_kline_page,
            max_pages=30,
        )

        results["std_board_quote"] = _probe_until_terminal(
            lambda start, count: client.std_pool.call(
                "get_board_quote_page",
                start,
                count,
                allow_none=True,
            ),
            page_size=80,
            max_pages=200,
        )

        end_text = datetime.now().strftime("%Y-%m-%d 23:59:59")
        stock_task = {
            "code": "688981",
            "freq": "d",
            "start_time": "2000-01-01 00:00:00",
            "end_time": end_text,
        }
        stock_chunk = client.get_stock_kline_rows_for_chunk_tasks(
            tasks=[dict(stock_task), dict(stock_task)],
            enable_cache=True,
        )
        results["integrated_stock_chunk_terminal_cache"] = {
            "network_page_calls": stock_chunk["chunk_network_page_calls"],
            "cache_hit_tasks": stock_chunk["chunk_hit_tasks"],
            "row_counts": [len(item["rows"]) for item in stock_chunk["results"]],
        }

        index_task = {
            "index_name": "中证2000",
            "freq": "d",
            "start_time": "2000-01-01 00:00:00",
            "end_time": end_text,
            "_index_route_source": str(index_route["source"]),
            "_index_route_market": int(index_route["market"]),
            "_index_route_code": str(index_route["code"]),
            "_index_route_name": str(index_route["name"]),
        }
        index_chunk = client.get_index_kline_rows_for_chunk_tasks(
            tasks=[dict(index_task), dict(index_task)],
            enable_cache=True,
        )
        results["integrated_index_chunk_terminal_cache"] = {
            "network_page_calls": index_chunk["chunk_network_page_calls"],
            "cache_hit_tasks": index_chunk["chunk_hit_tasks"],
            "row_counts": [len(item["rows"]) for item in index_chunk["results"]],
        }
        if stock_chunk["chunk_hit_tasks"] < 1 or index_chunk["chunk_hit_tasks"] < 1:
            raise AssertionError("末短页后的同窗任务未命中 chunk 缓存")

    print(json.dumps(results, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
