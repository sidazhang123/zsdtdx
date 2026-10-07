"""真实探测扩展行情 0x2422 码表分页长度与空页终止行为。"""

from __future__ import annotations

import json
from pathlib import Path

import zsdtdx
import yaml

from zsdtdx.net.exhq import TdxExHq_API
from zsdtdx.params import TDXParams


def _probe_kline_terminal(
    api: TdxExHq_API, *, market: int, code: str
) -> dict[str, object]:
    """连续请求扩展日线，验证末短页的下一页确实为空。"""
    page_size = int(TDXParams.MAX_EXTENDED_KLINE_COUNT)
    start = 0
    lengths: list[int] = []
    for _ in range(40):
        page = api.get_instrument_bars(
            TDXParams.KLINE_TYPE_DAILY,
            market,
            code,
            start,
            page_size,
        )
        if page is None:
            raise RuntimeError(f"扩展K线分页失败: {market}#{code}, start={start}")
        length = len(page)
        lengths.append(length)
        if length == 0:
            return {"page_lengths": lengths, "terminal": "empty"}
        if length < page_size:
            after = api.get_instrument_bars(
                TDXParams.KLINE_TYPE_DAILY,
                market,
                code,
                start + length,
                page_size,
            )
            if after is None:
                raise RuntimeError(
                    f"扩展K线短页后验证失败: {market}#{code}, start={start + length}"
                )
            return {
                "page_lengths": lengths,
                "terminal": "short",
                "page_after_short_length": len(after),
            }
        start += length
    raise RuntimeError(f"扩展K线超过40页仍未结束: {market}#{code}")


def _probe_host(host: str) -> dict[str, object]:
    """探测单个扩展行情节点，返回完整分页长度摘要。"""
    hostname, port = host.rsplit(":", 1)
    api = TdxExHq_API()
    pages: list[dict[str, int]] = []
    with api.connect(hostname, int(port)):
        advertised = api.get_instrument_count()
        start = 0
        for _ in range(200):
            page = api.get_instrument_info(start, 800)
            if page is None:
                raise RuntimeError(f"扩展码表分页失败: host={host}, start={start}")
            length = len(page)
            pages.append({"start": start, "length": length})
            if length == 0:
                break
            start += length
        else:
            raise RuntimeError(f"扩展码表超过 200 页仍未返回空页: host={host}")
        kline_cases = {
            "index_62_932000": _probe_kline_terminal(api, market=62, code="932000"),
            "future_30_CUL8": _probe_kline_terminal(api, market=30, code="CUL8"),
        }

    nonempty = [item["length"] for item in pages if item["length"] > 0]
    assert nonempty and pages[-1]["length"] == 0
    return {
        "host": host,
        "advertised_count": advertised,
        "downloaded_count": sum(nonempty),
        "page_count_with_data": len(nonempty),
        "page_lengths": nonempty,
        "empty_page_start": pages[-1]["start"],
        "all_nonfinal_pages_same_size": len(set(nonempty[:-1])) <= 1,
        "any_nonempty_page_below_800": any(length < 800 for length in nonempty),
        "kline_cases": kline_cases,
    }


def main() -> None:
    """逐节点连续请求真实扩展码表，输出每页长度、总量与首个空页偏移。"""
    config_path = Path(zsdtdx.__file__).with_name("config.yaml")
    config = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    hosts = list((config.get("hosts") or {}).get("extended") or [])
    if not hosts:
        raise RuntimeError("默认配置未提供扩展行情节点")
    results = [_probe_host(str(host)) for host in hosts]
    print(
        json.dumps(
            {"hosts": results},
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
