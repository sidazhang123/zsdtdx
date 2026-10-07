"""真实行情缓存生命周期验收：冷建、命中、过期重建与共享复用。"""

from __future__ import annotations

import datetime as dt
import json
import tempfile
import time
from pathlib import Path
from typing import Any, Dict

import zsdtdx
from zsdtdx.cache.block_file_cache import (
    BLOCK_FILE_TTL_SECONDS,
    load_fresh_block_files,
    save_block_files,
)
from zsdtdx.cache.catalog_disk_cache import (
    KIND_ETF,
    KIND_EX,
    KIND_STD,
    catalog_cache_file_path,
    load_catalog_cache,
    load_etf_catalog_cache,
    save_catalog_cache,
    save_etf_catalog_cache,
)
from zsdtdx.engine.unified_client import UnifiedTdxClient


DEFAULT_CONFIG = Path(zsdtdx.__file__).with_name("config.yaml")


def _client(cache_dir: Path) -> UnifiedTdxClient:
    """创建真实连接客户端，并把本轮缓存隔离到临时目录。"""
    client = UnifiedTdxClient(config_path=str(DEFAULT_CONFIG))
    client._catalog_cache_enabled = True
    client._catalog_cache_dir = cache_dir
    return client


def _forbid_download(*args: Any, **kwargs: Any) -> Any:
    """缓存命中阶段的网络哨兵：任何重拉都会使验收失败。"""
    raise AssertionError(f"有效缓存存在时发生了重复下载: args={args!r}")


def _load_all_cache_payloads(cache_dir: Path, today: str) -> Dict[str, Any]:
    """加载四类刚生成的快照，并断言格式与日期均完整。"""
    std = load_catalog_cache(
        catalog_cache_file_path(cache_dir, KIND_STD),
        KIND_STD,
        expected_cache_date=today,
    )
    ex = load_catalog_cache(
        catalog_cache_file_path(cache_dir, KIND_EX),
        KIND_EX,
        expected_cache_date=today,
    )
    etf = load_etf_catalog_cache(
        catalog_cache_file_path(cache_dir, KIND_ETF),
        expected_cache_date=today,
    )
    block = load_fresh_block_files(cache_dir=cache_dir)
    assert std is not None and std[1], "标准行情码表未完整落盘"
    assert ex is not None and ex[1], "扩展行情码表未完整落盘"
    assert etf is not None and etf[1] and etf[2], "ETF 名称/成分快照未完整落盘"
    assert block is not None and block["files"], "板块三文件快照未完整落盘"
    return {"std": std, "ex": ex, "etf": etf, "block": block}


def main() -> None:
    """执行真实冷建、零网络命中、人工过期后真实重建三阶段验收。"""
    started = time.perf_counter()
    today = dt.date.today().isoformat()
    yesterday = (dt.date.today() - dt.timedelta(days=1)).isoformat()
    with tempfile.TemporaryDirectory(prefix="zsdtdx_cache_acceptance_") as raw_dir:
        cache_dir = Path(raw_dir)

        # 阶段一：真实服务器冷启动，覆盖 std/ex 分页、ETF 多文件与板块三文件。
        first = _client(cache_dir)
        try:
            with first:
                stock_names = first.get_stock_code_name_map()
                futures = first.get_all_future_list(return_df=False)
                etf_names = first.get_etf_code_name_map()
                block_names = first.get_block_names()
                concepts = first.get_stock_concepts()
        finally:
            first.close()
        assert stock_names, "真实股票码表为空"
        assert futures, "真实商品期货列表为空"
        assert etf_names, "真实 ETF/LOF 列表为空"
        assert block_names, "真实板块指数目录为空"
        assert concepts.get("names"), "真实股票板块归属为空"
        payloads = _load_all_cache_payloads(cache_dir, today)

        # 阶段二：新客户端只准读盘；任一缓存漏用都会触发下载哨兵。
        second = _client(cache_dir)
        second._download_std_security_catalog = _forbid_download
        second._download_ex_instrument_catalog = _forbid_download
        second._download_named_hq_file = _forbid_download
        second._download_block_named_files = _forbid_download
        try:
            assert second.get_stock_code_name_map() == stock_names
            assert second.get_all_future_list(return_df=False) == futures
            assert second.get_etf_code_name_map() == etf_names
            assert second.get_block_names() == block_names
            cached_concepts = second.get_stock_concepts()
            assert cached_concepts == concepts
        finally:
            second.close()

        # 阶段三：把四类缓存显式改成过期，再由新客户端走真实网络重建。
        std_date, std_rows, _ = payloads["std"]
        ex_date, ex_rows, ex_market_names = payloads["ex"]
        etf_date, etf_name_rows, etf_board_rows = payloads["etf"]
        assert std_date == ex_date == etf_date == today
        save_catalog_cache(
            catalog_cache_file_path(cache_dir, KIND_STD),
            kind=KIND_STD,
            cache_date=yesterday,
            records=std_rows,
        )
        save_catalog_cache(
            catalog_cache_file_path(cache_dir, KIND_EX),
            kind=KIND_EX,
            cache_date=yesterday,
            records=ex_rows,
            market_names=ex_market_names,
        )
        save_etf_catalog_cache(
            catalog_cache_file_path(cache_dir, KIND_ETF),
            cache_date=yesterday,
            name_records=etf_name_rows,
            board_records=etf_board_rows,
        )
        save_block_files(
            payloads["block"]["files"],
            fetched_at=time.time() - BLOCK_FILE_TTL_SECONDS - 60,
            cache_dir=cache_dir,
        )

        third = _client(cache_dir)
        try:
            with third:
                rebuilt_stocks = third.get_stock_code_name_map()
                rebuilt_futures = third.get_all_future_list(return_df=False)
                rebuilt_etf = third.get_etf_code_name_map()
                rebuilt_blocks = third.get_block_names()
        finally:
            third.close()
        assert rebuilt_stocks and rebuilt_futures and rebuilt_etf and rebuilt_blocks
        rebuilt = _load_all_cache_payloads(cache_dir, today)
        assert rebuilt["block"]["fetched_at"] > payloads["block"]["fetched_at"]

        print(
            json.dumps(
                {
                    "status": "passed",
                    "cache_dir_isolated": True,
                    "stock_count": len(rebuilt_stocks),
                    "future_count": len(rebuilt_futures),
                    "etf_count": len(rebuilt_etf),
                    "block_count": len(rebuilt_blocks),
                    "concept_count": len(concepts["names"]),
                    "elapsed_seconds": round(time.perf_counter() - started, 3),
                },
                ensure_ascii=False,
                indent=2,
            )
        )


if __name__ == "__main__":
    main()
