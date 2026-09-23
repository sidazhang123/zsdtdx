"""离线验收码表磁盘缓存的序列化、日期失效与 kind 校验。"""

from pathlib import Path

import pytest

from zsdtdx.catalog_disk_cache import (
    KIND_ETF,
    KIND_EX,
    KIND_STD,
    catalog_cache_file_path,
    load_catalog_cache,
    load_etf_catalog_cache,
    save_catalog_cache,
    save_etf_catalog_cache,
    validate_catalog_payload,
    validate_etf_catalog_payload,
)


def test_catalog_cache_roundtrip(tmp_path: Path):
    path = catalog_cache_file_path(tmp_path, KIND_STD)
    records = [{"market": 1, "code": "600000", "name": "浦发银行"}]
    save_catalog_cache(
        path,
        kind=KIND_STD,
        cache_date="2026-09-17",
        records=records,
    )
    loaded = load_catalog_cache(path, KIND_STD, expected_cache_date="2026-09-17")
    assert loaded is not None
    cache_date, rows, names = loaded
    assert cache_date == "2026-09-17"
    assert rows[0]["code"] == "600000"
    assert names == {}


def test_catalog_cache_rejects_wrong_date_and_kind(tmp_path: Path):
    path = catalog_cache_file_path(tmp_path, KIND_EX)
    save_catalog_cache(
        path,
        kind=KIND_EX,
        cache_date="2026-09-17",
        records=[{"market": 30, "code": "CUL8", "name": "沪铜主连"}],
        market_names={30: "上海期货"},
    )
    assert load_catalog_cache(path, KIND_EX, expected_cache_date="2026-09-16") is None
    assert not path.exists()


def test_validate_catalog_payload_requires_kind_and_records():
    assert validate_catalog_payload({"format_version": 1}, KIND_STD) is None
    payload = {
        "format_version": 3,
        "kind": "std",
        "cache_date": "2026-09-17",
        "records": [{"market": 0, "code": "000001", "name": "平安银行"}],
        "market_names": {},
    }
    validated = validate_catalog_payload(payload, KIND_STD)
    assert validated is not None
    assert validated[1][0]["code"] == "000001"


def test_etf_catalog_cache_roundtrip(tmp_path: Path):
    """输入合法名称与板块。输出当日可加载。用途：ETF 落盘。边界：空列表拒绝。"""
    path = catalog_cache_file_path(tmp_path, KIND_ETF)
    save_etf_catalog_cache(
        path,
        cache_date="2026-09-21",
        name_records=[{"market": 0, "code": "159105", "name": "恒生生物科技ETF易方达"}],
        board_records=[
            {"market": 0, "code": "159105"},
            {"market": 1, "code": "510050"},
        ],
    )
    loaded = load_etf_catalog_cache(path, expected_cache_date="2026-09-21")
    assert loaded is not None
    cache_date, names, boards = loaded
    assert cache_date == "2026-09-21"
    assert names[0]["code"] == "159105"
    assert {row["code"] for row in boards} == {"159105", "510050"}
    assert load_etf_catalog_cache(path, expected_cache_date="2026-09-20") is None
    assert not path.exists()


def test_etf_catalog_cache_rejects_empty_lists(tmp_path: Path):
    """输入空板块。输出保存抛错。用途：禁止空快照落盘。边界：校验失败不写文件。"""
    path = catalog_cache_file_path(tmp_path, KIND_ETF)
    with pytest.raises(ValueError):
        save_etf_catalog_cache(
            path,
            cache_date="2026-09-21",
            name_records=[{"market": 0, "code": "159105", "name": "x"}],
            board_records=[],
        )
    assert not path.exists()
    assert (
        validate_etf_catalog_payload(
            {
                "format_version": 2,
                "kind": "etf",
                "cache_date": "2026-09-21",
                "name_records": [],
                "board_records": [{"market": 0, "code": "159105"}],
            }
        )
        is None
    )
