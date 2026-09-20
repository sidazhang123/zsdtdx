# -*- coding: utf-8 -*-
"""
模块：`test_etf_code_name.py`。

职责：
1. 离线验收 get_etf_code_name_map 名称 include/drop 与 399 排除。
2. 确认 A 股口径 _is_a_share_std 未因 ETF 配置变宽。

边界：
1. 不访问网络；mock std 码表。
"""

from zsdtdx.unified_client import UnifiedTdxClient


def _etf_client() -> UnifiedTdxClient:
    """输入：无。输出：带 ETF 规则的 mock 客户端。用途：离线单测。边界：无真实连接。"""
    client = UnifiedTdxClient.__new__(UnifiedTdxClient)
    client.market_rules = {
        "stock_prefix_sz": ["000", "001", "002", "003", "300"],
        "stock_prefix_sh": ["600", "601", "603", "605"],
        "etf_name_drop_substr": ["债", "货币", "增强", "红利", "现金流"],
    }
    client._std_catalog_records = [
        {"market": 0, "code": "159915", "name": "创业板ETF"},
        {"market": 1, "code": "510300", "name": "沪深300ETF"},
        {"market": 0, "code": "161725", "name": "白酒LOF"},
        {"market": 0, "code": "159001", "name": "货币ETF"},
        {"market": 1, "code": "511010", "name": "国债ETF"},
        {"market": 0, "code": "159992", "name": "银华增强ETF"},
        {"market": 0, "code": "399306", "name": "国证ETF"},
        {"market": 0, "code": "000001", "name": "平安银行"},
        {"market": 2, "code": "920000", "name": "某北交ETF"},
    ]
    client._std_catalog_date = "2099-01-01"
    client._ex_catalog_records = []
    client._catalog_cache_enabled = False

    def _ensure(*, need_std=False, need_ex=False, refresh=False):
        """输入：ensure 参数。输出：无。用途：跳过网络拉表。边界：仅占位。"""
        return None

    client.ensure_code_catalog = _ensure  # type: ignore[method-assign]
    return client


def test_get_etf_code_name_map_include_drop_and_prefix():
    """输入：mock 码表。输出：断言。用途：保留 ETF/LOF、剔除 drop、前缀正确。边界：离线。"""
    client = _etf_client()
    result = client.get_etf_code_name_map(use_cache=True)
    assert result["sz.159915"] == "创业板ETF"
    assert result["sh.510300"] == "沪深300ETF"
    assert result["sz.161725"] == "白酒LOF"
    assert "sz.159001" not in result
    assert "sh.511010" not in result
    assert "sz.159992" not in result
    assert "sz.399306" not in result
    assert "sz.000001" not in result
    assert "bj.920000" not in result
    assert "920000" not in result


def test_a_share_std_unchanged_by_etf_rules():
    """输入：mock 客户端。输出：断言。用途：15/51 段仍非 A 股。边界：离线。"""
    client = _etf_client()
    assert client._is_a_share_std(0, "000001") is True
    assert client._is_a_share_std(0, "159915") is False
    assert client._is_a_share_std(1, "510300") is False
    assert client._is_listed_std_stock(0, "159915") is False
