"""zsdtdx 对外导出入口。"""

from zsdtdx.simple_api import (
    StockKlineTask,
    get_all_future_list,
    get_client,
    get_company_info,
    get_future_kline,
    get_future_latest_price,
    get_runtime_failures,
    get_runtime_metadata,
    get_stock_code_name,
    get_stock_kline,
    get_stock_latest_price,
    get_supported_markets,
)
from zsdtdx.parallel_fetcher import StockKlineJob
from zsdtdx.unified_client import UnifiedTdxClient

__version__ = "1.1.0"

__all__ = [
    "__version__",
    "UnifiedTdxClient",
    "StockKlineTask",
    "StockKlineJob",
    "get_client",
    "get_supported_markets",
    "get_stock_code_name",
    "get_all_future_list",
    "get_stock_kline",
    "get_future_kline",
    "get_company_info",
    "get_stock_latest_price",
    "get_future_latest_price",
    "get_runtime_failures",
    "get_runtime_metadata",
]

