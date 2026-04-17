"""zsdtdx 对外导出入口。"""

from zsdtdx.simple_api import (
    IndexKlineTask,
    StockKlineTask,
    destroy_parallel_fetcher,
    get_all_future_list,
    get_client,
    get_company_info,
    get_future_kline,
    get_future_latest_price,
    get_index_kline,
    prewarm_parallel_fetcher,
    restart_parallel_fetcher,
    get_runtime_failures,
    get_runtime_metadata,
    get_stock_code_name,
    get_stock_kline,
    get_stock_latest_price,
    get_supported_markets,
    set_config_path,
)
from zsdtdx.parallel_fetcher import StockKlineJob
from zsdtdx.unified_client import UnifiedTdxClient

__version__ = "1.4.4"

__all__ = [
    "__version__",
    "UnifiedTdxClient",
    "StockKlineTask",
    "IndexKlineTask",
    "StockKlineJob",
    "set_config_path",
    "get_client",
    "get_supported_markets",
    "get_stock_code_name",
    "get_all_future_list",
    "get_stock_kline",
    "get_index_kline",
    "prewarm_parallel_fetcher",
    "restart_parallel_fetcher",
    "destroy_parallel_fetcher",
    "get_future_kline",
    "get_company_info",
    "get_stock_latest_price",
    "get_future_latest_price",
    "get_runtime_failures",
    "get_runtime_metadata",
]

