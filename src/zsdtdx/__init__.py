"""zsdtdx 对外导出入口。"""

from zsdtdx.kline_task import BlockKlineTask, IndexKlineTask, StockKlineTask
from zsdtdx.engine.parallel_fetcher import StockKlineJob
from zsdtdx.simple_api import (
    destroy_parallel_fetcher,
    get_all_future_list,
    get_block_kline,
    get_block_names,
    get_client,
    get_company_info,
    get_etf_code_name,
    get_future_kline,
    get_future_latest_price,
    get_index_kline,
    get_runtime_failures,
    get_runtime_metadata,
    get_stock_code_name,
    get_stock_concepts,
    get_stock_kline,
    get_stock_latest_price,
    prewarm_parallel_fetcher,
    restart_parallel_fetcher,
    set_config_path,
)
from zsdtdx.engine.unified_client import UnifiedTdxClient

__version__ = "2.3.0"

__all__ = [
    "__version__",
    "UnifiedTdxClient",
    "StockKlineTask",
    "IndexKlineTask",
    "BlockKlineTask",
    "StockKlineJob",
    "set_config_path",
    "get_client",
    "get_stock_code_name",
    "get_stock_concepts",
    "get_etf_code_name",
    "get_block_names",
    "get_all_future_list",
    "get_stock_kline",
    "get_stock_latest_price",
    "get_company_info",
    "get_index_kline",
    "get_block_kline",
    "get_future_kline",
    "get_future_latest_price",
    "prewarm_parallel_fetcher",
    "restart_parallel_fetcher",
    "destroy_parallel_fetcher",
    "get_runtime_failures",
    "get_runtime_metadata",
]
