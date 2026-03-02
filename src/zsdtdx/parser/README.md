# 协议解析目录（parser）

## 目录定位

该目录提供 pytdx 协议的请求打包与响应解析，每个文件对应一类命令。

## 文件清单

- `__init__.py`
- `base.py`
- `ex_get_history_instrument_bars_range.py`
- `ex_get_history_minute_time_data.py`
- `ex_get_history_transaction_data.py`
- `ex_get_instrument_bars.py`
- `ex_get_instrument_count.py`
- `ex_get_instrument_info.py`
- `ex_get_instrument_quote.py`
- `ex_get_instrument_quote_list.py`
- `ex_get_markets.py`
- `ex_get_minute_time_data.py`
- `ex_get_transaction_data.py`
- `ex_setup_commands.py`
- `get_company_info_category.py`
- `get_company_info_content.py`
- `get_finance_info.py`
- `get_history_minute_time_data.py`
- `get_history_transaction_data.py`
- `get_index_bars.py`
- `get_minute_time_data.py`
- `get_report_file.py`
- `get_security_bars.py`
- `get_security_count.py`
- `get_security_list.py`
- `get_security_quotes.py`
- `get_transaction_data.py`
- `get_xdxr_info.py`
- `raw_parser.py`
- `setup_commands.py`

## 职责边界

1. parser 层负责协议数据解析，`unified_client` 层负责路由、分页、重试和对外接口。
2. 修改协议字段或返回结构时，必须同步检查 `simple_api.py` 和上层用法文档。
3. 网络异常、重试策略和 host 轮换改动必须做回归验证。

## 维护建议

1. 新增或修改解析器时，至少覆盖“正常响应、空响应、异常响应”三类样本。
2. 提交前建议运行主要接口的快速调用链路检查（行情列表、K线、实时价）。
3. 注释与文档使用中文撰写，避免因缩写或口语化表达导致理解偏差。
