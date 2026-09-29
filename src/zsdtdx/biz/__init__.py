"""
模块：`biz` 子包。

职责：
1. 存放各领域业务门面（股票/指数/板块/期货 K 线、公司信息）。
2. 对上由 `simple_api` 的 get_* 调用；对下委托统一客户端与并行抓取器。

边界：
1. 不定义用户任务类（见包根 `kline_task.py`）。
2. 不解析协议包，不管理连接池生命周期。
"""

from __future__ import annotations

__all__: list[str] = []
