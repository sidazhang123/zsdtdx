"""
模块：`kline_task.py`。

职责：
1. 定义股票、指数、板块三种 K 线任务类，以及共同的校验与字典转换。
2. 作为用户构造 K 线 task 的对外类型入口（与 `simple_api` 的 get_* 调用入口并列；也可从 `zsdtdx` 直接导入）。

边界：
1. 不访问网络，不解析行情。
2. 抓取实现仍在 `biz/stock_kline` / `biz/index_kline` / `biz/block_kline`，由 `simple_api` 转发。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, ClassVar, Dict

from zsdtdx.util.helper import (
    TASK_FREQ_MAP,
    normalize_task_time_window,
    parse_task_datetime,
)


class KlineTask:
    """
    K 线任务基类。

    输入：子类提供一个名称字段，以及 freq/start_time/end_time。
    输出：`validate` 无返回；`to_dict` 输出标准化字典。
    用途：三种 K 线任务共用周期、时间窗口和字典转换。
    边界条件：子类必须设置 `symbol_field`，且该字段在实例上存在。
    """

    symbol_field: ClassVar[str] = ""

    def validate(self) -> None:
        """
        校验名称、周期和时间窗口。

        输入：当前对象的名称字段、freq、start_time、end_time。
        输出：无；失败抛 ValueError。
        用途：进入抓取前暴露非法任务。
        边界条件：周期不在 `TASK_FREQ_MAP` 时抛错。
        """
        field = self.symbol_field
        symbol = str(getattr(self, field, "") or "").strip()
        freq = str(getattr(self, "freq", "") or "").strip().lower()
        start_time = str(getattr(self, "start_time", "") or "").strip()
        end_time = str(getattr(self, "end_time", "") or "").strip()
        if symbol == "":
            raise ValueError(f"task.{field} 不能为空")
        if freq == "":
            raise ValueError("task.freq 不能为空")
        if start_time == "":
            raise ValueError("task.start_time 不能为空")
        if end_time == "":
            raise ValueError("task.end_time 不能为空")
        if freq not in TASK_FREQ_MAP:
            raise ValueError(f"不支持的频率: {getattr(self, 'freq', '')}")
        parse_task_datetime(start_time)
        parse_task_datetime(end_time)

    def to_dict(self) -> Dict[str, str]:
        """
        输出标准化任务字典。

        输入：当前任务对象。
        输出：名称字段、freq、start_time、end_time。仅日期时补齐为 09:30:00 / 16:00:00。
        用途：作为抓取器输入。
        边界条件：会先执行 validate。
        """
        self.validate()
        start_time, end_time = normalize_task_time_window(
            getattr(self, "start_time"), getattr(self, "end_time")
        )
        freq = str(getattr(self, "freq")).strip().lower()
        return {
            self.symbol_field: str(getattr(self, self.symbol_field)).strip(),
            "freq": TASK_FREQ_MAP[freq],
            "start_time": start_time,
            "end_time": end_time,
        }

    @classmethod
    def from_dict(cls, raw: Dict[str, Any]) -> "KlineTask":
        """
        从字典构造任务并校验。

        输入：raw 含子类名称字段、freq、start_time、end_time。
        输出：子类实例。
        用途：兼容 dict 任务输入。
        边界条件：raw 不是 dict 时抛 ValueError。
        """
        if not isinstance(raw, dict):
            raise ValueError(f"task 元素必须是 dict 或 {cls.__name__}")
        task = cls(
            **{
                cls.symbol_field: raw.get(cls.symbol_field),
                "freq": raw.get("freq"),
                "start_time": raw.get("start_time"),
                "end_time": raw.get("end_time"),
            }
        )
        task.validate()
        return task


@dataclass
class StockKlineTask(KlineTask):
    """
    股票 K 线任务。

    输入：code、freq、start_time、end_time。
    输出：`to_dict()` 为标准化任务字典。
    用途：传给 get_stock_kline。
    边界条件：校验与时间补齐由 `KlineTask` 完成；名称字段为 code。
    """

    code: Any
    freq: Any
    start_time: Any
    end_time: Any
    symbol_field: ClassVar[str] = "code"


@dataclass
class IndexKlineTask(KlineTask):
    """
    指数 K 线任务。

    输入：index_name、freq、start_time、end_time。
    输出：`to_dict()` 为标准化任务字典。
    用途：传给 get_index_kline。
    边界条件：校验与时间补齐由 `KlineTask` 完成；async 缺省任务不经过本类构造。
    """

    index_name: Any
    freq: Any
    start_time: Any
    end_time: Any
    symbol_field: ClassVar[str] = "index_name"


@dataclass
class BlockKlineTask(KlineTask):
    """
    板块指数 K 线任务。

    输入：block_name、freq、start_time、end_time。
    输出：`to_dict()` 为标准化任务字典。
    用途：传给 get_block_kline；block_name 来自 get_block_names。
    边界条件：校验与时间补齐由 `KlineTask` 完成；不接受空 task 自动展开全部板块。
    """

    block_name: Any
    freq: Any
    start_time: Any
    end_time: Any
    symbol_field: ClassVar[str] = "block_name"


__all__ = [
    "KlineTask",
    "StockKlineTask",
    "IndexKlineTask",
    "BlockKlineTask",
]
