"""
模块：`errors.py`。

职责：
1. 定义 zsdtdx 连接与协议调用相关的异常类型。
2. 供 socket 客户端与上层 API 在失败时抛出或包装。

边界：
1. 本模块仅声明异常类，不包含重试或恢复逻辑。
"""

# coding=utf-8


class TdxConnectionError(Exception):
    """
    当连接服务器出错的时候，会抛出的异常
    """
    pass

class TdxFunctionCallError(Exception):
    """
    通达信协议层 API 调用失败。

    输入：
    1. message: 人类可读错误摘要（建议含方法名与端点）。
    输出：
    1. 异常实例；`original_exception` 保存底层原因。
    用途：
    1. `raise_exception=True` 时由 socket 装饰器抛出，便于日志与上层捕获。
    边界条件：
    1. `original_exception` 可能为 None（仅消息、无包装原因时）。
    """

    def __init__(self, message: str = "", *, original_exception: BaseException | None = None):
        super().__init__(message)
        self.original_exception = original_exception




