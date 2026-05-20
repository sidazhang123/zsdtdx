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
    当函数调用出错的时候
    """
    def __init__(self, *args, **kwargs):
        """
        输入：
        1. *args: 输入参数，约束以协议定义与函数实现为准。
        2. **kwargs: 输入参数，约束以协议定义与函数实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `__init__` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
        super(TdxFunctionCallError, self).__init__(*args, **kwargs)
        self.original_exception = None




