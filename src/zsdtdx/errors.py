"""
模块：`errors.py`。

职责：
1. 提供 zsdtdx 体系中的协议封装、解析或对外接口能力。
2. 对上层暴露稳定调用契约，屏蔽底层协议数据细节。
3. 当前统计：类 2 个，函数 1 个。

边界：
1. 本模块仅负责当前文件定义范围，不承担其它分层编排职责。
2. 错误语义、重试策略与容错逻辑以实现与调用方约定为准。
"""

# coding=utf-8


class TdxConnectionError(Exception):
    """
    当连接服务器出错的时候，会抛出的异常
    """
    pass

class TdxFunctionCallError(Exception):
    """
    当行数调用出错的时候
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




