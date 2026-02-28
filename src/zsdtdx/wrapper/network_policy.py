"""网络策略模块：统一强制直连，禁止代理转发。"""

import os
import urllib.request

_PROXY_KEYS = [
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
]


def enforce_no_proxy():
    """
    强制关闭代理并返回当前代理环境快照。

    输入:
        无。
    输出:
        dict: 关键代理环境变量与当前值。
    用途:
        确保所有网络请求走直连，不通过系统或环境代理。
    边界条件:
        若变量不存在则返回空字符串，不抛异常。
    """
    for key in _PROXY_KEYS:
        os.environ[key] = ""
    os.environ["NO_PROXY"] = "*"
    os.environ["no_proxy"] = "*"

    # 兜底关闭 urllib 的系统代理发现。
    urllib.request.getproxies = lambda: {}
    return get_proxy_env_snapshot()


def get_proxy_env_snapshot():
    """
    获取代理相关环境变量快照。

    输入:
        无。
    输出:
        dict: 代理变量及值。
    用途:
        用于测试报告记录“无代理”执行状态。
    边界条件:
        未设置变量时返回空字符串。
    """
    keys = _PROXY_KEYS + ["NO_PROXY", "no_proxy"]
    return {key: os.environ.get(key, "") for key in keys}
