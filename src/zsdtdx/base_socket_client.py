"""
基础Socket客户端模块

职责：
1. 提供底层Socket通信能力，封装TCP连接管理。
2. 实现心跳保活、流量统计等功能。
3. 提供统一请求/响应处理框架。
4. 通过atexit注册清理函数，确保程序退出时关闭残留连接。

边界：
1. 本模块为底层模块，不处理业务逻辑（如市场路由、分页等）。
2. 所有子类应实现setup()方法完成协议初始化。
3. 连接异常时根据 `raise_exception` 配置决定返回空值或抛出异常。
"""

# coding=utf-8

import atexit
import socket
import weakref
import datetime
import functools
import threading
import time

import pandas as pd

from zsdtdx.errors import TdxConnectionError, TdxFunctionCallError
from zsdtdx.heartbeat import HqHeartBeatThread
from zsdtdx.log import log
from zsdtdx.parser.raw_parser import RawParser

# 连接超时（秒）
CONNECT_TIMEOUT = 5.0
# 接收包头长度
RECV_HEADER_LEN = 0x10
# 默认心跳间隔（秒）
DEFAULT_HEARTBEAT_INTERVAL = 10.0

# 全局客户端弱引用集合，用于程序退出时清理
_all_clients = weakref.WeakSet()
_all_clients_lock = threading.Lock()


def _register_client(client):
    """
    输入：客户端实例
    输出：无
    用途：注册客户端实例到全局弱引用集合
    边界：程序退出时通过atexit自动清理已注册客户端
    """
    with _all_clients_lock:
        _all_clients.add(client)


def _cleanup_all_clients():
    """
    输入：无
    输出：无
    用途：程序退出时（atexit触发）清理所有未关闭的客户端连接
    边界：忽略所有异常，确保清理不阻塞退出；弱引用自动释放已回收对象
    """
    with _all_clients_lock:
        clients = list(_all_clients)
    for client in clients:
        try:
            if hasattr(client, 'close'):
                client.close()
        except Exception:
            pass


# 注册退出清理函数
atexit.register(_cleanup_all_clients)


def update_last_ack_time(func):
    """
    输入：
    1. func: 需要包装的 socket 调用函数。
    输出：
    1. 返回包装后的调用函数。
    用途：
    1. 在每次请求前更新时间戳，并统一处理失败返回。
    边界条件：
    1. 当 `raise_exception=True` 时，失败会抛出 `TdxFunctionCallError`。
    """

    @functools.wraps(func)
    def wrapper(self, *args, **kw):
        """
        输入：
        1. *args: 被包装函数的位置参数。
        2. **kw: 被包装函数的关键字参数。
        输出：
        1. 返回原函数结果；失败时可能返回 `None` 或抛异常。
        用途：
        1. 执行底层请求并处理异常结果。
        边界条件：
        1. 当 `raise_exception=True` 且请求失败时抛 `TdxFunctionCallError`。
        """
        self.last_ack_time = time.time()
        log.debug("last ack time update to " + str(self.last_ack_time))
        try:
            ret = func(self, *args, **kw)
        except Exception as e:
            log.debug("hit exception on req exception is " + str(e))
            self.last_transaction_failed = True
            ret = None
            if self.raise_exception:
                to_raise = TdxFunctionCallError("calling function error")
                to_raise.original_exception = e
                raise to_raise
        """
        如果raise_exception=True 抛出异常
        如果raise_exception=False 返回None
        """
        return ret
    return wrapper


class TrafficStatSocket(socket.socket):
    """
    流量统计Socket
    
    职责：
    1. 继承socket.socket，增加流量统计功能。
    2. 记录发送/接收的包数和字节数。
    3. 记录第一个包发送时间，用于计算平均速率。
    
    边界：
    1. 所有统计字段在socket操作后由外部更新。
    """

    def __init__(self, sock, mode):
        """
        输入：socket对象、模式
        输出：无
        用途：初始化流量统计socket
        边界：调用父类__init__并初始化统计字段
        """
        super(TrafficStatSocket, self).__init__(sock, mode)
        # 流量统计
        self.send_pkg_num = 0
        self.recv_pkg_num = 0
        self.send_pkg_bytes = 0
        self.recv_pkg_bytes = 0
        self.first_pkg_send_time = None
        self.last_api_send_bytes = 0
        self.last_api_recv_bytes = 0


class BaseSocketClient(object):
    """
    基础Socket客户端
    
    职责：
    1. 管理TCP连接的生命周期（建立、断开、心跳）。
    2. 提供异常处理机制。
    3. 通过atexit注册确保程序退出时清理连接。
    
    边界：
    1. 子类需实现setup()方法完成协议初始化。
    2. 连接异常时根据raise_exception配置决定行为。
    """

    def __init__(self, multithread=False, heartbeat=False, raise_exception=False):
        """
        输入：是否多线程、是否启用心跳、是否抛出异常
        输出：无
        用途：初始化客户端
        边界：注册到全局清理列表，确保程序退出时自动关闭
        """
        self.need_setup = True
        if multithread or heartbeat:
            self.lock = threading.Lock()
        else:
            self.lock = None

        self.client = None
        self.heartbeat = heartbeat
        self.heartbeat_thread = None
        self.stop_event = None
        self.heartbeat_interval = DEFAULT_HEARTBEAT_INTERVAL
        self.last_ack_time = time.time()
        self.last_transaction_failed = False
        self.ip = None
        self.port = None
        self.raise_exception = raise_exception
        
        # 注册到全局清理列表
        _register_client(self)

    def connect(self, ip='101.227.73.20', port=7709, time_out=CONNECT_TIMEOUT, bindport=None, bindip='0.0.0.0'):
        """
        输入：服务器IP、端口、超时时间、绑定端口、绑定IP
        输出：是否连接成功（bool）或self（链式调用）
        用途：建立TCP连接并初始化协议
        边界：
        1. 如有现有连接先关闭
        2. 失败时根据raise_exception决定是否抛出异常
        3. 启用心跳时自动启动心跳线程
        """
        # 如果已有连接，先关闭
        if self.client is not None:
            self.disconnect()

        self.client = TrafficStatSocket(socket.AF_INET, socket.SOCK_STREAM)
        self.client.settimeout(time_out)
        log.debug("connecting to server : %s on port :%d" % (ip, port))
        try:
            self.ip = ip
            self.port = port
            if bindport is not None:
                self.client.bind((bindip, bindport))
            self.client.connect((ip, port))
        except socket.timeout:
            # print(str(e))
            log.debug("connection expired")
            self.client.close()
            self.client = None
            if self.raise_exception:
                raise TdxConnectionError("connection timeout error")
            return False
        except Exception:
            try:
                self.client.close()
            except Exception:
                pass
            self.client = None
            if self.raise_exception:
                raise TdxConnectionError("other errors")
            return False

        log.debug("connected!")

        if self.need_setup:
            try:
                self.setup()
            except Exception:
                # 初始化失败，关闭连接
                self.disconnect()
                raise

        if self.heartbeat:
            self.stop_event = threading.Event()
            self.heartbeat_thread = HqHeartBeatThread(
                self, self.stop_event, self.heartbeat_interval)
            self.heartbeat_thread.start()
        return self

    def disconnect(self):
        """
        输入：无
        输出：无
        用途：关闭TCP连接并停止心跳线程
        边界：
        1. 重复调用安全，忽略所有异常
        2. 先停止心跳线程再关闭socket
        3. 清理后重置client/ip/port为None
        """
        # 先停止心跳线程
        if self.heartbeat_thread and self.heartbeat_thread.is_alive():
            try:
                if self.stop_event:
                    self.stop_event.set()
                # 给线程一点时间自行退出
                self.heartbeat_thread.join(timeout=1.0)
            except Exception:
                pass
            self.heartbeat_thread = None

        # 关闭 socket 连接
        if self.client:
            log.debug("disconnecting")
            try:
                # 先尝试优雅关闭
                try:
                    self.client.shutdown(socket.SHUT_RDWR)
                except (OSError, socket.error):
                    # socket 可能已断开或从未连接
                    pass
                self.client.close()
            except Exception as e:
                log.debug(f"disconnect error (ignored): {e}")
                if self.raise_exception:
                    raise TdxConnectionError("disconnect err")
            finally:
                self.client = None
                self.ip = None
                self.port = None
            log.debug("disconnected")

    def close(self):
        """
        输入：无
        输出：无
        用途：disconnect的别名，支持with语句和统一关闭接口
        边界：直接调用disconnect()
        """
        self.disconnect()

    def get_traffic_stats(self):
        """
        获取流量统计的信息
        :return:
        """
        if self.client.first_pkg_send_time is not None:
            total_seconds = (datetime.datetime.now() -
                             self.client.first_pkg_send_time).total_seconds()
            if total_seconds != 0:
                send_bytes_per_second = self.client.send_pkg_bytes // total_seconds
                recv_bytes_per_second = self.client.recv_pkg_bytes // total_seconds
            else:
                send_bytes_per_second = None
                recv_bytes_per_second = None
        else:
            total_seconds = None
            send_bytes_per_second = None
            recv_bytes_per_second = None

        return {
            "send_pkg_num": self.client.send_pkg_num,
            "recv_pkg_num": self.client.recv_pkg_num,
            "send_pkg_bytes": self.client.send_pkg_bytes,
            "recv_pkg_bytes": self.client.recv_pkg_bytes,
            "first_pkg_send_time": self.client.first_pkg_send_time,
            "total_seconds": total_seconds,
            "send_bytes_per_second": send_bytes_per_second,
            "recv_bytes_per_second": recv_bytes_per_second,
            "last_api_send_bytes": self.client.last_api_send_bytes,
            "last_api_recv_bytes": self.client.last_api_recv_bytes,
        }

    # for debuging and testing protocol
    def send_raw_pkg(self, pkg):
        """
        输入：
        1. pkg: 输入参数，约束以协议定义与函数实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `send_raw_pkg` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
        cmd = RawParser(self.client, lock=self.lock)
        cmd.setParams(pkg)
        return cmd.call_api()

    def __enter__(self):
        """
        输入：
        1. 无显式输入参数。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `__enter__` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        输入：
        1. exc_type: 输入参数，约束以协议定义与函数实现为准。
        2. exc_val: 输入参数，约束以协议定义与函数实现为准。
        3. exc_tb: 输入参数，约束以协议定义与函数实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `__exit__` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
        self.close()

    def to_df(self, v):
        """
        输入：
        1. v: 输入参数，约束以协议定义与函数实现为准。
        输出：
        1. 返回值语义由函数实现定义；无返回时为 `None`。
        用途：
        1. 执行 `to_df` 对应的协议处理、数据解析或调用适配逻辑。
        边界条件：
        1. 网络异常、数据异常和重试策略按函数内部与调用方约定处理。
        """
        if isinstance(v, list):
            return pd.DataFrame(data=v)
        elif isinstance(v, dict):
            return pd.DataFrame(data=[v, ])
        else:
            return pd.DataFrame(data=[{'value': v}])
