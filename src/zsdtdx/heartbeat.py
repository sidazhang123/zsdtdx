"""
心跳线程模块

职责：
1. 提供行情连接的心跳保活机制。
2. 定期检查连接活跃状态，超时未收到响应时发送心跳包。
3. 使用守护线程模式，确保主进程退出时自动终止。

边界：
1. 本模块仅负责心跳逻辑，不承担数据获取职责。
2. 线程设置为daemon=True，主进程退出时自动终止，防止残留。
"""

# coding=utf-8

import time
from threading import Thread

from zsdtdx.log import log

# 默认心跳间隔：10秒
DEFAULT_HEARTBEAT_INTERVAL = 10.0


class HqHeartBeatThread(Thread):
    """
    行情心跳线程
    
    职责：
    1. 定期检查行情连接状态。
    2. 超时未收到响应时发送心跳包维持连接。
    3. 通过stop_event控制线程终止。
    
    边界：
    1. 设置为守护线程（daemon=True），主进程退出时自动终止。
    2. 发送心跳异常时仅记录日志，不中断循环。
    """

    def __init__(self, api, stop_event, heartbeat_interval=DEFAULT_HEARTBEAT_INTERVAL):
        """
        输入：api对象、停止事件、心跳间隔（秒）
        输出：无
        用途：初始化心跳线程
        边界：守护线程设置确保主进程退出时自动终止
        """
        self.api = api
        self.client = api.client
        self.stop_event = stop_event
        self.heartbeat_interval = heartbeat_interval
        super(HqHeartBeatThread, self).__init__()
        # 设置为守护线程，主进程退出时自动终止
        self.daemon = True

    def run(self):
        """
        输入：无
        输出：无
        用途：心跳循环，定期发送心跳包维持连接
        边界：通过stop_event控制终止；异常时记录日志并继续
        """
        while not self.stop_event.is_set():
            self.stop_event.wait(self.heartbeat_interval)
            if self.client and (time.time() - self.api.last_ack_time > self.heartbeat_interval):
                try:
                    # 发送心跳包
                    self.api.do_heartbeat()
                except Exception as e:
                    log.debug(str(e))


