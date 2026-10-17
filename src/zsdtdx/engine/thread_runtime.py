"""模块：`engine/thread_runtime.py`。

职责：维护有界线程、可中断尝试、按站配额与独占连接租约。
边界：不处理任务协议和结果格式；重启必须等受控工作退出后才允许新代运行。
"""

from __future__ import annotations

import contextvars
import heapq
import threading
import time
from concurrent.futures import ThreadPoolExecutor, wait

from zsdtdx.engine.unified_client import (
    AttemptSocketCloser,
    PersistentFailoverPool,
    _attempt_aborted,
    _attempt_socket_closer,
)

attempt_deadline = contextvars.ContextVar("zsdtdx_attempt_deadline", default=None)


def check_deadline():
    """输入无，输出无；取消或累计截止时间到期立即抛错。"""
    deadline = attempt_deadline.get()
    if _attempt_aborted() or (deadline is not None and time.monotonic() >= deadline):
        raise TimeoutError("chunk attempt timeout")


def remaining(default=15.0):
    """输入缺省预算，输出当前剩余秒数；已取消或到期抛错。"""
    check_deadline()
    deadline = attempt_deadline.get()
    return max(0.001, deadline - time.monotonic()) if deadline is not None else default


class LeasedFailoverPool(PersistentFailoverPool):
    """在既有请求重试阶梯外提供按站长连接租约，空闲连接可由下一线程独占复用。"""

    def __init__(self, original, controller, source):
        """输入原池与共享配额，输出无连接的新池；不复制元数据或启动线程。"""
        super().__init__(
            original.name,
            original.api_cls,
            original.hosts,
            original.connect_timeout,
            original.api_kwargs,
        )
        self.controller, self.source = controller, source
        self._idle_lock = threading.Lock()
        self._idle = {host: [] for host in self.hosts}
        self._closed = False
        self.peer = None
        self.configure_host_groups(
            {
                name: list(hosts)
                for name, hosts in getattr(original, "host_groups", {}).items()
                if name != "all"
            }
        )

    def _get_thread_data(self):
        """输入无，输出常驻工作线程的连接状态；注册只做一次，销毁时统一回收。"""
        data = getattr(self._local, "data", None)
        return data if data is not None else super()._get_thread_data()

    def reset_thread_connection(self, reconnect=True):
        """输入重连开关，输出恢复结果；重置必须连同真实准入一起释放。"""
        self.release_thread(success=False, discard=True)
        return self._ensure_connected() if reconnect else True

    def begin_attempt(self):
        """输入无，输出无；清除上一任务地址，未触网的失败不能降旧站配额。"""
        data = self._get_thread_data()
        data.pop("last_lease_host", None)
        data.pop("last_lease_at", None)

    def release_thread(self, *, success=True, congested=False, discard=False):
        """输入最终执行状态，输出无；先归还或关闭连接，再释放真实准入。"""
        data = self._get_thread_data()
        api, permit = data.get("api"), data.pop("lease_permit", None)
        data["api"], data["active_index"] = None, -1
        if api is not None:
            closer = _attempt_socket_closer.get()
            sock = getattr(api, "client", None)
            if closer is not None and not closer.detach(sock):
                discard = True
            if discard or self._closed:
                self._disconnect_api(api)
            else:
                if sock is not None:
                    sock._zsdtdx_deadline = None
                with self._idle_lock:
                    if not self._closed and permit is not None:
                        self._idle.setdefault(permit.host, []).append(api)
                    else:
                        self._disconnect_api(api)
        if permit is not None:
            self.controller.release(permit, success=success, congested=congested)
        elif congested and data.get("last_lease_host") is not None:
            self.controller.penalize_host(self.source, data["last_lease_host"])

    def _connect_to_index(self, index):
        """输入目标索引，输出是否建连；目标站也必须取得配额，原连接不带到新站。"""
        self.release_thread(success=False, discard=True)
        check_deadline()
        host = self.hosts[index]
        if host not in self._eligible_host_set():
            return False
        try:
            permit = self.controller.acquire(
                self.source,
                max_units=1,
                timeout=min(remaining(), 0.25),
                preferred_host=host,
                allowed_hosts=self._eligible_host_set(),
            )
        except TimeoutError:
            return False
        return self._install_connection(permit)

    def _install_connection(self, permit):
        """输入独占准入，输出连接结果；失败关闭 API 并归还准入，不泄漏创建中的 socket。"""
        api = None
        data = self._get_thread_data()
        data["last_lease_host"] = permit.host
        data["last_lease_at"] = time.monotonic()
        try:
            check_deadline()
            with self._idle_lock:
                cached = self._idle.setdefault(permit.host, [])
                api = cached.pop() if cached else None
            if api is None:
                api = self.api_cls(**self.api_kwargs)
                if not api.connect(
                    *permit.host, time_out=min(self.connect_timeout, remaining())
                ):
                    raise ConnectionError(f"{self.name} 无可用连接 {permit.host}")
            check_deadline()
            data["api"], data["active_index"], data["lease_permit"] = (
                api,
                self.hosts.index(permit.host),
                permit,
            )
            self._apply_thread_socket_read_timeout(data)
            with self._used_hosts_lock:
                self._global_used_hosts.add(f"{permit.host[0]}:{permit.host[1]}")
            return True
        except Exception:
            self._disconnect_api(api)
            data["api"] = None
            data.pop("lease_permit", None)
            self.controller.release(permit, success=False, congested=False)
            with self._stats_lock:
                self._global_stats["connect_failures"] += 1
            check_deadline()
            return False

    def _ensure_connected(self):
        """输入无，输出连接是否可用；首次按配额轮转选择站点，失败遍历其余站。"""
        check_deadline()
        if self.peer is not None:
            self.peer.release_thread()
        if self._get_thread_data().get("api") is not None:
            data = self._get_thread_data()
            active_index = int(data.get("active_index", -1))
            if 0 <= active_index < len(self.hosts):
                if self.hosts[active_index] in self._eligible_host_set():
                    return True
            self.release_thread(success=True)
        eligible = self._eligible_host_set()
        for _ in eligible:
            permit = self.controller.acquire(
                self.source,
                max_units=1,
                timeout=remaining(),
                allowed_hosts=eligible,
            )
            if self._install_connection(permit):
                return True
        return False

    def close(self):
        """输入无，输出无；关闭空闲及借出的 socket，重复调用安全。"""
        with self._idle_lock:
            self._closed = True
            idle = [api for values in self._idle.values() for api in values]
            self._idle.clear()
        for api in idle:
            self._disconnect_api(api)
        super().close()


class ThreadRuntime:
    """单代执行器；提交窗口和执行线程均有界，跟踪正在运行的取消登记器。"""

    def __init__(self, thread_count, window_multiplier=2):
        """输入线程和窗口上限，输出运行时；不预连接行情服务器。"""
        self.thread_count = max(1, int(thread_count))
        self.executor = ThreadPoolExecutor(
            max_workers=self.thread_count, thread_name_prefix="zsdtdx_fetch"
        )
        self._slots = threading.BoundedSemaphore(
            self.thread_count * max(1, int(window_multiplier))
        )
        self._lock = threading.Lock()
        self._futures = set()
        self._closers = set()
        self.stopped = threading.Event()
        self.metrics = {}
        self._condition = threading.Condition(self._lock)
        self._deadlines = []
        self._deadline_sequence = 0
        self._watchdog = threading.Thread(
            target=self._watch_deadlines, name="zsdtdx_deadlines", daemon=True
        )
        self._watchdog.start()

    def _watch_deadlines(self):
        """输入无，输出无；单个常驻看门狗按最近截止时间关闭 socket，不为每次尝试建线程。"""
        while True:
            with self._condition:
                while self._deadlines and self._deadlines[0][2] not in self._closers:
                    heapq.heappop(self._deadlines)
                if self.stopped.is_set():
                    return
                if not self._deadlines:
                    self._condition.wait()
                    continue
                deadline, _sequence, closer = self._deadlines[0]
                delay = deadline - time.monotonic()
                if delay > 0:
                    self._condition.wait(timeout=delay)
                    continue
                heapq.heappop(self._deadlines)
                self._closers.discard(closer)
            closer.close()

    def record_stage(self, stage, elapsed, cpu):
        """输入阶段与线程内计时，输出无；累计值用于区分内核和交付开销。"""
        with self._lock:
            item = self.metrics.setdefault(
                stage, {"thread_wall_seconds": 0.0, "cpu_seconds": 0.0, "calls": 0}
            )
            item["thread_wall_seconds"] += elapsed
            item["cpu_seconds"] += cpu
            item["calls"] += 1

    def submit(self, fn, *args, submit_timeout=None):
        """输入执行体与参数，输出 future；窗口满时有界轮询，停机拒绝新任务。"""
        submit_deadline = (
            None if submit_timeout is None else time.monotonic() + submit_timeout
        )
        while not self._slots.acquire(timeout=0.05):
            if self.stopped.is_set():
                raise RuntimeError("fetcher restarted or destroyed")
            if submit_deadline is not None and time.monotonic() >= submit_deadline:
                raise TimeoutError("thread submission timeout")
        with self._lock:
            if self.stopped.is_set():
                self._slots.release()
                raise RuntimeError("fetcher restarted or destroyed")
            state = {"started": None}

            def execute():
                """输入无，输出执行体结果；排队时间不消耗工作 watchdog。"""
                state["started"] = time.monotonic()
                return fn(*args)

            try:
                future = self.executor.submit(execute)
            except BaseException:
                self._slots.release()
                raise
            future._zsdtdx_state = state
            self._futures.add(future)

        def complete(done):
            """输入已结束 future，输出无；真实结束后才归还窗口。"""
            with self._lock:
                self._futures.discard(done)
            self._slots.release()

        future.add_done_callback(complete)
        return future

    def attempt(self, fn, timeout, *args):
        """输入有界执行体和预算，输出结果；定时关闭 socket，中断网络并禁止继续建连。"""
        closer = AttemptSocketCloser()
        deadline = time.monotonic() + max(0.001, timeout)
        with self._condition:
            if self.stopped.is_set():
                raise RuntimeError("fetcher restarted or destroyed")
            self._closers.add(closer)
            self._deadline_sequence += 1
            heapq.heappush(self._deadlines, (deadline, self._deadline_sequence, closer))
            self._condition.notify()
        token = _attempt_socket_closer.set(closer)
        clock_token = attempt_deadline.set(deadline)
        started, cpu_started = time.perf_counter(), time.thread_time()
        try:
            result = fn(*args)
            check_deadline()
            return result
        finally:
            disarmed = closer.disarm()
            self.record_stage(
                "columnar_attempt",
                time.perf_counter() - started,
                time.thread_time() - cpu_started,
            )
            _attempt_socket_closer.reset(token)
            attempt_deadline.reset(clock_token)
            with self._condition:
                self._closers.discard(closer)
                self._condition.notify()
            if not disarmed:
                raise TimeoutError("chunk attempt timeout")

    def stop(self, timeout=5.0):
        """输入回收预算，输出残留任务数；不强杀线程，也不伪报回收完成。"""
        deadline = time.monotonic() + timeout
        self.stopped.set()
        with self._condition:
            futures, closers = list(self._futures), list(self._closers)
            self._condition.notify_all()
        for closer in closers:
            closer.close()
        for future in futures:
            future.cancel()
        self.executor.shutdown(wait=False, cancel_futures=True)
        unfinished = [future for future in futures if not future.done()]
        if unfinished:
            _, pending = wait(unfinished, timeout=max(0.0, deadline - time.monotonic()))
            count = sum(not future.done() for future in pending)
            if count:
                return count
        threads = list(self.executor._threads)
        threads.append(self._watchdog)
        for thread in threads:
            if thread is not threading.current_thread():
                thread.join(timeout=max(0.0, deadline - time.monotonic()))
        return sum(thread.is_alive() for thread in threads)
