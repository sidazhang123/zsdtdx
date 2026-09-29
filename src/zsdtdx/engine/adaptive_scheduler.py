"""
模块：`adaptive_scheduler.py`。

职责：
1. 为 std/ex 行情任务提供跨 async job 共享的线程安全准入控制。
2. 按可达地址数与总进程数计算两侧同时在飞进程上限，并按地址摊开。
3. 连接拥塞时降低对应地址的进程配额并冷却；冷却结束后靠成功逐步回到拥塞前上限。

边界：
1. 本模块只管理父进程内的调度状态，不创建进程、线程或网络连接。
2. 配额是同时在飞 bundle 数；实际连接仍由 worker 内连接池建立。
3. 配置切换、进程池重启时由调用方显式 reset。
"""

from __future__ import annotations

import math
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional, Tuple


HostTuple = Tuple[str, int]
_DEFAULT_PROCESSES_PER_HOST = 4


def _normalize_source(source: str) -> str:
    """输入行情侧名称，输出 std/ex；非法值抛出 ValueError。"""
    normalized = str(source or "").strip().lower()
    if normalized not in {"std", "ex"}:
        raise ValueError(f"未知行情路由: {source}")
    return normalized


def _normalize_host(raw_host: Any) -> HostTuple:
    """输入 host 元组或 `host:port`，输出规范化二元组；非法值抛出 ValueError。"""
    if isinstance(raw_host, (tuple, list)) and len(raw_host) >= 2:
        host = str(raw_host[0]).strip()
        port = int(raw_host[1])
    else:
        text = str(raw_host or "").strip()
        host, separator, port_text = text.rpartition(":")
        if separator == "":
            raise ValueError(f"host 格式无效: {raw_host}")
        host = host.strip()
        port = int(port_text)
    if host == "" or port <= 0:
        raise ValueError(f"host 格式无效: {raw_host}")
    return host, port


def compute_side_process_budget(
    max_processes: int, host_count: int, processes_per_host: int
) -> int:
    """
    按总进程数与可达地址数计算某一侧同时在飞进程上限。

    输入：
    1. max_processes: 全局进程硬顶 C。
    2. host_count: 该侧可达地址数 H。
    3. processes_per_host: 每个地址允许同时摊到的进程数。
    输出：该侧进程预算。
    用途：地址比进程多时用满 C；进程比地址多时按 H × per_host 封顶。
    边界：无可达地址返回 0；结果不会超过 C。
    """
    total = max(1, int(max_processes))
    hosts = max(0, int(host_count))
    per_host = max(1, int(processes_per_host))
    if hosts <= 0:
        return 0
    if total <= hosts:
        return total
    return min(total, hosts * per_host)


@dataclass(frozen=True)
class AdaptivePermit:
    """
    自适应调度准入凭证。

    输入：
    1. source/host: 本次获批的行情侧与首选地址。
    2. units: 获批 chunk 槽位数。
    3. ticket: FIFO 准入序号。
    输出：
    1. 不可变凭证，由 release 原样归还。
    边界：
    1. 每个凭证只能释放一次；重复释放由控制器安全忽略。
    """

    source: str
    host: HostTuple
    units: int
    ticket: int


@dataclass
class _HostWindow:
    """单 host 进程配额内部状态。"""

    host: HostTuple
    process_cap: int = 1
    process_inflight: int = 0
    cooldown_until: float = 0.0
    failed_ceiling: Optional[int] = None
    selectable: bool = True


class AdaptiveConcurrencyController:
    """
    按地址数与总进程数计算配额的并发控制器。

    输入：
    1. max_processes: 全局同时在飞 bundle 的硬上限。
    2. inproc_limit: 单 bundle 最多包含的 chunk 数。
    3. processes_per_host_std/ex: 两侧每个地址允许同时摊到的进程数。
    4. decrease_factor/cooldown_seconds: 拥塞保险丝参数；冷却后成功时每次配额 +1，直到拥塞前上限。
    输出：
    1. 通过 acquire/release 管理准入凭证。
    边界：
    1. 没有可达 host 时 acquire 抛 RuntimeError；等待期间地址被清空同样立即失败。
    2. 两侧按地址配额分别排队。全局在飞窗口仍是两侧共享的硬顶，一侧占满窗口时另一侧会等待。
    """

    def __init__(
        self,
        *,
        max_processes: int,
        inproc_limit: int,
        processes_per_host_std: int = _DEFAULT_PROCESSES_PER_HOST,
        processes_per_host_ex: int = _DEFAULT_PROCESSES_PER_HOST,
        decrease_factor: float = 0.75,
        cooldown_seconds: float = 1.5,
    ) -> None:
        self._condition = threading.Condition(threading.RLock())
        self._max_processes = max(1, int(max_processes))
        self._inproc_limit = max(1, int(inproc_limit))
        self._processes_per_host = {
            "std": max(1, int(processes_per_host_std)),
            "ex": max(1, int(processes_per_host_ex)),
        }
        self._decrease_factor = min(0.95, max(0.1, float(decrease_factor)))
        self._cooldown_seconds = max(0.0, float(cooldown_seconds))
        self._hosts: Dict[str, Dict[HostTuple, _HostWindow]] = {
            "std": {},
            "ex": {},
        }
        self._side_process_budget = {"std": 0, "ex": 0}
        self._side_inflight = {"std": 0, "ex": 0}
        self._next_ticket = 1
        self._waiting_tickets: Dict[str, list[int]] = {"std": [], "ex": []}
        self._active_permits: Dict[int, AdaptivePermit] = {}
        self._inflight_processes = 0
        self._round_robin_cursor = {"std": 0, "ex": 0}

    @property
    def max_processes(self) -> int:
        """输入无，输出当前全局进程硬上限。"""
        with self._condition:
            return int(self._max_processes)

    @property
    def inproc_limit(self) -> int:
        """输入无，输出单 bundle 的 chunk 上限。"""
        with self._condition:
            return int(self._inproc_limit)

    def configure(
        self,
        *,
        max_processes: int,
        inproc_limit: int,
        processes_per_host_std: Optional[int] = None,
        processes_per_host_ex: Optional[int] = None,
        decrease_factor: Optional[float] = None,
        cooldown_seconds: Optional[float] = None,
    ) -> None:
        """
        更新控制器运行参数。

        输入：进程/进程内并发上限及可选每地址进程数、拥塞参数。
        输出：无。
        边界：不会撤销已发放凭证；新限制仅影响后续 acquire。
        """
        with self._condition:
            self._max_processes = max(1, int(max_processes))
            self._inproc_limit = max(1, int(inproc_limit))
            if processes_per_host_std is not None:
                self._processes_per_host["std"] = max(1, int(processes_per_host_std))
            if processes_per_host_ex is not None:
                self._processes_per_host["ex"] = max(1, int(processes_per_host_ex))
            if decrease_factor is not None:
                self._decrease_factor = min(0.95, max(0.1, float(decrease_factor)))
            if cooldown_seconds is not None:
                self._cooldown_seconds = max(0.0, float(cooldown_seconds))
            for source, mapping in self._hosts.items():
                formula = int(self._processes_per_host[source])
                for state in mapping.values():
                    if state.cooldown_until <= 0 and state.failed_ceiling is None:
                        state.process_cap = formula
                    else:
                        state.process_cap = max(1, min(int(state.process_cap), formula))
            self._recompute_side_budgets_locked()
            self._condition.notify_all()

    def update_hosts(self, source: str, hosts: Iterable[Any]) -> None:
        """
        更新某侧可达 host 快照。

        输入：source 与可达 host 序列。
        输出：无。
        边界：保留仍存在 host 的拥塞状态；已移除但仍在飞的 host 在释放后丢弃。
        """
        source_key = _normalize_source(source)
        normalized: list[HostTuple] = []
        seen: set[HostTuple] = set()
        for raw_host in hosts:
            try:
                item = _normalize_host(raw_host)
            except Exception:
                continue
            if item in seen:
                continue
            seen.add(item)
            normalized.append(item)

        with self._condition:
            previous = self._hosts[source_key]
            formula = int(self._processes_per_host[source_key])
            replacement: Dict[HostTuple, _HostWindow] = {}
            for item in normalized:
                existing = previous.get(item)
                if existing is None:
                    replacement[item] = _HostWindow(
                        host=item, process_cap=formula, selectable=True
                    )
                else:
                    existing.selectable = True
                    replacement[item] = existing
            for item, state in previous.items():
                if item not in replacement and int(state.process_inflight) > 0:
                    state.selectable = False
                    replacement[item] = state
            self._hosts[source_key] = replacement
            self._round_robin_cursor[source_key] = 0
            self._recompute_side_budgets_locked()
            self._condition.notify_all()

    def _recompute_side_budgets_locked(self) -> None:
        """输入无，按当前地址快照重算两侧进程预算。"""
        for source in ("std", "ex"):
            live_hosts = sum(
                1 for state in self._hosts[source].values() if bool(state.selectable)
            )
            self._side_process_budget[source] = compute_side_process_budget(
                self._max_processes,
                live_hosts,
                self._processes_per_host[source],
            )

    def _refresh_cooldown(self, source: str, state: _HostWindow, now: float) -> None:
        """
        输入侧名、host 状态与当前时间。
        输出：无。
        用途：冷却结束后重新允许派发；配额保持降后的值，由后续成功逐步抬回。
        边界：不在此处抬高 process_cap；failed_ceiling 继续作为回升目标保留。
        """
        if state.cooldown_until > 0 and now >= state.cooldown_until:
            state.cooldown_until = 0.0

    def _select_host_with_capacity(
        self, source: str, max_units: int, now: float
    ) -> Optional[Tuple[_HostWindow, int]]:
        """输入路由、需求和时间，输出可分配 host 及槽位数；无容量返回 None。"""
        states = list(self._hosts[source].values())
        if not states:
            return None
        start = int(self._round_robin_cursor[source]) % len(states)
        ordered = states[start:] + states[:start]
        for offset, state in enumerate(ordered):
            if not bool(state.selectable):
                continue
            self._refresh_cooldown(source, state, now)
            if state.cooldown_until > now:
                continue
            if int(state.process_inflight) >= max(1, int(state.process_cap)):
                continue
            units = min(max(1, int(max_units)), self._inproc_limit)
            self._round_robin_cursor[source] = (start + offset + 1) % len(states)
            return state, units
        return None

    def _side_may_progress_locked(self, source: str) -> bool:
        """
        输入侧名，输出该侧继续等待是否还有机会获批。

        输出：存在可选地址，或仍有不可选但在飞的地址时为 True。
        边界：调用方必须已持有控制器锁。
        """
        for state in self._hosts[source].values():
            if bool(state.selectable) or int(state.process_inflight) > 0:
                return True
        return False

    def acquire(
        self,
        source: str,
        *,
        max_units: int,
        timeout: Optional[float] = None,
    ) -> AdaptivePermit:
        """
        按本侧 FIFO 获取一个 bundle 准入凭证。

        输入：
        1. source: std/ex。
        2. max_units: 当前最多希望打包的 chunk 数。
        3. timeout: 等待秒数；None 表示一直等待。
        输出：AdaptivePermit。
        边界：超时抛 TimeoutError。无可达 host，或等待期间该侧已没有可选地址且没有在飞 bundle 时，
        timeout 为 None 抛 RuntimeError，有限超时抛 TimeoutError。
        """
        source_key = _normalize_source(source)
        deadline = (
            None if timeout is None else time.monotonic() + max(0.0, float(timeout))
        )
        with self._condition:
            if not self._hosts[source_key]:
                raise RuntimeError(f"{source_key} 无可达 host，无法调度")
            ticket = int(self._next_ticket)
            self._next_ticket += 1
            self._waiting_tickets[source_key].append(ticket)
            try:
                while True:
                    now = time.monotonic()
                    if not self._hosts[
                        source_key
                    ] or not self._side_may_progress_locked(source_key):
                        if deadline is None:
                            raise RuntimeError(f"{source_key} 无可达 host，无法调度")
                        raise TimeoutError(f"{source_key} 自适应调度准入等待超时")
                    waiting = self._waiting_tickets[source_key]
                    is_head = bool(waiting and int(waiting[0]) == ticket)
                    selected = None
                    side_budget = int(self._side_process_budget[source_key])
                    if (
                        is_head
                        and self._inflight_processes < self._max_processes
                        and self._side_inflight[source_key] < side_budget
                    ):
                        selected = self._select_host_with_capacity(
                            source_key, max_units, now
                        )
                    if selected is not None:
                        state, units = selected
                        waiting.pop(0)
                        state.process_inflight += 1
                        self._side_inflight[source_key] += 1
                        self._inflight_processes += 1
                        permit = AdaptivePermit(
                            source=source_key,
                            host=state.host,
                            units=int(units),
                            ticket=ticket,
                        )
                        self._active_permits[ticket] = permit
                        self._condition.notify_all()
                        return permit

                    if deadline is not None:
                        remaining = deadline - now
                        if remaining <= 0:
                            raise TimeoutError(f"{source_key} 自适应调度准入等待超时")
                        wait_for = min(remaining, 0.5)
                    else:
                        cooldowns = [
                            state.cooldown_until
                            for state in self._hosts[source_key].values()
                            if state.cooldown_until > now
                        ]
                        wait_for = max(0.05, min(cooldowns) - now) if cooldowns else 0.5
                    self._condition.wait(timeout=wait_for)
            except Exception:
                if ticket in self._waiting_tickets[source_key]:
                    self._waiting_tickets[source_key].remove(ticket)
                    self._condition.notify_all()
                raise

    def release(
        self,
        permit: AdaptivePermit,
        *,
        success: bool,
        congested: bool = False,
    ) -> None:
        """
        释放准入并反馈执行结果。

        输入：原凭证、是否成功、是否属于连接拥塞。
        输出：无。
        边界：
        1. 重复/未知凭证安全忽略。
        2. 拥塞时降低配额并冷却，同时记录拥塞前上限。
        3. 冷却结束后，成功时每次将配额 +1，直到回到拥塞前上限（且不超过公式值）。
        4. 普通业务失败不改变配额。
        """
        with self._condition:
            active = self._active_permits.pop(int(permit.ticket), None)
            if active is None:
                return
            self._inflight_processes = max(0, self._inflight_processes - 1)
            self._side_inflight[active.source] = max(
                0, int(self._side_inflight[active.source]) - 1
            )
            state = self._hosts[active.source].get(active.host)
            if state is not None:
                state.process_inflight = max(0, int(state.process_inflight) - 1)
                now = time.monotonic()
                if bool(congested):
                    failed_level = max(1, int(state.process_cap))
                    previous_ceiling = state.failed_ceiling
                    if previous_ceiling is not None:
                        state.failed_ceiling = max(int(previous_ceiling), failed_level)
                    else:
                        state.failed_ceiling = failed_level
                    state.process_cap = max(
                        1,
                        int(
                            math.floor(float(state.process_cap) * self._decrease_factor)
                        ),
                    )
                    state.cooldown_until = now + self._cooldown_seconds
                elif (
                    bool(success)
                    and state.cooldown_until <= now
                    and state.failed_ceiling is not None
                ):
                    formula = int(self._processes_per_host[active.source])
                    target = max(1, min(formula, int(state.failed_ceiling)))
                    if int(state.process_cap) < target:
                        state.process_cap = min(target, int(state.process_cap) + 1)
                    if int(state.process_cap) >= target:
                        state.failed_ceiling = None
                if int(state.process_inflight) == 0 and not bool(state.selectable):
                    self._hosts[active.source].pop(active.host, None)
                    self._recompute_side_budgets_locked()
            self._condition.notify_all()

    def reset(self) -> None:
        """
        清空学习状态与等待队列。

        输入/输出：无。
        边界：仅应在无业务或进程池生命周期切换时调用；已发凭证会失效。
        """
        with self._condition:
            self._hosts = {"std": {}, "ex": {}}
            self._side_process_budget = {"std": 0, "ex": 0}
            self._side_inflight = {"std": 0, "ex": 0}
            self._waiting_tickets = {"std": [], "ex": []}
            self._active_permits.clear()
            self._inflight_processes = 0
            self._round_robin_cursor = {"std": 0, "ex": 0}
            self._condition.notify_all()

    def snapshot(self) -> Dict[str, Any]:
        """输入无，输出可序列化调度状态快照；用于日志与离线测试。"""
        with self._condition:
            sides: Dict[str, Any] = {}
            for source, mapping in self._hosts.items():
                sides[source] = {
                    f"{host[0]}:{host[1]}": {
                        "process_cap": int(state.process_cap),
                        "process_inflight": int(state.process_inflight),
                        "cooldown_until": float(state.cooldown_until),
                        "failed_ceiling": state.failed_ceiling,
                    }
                    for host, state in mapping.items()
                }
            return {
                "max_processes": int(self._max_processes),
                "inproc_limit": int(self._inproc_limit),
                "inflight_processes": int(self._inflight_processes),
                "inflight_processes_by_source": {
                    source: int(count) for source, count in self._side_inflight.items()
                },
                "side_process_budget": {
                    source: int(count)
                    for source, count in self._side_process_budget.items()
                },
                "processes_per_host": {
                    source: int(count)
                    for source, count in self._processes_per_host.items()
                },
                "waiting_count": int(
                    sum(len(items) for items in self._waiting_tickets.values())
                ),
                "sides": sides,
            }
