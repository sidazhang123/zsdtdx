"""
基准公共模块：为 examples 下的 taskset 生成、sync/async 基准与三轮对比提供统一能力。

模块用途：
1. 固定任务集构建规则（200 股票 * 5 周期 * 15 时间窗口）。
2. 统一 fetcher 档位配置（sync / async / sync-顺序chunk无future）。
3. 统一 batch 运行统计口径（总体耗时 + 分批耗时 + chunk 页数监控）。

边界约束：
1. 本模块只依赖项目内 `zsdtdx` 对外 API，不依赖额外第三方基准库。
2. 任务集生成后需落盘，后续脚本必须复用同一 taskset，不重新随机。
"""

from __future__ import annotations

import datetime as dt
import json
import random
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import sys

# 统一优先加载仓库 `src` 源码，避免误用系统 site-packages 的旧版本。
EXAMPLES_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = EXAMPLES_DIR.parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import zsdtdx.parallel_fetcher as parallel_fetcher
from zsdtdx import get_client, get_stock_code_name, get_stock_kline, get_supported_markets as api_get_supported_markets

# ---------------------------------------------------------------------------
# 基准参数（统一配置入口）
# ---------------------------------------------------------------------------
TARGET_STOCK_COUNT = 200
FREQS = ["15", "30", "60", "d", "w"]
SAMPLE_COUNT = 15
SAMPLE_SEED = 20260228
SAMPLE_MIN_DAYS = 1
SAMPLE_MAX_DAYS = 3
EXCLUDED_SAMPLE_MONTHS = {1, 2, 5, 10}
DATE_RANGE_START = dt.date(2024, 6, 1)
DATE_RANGE_END = dt.date(2025, 12, 31)

# 分批大小：15000 任务按 500/批 -> 30 批，兼顾吞吐与内存占用。
TASK_BATCH_SIZE = 500

# 基准脚本元数据使用：当前不做早停。
ENABLE_EARLY_STOP = False

# async 至少使用的进程数（避免退化到单进程）。
ASYNC_MIN_PROCESS_WORKERS = 2

# 首次读取 fetcher 时缓存基线配置，保证后续各模式可回滚到一致状态。
_FETCHER_BASELINE: Optional[Dict[str, Any]] = None


class _NullQueue:
    """
    空队列：用于 async 测试时抑制实时队列对象的内存堆积。

    输入：
    1. `put(item)` 兼容队列协议。
    输出：
    1. 丢弃数据，无副作用。
    用途：
    1. 保留 async 行为，但避免 15000 任务时队列保留全部 payload。
    边界条件：
    1. 不影响 `job.result()` 返回值；仅影响中间队列消费链路。
    """

    def put(self, _item: Any) -> None:
        return None


@dataclass
class _LogEvent:
    """
    并行日志采样事件。

    输入：
    1. level/message/detail：与 `_emit_log` 形参一致。
    输出：
    1. 结构化日志对象。
    用途：
    1. 从 chunk 完成日志中提取页数/分片耗时相关监控指标。
    边界条件：
    1. detail 非 dict 时记录为空字典，避免后续聚合异常。
    """

    level: str
    message: str
    detail: Dict[str, Any]


class _BenchmarkLogCollector:
    """
    基准日志收集器：线程安全记录 `_emit_log` 事件。

    输入：
    1. 通过 `emit` 注入日志事件。
    输出：
    1. 可按 index 切片读取事件列表。
    用途：
    1. 在不改动内核并行代码的前提下，提取 chunk 级监控信息。
    边界条件：
    1. 仅在当前进程生效；worker 进程内日志不进入此收集器。
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._events: List[_LogEvent] = []

    def emit(self, level: str, message: str, detail: Optional[Dict[str, Any]] = None) -> None:
        """
        写入单条日志事件。

        输入：
        1. level: 日志级别。
        2. message: 日志文案。
        3. detail: 结构化附加信息。
        输出：
        1. 无返回值。
        用途：
        1. 替换并行层默认 `_emit_log`，沉淀可计算的性能指标。
        边界条件：
        1. detail 非 dict 时自动降级为空字典。
        """
        payload = detail if isinstance(detail, dict) else {}
        event = _LogEvent(level=str(level), message=str(message), detail=dict(payload))
        with self._lock:
            self._events.append(event)

        # 控制台仅输出 warning/error，避免 chunk 级 info 刷屏。
        if str(level).lower() in {"warning", "error"}:
            if payload:
                print(f"[{str(level).lower()}] {message} | {json.dumps(payload, ensure_ascii=False)}")
            else:
                print(f"[{str(level).lower()}] {message}")

    def mark(self) -> int:
        """
        返回当前事件游标。

        输入：
        1. 无显式输入。
        输出：
        1. 事件游标索引。
        用途：
        1. 批次执行前打点，批次结束后按区间切片统计。
        边界条件：
        1. 并发写入时通过锁保证索引一致性。
        """
        with self._lock:
            return int(len(self._events))

    def slice_from(self, start_index: int) -> List[_LogEvent]:
        """
        读取指定游标之后的事件切片。

        输入：
        1. start_index: 起始游标。
        输出：
        1. 事件列表（副本）。
        用途：
        1. 提取当前 batch 的 chunk 监控信息。
        边界条件：
        1. start_index 越界时返回空列表。
        """
        with self._lock:
            if start_index >= len(self._events):
                return []
            return list(self._events[start_index:])


def _safe_int(value: Any, default: int = 0) -> int:
    """
    安全转 int。

    输入：
    1. value: 任意值。
    2. default: 失败默认值。
    输出：
    1. int 结果。
    用途：
    1. 防御日志 detail 字段类型不稳定。
    边界条件：
    1. 转换失败时返回 default。
    """
    try:
        return int(value)
    except Exception:
        return int(default)


def _safe_float(value: Any, default: float = 0.0) -> float:
    """
    安全转 float。

    输入：
    1. value: 任意值。
    2. default: 失败默认值。
    输出：
    1. float 结果。
    用途：
    1. 统一计算吞吐/耗时指标。
    边界条件：
    1. 转换失败时返回 default。
    """
    try:
        return float(value)
    except Exception:
        return float(default)


def _capture_fetcher_baseline(fetcher: Any) -> Dict[str, Any]:
    """
    缓存 fetcher 基线配置。

    输入：
    1. fetcher: 并行抓取器实例。
    输出：
    1. 基线配置字典。
    用途：
    1. 每轮模式切换后可恢复到初始默认行为，保证对比公平。
    边界条件：
    1. 仅首次捕获，后续复用同一快照。
    """
    global _FETCHER_BASELINE
    if _FETCHER_BASELINE is None:
        _FETCHER_BASELINE = {
            "num_processes": _safe_int(getattr(fetcher, "num_processes", 1), 1),
            "task_chunk_inproc_future_workers": _safe_int(
                getattr(fetcher, "task_chunk_inproc_future_workers", 2), 2
            ),
            "task_chunk_force_parallel_when_single_process": bool(
                getattr(fetcher, "task_chunk_force_parallel_when_single_process", False)
            ),
            "task_chunk_max_inflight_multiplier": _safe_int(
                getattr(fetcher, "task_chunk_max_inflight_multiplier", 2), 2
            ),
            "task_chunk_cache_min_tasks": _safe_int(getattr(fetcher, "task_chunk_cache_min_tasks", 3), 3),
            "auto_prewarm_on_async": bool(getattr(fetcher, "auto_prewarm_on_async", True)),
        }
    return dict(_FETCHER_BASELINE)


def _iter_business_days(
    start_date: dt.date,
    end_date: dt.date,
    excluded_months: Sequence[int],
) -> List[dt.date]:
    """
    构建候选交易日列表（按工作日近似）。

    输入：
    1. start_date/end_date: 日期范围。
    2. excluded_months: 需剔除的月份集合。
    输出：
    1. 升序日期列表。
    用途：
    1. 作为 2-3 日连续样本窗口的候选池。
    边界条件：
    1. 周末剔除；月份命中 excluded_months 时剔除。
    """
    excluded = {int(item) for item in excluded_months}
    out: List[dt.date] = []
    cursor = start_date
    while cursor <= end_date:
        if cursor.weekday() < 5 and cursor.month not in excluded:
            out.append(cursor)
        cursor += dt.timedelta(days=1)
    return out


def _collect_stock_codes(target_count: int) -> List[str]:
    """
    收集基准股票池（默认取 sh/sz 前缀）。

    输入：
    1. target_count: 目标股票数量。
    输出：
    1. 规范化代码列表（如 `sh.600000`）。
    用途：
    1. 生成固定 200 股票任务集。
    边界条件：
    1. 可用代码不足时抛 RuntimeError。
    """
    with get_client() as _client:
        raw_map = get_stock_code_name(use_cache=True)

    if not isinstance(raw_map, dict):
        raise RuntimeError("get_stock_code_name 返回结构非法（需 dict）")

    codes = [str(item).strip() for item in raw_map.keys()]
    codes = [item for item in codes if item.startswith(("sh.", "sz."))]
    codes = sorted(set(codes))
    if len(codes) < int(target_count):
        raise RuntimeError(f"股票代码不足：target={target_count}, actual={len(codes)}")
    return codes[: int(target_count)]


def _build_samples(count: int, seed: int) -> List[Tuple[str, str]]:
    """
    构建时间样本窗口（2018-2020，随机 15 组连续 1-3 天）。

    输入：
    1. count: 样本窗口个数。
    2. seed: 随机种子，保证可复现。
    输出：
    1. `(start_date, end_date)` 列表，日期字符串格式 `YYYY-MM-DD`。
    用途：
    1. 为 taskset 提供固定时间窗口输入。
    边界条件：
    1. 每组窗口长度为 1~3 个连续“候选交易日”（非周六周日）。
    2. 窗口间不重叠（至少不共享候选交易日）。
    """
    rng = random.Random(int(seed))
    candidates = _iter_business_days(
        start_date=DATE_RANGE_START,
        end_date=DATE_RANGE_END,
        excluded_months=sorted(list(EXCLUDED_SAMPLE_MONTHS)),
    )
    if len(candidates) < int(count) * int(SAMPLE_MIN_DAYS):
        raise RuntimeError("候选交易日不足，无法构建样本窗口")

    excluded = set(int(item) for item in EXCLUDED_SAMPLE_MONTHS)
    available = set(candidates)
    samples: List[Tuple[str, str]] = []
    max_attempts = max(2000, int(count) * 400)
    attempts = 0

    while len(samples) < int(count):
        attempts += 1
        if attempts > max_attempts:
            raise RuntimeError(f"样本构建失败：达到最大尝试次数（{max_attempts}）")

        if len(available) < SAMPLE_MIN_DAYS:
            raise RuntimeError("样本构建失败：可用日期不足")

        ordered_available = sorted(available)
        start_day = ordered_available[rng.randrange(len(ordered_available))]

        # 在 1~3 天内随机优先级；窗口要求“日历连续”且每天均为候选交易日。
        preferred = rng.randint(int(SAMPLE_MIN_DAYS), int(SAMPLE_MAX_DAYS))
        fallback = list(range(int(SAMPLE_MAX_DAYS), int(SAMPLE_MIN_DAYS) - 1, -1))
        preferred_lengths = [preferred] + [item for item in fallback if item != preferred]
        selected_days: List[dt.date] = []
        for window_len in preferred_lengths:
            candidate_window = [start_day + dt.timedelta(days=offset) for offset in range(int(window_len))]
            is_valid = True
            for item_day in candidate_window:
                if item_day > DATE_RANGE_END:
                    is_valid = False
                    break
                if item_day.weekday() >= 5:
                    is_valid = False
                    break
                if item_day.month in excluded:
                    is_valid = False
                    break
                if item_day not in available:
                    is_valid = False
                    break
            if is_valid:
                selected_days = candidate_window
                break

        if len(selected_days) < SAMPLE_MIN_DAYS:
            # 该起点无法满足 1~3 天连续窗口，移除后继续尝试。
            available.discard(start_day)
            continue

        for day in selected_days:
            available.discard(day)
        samples.append((selected_days[0].isoformat(), selected_days[-1].isoformat()))

    samples.sort(key=lambda item: (item[0], item[1]))
    return samples


def build_tasks(
    codes: Sequence[str],
    freqs: Sequence[str],
    samples: Sequence[Tuple[str, str]],
) -> List[Dict[str, str]]:
    """
    组合任务列表。

    输入：
    1. codes: 股票代码列表。
    2. freqs: 周期列表。
    3. samples: 时间窗口列表。
    输出：
    1. task 字典列表（code/freq/start_time/end_time）。
    用途：
    1. 固定 `200 * 5 * 15` 任务集。
    边界条件：
    1. 输入任一为空时返回空列表。
    """
    tasks: List[Dict[str, str]] = []
    for code in list(codes or []):
        normalized_code = str(code).strip()
        if normalized_code == "":
            continue
        for freq in list(freqs or []):
            normalized_freq = str(freq).strip()
            if normalized_freq == "":
                continue
            for start_time, end_time in list(samples or []):
                tasks.append(
                    {
                        "code": normalized_code,
                        "freq": normalized_freq,
                        "start_time": str(start_time),
                        "end_time": str(end_time),
                    }
                )
    return tasks


def get_fetcher():
    """
    获取全局并行抓取器。

    输入：
    1. 无显式输入。
    输出：
    1. `ParallelKlineFetcher` 实例。
    用途：
    1. 各基准脚本统一从同一入口读取/调整并行参数。
    边界条件：
    1. 首次调用会按配置自动初始化实例。
    """
    return parallel_fetcher.get_fetcher()


def get_supported_markets(return_df: bool = False):
    """
    获取支持市场列表（带客户端上下文）。

    输入：
    1. return_df: 是否返回 DataFrame。
    输出：
    1. list 或 DataFrame（取决于 `return_df`）。
    用途：
    1. 触发一次基础连接检查，避免基准首批才暴露连接问题。
    边界条件：
    1. 连接失败时异常上抛，由上层脚本统一处理。
    """
    with get_client() as _client:
        return api_get_supported_markets(return_df=return_df)


def _configure_fetcher_for_mode(fetcher: Any, mode: str, async_process_workers: int) -> Dict[str, Any]:
    """
    配置 fetcher 为 sync 或 async 基准档位。

    输入：
    1. fetcher: 并行抓取器实例。
    2. mode: `sync` 或 `async`。
    3. async_process_workers: 目标进程数（通常取默认进程数或更高下限）。
    输出：
    1. 当前生效配置快照。
    用途：
    1. 保证各脚本在同一口径下切换模式并可记录复现实验参数。
    边界条件：
    1. mode 非法时抛 ValueError。
    """
    mode_key = str(mode or "").strip().lower()
    if mode_key not in {"sync", "async"}:
        raise ValueError("mode 仅支持 sync/async")

    baseline = _capture_fetcher_baseline(fetcher)
    target_workers = max(1, int(async_process_workers))

    fetcher.num_processes = int(target_workers)
    fetcher.task_chunk_cache_min_tasks = int(baseline["task_chunk_cache_min_tasks"])
    fetcher.task_chunk_max_inflight_multiplier = int(baseline["task_chunk_max_inflight_multiplier"])
    fetcher.task_chunk_force_parallel_when_single_process = bool(
        baseline["task_chunk_force_parallel_when_single_process"]
    )
    fetcher.task_chunk_inproc_future_workers = int(baseline["task_chunk_inproc_future_workers"])

    # async 档位保留自动预热；sync 档位关闭自动预热避免额外预热开销干扰统计。
    if mode_key == "async":
        fetcher.auto_prewarm_on_async = bool(baseline["auto_prewarm_on_async"])
    else:
        fetcher.auto_prewarm_on_async = False

    return {
        "mode": mode_key,
        "num_processes": int(fetcher.num_processes),
        "task_chunk_cache_min_tasks": int(fetcher.task_chunk_cache_min_tasks),
        "task_chunk_inproc_future_workers": int(fetcher.task_chunk_inproc_future_workers),
        "task_chunk_force_parallel_when_single_process": bool(fetcher.task_chunk_force_parallel_when_single_process),
        "task_chunk_max_inflight_multiplier": int(fetcher.task_chunk_max_inflight_multiplier),
        "auto_prewarm_on_async": bool(fetcher.auto_prewarm_on_async),
    }


def configure_sync_chunk_sequential_no_future(fetcher: Any, process_workers: Optional[int] = None) -> Dict[str, Any]:
    """
    配置“进程内顺序 chunk，不使用 inproc future 并发”档位。

    输入：
    1. fetcher: 并行抓取器实例。
    2. process_workers: 可选进程数；不传时保留当前值。
    输出：
    1. 当前生效配置快照。
    用途：
    1. 对比 `task_chunk_inproc_future_workers=1` 与默认并发值的差异。
    边界条件：
    1. 该配置仅关闭“进程内 chunk future 并发”，不关闭进程池并发。
    """
    baseline = _capture_fetcher_baseline(fetcher)
    if process_workers is not None:
        fetcher.num_processes = max(1, int(process_workers))
    fetcher.task_chunk_cache_min_tasks = int(baseline["task_chunk_cache_min_tasks"])
    fetcher.task_chunk_max_inflight_multiplier = int(baseline["task_chunk_max_inflight_multiplier"])
    fetcher.task_chunk_force_parallel_when_single_process = bool(
        baseline["task_chunk_force_parallel_when_single_process"]
    )
    fetcher.task_chunk_inproc_future_workers = 1
    fetcher.auto_prewarm_on_async = False
    return {
        "mode": "sync",
        "num_processes": int(fetcher.num_processes),
        "task_chunk_cache_min_tasks": int(fetcher.task_chunk_cache_min_tasks),
        "task_chunk_inproc_future_workers": int(fetcher.task_chunk_inproc_future_workers),
        "task_chunk_force_parallel_when_single_process": bool(fetcher.task_chunk_force_parallel_when_single_process),
        "task_chunk_max_inflight_multiplier": int(fetcher.task_chunk_max_inflight_multiplier),
        "auto_prewarm_on_async": bool(fetcher.auto_prewarm_on_async),
        "sync_chunk_policy": "sequential_inproc_no_future",
    }


def _chunk_tasks(tasks: Sequence[Dict[str, str]], batch_size: int) -> List[List[Dict[str, str]]]:
    """
    将任务切分为固定批次。

    输入：
    1. tasks: 任务列表。
    2. batch_size: 批大小。
    输出：
    1. 分批后的任务二维列表。
    用途：
    1. 控制单次调用负载，降低内存峰值并便于定位慢批次。
    边界条件：
    1. batch_size <= 0 时自动收敛为 1。
    """
    size = max(1, int(batch_size))
    output: List[List[Dict[str, str]]] = []
    cursor = 0
    tasks_list = [dict(item) for item in list(tasks or [])]
    while cursor < len(tasks_list):
        output.append(tasks_list[cursor : cursor + size])
        cursor += size
    return output


def _summarize_payloads(payloads: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """
    汇总 payload 基础统计。

    输入：
    1. payloads: 单批返回的 task payload 列表。
    输出：
    1. 总任务/成功失败/总行数/worker pid 集合等统计。
    用途：
    1. 与 chunk 监控拼接后形成 batch 级监控对象。
    边界条件：
    1. payload 结构异常时按空值容错，不抛异常中断基准。
    """
    total_tasks = int(len(payloads))
    failed_tasks = 0
    rows_total = 0
    worker_pids: set[int] = set()
    for item in list(payloads or []):
        if not isinstance(item, dict):
            failed_tasks += 1
            continue
        if str(item.get("error") or "").strip():
            failed_tasks += 1
        rows = item.get("rows")
        if isinstance(rows, list):
            rows_total += int(len(rows))
        pid = _safe_int(item.get("worker_pid"), 0)
        if pid > 0:
            worker_pids.add(pid)
    return {
        "total_tasks": int(total_tasks),
        "success_tasks": int(total_tasks - failed_tasks),
        "failed_tasks": int(failed_tasks),
        "rows_total": int(rows_total),
        "worker_pids": worker_pids,
    }


def _summarize_chunk_monitor(
    *,
    events: Sequence[_LogEvent],
    elapsed_seconds: float,
    task_count: int,
    rows_total: int,
) -> Dict[str, Any]:
    """
    汇总 chunk 级监控指标。

    输入：
    1. events: 当前 batch 的日志事件切片。
    2. elapsed_seconds: 当前 batch 耗时。
    3. task_count: 当前 batch 任务数。
    4. rows_total: 当前 batch 行数总计。
    输出：
    1. monitor 统计字典。
    用途：
    1. 提供“各部分耗时/页数/吞吐”的分解观察维度。
    边界条件：
    1. 无 chunk 事件时相关均值字段返回 0。
    """
    chunk_completed = 0
    chunk_failed = 0
    chunk_task_total = 0
    chunk_network_page_calls = 0
    chunk_hit_tasks = 0

    for event in list(events or []):
        if not isinstance(event, _LogEvent):
            continue
        detail = event.detail if isinstance(event.detail, dict) else {}
        stage = str(detail.get("stage", "")).strip()
        if stage == "chunk_completed":
            chunk_completed += 1
            chunk_task_total += _safe_int(detail.get("chunk_task_count"), 0)
            chunk_network_page_calls += _safe_int(detail.get("chunk_network_page_calls"), 0)
            chunk_hit_tasks += _safe_int(detail.get("chunk_hit_tasks"), 0)
        elif stage == "chunk_failed":
            chunk_failed += 1
            chunk_task_total += _safe_int(detail.get("chunk_task_count"), 0)

    chunk_count = int(chunk_completed + chunk_failed)
    elapsed = max(1e-9, float(elapsed_seconds))
    avg_pages_per_chunk = (float(chunk_network_page_calls) / float(chunk_count)) if chunk_count > 0 else 0.0
    avg_tasks_per_chunk = (float(chunk_task_total) / float(chunk_count)) if chunk_count > 0 else 0.0

    return {
        "chunk_count": int(chunk_count),
        "chunk_completed": int(chunk_completed),
        "chunk_failed": int(chunk_failed),
        "chunk_network_page_calls": int(chunk_network_page_calls),
        "chunk_hit_tasks": int(chunk_hit_tasks),
        "avg_pages_per_chunk": round(float(avg_pages_per_chunk), 4),
        "avg_tasks_per_chunk": round(float(avg_tasks_per_chunk), 4),
        "tasks_per_sec": round(float(task_count) / elapsed, 4),
        "rows_per_sec": round(float(rows_total) / elapsed, 4),
    }


def run_mode_batched(mode: str, tasks: Sequence[Dict[str, str]], batch_size: int = TASK_BATCH_SIZE) -> Dict[str, Any]:
    """
    按批执行 sync/async 测试并返回统一统计结果。

    输入：
    1. mode: `sync` 或 `async`。
    2. tasks: 全量任务列表（建议复用落盘 taskset）。
    3. batch_size: 批大小。
    输出：
    1. 模式级统计字典（含总体/分批/chunk 监控）。
    用途：
    1. 作为所有基准脚本统一测试执行入口。
    边界条件：
    1. 空任务直接返回 0 统计。
    2. mode 非法抛 ValueError。
    """
    mode_key = str(mode or "").strip().lower()
    if mode_key not in {"sync", "async"}:
        raise ValueError("mode 仅支持 sync/async")

    batches = _chunk_tasks(tasks=list(tasks or []), batch_size=batch_size)
    if not batches:
        return {
            "mode": mode_key,
            "elapsed_seconds": 0.0,
            "total_tasks": 0,
            "success_tasks": 0,
            "failed_tasks": 0,
            "rows_total": 0,
            "unique_worker_pids": 0,
            "tasks_per_sec": 0.0,
            "rows_per_sec": 0.0,
            "chunk_count": 0,
            "chunk_network_page_calls": 0,
            "avg_pages_per_chunk": 0.0,
            "batches": [],
        }

    collector = _BenchmarkLogCollector()
    original_emit = getattr(parallel_fetcher, "_emit_log")
    setattr(parallel_fetcher, "_emit_log", collector.emit)

    started = time.perf_counter()
    total_tasks = 0
    success_tasks = 0
    failed_tasks = 0
    rows_total = 0
    worker_pids: set[int] = set()
    all_chunk_count = 0
    all_chunk_pages = 0
    batch_reports: List[Dict[str, Any]] = []

    try:
        with get_client() as _client:
            for batch_index, batch_tasks in enumerate(batches, start=1):
                marker = collector.mark()
                batch_started = time.perf_counter()

                if mode_key == "sync":
                    payloads = get_stock_kline(task=batch_tasks, mode="sync")
                else:
                    job = get_stock_kline(task=batch_tasks, mode="async", queue=_NullQueue())
                    payloads = job.result()

                batch_elapsed = time.perf_counter() - batch_started
                payload_stat = _summarize_payloads(payloads)
                monitor = _summarize_chunk_monitor(
                    events=collector.slice_from(marker),
                    elapsed_seconds=batch_elapsed,
                    task_count=_safe_int(payload_stat.get("total_tasks"), 0),
                    rows_total=_safe_int(payload_stat.get("rows_total"), 0),
                )

                total_tasks += _safe_int(payload_stat.get("total_tasks"), 0)
                success_tasks += _safe_int(payload_stat.get("success_tasks"), 0)
                failed_tasks += _safe_int(payload_stat.get("failed_tasks"), 0)
                rows_total += _safe_int(payload_stat.get("rows_total"), 0)
                worker_pids.update(set(payload_stat.get("worker_pids") or set()))
                all_chunk_count += _safe_int(monitor.get("chunk_count"), 0)
                all_chunk_pages += _safe_int(monitor.get("chunk_network_page_calls"), 0)

                batch_reports.append(
                    {
                        "batch_index": int(batch_index),
                        "task_count": _safe_int(payload_stat.get("total_tasks"), 0),
                        "success_tasks": _safe_int(payload_stat.get("success_tasks"), 0),
                        "failed_tasks": _safe_int(payload_stat.get("failed_tasks"), 0),
                        "rows_total": _safe_int(payload_stat.get("rows_total"), 0),
                        "elapsed_seconds": round(float(batch_elapsed), 4),
                        "monitor": monitor,
                    }
                )
                print(
                    f"[{mode_key}] batch={batch_index}/{len(batches)} "
                    f"tasks={payload_stat.get('total_tasks', 0)} "
                    f"failed={payload_stat.get('failed_tasks', 0)} "
                    f"elapsed={batch_elapsed:.3f}s"
                )
    finally:
        setattr(parallel_fetcher, "_emit_log", original_emit)

    elapsed = time.perf_counter() - started
    elapsed_safe = max(1e-9, float(elapsed))
    avg_pages_per_chunk = (float(all_chunk_pages) / float(all_chunk_count)) if all_chunk_count > 0 else 0.0
    return {
        "mode": mode_key,
        "elapsed_seconds": round(float(elapsed), 4),
        "total_tasks": int(total_tasks),
        "success_tasks": int(success_tasks),
        "failed_tasks": int(failed_tasks),
        "rows_total": int(rows_total),
        "unique_worker_pids": int(len(worker_pids)),
        "tasks_per_sec": round(float(total_tasks) / elapsed_safe, 4),
        "rows_per_sec": round(float(rows_total) / elapsed_safe, 4),
        "chunk_count": int(all_chunk_count),
        "chunk_network_page_calls": int(all_chunk_pages),
        "avg_pages_per_chunk": round(float(avg_pages_per_chunk), 4),
        "batch_size": int(batch_size),
        "batch_count": int(len(batch_reports)),
        "batches": batch_reports,
    }


def _cleanup_parallel_pool(run_name: str = "") -> Dict[str, Any]:
    """
    清理并行进程池，避免脚本退出后残留 worker。

    输入：
    1. run_name: 调用方标识（用于日志/排障）。
    输出：
    1. 清理摘要字典。
    用途：
    1. 基准脚本 finally 阶段统一回收进程池。
    边界条件：
    1. 清理失败时返回 error 字段，不再二次抛异常中断退出。
    """
    try:
        summary = parallel_fetcher.force_restart_parallel_fetcher(prewarm=False)
        return {"ok": True, "run_name": str(run_name), "summary": summary}
    except Exception as exc:
        return {"ok": False, "run_name": str(run_name), "error": str(exc)}
