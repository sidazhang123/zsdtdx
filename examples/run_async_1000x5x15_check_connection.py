"""
1000*5*15 async 压测脚本：检测“进程内执行一些任务后连接不可用”类错误。

用途：
1. 固定按 `stock_count * freqs * sample_count` 生成任务（默认 1000*5*15）。
2. 仅执行 async 模式，按 batch 统计失败与耗时。
3. 匹配目标错误关键字并在命中后可选择提前停止。
4. 将完整结果写入 JSON，便于复现与对比。

边界：
1. 本脚本为执行入口，不改动项目主配置文件。
2. 参数统一来自同目录 YAML 配置，不通过 CLI 暴露可调参数。
"""

from __future__ import annotations

import datetime as dt
import json
import time
import traceback
from collections import Counter
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import yaml

import sync_async_benchmark_1000x5x15 as bench
import zsdtdx.parallel_fetcher as parallel_fetcher_module
from zsdtdx import get_stock_kline


EXAMPLES_DIR = Path(__file__).resolve().parent
CONFIG_FILENAME = "run_async_1000x5x15_check_connection.config.yaml"


class _NullQueue:
    """
    空队列实现：用于 async 模式下丢弃实时队列消息，避免内存堆积。

    输入：
    1. put(item): 任意队列项。
    输出：
    1. 无返回值。
    用途：
    1. 保留 async 链路行为，同时降低测试过程中的队列占用。
    边界条件：
    1. 仅影响实时队列，不影响 `job.result()` 的结果。
    """

    def put(self, _item: Any) -> None:
        return None


def _safe_int(value: Any, default: int = 0) -> int:
    """
    安全转换 int。

    输入：
    1. value: 任意值。
    2. default: 转换失败时默认值。
    输出：
    1. int 结果。
    用途：
    1. 统一解析 YAML 参数，避免类型异常中断。
    边界条件：
    1. 转换失败时返回 default。
    """
    try:
        return int(value)
    except Exception:
        return int(default)


def _safe_float(value: Any, default: float = 0.0) -> float:
    """
    安全转换 float。

    输入：
    1. value: 任意值。
    2. default: 转换失败时默认值。
    输出：
    1. float 结果。
    用途：
    1. 统一解析超时与阈值参数。
    边界条件：
    1. 转换失败时返回 default。
    """
    try:
        return float(value)
    except Exception:
        return float(default)


def _load_runtime_config() -> Tuple[Dict[str, Any], Path]:
    """
    读取运行配置。

    输入：
    1. 无显式输入参数。
    输出：
    1. `(config_dict, config_path)`。
    用途：
    1. 从 YAML 加载本次压测参数，避免硬编码与 CLI 参数扩散。
    边界条件：
    1. 配置文件不存在或结构非法时抛 RuntimeError。
    """
    config_path = EXAMPLES_DIR / CONFIG_FILENAME
    if not config_path.exists():
        raise RuntimeError(f"配置文件不存在: {config_path}")

    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise RuntimeError(f"配置文件结构非法（需为 dict）: {config_path}")
    return dict(raw), config_path


def _normalize_keywords(raw_keywords: Any) -> List[str]:
    """
    规范化目标错误关键字列表。

    输入：
    1. raw_keywords: YAML 中配置的关键字集合。
    输出：
    1. 去空去重后的关键字列表。
    用途：
    1. 统一错误匹配口径，避免空值和重复项干扰统计。
    边界条件：
    1. 输入非列表时返回空列表。
    """
    if not isinstance(raw_keywords, list):
        return []
    out: List[str] = []
    for item in raw_keywords:
        text = str(item or "").strip()
        if text and text not in out:
            out.append(text)
    return out


def _contains_target_error(text: str, keywords: Sequence[str]) -> bool:
    """
    判断错误文本是否命中目标关键字。

    输入：
    1. text: 待检查错误文本。
    2. keywords: 关键字列表。
    输出：
    1. 命中返回 True，否则 False。
    用途：
    1. 捕获“连接不可用/进程池不可用”等目标故障。
    边界条件：
    1. text 为空时返回 False。
    """
    plain = str(text or "").strip()
    if plain == "":
        return False
    plain_lower = plain.lower()
    for keyword in list(keywords or []):
        key = str(keyword or "").strip()
        if key == "":
            continue
        if key in plain:
            return True
        if key.lower() in plain_lower:
            return True
    return False


def _build_connection_error_keywords(target_keywords: Sequence[str]) -> List[str]:
    """
    构建“连接不可用”错误匹配关键字集合。

    输入：
    1. target_keywords: 配置中的目标关键字。
    输出：
    1. 去重后的关键字列表。
    用途：
    1. 统一“连接不可用/进程池不可用”类错误统计口径。
    边界条件：
    1. 输入为空时返回内置默认关键字集合。
    """
    defaults = [
        "进程内执行一些任务后连接不可用",
        "连接不可用",
        "无可用连接",
        "BrokenProcessPool",
        "process pool is not usable anymore",
        "A child process terminated abruptly",
        "connection unavailable",
        "no available connection",
    ]
    merged: List[str] = []
    for raw in list(defaults) + list(target_keywords or []):
        plain = str(raw or "").strip()
        if plain and plain not in merged:
            merged.append(plain)
    return merged


def _resolve_taskset_path(task_cfg: Dict[str, Any]) -> Optional[Path]:
    """
    解析 taskset 文件路径。

    输入：
    1. task_cfg: 任务配置字典。
    输出：
    1. 绝对 Path；未配置时返回 None。
    用途：
    1. 支持通过 `taskset_path` 或 `taskset_filename` 指定预生成任务集文件。
    边界条件：
    1. 相对路径按 examples 目录解析。
    """
    taskset_path_raw = str(task_cfg.get("taskset_path", "") or "").strip()
    taskset_filename_raw = str(task_cfg.get("taskset_filename", "") or "").strip()
    if taskset_path_raw == "" and taskset_filename_raw == "":
        return None
    if taskset_path_raw != "":
        candidate = Path(taskset_path_raw)
    else:
        candidate = Path(taskset_filename_raw)
    if not candidate.is_absolute():
        candidate = EXAMPLES_DIR / candidate
    return candidate.resolve()


def _normalize_taskset_samples(raw_samples: Any) -> List[Tuple[str, str]]:
    """
    标准化 taskset 中的样本窗口列表。

    输入：
    1. raw_samples: taskset 文件中的 samples 字段。
    输出：
    1. `(start_time, end_time)` 元组列表。
    用途：
    1. 统一 taskset 加载后的 samples 结构，便于复用既有报告逻辑。
    边界条件：
    1. 非法元素会被忽略。
    """
    out: List[Tuple[str, str]] = []
    for item in list(raw_samples or []):
        if not isinstance(item, (list, tuple)) or len(item) < 2:
            continue
        start_time = str(item[0] or "").strip()
        end_time = str(item[1] or "").strip()
        if start_time == "" or end_time == "":
            continue
        out.append((start_time, end_time))
    return out


def _normalize_taskset_tasks(raw_tasks: Any) -> List[Dict[str, str]]:
    """
    标准化 taskset 中的任务列表。

    输入：
    1. raw_tasks: taskset 文件中的 tasks 字段。
    输出：
    1. 标准 task 字典列表（code/freq/start_time/end_time）。
    用途：
    1. 确保从文件读取的任务结构与 `get_stock_kline` 入参契约一致。
    边界条件：
    1. 非法任务项会被忽略；若最终为空由上层抛错。
    """
    tasks: List[Dict[str, str]] = []
    for item in list(raw_tasks or []):
        if not isinstance(item, dict):
            continue
        code = str(item.get("code", "")).strip()
        freq = str(item.get("freq", "")).strip()
        start_time = str(item.get("start_time", "")).strip()
        end_time = str(item.get("end_time", "")).strip()
        if code == "" or freq == "" or start_time == "" or end_time == "":
            continue
        tasks.append(
            {
                "code": code,
                "freq": freq,
                "start_time": start_time,
                "end_time": end_time,
            }
        )
    return tasks


def _derive_taskset_meta_from_tasks(tasks: Sequence[Dict[str, str]]) -> Dict[str, Any]:
    """
    从任务列表推导 codes/freqs/samples 元信息。

    输入：
    1. tasks: 标准任务列表。
    输出：
    1. 包含 `codes/freqs/samples` 的字典。
    用途：
    1. 在 taskset 缺少部分元字段时提供稳定兜底。
    边界条件：
    1. 输入为空时返回空列表元信息。
    """
    codes: List[str] = []
    freqs: List[str] = []
    samples: List[Tuple[str, str]] = []
    seen_codes = set()
    seen_freqs = set()
    seen_samples = set()
    for item in list(tasks or []):
        code = str(item.get("code", "")).strip()
        freq = str(item.get("freq", "")).strip()
        start_time = str(item.get("start_time", "")).strip()
        end_time = str(item.get("end_time", "")).strip()
        if code and code not in seen_codes:
            seen_codes.add(code)
            codes.append(code)
        if freq and freq not in seen_freqs:
            seen_freqs.add(freq)
            freqs.append(freq)
        sample_key = (start_time, end_time)
        if start_time and end_time and sample_key not in seen_samples:
            seen_samples.add(sample_key)
            samples.append(sample_key)
    return {"codes": codes, "freqs": freqs, "samples": samples}


def _try_load_taskset(task_cfg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    按配置尝试加载预生成 taskset。

    输入：
    1. task_cfg: 任务配置字典。
    输出：
    1. 成功时返回任务字典；未启用 taskset 时返回 None。
    用途：
    1. 优先复用落盘任务集，避免运行时重复拉代码列表。
    边界条件：
    1. taskset_required=True 且文件缺失/结构非法时抛 RuntimeError。
    """
    use_taskset_file_first = bool(task_cfg.get("use_taskset_file_first", True))
    taskset_required = bool(task_cfg.get("taskset_required", False))
    taskset_path = _resolve_taskset_path(task_cfg)
    if not use_taskset_file_first:
        if taskset_required:
            raise RuntimeError("taskset_required=true 但 use_taskset_file_first=false，配置冲突")
        return None
    if taskset_path is None:
        if taskset_required:
            raise RuntimeError("taskset_required=true 但未配置 taskset_path/taskset_filename")
        return None
    if not taskset_path.exists():
        if taskset_required:
            raise RuntimeError(f"taskset 文件不存在: {taskset_path}")
        return None

    try:
        raw = json.loads(taskset_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RuntimeError(f"读取 taskset 失败: {taskset_path} | {type(exc).__name__}: {exc}") from exc

    if not isinstance(raw, dict):
        raise RuntimeError(f"taskset 结构非法（需为 dict）: {taskset_path}")
    tasks = _normalize_taskset_tasks(raw.get("tasks"))
    if not tasks:
        raise RuntimeError(f"taskset 无有效 tasks: {taskset_path}")

    derived = _derive_taskset_meta_from_tasks(tasks)
    raw_codes = [str(item).strip() for item in list(raw.get("codes") or []) if str(item).strip()]
    codes = raw_codes if raw_codes else list(derived["codes"])

    raw_samples = _normalize_taskset_samples(raw.get("samples"))
    samples = raw_samples if raw_samples else list(derived["samples"])

    meta = raw.get("meta")
    raw_freqs = []
    if isinstance(meta, dict):
        raw_freqs = [str(item).strip() for item in list(meta.get("freqs") or []) if str(item).strip()]
    freqs = raw_freqs if raw_freqs else list(derived["freqs"])

    return {
        "codes": list(codes),
        "samples": list(samples),
        "freqs": list(freqs),
        "tasks": list(tasks),
        "stock_count": int(len(codes)),
        "sample_count": int(len(samples)),
        "sample_seed": _safe_int((meta or {}).get("sample_seed", task_cfg.get("sample_seed", bench.SAMPLE_SEED)), bench.SAMPLE_SEED),
        "task_source": "taskset_file",
        "taskset_path": str(taskset_path),
        "taskset_meta": dict(meta) if isinstance(meta, dict) else {},
    }


def _build_tasks(task_cfg: Dict[str, Any]) -> Dict[str, Any]:
    """
    构建本次压测任务集。

    输入：
    1. task_cfg: 任务配置（股票数、频率、样本数、随机种子）。
    输出：
    1. 包含 `codes/samples/freqs/tasks` 的字典。
    用途：
    1. 固定生成 1000*5*15 输入，保证各次复测可比。
    边界条件：
    1. 代码池不足或样本构建失败时异常上抛。
    """
    loaded_taskset = _try_load_taskset(task_cfg)
    if loaded_taskset is not None:
        return dict(loaded_taskset)

    stock_count = max(1, _safe_int(task_cfg.get("stock_count", 1000), 1000))
    sample_count = max(1, _safe_int(task_cfg.get("sample_count", 15), 15))
    sample_seed = _safe_int(task_cfg.get("sample_seed", bench.SAMPLE_SEED), bench.SAMPLE_SEED)

    freqs_raw = task_cfg.get("freqs", list(bench.FREQS))
    if isinstance(freqs_raw, list):
        freqs = [str(item).strip() for item in freqs_raw if str(item).strip()]
    else:
        freqs = list(bench.FREQS)
    if not freqs:
        freqs = list(bench.FREQS)

    codes = bench._collect_stock_codes(stock_count)
    samples = bench._build_samples(sample_count, sample_seed)
    tasks = bench.build_tasks(codes=codes, freqs=freqs, samples=samples)
    return {
        "codes": codes,
        "samples": samples,
        "freqs": freqs,
        "tasks": tasks,
        "stock_count": stock_count,
        "sample_count": sample_count,
        "sample_seed": sample_seed,
        "task_source": "generated_runtime",
        "taskset_path": "",
        "taskset_meta": {},
    }


def _apply_async_fetcher_profile(fetcher: Any, fetcher_cfg: Dict[str, Any]) -> Dict[str, Any]:
    """
    应用 async fetcher 配置并返回生效快照。

    输入：
    1. fetcher: 并行抓取器实例。
    2. fetcher_cfg: fetcher 参数配置字典。
    输出：
    1. 生效配置快照字典。
    用途：
    1. 统一控制并发档位与超时参数，保证复测口径一致。
    边界条件：
    1. 属性不存在时自动跳过，避免兼容问题。
    """
    default_workers = max(1, _safe_int(getattr(fetcher, "num_processes", 1), 1))
    configured_workers = _safe_int(fetcher_cfg.get("async_process_workers", 0), 0)
    target_workers = configured_workers if configured_workers > 0 else default_workers
    target_workers = max(bench.ASYNC_MIN_PROCESS_WORKERS, int(target_workers))

    profile = bench._configure_fetcher_for_mode(
        fetcher=fetcher,
        mode="async",
        async_process_workers=target_workers,
    )

    overrides = {
        "parallel_total_timeout_seconds": fetcher_cfg.get("parallel_total_timeout_seconds"),
        "parallel_result_timeout_seconds": fetcher_cfg.get("parallel_result_timeout_seconds"),
        "task_chunk_cache_min_tasks": fetcher_cfg.get("task_chunk_cache_min_tasks"),
        "task_chunk_inproc_future_workers": fetcher_cfg.get("task_chunk_inproc_future_workers"),
        "task_chunk_max_inflight_multiplier": fetcher_cfg.get("task_chunk_max_inflight_multiplier"),
        "auto_prewarm_on_async": fetcher_cfg.get("auto_prewarm_on_async"),
        "auto_prewarm_timeout_seconds": fetcher_cfg.get("auto_prewarm_timeout_seconds"),
        "auto_prewarm_max_rounds": fetcher_cfg.get("auto_prewarm_max_rounds"),
        "auto_prewarm_require_all_workers": fetcher_cfg.get("auto_prewarm_require_all_workers"),
        "auto_prewarm_spread_standard_hosts": fetcher_cfg.get("auto_prewarm_spread_standard_hosts"),
    }

    for attr_name, raw_value in overrides.items():
        if raw_value is None:
            continue
        if not hasattr(fetcher, attr_name):
            continue
        if isinstance(getattr(fetcher, attr_name), bool):
            setattr(fetcher, attr_name, bool(raw_value))
        elif isinstance(getattr(fetcher, attr_name), int):
            setattr(fetcher, attr_name, _safe_int(raw_value, int(getattr(fetcher, attr_name))))
        elif isinstance(getattr(fetcher, attr_name), float):
            setattr(fetcher, attr_name, _safe_float(raw_value, float(getattr(fetcher, attr_name))))
        else:
            setattr(fetcher, attr_name, raw_value)

    profile["num_processes"] = int(getattr(fetcher, "num_processes", profile["num_processes"]))
    profile["task_chunk_cache_min_tasks"] = int(
        getattr(fetcher, "task_chunk_cache_min_tasks", profile["task_chunk_cache_min_tasks"])
    )
    profile["task_chunk_inproc_future_workers"] = int(
        getattr(fetcher, "task_chunk_inproc_future_workers", profile["task_chunk_inproc_future_workers"])
    )
    profile["task_chunk_max_inflight_multiplier"] = int(
        getattr(fetcher, "task_chunk_max_inflight_multiplier", profile["task_chunk_max_inflight_multiplier"])
    )
    profile["auto_prewarm_on_async"] = bool(getattr(fetcher, "auto_prewarm_on_async", profile["auto_prewarm_on_async"]))
    profile["auto_prewarm_spread_standard_hosts"] = bool(
        getattr(fetcher, "auto_prewarm_spread_standard_hosts", profile["auto_prewarm_spread_standard_hosts"])
    )
    profile["parallel_total_timeout_seconds"] = _safe_float(
        getattr(fetcher, "parallel_total_timeout_seconds", 0.0), 0.0
    )
    profile["parallel_result_timeout_seconds"] = _safe_float(
        getattr(fetcher, "parallel_result_timeout_seconds", 0.0), 0.0
    )
    profile["auto_prewarm_timeout_seconds"] = _safe_float(
        getattr(fetcher, "auto_prewarm_timeout_seconds", 0.0), 0.0
    )
    profile["auto_prewarm_max_rounds"] = _safe_int(getattr(fetcher, "auto_prewarm_max_rounds", 0), 0)
    profile["auto_prewarm_require_all_workers"] = bool(
        getattr(fetcher, "auto_prewarm_require_all_workers", True)
    )
    return profile


def _summarize_failure_reasons(
    payloads: Sequence[Dict[str, Any]],
    *,
    total_tasks: int,
    batch_exception: str,
    connection_keywords: Sequence[str],
) -> Dict[str, Any]:
    """
    汇总失败任务原因（no_data / 连接不可用 / 其他）。

    输入：
    1. payloads: 单批返回 payload 列表。
    2. total_tasks: 本批任务总数。
    3. batch_exception: 批次级异常文本。
    4. connection_keywords: 连接不可用类关键字列表。
    输出：
    1. 失败计数字典与错误分布摘要。
    用途：
    1. 输出明确的错误分类计数，便于区分业务空数据与连接类故障。
    边界条件：
    1. 出现 batch_exception 且 payload 不完整时，缺失任务按批次异常类型补齐分类计数。
    """
    failed_payload = 0
    no_data_count = 0
    connection_unavailable_count = 0
    other_error_count = 0
    errors: List[str] = []
    error_counter: Counter[str] = Counter()

    for item in list(payloads or []):
        if not isinstance(item, dict):
            failed_payload += 1
            other_error_count += 1
            errors.append("payload_not_dict")
            error_counter["payload_not_dict"] += 1
            continue
        err = str(item.get("error") or "").strip()
        if err:
            failed_payload += 1
            errors.append(err)
            error_counter[err] += 1
            if err == "no_data":
                no_data_count += 1
            elif _contains_target_error(err, connection_keywords):
                connection_unavailable_count += 1
            else:
                other_error_count += 1

    failed_tasks = int(failed_payload)
    batch_exception_text = str(batch_exception or "").strip()
    if batch_exception_text:
        errors.append(batch_exception_text)
        error_counter[batch_exception_text] += 1
        failed_tasks = max(int(failed_payload), int(total_tasks))
        missing_count = max(0, int(failed_tasks) - int(failed_payload))
        if missing_count > 0:
            if batch_exception_text == "no_data":
                no_data_count += int(missing_count)
            elif _contains_target_error(batch_exception_text, connection_keywords):
                connection_unavailable_count += int(missing_count)
            else:
                other_error_count += int(missing_count)

    top_error_counts = [
        {"error": str(item[0]), "count": int(item[1])}
        for item in sorted(error_counter.items(), key=lambda x: (-int(x[1]), str(x[0])))
    ]
    return {
        "failed_tasks": int(failed_tasks),
        "payload_failed_tasks": int(failed_payload),
        "no_data_count": int(no_data_count),
        "connection_unavailable_count": int(connection_unavailable_count),
        "other_error_count": int(other_error_count),
        "errors": errors,
        "top_error_counts": top_error_counts,
    }


def _install_prewarm_timing_hooks(fetcher: Any) -> Tuple[Dict[str, Any], Callable[[], None]]:
    """
    给 prewarm 与 probe 阶段安装耗时统计钩子。

    输入：
    1. fetcher: 当前并行抓取器实例。
    输出：
    1. `(trace_dict, restore_fn)`。
    用途：
    1. 统计 `get_stock_kline` 调用中的 prewarm 总耗时，以及可用IP探测（probe）耗时。
    边界条件：
    1. 若目标函数不存在，trace 保持默认值，restore 为空操作。
    """
    trace: Dict[str, Any] = {
        "prewarm_called": False,
        "prewarm_elapsed_seconds": 0.0,
        "prewarm_exception": "",
        "prewarm_summary": {},
        "probe_called": False,
        "probe_call_count": 0,
        "probe_elapsed_seconds": 0.0,
        "probe_exception": "",
        "probe_valid_standard_host_count": 0,
    }

    original_ensure_async_prewarm = getattr(fetcher, "_ensure_async_prewarm", None)
    original_probe_valid_standard_hosts = getattr(
        parallel_fetcher_module,
        "_probe_valid_standard_hosts_for_prewarm",
        None,
    )
    has_ensure_hook = bool(callable(original_ensure_async_prewarm))
    has_probe_hook = bool(callable(original_probe_valid_standard_hosts))

    if has_ensure_hook:
        def _timed_ensure_async_prewarm() -> Any:
            trace["prewarm_called"] = True
            started = time.perf_counter()
            try:
                summary = original_ensure_async_prewarm()
                trace["prewarm_summary"] = dict(summary) if isinstance(summary, dict) else {}
                return summary
            except Exception as exc:
                trace["prewarm_exception"] = f"{type(exc).__name__}: {exc}"
                raise
            finally:
                trace["prewarm_elapsed_seconds"] = round(time.perf_counter() - started, 4)

        setattr(fetcher, "_ensure_async_prewarm", _timed_ensure_async_prewarm)

    if has_probe_hook:
        def _timed_probe_valid_standard_hosts(config_path: Any) -> Any:
            trace["probe_called"] = True
            trace["probe_call_count"] = int(trace["probe_call_count"]) + 1
            started = time.perf_counter()
            try:
                hosts = original_probe_valid_standard_hosts(config_path)
                trace["probe_valid_standard_host_count"] = int(len(list(hosts or [])))
                return hosts
            except Exception as exc:
                trace["probe_exception"] = f"{type(exc).__name__}: {exc}"
                raise
            finally:
                spent = round(time.perf_counter() - started, 4)
                trace["probe_elapsed_seconds"] = round(float(trace["probe_elapsed_seconds"]) + float(spent), 4)

        setattr(parallel_fetcher_module, "_probe_valid_standard_hosts_for_prewarm", _timed_probe_valid_standard_hosts)

    def _restore_hooks() -> None:
        if has_ensure_hook:
            try:
                delattr(fetcher, "_ensure_async_prewarm")
            except Exception:
                setattr(fetcher, "_ensure_async_prewarm", original_ensure_async_prewarm)
        if has_probe_hook:
            setattr(
                parallel_fetcher_module,
                "_probe_valid_standard_hosts_for_prewarm",
                original_probe_valid_standard_hosts,
            )

    return trace, _restore_hooks


def _build_prewarm_timing_summary(fetcher: Any, trace: Dict[str, Any]) -> Dict[str, Any]:
    """
    构建 prewarm 与 probe 耗时摘要。

    输入：
    1. fetcher: 并行抓取器实例。
    2. trace: 钩子采样结果字典。
    输出：
    1. 可直接写入 JSON 的 prewarm 耗时摘要。
    用途：
    1. 统一对外暴露 prewarm 总耗时、probe 耗时、轮次与有效 host 计数。
    边界条件：
    1. 未触发 prewarm/probe 时返回 0 与空摘要，不抛异常。
    """
    prewarm_summary = trace.get("prewarm_summary")
    prewarm_summary_dict = dict(prewarm_summary) if isinstance(prewarm_summary, dict) else {}
    prewarm_elapsed = _safe_float(trace.get("prewarm_elapsed_seconds", 0.0), 0.0)
    summary_elapsed = _safe_float(prewarm_summary_dict.get("elapsed_seconds", 0.0), 0.0)
    if prewarm_elapsed <= 0 and summary_elapsed > 0:
        prewarm_elapsed = summary_elapsed

    return {
        "auto_prewarm_on_async": bool(getattr(fetcher, "auto_prewarm_on_async", False)),
        "auto_prewarm_spread_standard_hosts": bool(
            getattr(fetcher, "auto_prewarm_spread_standard_hosts", False)
        ),
        "prewarm_called": bool(trace.get("prewarm_called", False)),
        "prewarm_elapsed_seconds": float(round(prewarm_elapsed, 4)),
        "prewarm_exception": str(trace.get("prewarm_exception", "")),
        "prewarm_summary_elapsed_seconds": float(round(summary_elapsed, 4)),
        "prewarm_rounds_used": int(_safe_int(prewarm_summary_dict.get("rounds_used", 0), 0)),
        "prewarm_warmed_workers": int(_safe_int(prewarm_summary_dict.get("warmed_workers", 0), 0)),
        "probe_called": bool(trace.get("probe_called", False)),
        "probe_call_count": int(_safe_int(trace.get("probe_call_count", 0), 0)),
        "probe_elapsed_seconds": float(round(_safe_float(trace.get("probe_elapsed_seconds", 0.0), 0.0), 4)),
        "probe_valid_standard_host_count": int(
            _safe_int(trace.get("probe_valid_standard_host_count", 0), 0)
        ),
        "probe_exception": str(trace.get("probe_exception", "")),
        "prewarm_summary": prewarm_summary_dict,
    }


def main() -> None:
    """
    执行入口：运行 async 压测并写出 JSON 报告。

    输入：
    1. 无显式输入参数（参数来自同目录 YAML 配置）。
    输出：
    1. 控制台进度日志 + JSON 结果文件。
    用途：
    1. 复测并定位“进程内执行一些任务后连接不可用”是否发生。
    边界条件：
    1. 任意阶段异常都会写入结果文件，并在 finally 回收并行进程池。
    """
    config, config_path = _load_runtime_config()
    task_cfg = dict(config.get("task") or {})
    run_cfg = dict(config.get("run") or {})
    fetcher_cfg = dict(config.get("fetcher") or {})
    output_cfg = dict(config.get("output") or {})

    keywords = _normalize_keywords(run_cfg.get("target_error_keywords", []))
    connection_keywords = _build_connection_error_keywords(keywords)
    total_budget_seconds = max(0.0, _safe_float(run_cfg.get("total_budget_seconds", 0), 0.0))
    result_timeout_seconds = max(0.0, _safe_float(run_cfg.get("job_result_timeout_seconds", 0), 0.0))
    slow_batch_threshold_seconds = max(0.0, _safe_float(run_cfg.get("slow_batch_threshold_seconds", 20.0), 20.0))
    max_error_samples_per_batch = max(1, _safe_int(run_cfg.get("max_error_samples_per_batch", 8), 8))
    run_preflight_supported_markets = bool(run_cfg.get("run_preflight_supported_markets", True))

    result_filename = str(output_cfg.get("result_filename", "async_1000x5x15_connection_check_result.json")).strip()
    if result_filename == "":
        result_filename = "async_1000x5x15_connection_check_result.json"
    result_path = EXAMPLES_DIR / result_filename

    report: Dict[str, Any] = {
        "started_at": dt.datetime.now().isoformat(timespec="seconds"),
        "config_path": str(config_path),
        "result_path": str(result_path),
        "task_config": task_cfg,
        "run_config": run_cfg,
        "fetcher_config": fetcher_cfg,
        "keywords": list(keywords),
        "connection_keywords": list(connection_keywords),
        "ok": False,
        "target_error_hit": False,
        "target_error_text": "",
        "target_error_batch_index": None,
        "stopped_due_to_budget": False,
        "batches": [],
        "summary": {},
    }

    started = time.perf_counter()
    fetcher = None
    try:
        task_data = _build_tasks(task_cfg)
        tasks = list(task_data["tasks"])
        codes = list(task_data["codes"])
        samples = list(task_data["samples"])
        freqs = list(task_data["freqs"])
        task_source = str(task_data.get("task_source", "generated_runtime"))
        taskset_path = str(task_data.get("taskset_path", ""))
        taskset_meta = dict(task_data.get("taskset_meta") or {})

        fetcher = bench.get_fetcher()
        profile = _apply_async_fetcher_profile(fetcher=fetcher, fetcher_cfg=fetcher_cfg)

        report["meta"] = {
            "stock_count": int(len(codes)),
            "freqs": freqs,
            "sample_count": int(len(samples)),
            "task_count": int(len(tasks)),
            "dispatch_strategy": "single_full_call",
            "batch_size": int(len(tasks)),
            "batch_count": 1,
            "samples": samples,
            "task_source": task_source,
            "taskset_path": taskset_path,
            "taskset_meta": taskset_meta,
        }
        report["profile"] = profile

        print("=" * 96)
        print("async connection check benchmark (1000*5*15)")
        print("=" * 96)
        print(f"config_path  : {config_path}")
        print(f"stock_count  : {len(codes)}")
        print(f"freqs        : {freqs}")
        print(f"sample_count : {len(samples)}")
        print(f"task_count   : {len(tasks)}")
        print(f"task_source  : {task_source}")
        if taskset_path:
            print(f"taskset_path : {taskset_path}")
        print("dispatch_strategy : single_full_call")
        print(f"batch_size   : {len(tasks)}")
        print("batch_count  : 1")
        print(f"keywords     : {keywords}")
        print(f"profile      : {json.dumps(profile, ensure_ascii=False)}")

        if run_preflight_supported_markets:
            _ = bench.get_supported_markets(return_df=False)

        batch_started = time.perf_counter()
        payloads: List[Dict[str, Any]] = []
        batch_exception = ""
        target_error_text = ""
        no_data_count = 0
        connection_unavailable_count = 0
        other_error_count = 0
        top_error_counts: List[Dict[str, Any]] = []
        get_stock_kline_elapsed_seconds = 0.0
        get_stock_kline_submit_elapsed_seconds = 0.0
        job_result_wait_elapsed_seconds = 0.0
        prewarm_timing: Dict[str, Any] = {}
        effective_timeout_seconds = 0.0
        if result_timeout_seconds > 0 and total_budget_seconds > 0:
            effective_timeout_seconds = min(float(result_timeout_seconds), float(total_budget_seconds))
        elif result_timeout_seconds > 0:
            effective_timeout_seconds = float(result_timeout_seconds)
        elif total_budget_seconds > 0:
            effective_timeout_seconds = float(total_budget_seconds)

        prewarm_trace, restore_prewarm_hooks = _install_prewarm_timing_hooks(fetcher=fetcher)
        call_stage = "init"
        call_started = time.perf_counter()
        submit_started = call_started
        result_started = call_started
        try:
            call_stage = "submit"
            job = get_stock_kline(task=tasks, mode="async", queue=_NullQueue())
            get_stock_kline_submit_elapsed_seconds = round(time.perf_counter() - submit_started, 4)
            call_stage = "result_wait"
            result_started = time.perf_counter()
            if effective_timeout_seconds > 0:
                payloads = list(job.result(timeout=effective_timeout_seconds) or [])
            else:
                payloads = list(job.result() or [])
            job_result_wait_elapsed_seconds = round(time.perf_counter() - result_started, 4)
            call_stage = "done"
        except Exception as exc:
            batch_exception = f"{type(exc).__name__}: {exc}"
            now = time.perf_counter()
            if call_stage == "submit":
                get_stock_kline_submit_elapsed_seconds = round(now - submit_started, 4)
            elif call_stage == "result_wait":
                job_result_wait_elapsed_seconds = round(now - result_started, 4)
        finally:
            restore_prewarm_hooks()
            get_stock_kline_elapsed_seconds = round(time.perf_counter() - call_started, 4)
            prewarm_timing = _build_prewarm_timing_summary(fetcher=fetcher, trace=prewarm_trace)

        batch_elapsed = round(time.perf_counter() - batch_started, 4)
        payload_stat = _summarize_failure_reasons(
            payloads=payloads,
            total_tasks=len(tasks),
            batch_exception=batch_exception,
            connection_keywords=connection_keywords,
        )
        failed_tasks = int(payload_stat["failed_tasks"])
        no_data_count = int(payload_stat["no_data_count"])
        connection_unavailable_count = int(payload_stat["connection_unavailable_count"])
        other_error_count = int(payload_stat["other_error_count"])
        top_error_counts = list(payload_stat["top_error_counts"])
        errors = list(payload_stat["errors"])

        if _contains_target_error(batch_exception, keywords):
            target_error_text = batch_exception
        for err in errors:
            if target_error_text == "" and _contains_target_error(err, keywords):
                target_error_text = err

        error_samples: List[str] = []
        for err in errors:
            plain = str(err).strip()
            if plain and plain not in error_samples:
                error_samples.append(plain)
            if len(error_samples) >= max_error_samples_per_batch:
                break

        is_slow_batch = bool(batch_elapsed >= slow_batch_threshold_seconds) if slow_batch_threshold_seconds > 0 else False
        total_target_error_hits = 1 if target_error_text else 0
        slow_batch_count = 1 if is_slow_batch else 0
        if target_error_text:
            report["target_error_hit"] = True
            report["target_error_text"] = str(target_error_text)
            report["target_error_batch_index"] = 1
        if total_budget_seconds > 0 and batch_elapsed >= total_budget_seconds:
            report["stopped_due_to_budget"] = True

        report["batches"].append(
            {
                "batch_index": 1,
                "batch_total": 1,
                "task_count": int(len(tasks)),
                "payload_count": int(len(payloads)),
                "failed_tasks": int(failed_tasks),
                "no_data_count": int(no_data_count),
                "connection_unavailable_count": int(connection_unavailable_count),
                "other_error_count": int(other_error_count),
                "elapsed_seconds": float(batch_elapsed),
                "get_stock_kline_elapsed_seconds": float(get_stock_kline_elapsed_seconds),
                "get_stock_kline_submit_elapsed_seconds": float(get_stock_kline_submit_elapsed_seconds),
                "job_result_wait_elapsed_seconds": float(job_result_wait_elapsed_seconds),
                "prewarm_timing": prewarm_timing,
                "is_slow_batch": bool(is_slow_batch),
                "target_error_text": str(target_error_text),
                "exception": str(batch_exception),
                "error_samples": error_samples,
                "top_error_counts": top_error_counts[:20],
            }
        )

        print(
            f"[async] batch=1/1 "
            f"tasks={len(tasks)} failed={failed_tasks} "
            f"no_data={no_data_count} conn_unavailable={connection_unavailable_count} "
            f"elapsed={batch_elapsed:.3f}s slow={is_slow_batch} "
            f"kline={get_stock_kline_elapsed_seconds:.3f}s "
            f"prewarm={_safe_float(prewarm_timing.get('prewarm_elapsed_seconds', 0.0), 0.0):.3f}s "
            f"probe={_safe_float(prewarm_timing.get('probe_elapsed_seconds', 0.0), 0.0):.3f}s "
            f"target_hit={bool(target_error_text)}"
        )

        elapsed_seconds = round(time.perf_counter() - started, 4)
        processed_batches = int(len(report["batches"]))
        processed_tasks = int(sum(int(item.get("task_count", 0)) for item in report["batches"]))

        report["summary"] = {
            "elapsed_seconds": float(elapsed_seconds),
            "processed_batches": int(processed_batches),
            "processed_tasks": int(processed_tasks),
            "failed_tasks": int(failed_tasks),
            "no_data_count": int(no_data_count),
            "connection_unavailable_count": int(connection_unavailable_count),
            "other_error_count": int(other_error_count),
            "slow_batch_count": int(slow_batch_count),
            "target_error_hits": int(total_target_error_hits),
            "target_error_hit": bool(report["target_error_hit"]),
            "get_stock_kline_elapsed_seconds": float(get_stock_kline_elapsed_seconds),
            "get_stock_kline_submit_elapsed_seconds": float(get_stock_kline_submit_elapsed_seconds),
            "job_result_wait_elapsed_seconds": float(job_result_wait_elapsed_seconds),
            "prewarm_elapsed_seconds": float(_safe_float(prewarm_timing.get("prewarm_elapsed_seconds", 0.0), 0.0)),
            "probe_elapsed_seconds": float(_safe_float(prewarm_timing.get("probe_elapsed_seconds", 0.0), 0.0)),
        }
        report["ok"] = True
    except Exception as exc:
        report["ok"] = False
        report["fatal_error_type"] = type(exc).__name__
        report["fatal_error"] = str(exc)
        report["fatal_traceback"] = traceback.format_exc()
        print(traceback.format_exc())
    finally:
        cleanup = bench._cleanup_parallel_pool(run_name="async-1000x5x15-check-connection")
        report["cleanup"] = cleanup
        report["finished_at"] = dt.datetime.now().isoformat(timespec="seconds")
        report["elapsed_seconds"] = round(time.perf_counter() - started, 4)
        result_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

        print("-" * 96)
        print(f"saved: {result_path}")
        print(
            json.dumps(
                {
                    "ok": report.get("ok", False),
                    "target_error_hit": report.get("target_error_hit", False),
                    "target_error_text": report.get("target_error_text", ""),
                    "target_error_batch_index": report.get("target_error_batch_index"),
                    "summary": report.get("summary", {}),
                },
                ensure_ascii=False,
                indent=2,
            )
        )


if __name__ == "__main__":
    main()
