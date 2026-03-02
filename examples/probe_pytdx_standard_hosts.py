"""
pytdx 标准行情 IP 池探测脚本。

用途：
1. 读取 `src/zsdtdx/config.yaml` 的 `hosts.standard`。
2. 使用 site-packages 的 `pytdx.hq.TdxHq_API` 对每个 host 逐个做连通性探测。
3. 同时验证最小查询（`get_security_count`），并输出 JSON 结果文件。

边界：
1. 本脚本只负责探测，不修改任何配置。
2. 为保证复现一致，超时档位使用脚本常量，不通过 CLI 暴露。
"""

from __future__ import annotations

import datetime as dt
import json
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

import yaml

try:
    import pytdx
    from pytdx.hq import TdxHq_API
except Exception as exc:  # pragma: no cover - 导入失败为环境问题
    raise RuntimeError("未检测到 pytdx，请先安装 pytdx 后再执行本脚本。") from exc


EXAMPLES_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = EXAMPLES_DIR.parent
CONFIG_PATH = PROJECT_ROOT / "src" / "zsdtdx" / "config.yaml"
RESULT_FILENAME = "pytdx_standard_hosts_probe_result.json"

# 固定超时档位：用于区分“短超时误判”与“长超时仍失败”的情况。
TIMEOUT_ROUNDS = [2.0, 5.0, 8.0]

# 最小查询市场：1=上海。若 connect 成功，会用该接口进一步验证链路可用性。
PROBE_MARKET = 1


def _load_standard_hosts(config_path: Path) -> List[Tuple[str, int]]:
    """
    从配置文件读取标准行情 host 列表。

    输入：
    1. config_path: `config.yaml` 文件路径。
    输出：
    1. 标准化后的 `(host, port)` 列表。
    用途：
    1. 统一读取工程配置中的标准行情 IP 池，避免手工维护测试列表。
    边界条件：
    1. 配置缺失或字段为空时返回空列表。
    """
    if not config_path.exists():
        raise FileNotFoundError(f"配置文件不存在: {config_path}")

    raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    host_items = list((((raw.get("hosts") or {}).get("standard")) or []))

    hosts: List[Tuple[str, int]] = []
    for item in host_items:
        text = str(item).strip()
        if text == "" or ":" not in text:
            continue
        host, port_str = text.rsplit(":", 1)
        try:
            hosts.append((host.strip(), int(port_str)))
        except Exception:
            continue
    return hosts


def _probe_one_host(host: str, port: int, timeout: float, market: int) -> Dict[str, Any]:
    """
    使用 pytdx 探测单个 host 的连接与最小查询。

    输入：
    1. host/port: 目标服务地址。
    2. timeout: `connect` 超时秒数。
    3. market: `get_security_count` 的市场参数。
    输出：
    1. 单 host 探测结果字典。
    用途：
    1. 区分“仅 connect 成功”与“connect + query 均可用”两种状态。
    边界条件：
    1. 任意异常统一写入 `error` 字段，不向上抛出中断整轮探测。
    """
    record: Dict[str, Any] = {
        "host": f"{host}:{port}",
        "timeout": float(timeout),
        "connect_ok": False,
        "query_ok": False,
        "security_count": None,
        "connect_elapsed_ms": None,
        "query_elapsed_ms": None,
        "error": "",
    }

    api = TdxHq_API()
    started = time.perf_counter()
    try:
        ok = bool(api.connect(host, int(port), time_out=float(timeout)))
        record["connect_ok"] = bool(ok)
        record["connect_elapsed_ms"] = round((time.perf_counter() - started) * 1000.0, 2)

        if not ok:
            record["error"] = "connect returned False"
            return record

        query_started = time.perf_counter()
        count_value = api.get_security_count(int(market))
        record["security_count"] = int(count_value) if count_value is not None else None
        record["query_ok"] = count_value is not None
        record["query_elapsed_ms"] = round((time.perf_counter() - query_started) * 1000.0, 2)
        return record
    except Exception as exc:
        record["error"] = str(exc)
        record["connect_elapsed_ms"] = round((time.perf_counter() - started) * 1000.0, 2)
        return record
    finally:
        try:
            api.disconnect()
        except Exception:
            pass


def _run_round(hosts: List[Tuple[str, int]], timeout: float, market: int) -> Dict[str, Any]:
    """
    执行单个超时档位的一整轮 host 探测。

    输入：
    1. hosts: 待探测 host 列表。
    2. timeout: 当前轮的连接超时秒数。
    3. market: 查询市场参数。
    输出：
    1. 包含 summary 与 rows 的结果字典。
    用途：
    1. 将原始逐 host 结果与聚合统计统一封装，便于后续对比不同 timeout。
    边界条件：
    1. hosts 为空时返回空 rows，统计值为 0。
    """
    rows: List[Dict[str, Any]] = []
    for host, port in hosts:
        rows.append(_probe_one_host(host=host, port=port, timeout=float(timeout), market=int(market)))

    summary = {
        "timeout": float(timeout),
        "host_count": int(len(rows)),
        "connect_ok_count": int(sum(1 for item in rows if bool(item.get("connect_ok")))),
        "query_ok_count": int(sum(1 for item in rows if bool(item.get("query_ok")))),
        "all_unavailable": bool(all(not bool(item.get("query_ok")) for item in rows)) if rows else True,
    }
    return {"summary": summary, "rows": rows}


def _build_report(config_path: Path, hosts: List[Tuple[str, int]]) -> Dict[str, Any]:
    """
    构建完整探测报告。

    输入：
    1. config_path: 配置文件路径。
    2. hosts: 标准行情 host 列表。
    输出：
    1. 探测报告字典。
    用途：
    1. 聚合元数据与多轮探测结果，作为结果文件与控制台输出的统一数据源。
    边界条件：
    1. hosts 为空时仍返回结构化报告，便于自动化流程处理。
    """
    rounds: Dict[str, Any] = {}
    for timeout in TIMEOUT_ROUNDS:
        rounds[str(timeout)] = _run_round(hosts=hosts, timeout=float(timeout), market=int(PROBE_MARKET))

    pytdx_version = getattr(pytdx, "__version__", "unknown")
    return {
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"),
        "config_path": str(config_path),
        "probe_engine": "site-packages pytdx.hq.TdxHq_API",
        "pytdx_version": str(pytdx_version),
        "pytdx_file": str(getattr(pytdx, "__file__", "")),
        "market": int(PROBE_MARKET),
        "host_count": int(len(hosts)),
        "hosts": [f"{item[0]}:{item[1]}" for item in hosts],
        "rounds": rounds,
    }


def main() -> None:
    """
    脚本入口：执行探测并保存结果文件。

    输入：
    1. 无显式输入参数。
    输出：
    1. 控制台摘要与 `examples/pytdx_standard_hosts_probe_result.json`。
    用途：
    1. 提供可重复执行的标准行情 IP 池可用性自测入口。
    边界条件：
    1. 配置文件不存在或 pytdx 未安装时抛出异常并终止。
    """
    hosts = _load_standard_hosts(CONFIG_PATH)
    report = _build_report(config_path=CONFIG_PATH, hosts=hosts)

    out_path = EXAMPLES_DIR / RESULT_FILENAME
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print("=" * 96)
    print("pytdx standard hosts probe")
    print("=" * 96)
    print(f"config_path : {CONFIG_PATH}")
    print(f"pytdx_file  : {report.get('pytdx_file', '')}")
    print(f"host_count  : {len(hosts)}")
    for timeout in TIMEOUT_ROUNDS:
        summary = ((report.get("rounds") or {}).get(str(timeout)) or {}).get("summary") or {}
        print(
            f"timeout={timeout}s "
            f"connect_ok={summary.get('connect_ok_count', 0)} "
            f"query_ok={summary.get('query_ok_count', 0)} "
            f"all_unavailable={summary.get('all_unavailable', True)}"
        )
    print(f"saved: {out_path}")


if __name__ == "__main__":
    main()
