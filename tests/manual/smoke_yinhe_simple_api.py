"""
模块：`smoke_yinhe_simple_api.py`。

职责：
1. 针对银河证券行情节点，冒烟验收 simple_api 各对外函数能否返回并解析数值。
2. 使用小样本（少量代码 / 短时间窗口），不跑全市场。
3. 将分函数结果打印到控制台，并写入 artifacts JSON。

边界：
1. 需要访问银河标准/扩展行情服务器，不由 pytest 收集。
2. 收盘后最新价仍应能解析为正数；无交易日 K 线允许较少根数，但字段必须可解析。
3. 进程退出前销毁并行进程池。
"""

from __future__ import annotations

import json
import re
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional

_ROOT = Path(__file__).resolve().parents[2]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

_ART = _ROOT / "tests" / "manual" / "artifacts"
_DT_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:00$")
START_DAY = "2026-09-11"
END_DAY = "2026-09-18"


def _fail(reason: str) -> None:
    """
    输入：reason 失败原因。
    输出：抛出 RuntimeError。
    用途：统一把校验失败转成异常，由 run_case 捕获。
    边界：reason 为空时仍抛错。
    """
    raise RuntimeError(reason or "未知失败")


def _require(cond: bool, reason: str) -> None:
    """
    输入：cond 条件；reason 失败原因。
    输出：条件为假时抛错，否则无返回。
    用途：断言封装。
    边界：cond 为真时不抛错。
    """
    if not cond:
        _fail(reason)


def _check_ohlc(row: Dict[str, Any], *, need_volume: bool = True) -> Optional[str]:
    """
    输入：row K 线字典；need_volume 是否要求成交量字段。
    输出：通过返回 None，失败返回原因字符串。
    用途：校验 datetime / OHLC / volume 能否解析且自洽。
    边界：缺字段、无法转 float、high<low 均判失败。
    """
    dt_text = str(row.get("datetime", ""))
    if not _DT_RE.match(dt_text):
        return f"datetime格式非法:{dt_text}"
    try:
        o = float(row.get("open"))
        h = float(row.get("high"))
        low = float(row.get("low"))
        c = float(row.get("close"))
    except Exception:
        return "OHLC无法转float"
    if min(o, h, low, c) <= 0:
        return f"OHLC非正:{o}/{h}/{low}/{c}"
    if h + 1e-9 < max(o, c) or low - 1e-9 > min(o, c) or h < low:
        return "OHLC不自洽"
    if need_volume:
        vol = row.get("volume")
        try:
            if float(vol) < 0:
                return "volume<0"
        except Exception:
            return "volume非法"
    return None


def _summarize_kline_payloads(payloads: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    输入：sync K 线 payload 列表。
    输出：ok/empty/error 计数与样本。
    用途：从 get_stock_kline / get_index_kline 的 sync 返回值抽取可解析性。
    边界：payload 非 dict 或缺少 rows 记为 error。
    """
    stats = {
        "n": len(payloads),
        "ok": 0,
        "empty": 0,
        "error": 0,
        "samples": [],
        "errors": [],
    }
    for payload in payloads:
        if not isinstance(payload, dict):
            stats["error"] += 1
            stats["errors"].append("payload非dict")
            continue
        err = payload.get("error")
        rows = list(payload.get("rows") or [])
        task = dict(payload.get("task") or {})
        if err:
            stats["error"] += 1
            if len(stats["errors"]) < 4:
                stats["errors"].append({"task": task, "error": str(err)[:200]})
            continue
        if not rows:
            stats["empty"] += 1
            continue
        bad = None
        for row in rows:
            if not isinstance(row, dict):
                bad = "row非dict"
                break
            bad = _check_ohlc(row)
            if bad:
                break
        if bad:
            stats["error"] += 1
            if len(stats["errors"]) < 4:
                stats["errors"].append({"task": task, "reason": bad})
            continue
        stats["ok"] += 1
        if len(stats["samples"]) < 2:
            stats["samples"].append(
                {
                    "task": task,
                    "n_rows": len(rows),
                    "first": {
                        "datetime": rows[0].get("datetime"),
                        "open": rows[0].get("open"),
                        "close": rows[0].get("close"),
                        "volume": rows[0].get("volume"),
                    },
                }
            )
    return stats


def case_set_config_and_client() -> Dict[str, Any]:
    """
    输入：无。
    输出：配置路径与客户端类型。
    用途：验收 set_config_path / get_client。
    边界：配置文件必须存在。
    """
    from zsdtdx import get_client, set_config_path

    cfg = _SRC / "zsdtdx" / "config.yaml"
    path = set_config_path(str(cfg), async_background_probe=False)
    _require(Path(path).is_file(), f"配置不存在: {path}")
    with get_client() as client:
        _require(client is not None, "get_client 返回空")
        return {"config_path": path, "client_type": type(client).__name__}


def case_supported_markets() -> Dict[str, Any]:
    """
    输入：无。
    输出：市场数量与必要市场是否齐全。
    用途：验收 get_supported_markets 解析 market/name/source。
    边界：必须同时有标准与扩展市场。
    """
    from zsdtdx import get_client, get_supported_markets

    with get_client():
        rows = get_supported_markets(return_df=False)
    _require(isinstance(rows, list) and rows, "市场列表为空")
    names = {str(r.get("name", "")).strip() for r in rows}
    sources = {str(r.get("source", "")).strip() for r in rows}
    need = {"深圳", "上海", "北京"}
    missing = sorted(need - names)
    _require(not missing, f"标准市场缺失: {missing}")
    _require("std" in sources and "ex" in sources, f"source 不完整: {sources}")
    for row in rows[:3]:
        _require(isinstance(row.get("market"), int), f"market 非 int: {row}")
        _require(str(row.get("name", "")).strip() != "", f"name 为空: {row}")
    return {
        "n": len(rows),
        "sources": sorted(sources),
        "sample": [{"market": r.get("market"), "name": r.get("name"), "source": r.get("source")} for r in rows[:6]],
    }


def case_stock_code_name() -> Dict[str, Any]:
    """
    输入：无。
    输出：码表规模与样本。
    用途：验收 get_stock_code_name 能解析带前缀代码与中文名。
    边界：必须含浦发银行、平安银行。
    """
    from zsdtdx import get_client, get_stock_code_name

    with get_client():
        mp = get_stock_code_name(use_cache=True)
    _require(isinstance(mp, dict) and len(mp) > 1000, f"码表过少: {len(mp) if isinstance(mp, dict) else mp}")
    _require(mp.get("sh.600000") == "浦发银行", f"sh.600000 异常: {mp.get('sh.600000')}")
    _require(mp.get("sz.000001") == "平安银行", f"sz.000001 异常: {mp.get('sz.000001')}")
    return {"n": len(mp), "sh.600000": mp.get("sh.600000"), "sz.000001": mp.get("sz.000001")}


def case_future_list() -> Dict[str, Any]:
    """
    输入：无。
    输出：期货清单规模与市场集合。
    用途：验收 get_all_future_list 解析 code/name/market_name。
    边界：必须含沪铜主连 CUL8，且覆盖四大商品交易所。
    """
    from zsdtdx import get_all_future_list, get_client

    with get_client():
        rows = get_all_future_list(return_df=False, use_cache=True)
    _require(isinstance(rows, list) and rows, "期货列表为空")
    markets = {str(r.get("market_name", "")).strip() for r in rows}
    expect = {"郑州商品", "大连商品", "上海期货", "广州期货"}
    _require(expect.issubset(markets), f"期货市场不全: {sorted(markets)}")
    codes = {str(r.get("code", "")).upper() for r in rows}
    _require("CUL8" in codes, "缺少 CUL8")
    sample = next((r for r in rows if str(r.get("code", "")).upper() == "CUL8"), None)
    _require(sample is not None and str(sample.get("name", "")).strip() != "", f"CUL8 名称空: {sample}")
    return {"n": len(rows), "markets": sorted(markets), "CUL8": sample}


def case_stock_latest_price() -> Dict[str, Any]:
    """
    输入：无。
    输出：样本最新价。
    用途：验收 get_stock_latest_price 能解析 A 股与港股最新价。
    边界：A 股价必须为正；港股若服务端无数据允许 None，但接口必须返回键。
    """
    from zsdtdx import get_client, get_stock_latest_price

    with get_client():
        mp = get_stock_latest_price(["600000", "000001", "09988"])
    _require(isinstance(mp, dict), f"最新价非 dict: {type(mp)}")
    for code in ("600000", "000001"):
        val = mp.get(code)
        _require(val is not None and float(val) > 0, f"{code} 最新价异常: {val}")
    _require("09988" in mp, "缺少港股 09988 键")
    hk = mp.get("09988")
    if hk is not None:
        _require(float(hk) > 0, f"09988 最新价非正: {hk}")
    return {"600000": mp.get("600000"), "000001": mp.get("000001"), "09988": hk}


def case_future_latest_price() -> Dict[str, Any]:
    """
    输入：无。
    输出：沪铜主连最新价。
    用途：验收 get_future_latest_price 能按品种代码补全主连并解析价格。
    边界：CUL8 必须为正数。
    """
    from zsdtdx import get_client, get_future_latest_price

    with get_client():
        mp = get_future_latest_price("CU")
    _require(isinstance(mp, dict) and "CUL8" in mp, f"CU 主连缺失: {mp}")
    val = mp.get("CUL8")
    _require(val is not None and float(val) > 0, f"CUL8 最新价异常: {val}")
    return {"CUL8": val, "keys": sorted(mp.keys())}


def case_company_info() -> Dict[str, Any]:
    """
    输入：无。
    输出：分类数与正文长度。
    用途：验收 get_company_info 能解析分类名与正文。
    边界：至少一条非空 content。
    """
    from zsdtdx import get_client, get_company_info

    with get_client():
        rows = get_company_info(codes=["600000"], category=["公司概况"], return_df=False, mode="sync")
    _require(isinstance(rows, list) and rows, "公司信息为空")
    contents = [str(r.get("content", "")).strip() for r in rows]
    _require(any(contents), "公司信息正文全空")
    cats = [str(r.get("category", "")).strip() for r in rows]
    _require(any(cats), "分类名为空")
    return {
        "n": len(rows),
        "categories": cats,
        "content_len": max(len(x) for x in contents),
        "content_head": next(x for x in contents if x)[:80],
    }


def case_stock_kline_sync() -> Dict[str, Any]:
    """
    输入：无。
    输出：sync K 线解析摘要。
    用途：验收 get_stock_kline(mode=sync) 返回 rows 且 OHLC 可解析。
    边界：至少一只票有有效 K 线。
    """
    from zsdtdx import StockKlineTask, get_stock_kline

    payloads = get_stock_kline(
        task=[
            StockKlineTask(code="600000", freq="d", start_time=START_DAY, end_time=END_DAY),
            {"code": "000001", "freq": "60", "start_time": START_DAY, "end_time": END_DAY},
        ],
        mode="sync",
    )
    stats = _summarize_kline_payloads(list(payloads or []))
    _require(stats["ok"] >= 1, f"股票 sync K 线无可解析结果: {stats}")
    return stats


def case_index_kline_sync() -> Dict[str, Any]:
    """
    输入：无。
    输出：指数 sync K 线解析摘要。
    用途：验收 get_index_kline(mode=sync) 按名称路由并解析 OHLC。
    边界：上证指数日线必须有有效 rows。
    """
    from zsdtdx import IndexKlineTask, get_index_kline

    payloads = get_index_kline(
        task=[
            IndexKlineTask(index_name="上证指数", freq="d", start_time=START_DAY, end_time=END_DAY),
            IndexKlineTask(index_name="中证1000", freq="15", start_time=START_DAY, end_time=END_DAY),
        ],
        mode="sync",
    )
    stats = _summarize_kline_payloads(list(payloads or []))
    _require(stats["ok"] >= 1, f"指数 sync K 线无可解析结果: {stats}")
    return stats


def case_future_kline() -> Dict[str, Any]:
    """
    输入：无。
    输出：期货 K 线行数与样本。
    用途：验收 get_future_kline 返回 DataFrame 且字段可解析。
    边界：至少 1 行有效 OHLC。
    """
    from zsdtdx import get_client, get_future_kline

    with get_client():
        df = get_future_kline(codes="CU", freq="d", start_time=START_DAY, end_time=END_DAY)
    _require(df is not None and hasattr(df, "empty") and not df.empty, f"期货 K 线为空: {df}")
    recs = df.to_dict(orient="records")
    bad = 0
    for rec in recs:
        row = {
            "datetime": str(rec.get("datetime", "")),
            "open": rec.get("open"),
            "high": rec.get("high"),
            "low": rec.get("low"),
            "close": rec.get("close"),
            "volume": rec.get("volume"),
        }
        if _check_ohlc(row):
            bad += 1
    _require(bad == 0, f"期货 K 线解析失败 {bad}/{len(recs)}")
    sample = recs[0]
    return {
        "rows": len(recs),
        "columns": list(df.columns),
        "sample": {
            "code": sample.get("code"),
            "datetime": sample.get("datetime"),
            "open": sample.get("open"),
            "close": sample.get("close"),
            "settlement_price": sample.get("settlement_price"),
        },
    }


def case_stock_kline_async() -> Dict[str, Any]:
    """
    输入：无。
    输出：async 事件统计。
    用途：验收 get_stock_kline(mode=async) 能经进程池回传可解析 rows。
    边界：必须收到 done，且至少 1 个 data 任务可解析。
    """
    from zsdtdx import destroy_parallel_fetcher, get_stock_kline

    job = get_stock_kline(
        task=[{"code": "600000", "freq": "d", "start_time": START_DAY, "end_time": END_DAY}],
        mode="async",
    )
    payloads: List[Dict[str, Any]] = []
    done = None
    while True:
        event = job.queue.get(timeout=180)
        if str(event.get("event", "")).strip().lower() == "done":
            done = event
            break
        if str(event.get("event", "")).strip().lower() == "data":
            payloads.append(event)
    destroy_info = dict(destroy_parallel_fetcher())
    try:
        job.result()
    except Exception as exc:
        _fail(f"async job.result 失败: {exc}")
    stats = _summarize_kline_payloads(payloads)
    _require(done is not None, "未收到 done")
    _require(stats["ok"] >= 1, f"async 无可解析 K 线: {stats}")
    return {"stats": stats, "done": done, "destroy": destroy_info}


def case_runtime() -> Dict[str, Any]:
    """
    输入：无。
    输出：failures 行数与 metadata 关键字段。
    用途：验收 get_runtime_failures / get_runtime_metadata。
    边界：metadata 必须为非空 dict，并含当前银河 host 线索。
    """
    from zsdtdx import get_client, get_runtime_failures, get_runtime_metadata, get_stock_latest_price

    with get_client():
        get_stock_latest_price("600000")
        failures = get_runtime_failures()
        meta = get_runtime_metadata()
    _require(failures is not None, "failures 为空对象")
    _require(isinstance(meta, dict) and meta, "metadata 为空")
    std_host = str(meta.get("std_active_host") or "")
    _require(":" in std_host, f"std_active_host 异常: {std_host}")
    return {
        "failures_type": type(failures).__name__,
        "failures_rows": int(getattr(failures, "shape", [0])[0]),
        "meta_keys": sorted(meta.keys()),
        "std_active_host": meta.get("std_active_host"),
        "ex_active_host": meta.get("ex_active_host"),
        "config_path": meta.get("config_path"),
    }


CASES = [
    ("set_config_path/get_client", case_set_config_and_client),
    ("get_supported_markets", case_supported_markets),
    ("get_stock_code_name", case_stock_code_name),
    ("get_all_future_list", case_future_list),
    ("get_stock_latest_price", case_stock_latest_price),
    ("get_future_latest_price", case_future_latest_price),
    ("get_company_info", case_company_info),
    ("get_stock_kline_sync", case_stock_kline_sync),
    ("get_index_kline_sync", case_index_kline_sync),
    ("get_future_kline", case_future_kline),
    ("get_stock_kline_async", case_stock_kline_async),
    ("get_runtime_failures/metadata", case_runtime),
]


def main() -> int:
    """
    输入：无。
    输出：全部通过返回 0，任一失败返回 1。
    用途：顺序执行银河 simple_api 冒烟并落盘报告。
    边界：单个 case 失败不中断后续 case。
    """
    from zsdtdx import destroy_parallel_fetcher

    report: Dict[str, Any] = {"ok": True, "cases": []}
    t0 = time.time()
    try:
        for name, fn in CASES:
            started = time.time()
            item: Dict[str, Any] = {"name": name}
            print(f"\n===== {name} =====", flush=True)
            try:
                item["result"] = fn()
                item["ok"] = True
                print(json.dumps(item["result"], ensure_ascii=False, default=str)[:1200], flush=True)
                print(f"PASS {name} {time.time() - started:.1f}s", flush=True)
            except Exception:
                item["ok"] = False
                item["error"] = traceback.format_exc()
                report["ok"] = False
                print(item["error"], flush=True)
                print(f"FAIL {name} {time.time() - started:.1f}s", flush=True)
            item["elapsed_seconds"] = round(time.time() - started, 3)
            report["cases"].append(item)
    finally:
        try:
            destroy_parallel_fetcher()
        except Exception:
            pass
    report["elapsed_seconds"] = round(time.time() - t0, 3)
    _ART.mkdir(parents=True, exist_ok=True)
    out = _ART / "smoke_yinhe_simple_api.json"
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(f"\nREPORT={out} overall_ok={report['ok']} elapsed={report['elapsed_seconds']}s", flush=True)
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
