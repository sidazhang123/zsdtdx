"""
模块：`run_company_info_full_bench.py`。

职责：
1. 拉取京沪深（szsh+bj）全部股票的全部 F10 标签正文，落盘并记录总耗时。
2. 支持 sequential（当前 simple_api 顺序）与 parallel（目标态并行）两种模式。
3. 提供 compare 子命令，对比两次运行的正文哈希与耗时。

边界：
1. 需访问行情服务器；不由 pytest 收集。
2. 产物写入 tests/manual/artifacts/company_info_full_bench/<run_id>/。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

_ROOT = Path(__file__).resolve().parents[2]
_SRC = _ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

_ART_ROOT = _ROOT / "tests" / "manual" / "artifacts" / "company_info_full_bench"


def _progress(msg: str) -> None:
    """输入进度文本，输出无；用于控制台即时刷出进度。"""
    print(msg, flush=True)


def _list_jing_hu_shen_codes(client: Any) -> List[str]:
    """
    输入统一客户端，输出京沪深股票代码列表（无市场前缀）。

    用途：固定 scopes=szsh+bj，不受 get_stock_code_name 默认仅 szsh 影响。
    边界：仅保留标准行情路由命中的代码，按代码排序。
    """
    client.get_all_stock_list(return_df=True, refresh=False)
    scopes = {"szsh", "bj"}
    codes: List[str] = []
    for code in sorted(client._stock_route.keys()):  # noqa: SLF001
        route = client._stock_route.get(code)  # noqa: SLF001
        if route is None:
            continue
        if client._route_in_scopes(route, scopes):  # noqa: SLF001
            codes.append(str(code))
    return codes


def _row_key(code: str, category: str) -> str:
    """输入代码与分类名，输出稳定对比键。"""
    return f"{code}\t{category}"


def _content_sha1(content: str) -> str:
    """输入正文，输出 sha1 十六进制。"""
    return hashlib.sha1(str(content).encode("utf-8")).hexdigest()


def _write_json(path: Path, payload: Any) -> None:
    """输入路径与对象，写出 UTF-8 JSON。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _append_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> int:
    """输入路径与行迭代器，追加 JSONL；输出写入行数。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with path.open("a", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
            n += 1
    return n


def _normalize_rows(
    code: str, rows: Any
) -> Tuple[List[Dict[str, Any]], Dict[str, str]]:
    """
    输入单股返回与代码，输出 (落盘行列表, key->sha1)。

    边界：兼容 DataFrame / list[dict]；空结果仍记一条空分类占位不算入哈希表。
    """
    records: List[Dict[str, Any]]
    if rows is None:
        records = []
    elif hasattr(rows, "to_dict"):
        records = list(rows.to_dict(orient="records"))
    else:
        records = list(rows)

    out_rows: List[Dict[str, Any]] = []
    digests: Dict[str, str] = {}
    for item in records:
        cat = str(item.get("category", "")).strip()
        content = str(item.get("content", ""))
        digest = _content_sha1(content)
        key = _row_key(str(code), cat)
        digests[key] = digest
        out_rows.append(
            {
                "code": str(code),
                "category": cat,
                "content": content,
                "content_sha1": digest,
                "content_chars": len(content),
            }
        )
    return out_rows, digests


def run_sequential(
    *,
    out_dir: Path,
    limit: Optional[int] = None,
    progress_every: int = 20,
) -> Dict[str, Any]:
    """
    输入输出目录与可选上限，顺序拉取全部标签并落盘。

    输出：meta 摘要字典。
    用途：作为并行改造前的基线。
    """
    from zsdtdx import destroy_parallel_fetcher, get_client, get_company_info

    out_dir.mkdir(parents=True, exist_ok=True)
    content_path = out_dir / "content.jsonl"
    digest_path = out_dir / "digests.json"
    meta_path = out_dir / "meta.json"
    progress_path = out_dir / "progress.json"
    if content_path.exists():
        content_path.unlink()

    digests: Dict[str, str] = {}
    ok_codes = 0
    fail_codes = 0
    total_rows = 0
    failures: List[Dict[str, Any]] = []

    started = time.perf_counter()
    with get_client() as client:
        codes = _list_jing_hu_shen_codes(client)
        if limit is not None:
            codes = codes[: max(0, int(limit))]
        total_codes = len(codes)
        _write_json(
            progress_path,
            {
                "mode": "sequential",
                "phase": "started",
                "total_codes": total_codes,
                "done_codes": 0,
                "elapsed_seconds": 0.0,
            },
        )
        _progress(f"[sequential] 待拉取股票数={total_codes}")

        for idx, code in enumerate(codes, start=1):
            try:
                rows = get_company_info(codes=[code], category=None, return_df=False, mode="sync")
                out_rows, part = _normalize_rows(code, rows)
                digests.update(part)
                total_rows += _append_jsonl(content_path, out_rows)
                ok_codes += 1
            except Exception as exc:
                fail_codes += 1
                failures.append({"code": code, "error": str(exc)})
                _append_jsonl(
                    content_path,
                    [
                        {
                            "code": code,
                            "category": "",
                            "content": "",
                            "content_sha1": _content_sha1(""),
                            "content_chars": 0,
                            "error": str(exc),
                        }
                    ],
                )

            if idx % max(1, int(progress_every)) == 0 or idx == total_codes:
                elapsed = time.perf_counter() - started
                payload = {
                    "mode": "sequential",
                    "phase": "running",
                    "total_codes": total_codes,
                    "done_codes": idx,
                    "ok_codes": ok_codes,
                    "fail_codes": fail_codes,
                    "total_rows": total_rows,
                    "elapsed_seconds": round(elapsed, 3),
                    "codes_per_second": round(idx / elapsed, 4) if elapsed > 0 else 0.0,
                }
                _write_json(progress_path, payload)
                _progress(
                    f"[sequential] {idx}/{total_codes} "
                    f"ok={ok_codes} fail={fail_codes} rows={total_rows} "
                    f"elapsed={elapsed:.1f}s"
                )

    try:
        destroy_parallel_fetcher()
    except Exception:
        pass

    elapsed = time.perf_counter() - started
    _write_json(digest_path, digests)
    meta = {
        "mode": "sequential",
        "total_codes": total_codes,
        "ok_codes": ok_codes,
        "fail_codes": fail_codes,
        "total_rows": total_rows,
        "digest_keys": len(digests),
        "elapsed_seconds": round(elapsed, 3),
        "codes_per_second": round(total_codes / elapsed, 4) if elapsed > 0 else 0.0,
        "content_path": str(content_path),
        "digest_path": str(digest_path),
        "failures": failures[:200],
        "failure_count": len(failures),
    }
    _write_json(meta_path, meta)
    _write_json(progress_path, {**meta, "phase": "done"})
    _progress(f"[sequential] 完成 elapsed={elapsed:.1f}s rows={total_rows}")
    return meta


def run_parallel(
    *,
    out_dir: Path,
    limit: Optional[int] = None,
    progress_every: int = 50,
) -> Dict[str, Any]:
    """
    输入输出目录与可选上限，并行拉取全部标签并落盘。

    输出：meta 摘要字典。
    用途：目标态验收；底层走 fetch_company_info_parallel（list[dict]）。
    """
    from zsdtdx import (
        destroy_parallel_fetcher,
        get_client,
        prewarm_parallel_fetcher,
    )
    from zsdtdx.parallel_fetcher import fetch_company_info_parallel

    out_dir.mkdir(parents=True, exist_ok=True)
    content_path = out_dir / "content.jsonl"
    digest_path = out_dir / "digests.json"
    meta_path = out_dir / "meta.json"
    progress_path = out_dir / "progress.json"
    if content_path.exists():
        content_path.unlink()

    digests: Dict[str, str] = {}
    total_rows = 0
    started = time.perf_counter()
    ok_codes = 0
    fail_codes = 0
    failures: List[Dict[str, Any]] = []

    with get_client() as client:
        codes = _list_jing_hu_shen_codes(client)
        if limit is not None:
            codes = codes[: max(0, int(limit))]
        total_codes = len(codes)
        _write_json(
            progress_path,
            {
                "mode": "parallel",
                "phase": "prewarm",
                "total_codes": total_codes,
                "done_codes": 0,
                "elapsed_seconds": 0.0,
            },
        )
        _progress(f"[parallel] 待拉取股票数={total_codes}，开始预热进程池")
        prewarm = prewarm_parallel_fetcher()
        _progress(f"[parallel] 预热完成: {json.dumps(prewarm, ensure_ascii=False)}")
        _write_json(
            progress_path,
            {
                "mode": "parallel",
                "phase": "fetching",
                "total_codes": total_codes,
                "done_codes": 0,
                "elapsed_seconds": round(time.perf_counter() - started, 3),
            },
        )
        _progress("[parallel] 开始并行抓取（单次提交全量 codes）")

        # 直接走并行实现并关闭二次预热；按股票汇总以便进度与失败统计。
        rows = fetch_company_info_parallel(
            codes=codes,
            category=None,
            auto_prewarm=False,
        )
        by_code: Dict[str, List[Dict[str, Any]]] = {c: [] for c in codes}
        records = list(rows or [])
        for item in records:
            code = str(item.get("code", "")).strip()
            if code in by_code:
                by_code[code].append(item)
            else:
                by_code.setdefault(code, []).append(item)

        for idx, code in enumerate(codes, start=1):
            out_rows, part = _normalize_rows(code, by_code.get(code, []))
            digests.update(part)
            total_rows += _append_jsonl(content_path, out_rows)
            if out_rows:
                ok_codes += 1
            else:
                fail_codes += 1
                failures.append({"code": code, "error": "empty_result"})
            if idx % max(1, int(progress_every)) == 0 or idx == total_codes:
                elapsed = time.perf_counter() - started
                payload = {
                    "mode": "parallel",
                    "phase": "writing",
                    "total_codes": total_codes,
                    "done_codes": idx,
                    "ok_codes": ok_codes,
                    "fail_codes": fail_codes,
                    "total_rows": total_rows,
                    "elapsed_seconds": round(elapsed, 3),
                    "codes_per_second": round(idx / elapsed, 4) if elapsed > 0 else 0.0,
                }
                _write_json(progress_path, payload)
                _progress(
                    f"[parallel] write {idx}/{total_codes} "
                    f"ok={ok_codes} fail={fail_codes} rows={total_rows} "
                    f"elapsed={elapsed:.1f}s"
                )

    try:
        destroy_parallel_fetcher()
    except Exception:
        pass

    elapsed = time.perf_counter() - started
    _write_json(digest_path, digests)
    meta = {
        "mode": "parallel",
        "total_codes": total_codes,
        "ok_codes": ok_codes,
        "fail_codes": fail_codes,
        "total_rows": total_rows,
        "digest_keys": len(digests),
        "elapsed_seconds": round(elapsed, 3),
        "codes_per_second": round(total_codes / elapsed, 4) if elapsed > 0 else 0.0,
        "content_path": str(content_path),
        "digest_path": str(digest_path),
        "failures": failures[:200],
        "failure_count": len(failures),
    }
    _write_json(meta_path, meta)
    _write_json(progress_path, {**meta, "phase": "done"})
    _progress(f"[parallel] 完成 elapsed={elapsed:.1f}s rows={total_rows}")
    return meta


def compare_runs(baseline_dir: Path, target_dir: Path, out_path: Path) -> Dict[str, Any]:
    """
    输入基线目录与目标目录，输出对比摘要并落盘。

    用途：核对正文哈希一致性与耗时加速比。
    """
    base_meta = json.loads((baseline_dir / "meta.json").read_text(encoding="utf-8"))
    tgt_meta = json.loads((target_dir / "meta.json").read_text(encoding="utf-8"))
    base_digests = json.loads((baseline_dir / "digests.json").read_text(encoding="utf-8"))
    tgt_digests = json.loads((target_dir / "digests.json").read_text(encoding="utf-8"))

    base_keys = set(base_digests)
    tgt_keys = set(tgt_digests)
    only_base = sorted(base_keys - tgt_keys)
    only_tgt = sorted(tgt_keys - base_keys)
    common = base_keys & tgt_keys
    mismatch = sorted(k for k in common if base_digests[k] != tgt_digests[k])

    base_sec = float(
        base_meta.get("baseline_elapsed_seconds_official")
        or base_meta.get("elapsed_seconds")
        or 0.0
    )
    tgt_sec = float(tgt_meta.get("elapsed_seconds") or 0.0)
    speedup = (base_sec / tgt_sec) if tgt_sec > 0 else None

    # 仅对基线已有键做内容对照（顺序中途终止时基线为子集）
    overlap_mismatch = mismatch
    coverage = (
        round(len(common) / len(base_keys), 4) if base_keys else None
    )

    report = {
        "baseline_meta": base_meta,
        "target_meta": tgt_meta,
        "digest_keys_baseline": len(base_keys),
        "digest_keys_target": len(tgt_keys),
        "only_in_baseline": only_base[:100],
        "only_in_baseline_count": len(only_base),
        "only_in_target": only_tgt[:100],
        "only_in_target_count": len(only_tgt),
        "common_keys": len(common),
        "baseline_coverage_by_target": coverage,
        "content_mismatch": overlap_mismatch[:100],
        "content_mismatch_count": len(overlap_mismatch),
        "elapsed_baseline_seconds": base_sec,
        "elapsed_baseline_minutes_official": round(base_sec / 60.0, 2),
        "elapsed_target_seconds": tgt_sec,
        "speedup_vs_official_baseline": None
        if speedup is None
        else round(speedup, 3),
        # 基线可能是中途终止子集：目标覆盖全部基线键且公共键无正文差异即视为对照通过。
        "content_equal_on_baseline_subset": len(only_base) == 0
        and len(overlap_mismatch) == 0,
        "content_equal_full_universe": len(only_base) == 0
        and len(only_tgt) == 0
        and len(overlap_mismatch) == 0,
    }
    _write_json(out_path, report)
    _progress(
        f"[compare] subset_equal={report['content_equal_on_baseline_subset']} "
        f"mismatch={report['content_mismatch_count']} "
        f"speedup={report['speedup_vs_official_baseline']} "
        f"base={base_sec:.1f}s target={tgt_sec:.1f}s "
        f"coverage={report['baseline_coverage_by_target']}"
    )
    return report


def main(argv: Optional[Sequence[str]] = None) -> int:
    """
    输入 CLI 参数，输出进程退出码。

    子命令：
    - sequential / parallel：跑全量 F10 并落盘
    - compare：对比两次 digests/meta
    """
    parser = argparse.ArgumentParser(description="京沪深全股 F10 全标签基线/并行对比")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_seq = sub.add_parser("sequential", help="顺序基线（当前 simple_api）")
    p_seq.add_argument("--run-id", default="sequential")
    p_seq.add_argument("--limit", type=int, default=None)
    p_seq.add_argument("--progress-every", type=int, default=20)

    p_par = sub.add_parser("parallel", help="并行目标态")
    p_par.add_argument("--run-id", default="parallel")
    p_par.add_argument("--limit", type=int, default=None)
    p_par.add_argument("--progress-every", type=int, default=50)

    p_cmp = sub.add_parser("compare", help="对比两次运行")
    p_cmp.add_argument("--baseline", required=True)
    p_cmp.add_argument("--target", required=True)
    p_cmp.add_argument("--out", default="compare.json")

    args = parser.parse_args(argv)
    if args.cmd == "sequential":
        out_dir = _ART_ROOT / str(args.run_id)
        run_sequential(
            out_dir=out_dir,
            limit=args.limit,
            progress_every=args.progress_every,
        )
        return 0
    if args.cmd == "parallel":
        out_dir = _ART_ROOT / str(args.run_id)
        run_parallel(
            out_dir=out_dir,
            limit=args.limit,
            progress_every=args.progress_every,
        )
        return 0
    if args.cmd == "compare":
        baseline_dir = Path(args.baseline)
        if not baseline_dir.is_absolute():
            baseline_dir = _ART_ROOT / baseline_dir
        target_dir = Path(args.target)
        if not target_dir.is_absolute():
            target_dir = _ART_ROOT / target_dir
        out_path = Path(args.out)
        if not out_path.is_absolute():
            out_path = _ART_ROOT / out_path
        report = compare_runs(baseline_dir, target_dir, out_path)
        return 0 if report.get("content_equal_on_baseline_subset") else 2
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
