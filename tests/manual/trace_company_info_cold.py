"""公开 F10 冷启动压测和逐帧取证；默认不预装路由，异常保留目录/请求/回包/拼接字节。"""

from __future__ import annotations

import argparse
import gc
import hashlib
import json
import os
import re
import struct
import sys
import threading
import time
import zlib
from pathlib import Path

import psutil
import yaml

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, os.environ.get("ZSDTDX_DIAG_SRC", str(ROOT / "src")))

from zsdtdx import destroy_parallel_fetcher, get_company_info, set_config_path  # noqa: E402
from zsdtdx.engine.unified_client import UnifiedTdxClient  # noqa: E402
from zsdtdx.parser.base import BaseParser  # noqa: E402
from zsdtdx.parser import base as protocol  # noqa: E402


def write_json(path, value):
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--universe", required=True, help="包含 valid_codes 或 codes 的 JSON"
    )
    parser.add_argument("--output", required=True)
    parser.add_argument("--limit", type=int, default=400)
    parser.add_argument("--rounds", type=int, default=1)
    parser.add_argument("--host", help="限定为单个 host:port；不修改生产配置")
    parser.add_argument(
        "--warm-routes",
        action="store_true",
        help="仅用于旧版错位根因复现，不作为冷启动验收",
    )
    parser.add_argument(
        "--trace-codes",
        default="601319,601718",
        help="持续保存问题股的全部帧；异常股自动加入",
    )
    args = parser.parse_args()
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    source = json.loads(Path(args.universe).read_text(encoding="utf-8"))
    codes = source.get("valid_codes", source.get("codes", []))
    if args.limit:
        # 均匀覆盖沪深京；报告中的两个异常股始终纳入。
        codes = list(
            dict.fromkeys(
                [
                    *codes[:: max(1, len(codes) // args.limit)][: args.limit],
                    "601319",
                    "601718",
                ]
            )
        )
    config = yaml.safe_load(
        (ROOT / "src/zsdtdx/config.yaml").read_text(encoding="utf-8")
    )
    config["parallel"]["thread_workers"] = 60
    config.setdefault("catalog_cache", {}).update(
        enabled=True, path=str(output / "cache")
    )
    if args.host:
        config["hosts"]["standard"] = {"szshbj": [args.host], "szsh_only": []}
        config["parallel"]["threads_per_host_std"] = 60
    config.setdefault("logging", {})["level"] = "ERROR"
    config_path = output / "config.yaml"
    config_path.write_text(yaml.safe_dump(config, allow_unicode=True), encoding="utf-8")
    local = threading.local()
    lock = threading.Lock()
    active_sockets = {}
    tracked_codes = set(args.trace_codes.split(","))
    frame_sequence = [0]
    stats = {
        "frames": 0,
        "socket_overlap": 0,
        "parser_mismatch": 0,
        "directory_parse_mismatch": 0,
        "request_directory_mismatch": 0,
        "assembly_mismatch": 0,
        "host_mismatch": 0,
        "bad_heads": 0,
        "directory_loads": 0,
        "response_identity_mismatch": 0,
        "wire_body_mismatch": 0,
    }
    original_call = BaseParser._call_api
    original_recv = protocol._recv_exact
    original_download = UnifiedTdxClient._download_std_security_catalog

    def fallback_identity(text, code, category):
        title = re.match(
            r"☆([^☆\r\n]+)☆\s*◇\s*([0-9]{6})(?![0-9])",
            str(text).lstrip("\ufeff\x00 \t\r\n")[:240],
        )
        return bool(
            title
            and "".join(title.group(1).split()) == "".join(str(category).split())
            and title.group(2) == str(code)
        )

    original_identity = getattr(
        UnifiedTdxClient, "_company_info_identity_matches", fallback_identity
    )
    original_fetch = UnifiedTdxClient._fetch_company_content

    def download(client):
        with lock:
            stats["directory_loads"] += 1
        return original_download(client)

    def dump_attempt(state, cause):
        with lock:
            tracked_codes.add(state.get("code", ""))
            number = stats["bad_heads"]
            stats["bad_heads"] += 1
        folder = output / f"anomaly_{number:04d}_{state.get('code', 'unknown')}"
        folder.mkdir(exist_ok=True)
        records = []
        for i, frame in enumerate(state.get("frames", [])):
            body = frame["body"]
            (folder / f"{i:04d}_response.bin").write_bytes(body)
            (folder / f"{i:04d}_request.bin").write_bytes(frame["request"])
            (folder / f"{i:04d}_wire.bin").write_bytes(frame["wire"])
            records.append(
                {k: v for k, v in frame.items() if k not in {"body", "request", "wire"}}
            )
        write_json(folder / "frames.json", {"cause": cause, "frames": records})
        return folder

    def read_wire(sock, size, *, header=False):
        result = original_recv(sock, size, header=header)
        if getattr(local, "recording", False):
            if header:
                local.header = result
            else:
                local.wire = result
        return result

    def traced_call(command):
        request = bytes(command.send_pkg or b"")
        if len(request) < 12 or request[10:12] not in (b"\xcf\x02", b"\xd0\x02"):
            return original_call(command)
        sock = command.client
        peer = sock.getpeername()
        socket_key = id(sock)
        tid = threading.get_ident()
        with lock:
            if socket_key in active_sockets:
                stats["socket_overlap"] += 1
            active_sockets[socket_key] = tid
        original_parse = command.parseResponse
        local.recording = True

        def parse(body):
            code = request[14:20].decode("ascii")
            directory = request[10:12] == b"\xcf\x02"
            if directory:
                local.state = {
                    "code": code,
                    "peer": peer,
                    "frames": [],
                    "directory": original_parse(body),
                }
                independent = []
                for seq in range(struct.unpack_from("<H", body)[0]):
                    name, filename, start, length = struct.unpack_from(
                        "<64s80sII", body, 2 + seq * 152
                    )
                    independent.append(
                        dict(
                            index=seq,
                            name=name.split(b"\0")[0].decode("gbk"),
                            filename=filename.split(b"\0")[0].decode("ascii"),
                            start=start,
                            length=length,
                        )
                    )
                with lock:
                    stats["directory_parse_mismatch"] += (
                        local.state["directory"] != independent
                    )
            state = getattr(local, "state", {"code": code, "peer": peer, "frames": []})
            result = original_parse(body)
            frame = {
                "peer": peer,
                "socket_id": socket_key,
                "thread_id": tid,
                "code": code,
                "request": request,
                "body": bytes(body),
                "body_sha256": hashlib.sha256(body).hexdigest(),
                "command": "directory" if directory else "content",
                "header_hex": local.header.hex(),
                "wire": local.wire,
            }
            compressed, uncompressed = struct.unpack_from("<HH", local.header, 12)
            independent_body = (
                local.wire
                if compressed == uncompressed
                else zlib.decompress(local.wire)
            )
            with lock:
                stats["wire_body_mismatch"] += independent_body != body
            if not directory:
                index, filename, start, length, _ = struct.unpack_from(
                    "<H80sIII", request, 20
                )
                frame.update(
                    index=index,
                    filename=filename.rstrip(b"\0").decode("ascii"),
                    start=start,
                    length=length,
                    declared=struct.unpack_from("<H", body, 10)[0],
                    prefix_hex=body[:10].hex(),
                )
                # 独立检查切片边界；不信任生产 parser 的切片结果。
                expected = body[12 : 12 + frame["declared"]]
                with lock:
                    stats["parser_mismatch"] += result != expected
                    stats["host_mismatch"] += state["peer"] != peer
                    stats["response_identity_mismatch"] += body[:8] != request[12:20]
                category = next(
                    (
                        item
                        for item in state.get("directory", [])
                        if item["index"] == index
                    ),
                    None,
                )
                if category:
                    actual_filename = filename.rstrip(b"\0").decode("ascii")
                    with lock:
                        stats["request_directory_mismatch"] += not (
                            code == state["code"]
                            and actual_filename == category["filename"]
                            and start >= category["start"]
                            and start - category["start"] + length == category["length"]
                        )
                if category and start == category["start"]:
                    head = expected.decode("gbk", "ignore")
                    if not original_identity(head, code, category["name"]):
                        state["frames"].append(frame)
                        dump_attempt(
                            state, "first_page_original_bytes_have_wrong_title"
                        )
                        state["frames"].pop()
            state["frames"].append(frame)
            with lock:
                keep = code in tracked_codes
                frame_sequence[0] += 1
                sequence = frame_sequence[0]
            if keep:
                folder = output / f"trace_{code}"
                folder.mkdir(exist_ok=True)
                (folder / f"{sequence:07d}_response.bin").write_bytes(frame["body"])
                (folder / f"{sequence:07d}_request.bin").write_bytes(request)
                (folder / f"{sequence:07d}_wire.bin").write_bytes(frame["wire"])
                write_json(
                    folder / f"{sequence:07d}.json",
                    {
                        k: v
                        for k, v in frame.items()
                        if k not in {"body", "request", "wire"}
                    },
                )
            # 每股只留内存中的当前尝试；异常才落盘，长文也不累计到下一股。
            local.state = state
            with lock:
                stats["frames"] += 1
            return result

        command.parseResponse = parse
        try:
            return original_call(command)
        finally:
            command.parseResponse = original_parse
            local.recording = False
            with lock:
                active_sockets.pop(socket_key, None)

    def identity(content, code, category):
        valid = original_identity(content, code, category)
        if not valid:
            state = getattr(local, "state", {})
            folder = dump_attempt(state, "assembled_text_has_wrong_title")
            (folder / "assembled.txt").write_text(content, encoding="utf-8")
        return valid

    def fetch(client, *values, **kwargs):
        prior = len(getattr(local, "state", {}).get("frames", []))
        text, status = original_fetch(client, *values, **kwargs)
        frames = getattr(local, "state", {}).get("frames", [])[prior:]
        raw = b"".join(
            frame["body"][12 : 12 + frame["declared"]]
            for frame in frames
            if frame["command"] == "content"
        )
        with lock:
            stats["assembly_mismatch"] += text != raw.decode("gbk", "ignore")
        return text, status

    BaseParser._call_api = traced_call
    protocol._recv_exact = read_wire
    UnifiedTdxClient._download_std_security_catalog = download
    UnifiedTdxClient._company_info_identity_matches = staticmethod(identity)
    UnifiedTdxClient._fetch_company_content = fetch
    reports = []
    proc = psutil.Process()
    peak_rss = [0]
    stop = threading.Event()

    def sample():
        while not stop.wait(0.1):
            peak_rss[0] = max(peak_rss[0], proc.memory_info().rss)

    sampler = threading.Thread(target=sample, daemon=True)
    sampler.start()
    try:
        for round_no in range(args.rounds):
            # 销毁共享客户端后只调用公开接口，禁止人工注入/预装路由。
            destroy_parallel_fetcher()
            set_config_path(str(config_path))
            if args.warm_routes:
                from zsdtdx.engine.parallel_fetcher import _ensure_shared_client_context
                from zsdtdx.engine.unified_client import (
                    _ensure_availability_hosts_cache,
                )

                _ensure_availability_hosts_cache(
                    config_path=str(config_path), required_sources={"standard"}
                )
                _ensure_shared_client_context().get_all_stock_list(return_df=False)
            started = time.perf_counter()
            job = get_company_info(
                codes,
                category=source.get("categories")
                or [
                    "最新提示",
                    "财务分析",
                    "经营分析",
                    "股东研究",
                    "主力追踪",
                    "技术核心",
                ],
                mode="async",
            )
            messages = []
            while True:
                event = job.queue.get(timeout=600)
                messages.append({k: v for k, v in event.items() if k != "rows"})
                if event.get("event") == "done":
                    break
            rows = job.result()
            bad = [
                dict(
                    code=row["code"],
                    category=row["category"],
                    head=row["content"][:240],
                )
                for row in rows
                if not original_identity(
                    row["content"], row["code"].split(".")[-1], row["category"]
                )
            ]
            report = {
                "warm_routes": args.warm_routes,
                "source_file": sys.modules[UnifiedTdxClient.__module__].__file__,
                "source_sha256": hashlib.sha256(
                    Path(sys.modules[UnifiedTdxClient.__module__].__file__).read_bytes()
                ).hexdigest(),
                "round": round_no,
                "seconds": time.perf_counter() - started,
                "requested": len(codes),
                "rows": len(rows),
                "done": messages[-1],
                "errors": [e for e in messages[:-1] if e.get("error")],
                "bad_rows": bad,
                "peak_rss_mib": peak_rss[0] / 1024**2,
                "trace": dict(stats),
            }
            reports.append(report)
            write_json(output / "report.json", reports)
            print(json.dumps(report, ensure_ascii=False), flush=True)
            del rows, job, messages
            gc.collect()
    finally:
        destroy_parallel_fetcher()
        stop.set()
        sampler.join()
        BaseParser._call_api = original_call
        protocol._recv_exact = original_recv
        UnifiedTdxClient._download_std_security_catalog = original_download
        UnifiedTdxClient._company_info_identity_matches = staticmethod(
            original_identity
        )
        UnifiedTdxClient._fetch_company_content = original_fetch


if __name__ == "__main__":
    main()
