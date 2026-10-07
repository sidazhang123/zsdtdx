"""
模块：`cache/block_file_cache.py`。

职责：
1. 把板块三份命名文件按 6 小时有效期写入本地缓存。
2. 供板块名称、板块 K 线与 `get_stock_stat`（zhb/tdxstat、tdxhy）共用：
   未过期则直接读取，过期则由调用方重新下载。

边界：
1. 不发起网络请求，不解析板块目录或统计表。
2. 三份文件必须同时存在且非空，否则视为无效缓存。
3. 多进程刷新由调用方的同目录文件锁串行化，写入本身使用原子替换。
"""

from __future__ import annotations

import math
import os
import pickle
import tempfile
import threading
import time
from pathlib import Path
from typing import Dict, Optional

from zsdtdx.cache.catalog_disk_cache import resolve_catalog_cache_dir
from zsdtdx.parser.block_index_catalog import REQUIRED_BLOCK_FILES

# 6 小时过期。两个对外函数共用这一份缓存。
BLOCK_FILE_TTL_SECONDS = 6 * 3600
_CACHE_FORMAT_VERSION = 1
_CACHE_FILENAME = "block_named_files.pkl"
_LOCK = threading.Lock()


def block_file_cache_path(cache_dir: Optional[Path] = None) -> Optional[Path]:
    """
    输入缓存目录，输出板块文件缓存路径。

    输入：cache_dir 为空时使用码表缓存目录。
    输出：pkl 路径；目录不可写时为 None。
    用途：定位三份文件的共享缓存。
    边界条件：不创建业务文件，只解析目录。
    """
    directory = cache_dir if cache_dir is not None else resolve_catalog_cache_dir()
    if directory is None:
        return None
    return Path(directory) / _CACHE_FILENAME


def _atomic_write_bytes(path: Path, data: bytes) -> None:
    """
    输入目标路径与字节，输出无。

    输入：path 为最终文件；data 为 pickle 字节。
    输出：无。
    用途：临时文件写完后替换，避免读到半截缓存。
    边界条件：失败时删除临时文件并原样抛出。
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fd: Optional[int] = None
    tmp_path: Optional[Path] = None
    try:
        fd, tmp_name = tempfile.mkstemp(
            dir=str(path.parent), prefix=".block_files_", suffix=".tmp"
        )
        tmp_path = Path(tmp_name)
        with os.fdopen(fd, "wb") as handle:
            fd = None
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(str(tmp_path), str(path))
    except Exception:
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                pass
        if tmp_path is not None:
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass
        raise


def _snapshot_ok(snapshot: object) -> bool:
    """
    输入反序列化对象，输出是否为合法三文件快照。

    输入：snapshot 为 pickle 对象。
    输出：True 表示版本、时间和三份非空原文都在。
    用途：加载后校验。
    边界条件：不检查是否过期。
    """
    if not isinstance(snapshot, dict):
        return False
    if int(snapshot.get("version", 0) or 0) != _CACHE_FORMAT_VERSION:
        return False
    try:
        fetched_at = float(snapshot.get("fetched_at"))
    except (TypeError, ValueError):
        return False
    if not math.isfinite(fetched_at) or fetched_at <= 0:
        return False
    files = snapshot.get("files")
    if not isinstance(files, dict):
        return False
    for name in REQUIRED_BLOCK_FILES:
        raw = files.get(name)
        if not isinstance(raw, (bytes, bytearray)) or len(raw) == 0:
            return False
    return True


def load_fresh_block_files(
    *,
    now: Optional[float] = None,
    cache_dir: Optional[Path] = None,
) -> Optional[Dict[str, object]]:
    """
    输入当前时间，输出未过期的三文件快照。

    输入：
    1. now: Unix 秒；为空时取当前时间。
    2. cache_dir: 缓存目录；为空时用默认目录。
    输出：
    1. `{"fetched_at", "files"}`；缺失、损坏或超过 6 小时时为 None。
    用途：
    1. 板块名称与板块 K 线在下载前先读缓存。
    边界条件：
    1. 损坏文件会被删除。
    2. 过期文件保留在磁盘上，由下次成功下载覆盖。
    """
    path = block_file_cache_path(cache_dir)
    if path is None or not path.is_file():
        return None
    current = time.time() if now is None else float(now)
    if now is None:
        try:
            mtime_age = current - path.stat().st_mtime
        except OSError:
            return None
        if not math.isfinite(current) or not math.isfinite(mtime_age) or mtime_age < 0:
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass
            return None
        # 文件修改时间不会早于原子写入完成时间。先挡住显然过期的旧文件，
        # 避免为已过期的大型 pickle 支付反序列化成本；最终仍以下方 fetched_at 为准。
        if mtime_age >= BLOCK_FILE_TTL_SECONDS:
            return None
    try:
        with open(path, "rb") as handle:
            snapshot = pickle.loads(handle.read())
    except Exception:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass
        return None
    if not _snapshot_ok(snapshot):
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass
        return None
    fetched_at = float(snapshot["fetched_at"])
    age = current - fetched_at
    if not math.isfinite(current) or not math.isfinite(age) or age < 0:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass
        return None
    if age >= BLOCK_FILE_TTL_SECONDS:
        return None
    files = {name: bytes(snapshot["files"][name]) for name in REQUIRED_BLOCK_FILES}
    return {"fetched_at": fetched_at, "files": files}


def save_block_files(
    files: Dict[str, bytes],
    *,
    fetched_at: Optional[float] = None,
    cache_dir: Optional[Path] = None,
) -> float:
    """
    输入三份原文，输出写入的时间戳。

    输入：
    1. files: 三份命名文件原文。
    2. fetched_at: 写入时间；为空时取当前时间。
    3. cache_dir: 缓存目录。
    输出：
    1. 实际写入的 fetched_at。
    用途：
    1. 下载成功后刷新 6 小时缓存。
    边界条件：
    1. 缺文件或目录不可写时抛 ValueError / OSError。
    """
    payload = {}
    for name in REQUIRED_BLOCK_FILES:
        raw = files.get(name)
        if not isinstance(raw, (bytes, bytearray)) or len(raw) == 0:
            raise ValueError(f"板块文件为空: {name}")
        payload[name] = bytes(raw)
    path = block_file_cache_path(cache_dir)
    if path is None:
        raise ValueError("板块文件缓存目录不可写")
    stamp = time.time() if fetched_at is None else float(fetched_at)
    if not math.isfinite(stamp) or stamp <= 0:
        raise ValueError(f"板块文件缓存时间戳非法: {fetched_at}")
    blob = pickle.dumps(
        {"version": _CACHE_FORMAT_VERSION, "fetched_at": stamp, "files": payload},
        protocol=pickle.HIGHEST_PROTOCOL,
    )
    with _LOCK:
        _atomic_write_bytes(path, blob)
    return stamp


def invalidate_block_files(cache_dir: Optional[Path] = None) -> None:
    """删除板块文件快照；用于语义解析失败后强制下次重新下载。

    输入：cache_dir 为空时使用默认缓存目录。
    输出：无；文件不存在时安全返回。
    边界：删除失败原样抛出 OSError，由调用方记录但不复用该内存快照。
    """
    path = block_file_cache_path(cache_dir)
    if path is not None:
        path.unlink(missing_ok=True)
