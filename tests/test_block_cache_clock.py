"""
模块：`test_block_cache_clock.py`。

职责：验证板块文件时钟精度差异及读取期间刷新，不误删有效来源。
边界：仅使用临时文件和确定性时钟，不联网，不改变真正未来来源的拒绝语义。
"""

import os

from zsdtdx.cache.block_file_cache import (
    block_file_cache_path,
    load_fresh_block_files,
    save_block_files,
)


def test_future_file_mtime_uses_valid_source_timestamp(tmp_path, monkeypatch):
    """输入稍晚的文件时间及有效 fetched_at，输出有效原文；mtime 不独立判作未来来源。"""
    current = 1_800_000_000.0
    files = {
        name: b"valid" for name in ("infoharbor_block.dat", "tdxhy.cfg", "zhb.zip")
    }
    save_block_files(files, fetched_at=current - 1, cache_dir=tmp_path)
    path = block_file_cache_path(tmp_path)
    os.utime(path, (current + 0.001, current + 0.001))
    monkeypatch.setattr("zsdtdx.cache.block_file_cache.time.time", lambda: current)
    snapshot = load_fresh_block_files(cache_dir=tmp_path)
    assert snapshot == {"fetched_at": current - 1, "files": files}
    assert path.exists()


def test_truly_future_source_is_still_rejected(tmp_path, monkeypatch):
    """输入未来 fetched_at，输出无缓存并删除非法快照；文件时间容差不放宽来源校验。"""
    current = 1_800_000_000.0
    files = {
        name: b"valid" for name in ("infoharbor_block.dat", "tdxhy.cfg", "zhb.zip")
    }
    save_block_files(files, fetched_at=current + 1, cache_dir=tmp_path)
    path = block_file_cache_path(tmp_path)
    os.utime(path, (current + 0.001, current + 0.001))
    monkeypatch.setattr("zsdtdx.cache.block_file_cache.time.time", lambda: current)
    assert load_fresh_block_files(cache_dir=tmp_path) is None
    assert not path.exists()


def test_source_refreshed_during_read_is_valid_at_completion(tmp_path, monkeypatch):
    """输入读取期间已刷新的来源，输出读完时已有效的快照，避免用读前时刻误判。"""
    started = 1_800_000_000.0
    files = {
        name: b"valid" for name in ("infoharbor_block.dat", "tdxhy.cfg", "zhb.zip")
    }
    save_block_files(files, fetched_at=started + 1, cache_dir=tmp_path)
    path = block_file_cache_path(tmp_path)
    os.utime(path, (started + 1, started + 1))
    ticks = iter([started, started + 2])
    monkeypatch.setattr("zsdtdx.cache.block_file_cache.time.time", lambda: next(ticks))
    assert load_fresh_block_files(cache_dir=tmp_path) == {
        "fetched_at": started + 1,
        "files": files,
    }
    assert path.exists()
