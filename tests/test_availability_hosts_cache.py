"""TCP 可用地址缓存、set_config_path 异步预热与 worker 槽位分配验收。"""

from __future__ import annotations

import copy
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from zsdtdx.helper import _apply_active_config_path, _validate_config_or_raise
from zsdtdx.parallel_fetcher import _build_worker_host_slot_assignments
import zsdtdx.unified_client as uc
from zsdtdx.unified_client import (
    _cache_usable_for_cfg,
    _ensure_availability_hosts_cache,
    compute_hosts_fingerprint,
    get_probe_result_cache,
    resolve_presorted_hosts_for_connection,
    rotate_hosts_list,
)


def _minimal_cfg(std_hosts=None, ex_hosts=None):
    std_hosts = std_hosts or ["1.1.1.1:7709"]
    cfg = {
        "hosts": {"standard": std_hosts, "extended": ex_hosts or []},
        "pool": {"probe_timeout": 0.01},
    }
    return cfg


def _reset_probe_cache():
    with uc._probe_result_cache_lock:
        uc._probe_result_cache.clear()
        uc._last_tcp_probe_hosts_fingerprint = None


@pytest.fixture(autouse=True)
def reset_cache_and_globals():
    import zsdtdx.helper as helper_mod

    saved_active_path = helper_mod._ACTIVE_CONFIG_PATH
    saved_notice = helper_mod._DEFAULT_CONFIG_NOTICE_PRINTED
    _reset_probe_cache()
    yield
    _reset_probe_cache()
    helper_mod._ACTIVE_CONFIG_PATH = saved_active_path
    helper_mod._DEFAULT_CONFIG_NOTICE_PRINTED = saved_notice


def test_cache_usable_requires_fingerprint_and_nonempty():
    cfg = _minimal_cfg()
    assert not _cache_usable_for_cfg(cfg)
    std, ex = [("1.1.1.1", 7709)], []
    fp = compute_hosts_fingerprint(std, ex)
    with uc._probe_result_cache_lock:
        uc._probe_result_cache["standard"] = list(std)
        uc._last_tcp_probe_hosts_fingerprint = fp
    assert _cache_usable_for_cfg(cfg)


def test_cache_unusable_when_fingerprint_changes():
    cfg_a = _minimal_cfg(["1.1.1.1:7709"])
    std_a = [("1.1.1.1", 7709)]
    fp_a = compute_hosts_fingerprint(std_a, [])
    with uc._probe_result_cache_lock:
        uc._probe_result_cache["standard"] = list(std_a)
        uc._last_tcp_probe_hosts_fingerprint = fp_a
    assert _cache_usable_for_cfg(cfg_a)

    cfg_b = _minimal_cfg(["2.2.2.2:7709"])
    assert not _cache_usable_for_cfg(cfg_b)


@patch("zsdtdx.unified_client._tcp_probe_and_trim_available_hosts")
def test_ensure_skips_when_cache_usable(mock_probe):
    cfg = _minimal_cfg()
    std = [("1.1.1.1", 7709)]
    fp = compute_hosts_fingerprint(std, [])
    with uc._probe_result_cache_lock:
        uc._probe_result_cache["standard"] = list(std)
        uc._last_tcp_probe_hosts_fingerprint = fp

    result = _ensure_availability_hosts_cache(cfg=cfg, force=False)
    assert result.get("skipped") is True
    mock_probe.assert_not_called()


@patch("zsdtdx.unified_client._tcp_probe_and_trim_available_hosts")
def test_ensure_writes_cache_once(mock_probe):
    mock_probe.side_effect = lambda hosts, timeout, fallback, name: list(hosts)[:1] or list(fallback)

    cfg = _minimal_cfg(["1.1.1.1:7709", "2.2.2.2:7709"])
    result = _ensure_availability_hosts_cache(cfg=cfg, force=True)
    assert result.get("skipped") is False
    cache = get_probe_result_cache()
    assert cache.get("standard")
    assert mock_probe.call_count >= 1


@patch("zsdtdx.unified_client._tcp_probe_and_trim_available_hosts")
def test_concurrent_ensure_single_flight(mock_probe):
    mock_probe.side_effect = lambda hosts, timeout, fallback, name: list(hosts)

    cfg = _minimal_cfg(["1.1.1.1:7709", "2.2.2.2:7709"])
    barrier = threading.Barrier(2)
    probe_calls = []

    def slow_probe(hosts, timeout, fallback, name):
        probe_calls.append(1)
        time.sleep(0.15)
        return list(hosts)

    mock_probe.side_effect = slow_probe

    def worker():
        barrier.wait()
        _ensure_availability_hosts_cache(cfg=cfg, force=True)

    t1 = threading.Thread(target=worker)
    t2 = threading.Thread(target=worker)
    t1.start()
    t2.start()
    t1.join(timeout=30)
    t2.join(timeout=30)
    assert len(probe_calls) >= 1


def test_resolve_worker_sync_if_missing_false_raises():
    with pytest.raises(RuntimeError, match="sync_if_missing=False"):
        resolve_presorted_hosts_for_connection(
            cfg=_minimal_cfg(),
            sync_if_missing=False,
        )


@patch("zsdtdx.unified_client._tcp_probe_and_trim_available_hosts")
def test_resolve_uses_presorted_without_ensure(mock_probe, tmp_path):
    presorted = {
        "standard": [("9.9.9.9", 7709)],
        "extended": [],
    }
    out = resolve_presorted_hosts_for_connection(
        presorted_hosts=presorted,
        sync_if_missing=False,
    )
    assert out["standard"] == presorted["standard"]
    mock_probe.assert_not_called()


def test_rotate_hosts_list():
    hosts = [("a", 1), ("b", 2), ("c", 3)]
    assert rotate_hosts_list(hosts, 1) == [("b", 2), ("c", 3), ("a", 1)]
    assert rotate_hosts_list(hosts, 4) == rotate_hosts_list(hosts, 1)


def test_build_worker_slot_assignments_rotate_not_shuffle():
    snapshot = {
        "standard": [("h1", 7709), ("h2", 7709), ("h3", 7709)],
        "extended": [],
    }
    assignments = _build_worker_host_slot_assignments(snapshot, 4)
    assert len(assignments) == 4
    full_set = set(snapshot["standard"])
    for item in assignments:
        assert set(item["standard"]) == full_set
        assert len(item["standard"]) == 3


def test_validate_config_or_raise_no_client(tmp_path):
    cfg_path = tmp_path / "ok.yaml"
    cfg_path.write_text(
        yaml.dump(_minimal_cfg()),
        encoding="utf-8",
    )
    with open(cfg_path, "r", encoding="utf-8") as fp:
        cfg = yaml.safe_load(fp)
    _validate_config_or_raise(cfg, str(cfg_path.resolve()))


@patch("zsdtdx.unified_client._ensure_availability_hosts_cache")
def test_set_config_path_async_returns_before_ensure(mock_ensure, tmp_path):
    cfg_path = tmp_path / "user.yaml"
    cfg_path.write_text(yaml.dump(_minimal_cfg()), encoding="utf-8")

    started = threading.Event()
    release = threading.Event()

    def slow_ensure(**kwargs):
        started.set()
        release.wait(timeout=5)

    mock_ensure.side_effect = slow_ensure

    resolved = _apply_active_config_path(
        str(cfg_path),
        async_background_probe=True,
    )
    assert Path(resolved).resolve() == cfg_path.resolve()
    assert started.wait(timeout=2.0)
    release.set()
    time.sleep(0.05)
    mock_ensure.assert_called()


@patch("zsdtdx.unified_client._ensure_availability_hosts_cache")
def test_set_config_path_sync_calls_ensure(mock_ensure, tmp_path):
    cfg_path = tmp_path / "user.yaml"
    cfg_path.write_text(yaml.dump(_minimal_cfg()), encoding="utf-8")
    _apply_active_config_path(str(cfg_path), async_background_probe=False)
    # set_active_config_path 会构造 ParallelKlineFetcher，其 __init__ 也会 ensure 一次
    assert mock_ensure.call_count >= 1
    assert mock_ensure.call_count <= 2


def test_config_yaml_no_probe_on_init():
    cfg_path = Path(__file__).resolve().parents[1] / "src" / "zsdtdx" / "config.yaml"
    text = cfg_path.read_text(encoding="utf-8")
    assert "probe_on_init" not in text
