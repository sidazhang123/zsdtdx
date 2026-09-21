"""配置深合并：用户 YAML 覆盖内置同名键、丢弃未知字段、列表整段替换。"""

from __future__ import annotations

from pathlib import Path

import yaml

from zsdtdx.helper import _apply_active_config_path
from zsdtdx.unified_client import (
    _DEFAULT_CONFIG_PATH,
    _deep_merge_config_overlay,
    _load_builtin_zsdtdx_config,
    _load_merged_zsdtdx_config,
)


def test_deep_merge_overrides_leaf_keeps_sibling():
    base = {"pool": {"connect_timeout": 1.5, "probe_timeout": 0.8}, "a": 1}
    overlay = {"pool": {"connect_timeout": 3.0}, "unknown": 9}
    merged = _deep_merge_config_overlay(base, overlay)
    assert merged["pool"]["connect_timeout"] == 3.0
    assert merged["pool"]["probe_timeout"] == 0.8
    assert merged["a"] == 1
    assert "unknown" not in merged


def test_deep_merge_list_full_replace():
    base = {"hosts": {"standard": ["1.1.1.1:7709", "2.2.2.2:7709"]}}
    overlay = {"hosts": {"standard": ["9.9.9.9:7709"]}}
    merged = _deep_merge_config_overlay(base, overlay)
    assert merged["hosts"]["standard"] == ["9.9.9.9:7709"]


def test_load_merged_default_path_equals_builtin():
    builtin = _load_builtin_zsdtdx_config()
    loaded = _load_merged_zsdtdx_config(str(_DEFAULT_CONFIG_PATH))
    assert loaded == builtin
    assert loaded is not builtin


def test_load_merged_partial_user_yaml(tmp_path):
    builtin = _load_builtin_zsdtdx_config()
    user_path = tmp_path / "partial.yaml"
    user_path.write_text(
        yaml.dump(
            {
                "pool": {"connect_timeout": 9.5},
                "not_a_real_section": {"x": 1},
                "client": {"bogus_key": False},
            },
            allow_unicode=True,
        ),
        encoding="utf-8",
    )
    merged = _load_merged_zsdtdx_config(str(user_path))
    assert merged["pool"]["connect_timeout"] == 9.5
    assert merged["pool"]["probe_timeout"] == builtin["pool"]["probe_timeout"]
    assert merged["hosts"]["standard"] == builtin["hosts"]["standard"]
    assert "not_a_real_section" not in merged
    assert "bogus_key" not in merged["client"]
    assert "preconnect_on_enter" in merged["client"]


def test_set_config_path_accepts_partial_without_hosts(tmp_path, monkeypatch):
    """不完整 YAML（无 hosts）合并后校验应通过。"""
    user_path = tmp_path / "only_pool.yaml"
    user_path.write_text(
        "pool:\n  connect_timeout: 2.5\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "zsdtdx.unified_client._ensure_availability_hosts_cache",
        lambda **kwargs: {"skipped": True},
    )
    monkeypatch.setattr(
        "zsdtdx.parallel_fetcher.set_active_config_path",
        lambda path: None,
    )

    resolved = _apply_active_config_path(
        str(user_path),
        async_background_probe=False,
    )
    assert Path(resolved).resolve() == user_path.resolve()
    merged = _load_merged_zsdtdx_config(resolved)
    assert merged["pool"]["connect_timeout"] == 2.5
    assert merged["hosts"]["standard"]
