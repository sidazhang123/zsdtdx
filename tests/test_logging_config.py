"""YAML 日志配置回归测试。"""

import logging

import pytest

from zsdtdx.util import log as log_module


def test_logging_levels_are_driven_by_config():
    """唯一全局阈值过滤代码中固定的 debug/info/error 级别。"""
    try:
        log_module.configure_logging({"logging": {"level": "ERROR"}})
        assert log_module.log.level == logging.ERROR
        assert log_module.resolve_event_level("debug") == logging.DEBUG
        assert log_module.resolve_event_level("info") == logging.INFO
        assert log_module.resolve_event_level("error") == logging.ERROR
        assert log_module.is_event_enabled("error")
        assert not log_module.is_event_enabled("debug")
        assert not log_module.is_event_enabled("info")
    finally:
        log_module.configure_logging({})


def test_logging_level_only_accepts_supported_values():
    """配置仅接受 DEBUG/INFO/ERROR/OFF，不允许按事件重新定义级别。"""
    try:
        with pytest.raises(ValueError, match="仅支持"):
            log_module.configure_logging({"logging": {"level": "WARNING"}})
        log_module.configure_logging({"logging": {"level": "OFF"}})
        assert not log_module.is_event_enabled("error")
    finally:
        log_module.configure_logging({})


def test_event_level_cannot_be_overridden_by_legacy_fields():
    """旧事件级别字段即使传入，也不能改变代码写死的日志级别。"""
    try:
        log_module.configure_logging(
            {
                "logging": {
                    "level": "INFO",
                    "error_level": "OFF",
                    "parallel_chunk_level": "ERROR",
                }
            }
        )
        assert log_module.resolve_event_level("debug") == logging.DEBUG
        assert log_module.resolve_event_level("error") == logging.ERROR
        assert log_module.is_event_enabled("error")
    finally:
        log_module.configure_logging({})


def test_reconfigure_does_not_duplicate_stream_handler():
    """多次 set_config_path/配置加载只更新同一个包级 handler。"""
    before = len(log_module.log.handlers)
    log_module.configure_logging({"logging": {"level": "INFO"}})
    log_module.configure_logging({"logging": {"level": "ERROR"}})
    assert len(log_module.log.handlers) == before
    log_module.configure_logging({})
