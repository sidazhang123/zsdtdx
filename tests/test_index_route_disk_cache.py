"""index_route_disk_cache 单元测试。

用途：
1. 验证指数路由缓存的保存/加载契约。
2. 验证自然日日期戳失效与路径解析行为。
"""

from __future__ import annotations

import datetime as dt
import tempfile
import unittest
from pathlib import Path

from zsdtdx.index_route_disk_cache import (
    load_index_route_cache,
    resolve_index_route_cache_file_path,
    save_index_route_cache,
)


class IndexRouteDiskCacheTests(unittest.TestCase):
    """指数路由磁盘缓存测试集合。"""

    def _sample_payload(self) -> tuple[dict[str, dict[str, object]], list[dict[str, object]]]:
        """
        构造最小可用缓存样本。

        输入：
        1. 无显式输入参数。
        输出：
        1. (name_route, catalog) 示例数据。
        用途：
        1. 复用测试样本，降低重复样板代码。
        边界条件：
        1. 样本保持字段完整，满足缓存校验逻辑。
        """
        route = {
            "上证指数": {
                "name": "上证指数",
                "source": "std",
                "market": 1,
                "code": "000001",
                "market_name": "上海",
            }
        }
        catalog = [dict(route["上证指数"])]
        return route, catalog

    def test_save_then_load_same_day_success(self) -> None:
        """同日缓存保存后可正确读取。"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            cache_file = Path(tmp_dir) / "index_route_cache.pkl"
            route, catalog = self._sample_payload()
            cache_date = dt.date.today().isoformat()
            save_index_route_cache(
                cache_file,
                fingerprint="fp-demo",
                name_route=route,
                catalog=catalog,
                cache_date=cache_date,
            )
            loaded = load_index_route_cache(
                cache_file,
                expected_fingerprint="fp-demo",
                expected_cache_date=cache_date,
            )
            self.assertIsNotNone(loaded)
            loaded_route, loaded_catalog, loaded_date = loaded  # type: ignore[misc]
            self.assertEqual(loaded_date, cache_date)
            self.assertIn("上证指数", loaded_route)
            self.assertEqual(len(loaded_catalog), 1)

    def test_load_cross_day_returns_none(self) -> None:
        """跨日读取会视为失效并返回 None。"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            cache_file = Path(tmp_dir) / "index_route_cache.pkl"
            route, catalog = self._sample_payload()
            save_index_route_cache(
                cache_file,
                fingerprint="fp-demo",
                name_route=route,
                catalog=catalog,
                cache_date="2026-04-20",
            )
            loaded = load_index_route_cache(
                cache_file,
                expected_fingerprint="fp-demo",
                expected_cache_date="2026-04-21",
            )
            self.assertIsNone(loaded)

    def test_resolve_cache_path_from_directory(self) -> None:
        """配置目录时自动拼接默认缓存文件名。"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            resolved = resolve_index_route_cache_file_path(config_path=tmp_dir)
            self.assertIsNotNone(resolved)
            self.assertEqual(str(resolved.name), "index_route_cache.pkl")

    def test_resolve_cache_path_from_file(self) -> None:
        """配置文件路径时按原路径返回。"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            target_file = Path(tmp_dir) / "custom_route_cache.pkl"
            resolved = resolve_index_route_cache_file_path(config_path=str(target_file))
            self.assertIsNotNone(resolved)
            self.assertEqual(resolved, target_file.resolve())

    def test_load_fingerprint_mismatch_returns_none(self) -> None:
        """配置指纹不匹配时返回 None。"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            cache_file = Path(tmp_dir) / "index_route_cache.pkl"
            route, catalog = self._sample_payload()
            cache_date = dt.date.today().isoformat()
            save_index_route_cache(
                cache_file,
                fingerprint="fp-a",
                name_route=route,
                catalog=catalog,
                cache_date=cache_date,
            )
            loaded = load_index_route_cache(
                cache_file,
                expected_fingerprint="fp-b",
                expected_cache_date=cache_date,
            )
            self.assertIsNone(loaded)

    def test_save_invalid_date_raises(self) -> None:
        """非法 cache_date 会抛 ValueError。"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            cache_file = Path(tmp_dir) / "index_route_cache.pkl"
            route, catalog = self._sample_payload()
            with self.assertRaises(ValueError):
                save_index_route_cache(
                    cache_file,
                    fingerprint="fp-demo",
                    name_route=route,
                    catalog=catalog,
                    cache_date="2026/04/20",
                )


if __name__ == "__main__":
    unittest.main()
