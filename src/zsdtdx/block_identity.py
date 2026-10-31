"""
模块：`block_identity.py`。

职责：
1. 统一板块的六类标记、中文分类和二元组标识。不包含补充成分。

边界：
1. 不访问网络；分类属于板块身份，不能跨分类按名称合并。
"""

from typing import Any, Tuple

BLOCK_KIND_NAMES = {
    "GN": "概念",
    "FG": "风格",
    "ZS": "指数成分",
    "HY": "通达信基础行业",
    "RS": "研究行业中类",
    "RL": "研究行业小类",
}
BLOCK_NAME_KINDS = {name: kind for kind, name in BLOCK_KIND_NAMES.items()}
BlockIdentity = Tuple[str, str]


def normalize_block_identity(value: Any) -> BlockIdentity:
    """输入二元 tuple/list，输出（中文分类名称，板块名称）；拒绝裸名称及未知分类。"""
    if not isinstance(value, (tuple, list)) or len(value) != 2:
        raise ValueError("block_name 必须是（中文分类名称，板块名称）二元组")
    category, name = value
    if not isinstance(category, str) or not isinstance(name, str):
        raise ValueError("block_name 的分类和名称必须是字符串")
    category, name = category.strip(), name.strip()
    if category not in BLOCK_NAME_KINDS:
        raise ValueError(f"未知板块分类: {category}")
    if not name:
        raise ValueError("板块名称不能为空")
    return category, name


def block_identity(kind: str, name: str) -> BlockIdentity:
    """输入内部标记与名称，输出统一板块身份；内部未知标记同样拒绝。"""
    return normalize_block_identity((BLOCK_KIND_NAMES[kind], name))
