"""码表磁盘缓存（标准/扩展码表与 ETF/LOF 名称板块）。

用途：
1. 按自然日将 std / ex 未过滤码表、以及 ETF/LOF 名称与板块成分持久化到用户可写目录。
2. 通过原子替换写入与加载后校验，降低文件损坏导致崩溃的概率。

边界：
1. 不负责网络下载与业务过滤，仅做序列化/反序列化与路径解析。
2. std、ex、etf 分文件存储，可独立命中或过期。
3. 多进程并发写可能产生竞态；当前以最后一次写入为准，不引入额外依赖锁。
"""

from __future__ import annotations

import os
import pickle
import re
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# 与 0x044D 16 字节 GBK 名称记录对应；版本不符的磁盘缓存直接丢弃。
# v3：ETF 名称合并 zhb.zip/ilong.dat（同码覆盖 infoharbor）。
_CATALOG_CACHE_FORMAT_VERSION = 3
KIND_STD = "std"
KIND_EX = "ex"
KIND_ETF = "etf"
_VALID_KINDS = {KIND_STD, KIND_EX}
_STD_FILENAME = "std_security_list.pkl"
_EX_FILENAME = "ex_instrument_info.pkl"
_ETF_FILENAME = "etf_code_name.pkl"


def default_zsdtdx_user_cache_dir() -> Path:
    """返回 zsdtdx 默认用户缓存根目录（pip 安装后仍可写）。

    输入:
    1. 无显式输入参数。
    输出:
    1. 绝对路径 Path。
    用途:
    1. Windows 使用 LOCALAPPDATA；非 Windows 使用 XDG_CACHE_HOME 或 ~/.cache。
    边界条件:
    1. 目录可能尚不存在，调用方负责 mkdir。
    """
    if os.name == "nt":
        local = os.environ.get("LOCALAPPDATA", "").strip()
        if local:
            return Path(local) / "zsdtdx" / "cache"
        return Path.home() / "AppData" / "Local" / "zsdtdx" / "cache"
    xdg = os.environ.get("XDG_CACHE_HOME", "").strip()
    base = Path(xdg) if xdg else Path.home() / ".cache"
    return base / "zsdtdx"


def _is_valid_cache_date(cache_date: Any) -> bool:
    """校验缓存日期戳格式是否为 YYYY-MM-DD。"""
    return isinstance(cache_date, str) and bool(
        re.match(r"^\d{4}-\d{2}-\d{2}$", cache_date)
    )


def _ensure_directory_writable(target_dir: Path) -> bool:
    """探测目录是否可写；目录不存在时会尝试创建。"""
    try:
        target_dir.mkdir(parents=True, exist_ok=True)
    except Exception:
        return False
    fd: Optional[int] = None
    probe_path: Optional[Path] = None
    try:
        fd, probe_name = tempfile.mkstemp(
            dir=str(target_dir), prefix=".zsdtdx_probe_", suffix=".tmp"
        )
        probe_path = Path(probe_name)
        os.close(fd)
        fd = None
        probe_path.unlink(missing_ok=True)
        return True
    except Exception:
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                pass
        if probe_path is not None:
            try:
                probe_path.unlink(missing_ok=True)
            except OSError:
                pass
        return False


def resolve_catalog_cache_dir(config_path: Any = None) -> Optional[Path]:
    """解析码表缓存目录并保证可写。

    输入:
    1. config_path: 用户配置的文件或目录路径；为空时使用默认路径。
    输出:
    1. 可写缓存目录绝对路径；若无可写目录则返回 None。
    用途:
    1. 兼容 Windows/Linux 与 pip 安装只读目录场景。
    边界条件:
    1. 传入文件路径时使用其父目录；不可写时回退系统临时目录。
    """
    candidates: List[Path] = []
    raw = str(config_path or "").strip()
    if raw != "":
        user_path = Path(raw).expanduser()
        if user_path.suffix:
            candidates.append(user_path.parent.resolve())
        else:
            candidates.append(user_path.resolve())
    candidates.append(default_zsdtdx_user_cache_dir().resolve())
    candidates.append((Path(tempfile.gettempdir()) / "zsdtdx" / "cache").resolve())
    for path in candidates:
        if _ensure_directory_writable(path):
            return path
    return None


def catalog_cache_file_path(cache_dir: Path, kind: str) -> Path:
    """输入缓存目录与 kind（std/ex/etf），输出对应 pickle 文件路径。"""
    kind_key = str(kind or "").strip().lower()
    if kind_key == KIND_STD:
        return Path(cache_dir) / _STD_FILENAME
    if kind_key == KIND_EX:
        return Path(cache_dir) / _EX_FILENAME
    if kind_key == KIND_ETF:
        return Path(cache_dir) / _ETF_FILENAME
    raise ValueError(f"未知码表缓存 kind: {kind}")


def _atomic_write_bytes(path: Path, data: bytes, tmp_prefix: str) -> None:
    """
    输入目标路径与字节，输出无。

    输入：
    1. path: 最终 pickle 路径。
    2. data: 已序列化内容。
    3. tmp_prefix: 临时文件前缀。
    输出：无。
    用途：先写临时文件再 os.replace。
    边界：失败时尽量删除临时文件后原样抛出。
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd: Optional[int] = None
    tmp_path: Optional[Path] = None
    try:
        fd, tmp_name = tempfile.mkstemp(
            dir=str(path.parent),
            prefix=tmp_prefix,
            suffix=".tmp",
        )
        tmp_path = Path(tmp_name)
        with os.fdopen(fd, "wb") as tmp_fp:
            fd = None
            tmp_fp.write(data)
            tmp_fp.flush()
            os.fsync(tmp_fp.fileno())
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


def _read_pickle_obj(path: Path) -> Optional[Any]:
    """
    输入路径，输出反序列化对象或 None。

    输入：pickle 文件路径。
    输出：对象；损坏或过短时删文件并返回 None。
    用途：std/ex/etf 缓存共用读取。
    边界：文件不存在返回 None。
    """
    path = Path(path)
    if not path.is_file():
        return None
    try:
        with open(path, "rb") as fp:
            raw = fp.read()
        if len(raw) < 8:
            raise ValueError("too_short")
        return pickle.loads(raw)
    except Exception:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass
        return None


def _is_valid_catalog_record(obj: Any) -> bool:
    """校验单条码表记录：必须含 code 与非负 market。"""
    if not isinstance(obj, dict):
        return False
    code = str(obj.get("code", "")).strip()
    if code == "":
        return False
    try:
        market = int(obj.get("market", -1))
    except Exception:
        return False
    if market < 0:
        return False
    if "name" in obj and obj["name"] is not None and not isinstance(obj["name"], str):
        return False
    return True


def validate_catalog_payload(
    obj: Any, expected_kind: str
) -> Optional[Tuple[str, List[Dict[str, Any]], Dict[int, str]]]:
    """校验反序列化后的码表缓存根对象。

    输入:
    1. obj: pickle 加载结果。
    2. expected_kind: std 或 ex。
    输出:
    1. (cache_date, records, market_names) 或 None。
    用途:
    1. 避免损坏 pickle 污染内存。
    边界条件:
    1. 结构、kind、日期或 format_version 不符时返回 None。
    """
    if not isinstance(obj, dict):
        return None
    if int(obj.get("format_version", -1)) != _CATALOG_CACHE_FORMAT_VERSION:
        return None
    kind = str(obj.get("kind", "")).strip().lower()
    if kind != str(expected_kind or "").strip().lower() or kind not in _VALID_KINDS:
        return None
    cache_date = str(obj.get("cache_date", "")).strip()
    if not _is_valid_cache_date(cache_date):
        return None
    records = obj.get("records")
    if not isinstance(records, list) or not records:
        return None
    clean_records: List[Dict[str, Any]] = []
    for item in records:
        if not _is_valid_catalog_record(item):
            return None
        clean_records.append(dict(item))
    raw_names = obj.get("market_names") or {}
    if not isinstance(raw_names, dict):
        return None
    market_names: Dict[int, str] = {}
    for key, val in raw_names.items():
        try:
            market_names[int(key)] = "" if val is None else str(val)
        except Exception:
            return None
    return cache_date, clean_records, market_names


def load_catalog_cache(
    path: Path,
    expected_kind: str,
    expected_cache_date: Optional[str] = None,
) -> Optional[Tuple[str, List[Dict[str, Any]], Dict[int, str]]]:
    """从磁盘加载一侧码表缓存。

    输入:
    1. path: 缓存文件路径。
    2. expected_kind: std 或 ex。
    3. expected_cache_date: 期望自然日；不一致则视为未命中。
    输出:
    1. (cache_date, records, market_names) 或 None。
    用途:
    1. 同日复用已下载的未过滤码表。
    边界条件:
    1. 文件不存在返回 None；校验失败会删除该文件（若可删）。
    """
    path = Path(path)
    obj = _read_pickle_obj(path)
    if obj is None:
        return None

    validated = validate_catalog_payload(obj, expected_kind=expected_kind)
    if validated is None:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass
        return None
    cache_date, _, _ = validated
    expected_date = str(expected_cache_date or "").strip()
    if expected_date != "" and cache_date != expected_date:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass
        return None
    return validated


def save_catalog_cache(
    path: Path,
    *,
    kind: str,
    cache_date: str,
    records: List[Dict[str, Any]],
    market_names: Optional[Dict[int, str]] = None,
) -> None:
    """将一侧码表缓存原子写入磁盘。

    输入:
    1. path: 目标文件路径。
    2. kind: std 或 ex。
    3. cache_date: YYYY-MM-DD。
    4. records: 未过滤码表记录。
    5. market_names: 扩展市场号到中文名；std 可为空。
    输出:
    1. 无；失败抛 OSError/IOError 等。
    用途:
    1. 当天首次下载后供同日后续进程复用。
    边界条件:
    1. 先写临时文件再 os.replace；kind/日期非法或记录为空时抛错。
    """
    kind_key = str(kind or "").strip().lower()
    if kind_key not in _VALID_KINDS:
        raise ValueError(f"未知码表缓存 kind: {kind}")
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    normalized_date = str(cache_date or "").strip()
    if not _is_valid_cache_date(normalized_date):
        raise ValueError(f"cache_date 格式非法: {cache_date}")
    clean_records = [dict(item) for item in records]
    if not clean_records:
        raise ValueError("码表缓存记录不能为空")
    payload = {
        "format_version": _CATALOG_CACHE_FORMAT_VERSION,
        "kind": kind_key,
        "cache_date": normalized_date,
        "records": clean_records,
        "market_names": {
            int(k): ("" if v is None else str(v))
            for k, v in dict(market_names or {}).items()
        },
    }
    data = pickle.dumps(payload, protocol=pickle.HIGHEST_PROTOCOL)
    _atomic_write_bytes(path, data, tmp_prefix=f".zsdtdx_catalog_{kind_key}_")


def _is_valid_etf_name_record(obj: Any) -> bool:
    """输入名称记录，输出是否含 market/code。用途：ETF 名称缓存校验。边界：name 缺省视为空串。"""
    if not _is_valid_catalog_record(obj):
        return False
    name = obj.get("name", "")
    return name is None or isinstance(name, str)


def _is_valid_etf_board_record(obj: Any) -> bool:
    """输入板块记录，输出是否为深/沪 6 位代码。用途：ETF 板块缓存校验。边界：只要 market/code。"""
    if not isinstance(obj, dict):
        return False
    code = str(obj.get("code", "")).strip()
    if len(code) != 6 or not code.isdigit():
        return False
    try:
        market = int(obj.get("market", -1))
    except Exception:
        return False
    return market in (0, 1)


def validate_etf_catalog_payload(
    obj: Any,
) -> Optional[Tuple[str, List[Dict[str, Any]], List[Dict[str, Any]]]]:
    """
    输入 pickle 对象，输出 (日期, 名称记录, 板块记录) 或 None。

    输入：反序列化根对象。
    输出：三元组；结构不符返回 None。
    用途：避免空列表或损坏 ETF 缓存进入内存。
    边界：kind 必须为 etf；名称与板块均须非空且逐条合法。
    """
    if not isinstance(obj, dict):
        return None
    if int(obj.get("format_version", -1)) != _CATALOG_CACHE_FORMAT_VERSION:
        return None
    if str(obj.get("kind", "")).strip().lower() != KIND_ETF:
        return None
    cache_date = str(obj.get("cache_date", "")).strip()
    if not _is_valid_cache_date(cache_date):
        return None
    names = obj.get("name_records")
    boards = obj.get("board_records")
    if not isinstance(names, list) or not names:
        return None
    if not isinstance(boards, list) or not boards:
        return None
    clean_names: List[Dict[str, Any]] = []
    for item in names:
        if not _is_valid_etf_name_record(item):
            return None
        clean_names.append(
            {
                "market": int(item["market"]),
                "code": str(item["code"]).strip(),
                "name": "" if item.get("name") is None else str(item.get("name")),
            }
        )
    clean_boards: List[Dict[str, Any]] = []
    for item in boards:
        if not _is_valid_etf_board_record(item):
            return None
        clean_boards.append(
            {"market": int(item["market"]), "code": str(item["code"]).strip()}
        )
    return cache_date, clean_names, clean_boards


def load_etf_catalog_cache(
    path: Path,
    expected_cache_date: Optional[str] = None,
) -> Optional[Tuple[str, List[Dict[str, Any]], List[Dict[str, Any]]]]:
    """
    输入路径与期望日期，输出 ETF 名称/板块缓存或 None。

    输入：
    1. path: `etf_code_name.pkl`。
    2. expected_cache_date: 自然日；不一致则未命中。
    输出：(cache_date, name_records, board_records) 或 None。
    用途：同日复用已下载的 ETF/LOF 名称与板块。
    边界：空列表、损坏或日期不符会删除文件。
    """
    path = Path(path)
    obj = _read_pickle_obj(path)
    if obj is None:
        return None
    validated = validate_etf_catalog_payload(obj)
    if validated is None:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass
        return None
    cache_date, _, _ = validated
    expected_date = str(expected_cache_date or "").strip()
    if expected_date != "" and cache_date != expected_date:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass
        return None
    return validated


def save_etf_catalog_cache(
    path: Path,
    *,
    cache_date: str,
    name_records: List[Dict[str, Any]],
    board_records: List[Dict[str, Any]],
) -> None:
    """
    输入日期与两份记录，输出无。

    输入：路径、YYYY-MM-DD、名称列表、板块列表。
    输出：无；失败抛错。
    用途：当天首次 HQ 下载后供后续进程复用。
    边界：任一侧为空或校验失败时抛 ValueError，不写盘。
    """
    payload = {
        "format_version": _CATALOG_CACHE_FORMAT_VERSION,
        "kind": KIND_ETF,
        "cache_date": str(cache_date or "").strip(),
        "name_records": [dict(item) for item in name_records],
        "board_records": [dict(item) for item in board_records],
    }
    validated = validate_etf_catalog_payload(payload)
    if validated is None:
        raise ValueError("ETF 缓存记录非法或为空")
    cache_date, clean_names, clean_boards = validated
    data = pickle.dumps(
        {
            "format_version": _CATALOG_CACHE_FORMAT_VERSION,
            "kind": KIND_ETF,
            "cache_date": cache_date,
            "name_records": clean_names,
            "board_records": clean_boards,
        },
        protocol=pickle.HIGHEST_PROTOCOL,
    )
    _atomic_write_bytes(path, data, tmp_prefix=".zsdtdx_catalog_etf_")
