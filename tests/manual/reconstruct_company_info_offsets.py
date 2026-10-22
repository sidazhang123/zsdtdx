"""用原始合并文件验证历史错位头部的绝对位置及完整哈希；不改生产文件。"""

import argparse
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))
from zsdtdx.net.hq import TdxHq_API  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bad-rows", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    bad_rows = json.loads(Path(args.bad_rows).read_text(encoding="utf-8"))
    results = []
    for host in ("27.151.2.90", "123.125.108.101"):
        api = TdxHq_API(raise_exception=True)
        try:
            api.connect(host, 7709, time_out=5)
            for code in ("601319", "601718"):
                directory = api.get_company_info_category(1, code)
                filename = directory[0]["filename"]
                if not filename.endswith(".txt"):
                    raise RuntimeError(f"不是合并文件: {host} {directory}")
                total = max(row["start"] + row["length"] for row in directory)
                pages = []
                for start in range(0, total, 30720):
                    part = api.get_company_info_content(
                        1, code, filename, start, total - start, 0
                    )
                    if len(part) != min(30720, total - start):
                        raise RuntimeError(
                            f"完整文件短页: {host} {code} {start} {len(part)}"
                        )
                    pages.append(part)
                raw = b"".join(pages)
                label = f"{host}_{code}"
                (output / f"{label}.bin").write_bytes(raw)
                (output / f"{label}_directory.json").write_text(
                    json.dumps(directory, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                for row in bad_rows:
                    if row["code"] != code:
                        continue
                    needle = row["head"].encode("gbk", "ignore")
                    position = raw.find(needle)
                    category = next(
                        item for item in directory if item["name"] == row["category"]
                    )
                    # 表格头可能重复；必须检索每处起点并以完整 SHA 选中真实片段。
                    occurrences = []
                    cursor = position
                    while cursor >= 0:
                        occurrences.append(cursor)
                        fragment = raw[cursor : cursor + category["length"]].decode(
                            "gbk", "ignore"
                        )
                        if (
                            hashlib.sha256(fragment.encode()).hexdigest()
                            == row["sha256"]
                        ):
                            position = cursor
                            break
                        cursor = raw.find(needle, cursor + 1)
                    candidate = (
                        raw[position : position + category["length"]]
                        if position >= 0
                        else b""
                    )
                    text = candidate.decode("gbk", "ignore")
                    result = dict(
                        host=host,
                        code=code,
                        category=row["category"],
                        position=position,
                        correct_start=category["start"],
                        declared_length=category["length"],
                        characters=len(text),
                        expected_characters=row["characters"],
                        sha256=hashlib.sha256(text.encode()).hexdigest(),
                        expected_sha256=row["sha256"],
                        full_hash_equal=hashlib.sha256(text.encode()).hexdigest()
                        == row["sha256"],
                        searched_positions=occurrences,
                    )
                    results.append(result)
                    print(json.dumps(result, ensure_ascii=False), flush=True)
                (output / "report.json").write_text(
                    json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8"
                )
        finally:
            api.disconnect()


if __name__ == "__main__":
    main()
