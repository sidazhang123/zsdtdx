"""串行运行远端冷启动、全量与合并文件单站复验；只写当前隔离诊断目录。"""

import json
import subprocess
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[2]
    trace = root / "tests/manual/trace_company_info_cold.py"
    scenarios = [
        ("final_cold400", "sample400.json", ["--rounds", "2"]),
        ("final_full5579", "universe.json", []),
        ("final_txt_single5579", "universe.json", ["--host", "27.151.2.90:7709"]),
    ]
    status = []
    for name, universe, extra in scenarios:
        command = [
            sys.executable,
            str(trace),
            "--universe",
            str(root / universe),
            "--limit",
            "0",
            "--output",
            str(root / name),
            *extra,
        ]
        with (root / f"{name}.log").open("w", encoding="utf-8") as stream:
            result = subprocess.run(
                command, stdout=stream, stderr=subprocess.STDOUT, cwd=root
            )
        status.append({"scenario": name, "exit": result.returncode})
        (root / "final_suite_status.json").write_text(
            json.dumps(status, indent=2), encoding="utf-8"
        )
        if result.returncode:
            raise RuntimeError(f"验收失败: {name}")


if __name__ == "__main__":
    main()
