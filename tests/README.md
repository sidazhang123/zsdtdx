# tests

## pytest（自动收集）

根目录下 `test_*.py` 由 pytest 收集，不进入 `manual/`：

```bash
py -m pytest tests/ -q
```

## manual（手工脚本）

冒烟、单点探测、需直接 `py` 运行的离线单测见 [`manual/README.md`](manual/README.md)。
