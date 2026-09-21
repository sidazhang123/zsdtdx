# zsdtdx — Agent 工程指南

本文件面向需要阅读和修改本项目的 AI coding agent。项目全部源码、注释与文档以**中文**为主，新增注释/文档请保持中文。

## 1. 项目概述

`zsdtdx` 是一个面向 A 股/期货行情场景的 Python 封装库，参考 pytdx 生态提供统一 API、连接池、重试和并行抓取能力。部分请求组包与回包解析已按实盘抓包重新实现，并非对 `pytdx` 的直接二次封装或运行时依赖；归属说明见 `THIRD_PARTY_NOTICES.md`。

- **名称**：`zsdtdx`
- **版本**：`2.0.3`（同时定义在 `pyproject.toml` 与 `src/zsdtdx/__init__.py`）
- **许可证**：MIT（见 `LICENSE`）
- **Python 要求**：`>=3.10`
- **核心依赖**：`numpy`、`pandas`、`PyYAML`、`six`、`psutil`

主要对外能力：

- 标准/扩展行情连接管理（`get_client`、`set_config_path`）
- 市场、股票代码表、期货列表查询
- 股票 K 线/指数 K 线（同步 + 异步并行）
- 商品期货 K 线
- 实时最新价（股票/期货）
- 公司信息
- 运行时失败/元数据 introspection

## 2. 目录结构

```
.
├── pyproject.toml              # 项目配置、pytest 入口、setuptools 打包
├── MANIFEST.in                 # 打包时额外包含的静态文件
├── README.md                   # 完整 API 文档与默认 config.yaml 示例
├── CHANGELOG.md                # 版本变更记录
├── LICENSE / THIRD_PARTY_NOTICES.md
├── src/zsdtdx/                 # 主包
│   ├── __init__.py             # 对外导出入口
│   ├── simple_api.py           # 最外层 get_* 风格 API 与任务模型
│   ├── unified_client.py       # 统一高层客户端、配置加载、TCP 探测、连接池
│   ├── parallel_fetcher.py     # 异步并行抓取、进程池、chunk 调度、缓存
│   ├── base_socket_client.py   # 底层 TCP socket 客户端、流量统计
│   ├── hq.py                   # 标准行情协议 API（TdxHq_API）
│   ├── exhq.py                 # 扩展行情协议 API（TdxExHq_API）
│   ├── helper.py               # 二进制解码、K 线数值格式化、通用工具
│   ├── params.py               # 市场/K线类型等协议常量
│   ├── errors.py               # 异常类型
│   ├── log.py                  # 包级日志，受 TDX_DEBUG 环境变量控制
│   ├── catalog_disk_cache.py       # 标准/扩展码表日级磁盘缓存
│   ├── config.yaml             # 包内默认配置
│   └── parser/                 # 通达信协议解析器集合
│       ├── base.py
│       ├── get_security_bars.py
│       ├── get_index_bars.py
│       ├── ex_get_instrument_bars.py
│       └── ...（每条协议一个文件）
├── tests/                      # pytest 自动收集
│   ├── test_*.py               # 离线/单元/回归测试
│   └── manual/                 # 手工执行脚本（pytest 不进入）
└── dist/                       # 已构建的 wheel/sdist 产物
```

## 3. 构建与安装

项目使用 `setuptools` 构建：

```bash
# 开发安装（推荐）
pip install -e .

# 构建 wheel/sdist
python -m build
```

打包关键点：

- `pyproject.toml` 中 `tool.setuptools.dynamic.version` 从 `zsdtdx.__version__` 读取版本号。
- `MANIFEST.in` 显式包含 `README.md`、`CHANGELOG.md`、`LICENSE`、`THIRD_PARTY_NOTICES.md` 以及 `src/zsdtdx/*.yaml`。
- 安装后 `config.yaml` 随包分发，作为未调用 `set_config_path()` 时的默认配置。

## 4. 测试命令

### 4.1 pytest（自动收集，离线）

```bash
py -m pytest tests/ -q
```

- 当前共有 110 个用例，全部离线可跑。
- `pyproject.toml` 已配置 `pythonpath = ["src"]`、`testpaths = ["tests"]`、`norecursedirs = ["manual", ...]`。
- 不要修改 `tests/` 下现有用例的语义，除非修复接口变更导致的编译/调用错误。

### 4.2 手工脚本（需网络或长时运行）

`tests/manual/` 下脚本不由 pytest 收集，需直接 `py tests/manual/<脚本名>.py` 运行：

```bash
py tests/manual/smoke_index_kline_sync_async_demo.py
py tests/manual/weaknet_inject_retry.py            # 弱网注入，离线
py tests/manual/run_stock_kline_async_full_compare.py
```

注意：

- 多数手工脚本需要访问真实行情服务器，运行前确认网络可达。
- 全量基准脚本会创建 `tests/manual/artifacts/` 产物，验收条件见 `tests/manual/README.md`。
- 手工脚本产生的日志/JSON 产物已被 `.gitignore` 排除。

## 5. 运行与调用约定

### 5.1 配置

启动阶段建议调用：

```python
from zsdtdx import set_config_path
set_config_path(r"D:\configs\zsdtdx.yaml")
```

- 不调用时自动使用包内 `src/zsdtdx/config.yaml`。
- `set_config_path(..., async_background_probe=True)` 默认后台探测 TCP host，不阻塞启动。
- 用户 YAML 可为不完整：以包内默认为底深合并覆盖同名键，丢弃内置不存在的字段；列表（如 `hosts.standard`）整段替换。
- 配置文件格式与完整示例见 `README.md`。

### 5.2 主进程连接上下文

需要主进程连接的 API 应包裹在 `with get_client():` 中：

```python
from zsdtdx import get_client, get_supported_markets, get_stock_latest_price

with get_client():
    markets = get_supported_markets(return_df=True)
    prices = get_stock_latest_price(["600000", "000001"])
```

属于主进程上下文的 API：

- `get_supported_markets`
- `get_stock_code_name`
- `get_etf_code_name`
- `get_all_future_list`
- `get_future_kline`
- `get_company_info`
- `get_stock_latest_price`
- `get_future_latest_price`
- `get_runtime_failures`
- `get_runtime_metadata`

### 5.3 异步并行 K 线

`get_stock_kline` / `get_index_kline` 的 `mode="async"` 使用独立 worker 进程池，worker 内部自建连接，**不需要** `with get_client()`。进程池生命周期可通过以下函数管理：

- `prewarm_parallel_fetcher()`：预热 worker 与连接。
- `restart_parallel_fetcher(...)`：强制重启进程池。
- `destroy_parallel_fetcher()`：释放资源（建议脚本退出前调用）。

K 线数据契约：

- 仅传日期时，股票/指数任务补齐为 `09:30:00` / `16:00:00`。
- `rows` 中 `datetime` 固定为 `YYYY-MM-DD HH:MM:SS`，秒位 `:00`。

## 6. 代码风格与开发规范

### 6.1 语言

- 源码注释、docstring、提交信息、README/CHANGELOG 均以**中文**为主。
- 代码标识符（类名/函数名/变量名）保持项目现有风格（snake_case）。

### 6.2 格式化

- 使用 `ruff format` 对 `src/` 进行格式化（CHANGELOG v1.4.9 起已统一）。
- 当前没有显式的 `ruff.toml` 或 `[tool.ruff]` 配置，使用 ruff 默认规则。
- 提交前建议执行：

  ```bash
  ruff format src
  ruff check src --fix
  ```

### 6.3 模块注释风格

每个模块顶部通常包含：

```text
"""
模块：`xxx.py`。

职责：
1. ...
2. ...

边界：
1. ...
2. ...
"""
```

新增模块或重大修改时，请保持该结构。

### 6.4 依赖管理

- 新增依赖前先在 `pyproject.toml` 的 `[project] dependencies` 中声明。
- 避免引入不必要的重量级第三方库；项目仅依赖 `numpy/pandas/PyYAML/six/psutil`。

## 7. 关键架构说明

### 7.1 连接与重试

- `BaseSocketClient` 管理底层 TCP 连接、流量统计。
- `TdxHq_API` / `TdxExHq_API` 分别封装标准/扩展行情协议。
- `UnifiedTdxClient` 在之上提供持久化连接池、自动 host 切换、TCP 延迟探测。
- 重试策略已收敛为固定三步：同连接再试 → 同 host 重连 → rotate 到新 host 重复三步。

### 7.2 TCP 可用地址探测

- 由 `_ensure_availability_hosts_cache` 统一写入进程内缓存。
- 主进程探测一次后，通过 snapshot 传递给 spawn 启动的 worker，避免每个 worker 重复探测。
- 探测结果裁剪逻辑：剔除不可达 → 按延迟升序 → 仅当**配置侧**地址数 > 3 且可达数 ≥ 2 时去掉最慢 1 个；配置侧 ≤ 3 的短池保留全部可达站（阈值写死为 3）。

### 7.3 并行抓取

- `parallel_fetcher.py` 维护全局进程池与任务调度。
- 任务按 `(code, freq)` 或 `(index_name, freq)` 分 chunk。
- worker 内使用 asyncio + `to_thread` 做 chunk 级并发。
- chunk 级超时、重试、连接自愈（`chunk_reconnect_on_unavailable`）由配置驱动。
- 指数路由在使用时从当日 std/ex 码表过滤得到，进程内保留名称映射。

## 8. 安全与部署注意事项

- 配置文件 `config.yaml` 可能包含真实行情服务器地址，不要提交含敏感内网地址的自定义配置。
- 码表磁盘缓存使用 pickle，std / ex 分文件按自然日保存未过滤原文，加载时会校验格式版本、kind 与记录结构。
- 包内默认 `config.yaml` 中的 host 为公网行情节点，通常可直接使用。
- 发布新版本时同步更新三处版本号：
  1. `pyproject.toml` 中的 `version`
  2. `src/zsdtdx/__init__.py` 中的 `__version__`
  3. `CHANGELOG.md`

## 9. 常用修改入口

| 修改目标 | 推荐入口文件 |
|---|---|
| 新增/调整对外 API | `src/zsdtdx/simple_api.py` |
| 连接池/重试/TCP 探测 | `src/zsdtdx/unified_client.py` |
| 异步并行调度 | `src/zsdtdx/parallel_fetcher.py` |
| 协议解析字段/数值刻度 | `src/zsdtdx/helper.py` + `src/zsdtdx/parser/*.py` |
| 码表磁盘缓存 | `src/zsdtdx/catalog_disk_cache.py` |
| 默认配置项 | `src/zsdtdx/config.yaml` + `README.md` 中的示例 |
| 版本号 | `pyproject.toml`、`src/zsdtdx/__init__.py`、`CHANGELOG.md` |
