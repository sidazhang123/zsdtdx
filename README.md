# zsdtdx

`zsdtdx` 是面向 A 股/期货行情场景的 Python 封装库，参考 pytdx 生态提供统一 API、连接池、重试和并行抓取能力。部分请求组包与回包解析已按实盘抓包重新实现，**并非**对 `pytdx` 的直接二次封装或运行时依赖。

## 安装

```bash
pip install zsdtdx
```

## API 概览

用户日常只需关注两层对外入口（均可从 `zsdtdx` 直接导入）：

1. **`simple_api`**：`get_*` / `set_config_path` / `get_client` / 进程池生命周期等调用入口。
2. **`kline_task`**：构造股票/指数/板块 K 线任务列表的类型入口
   （`StockKlineTask` / `IndexKlineTask` / `BlockKlineTask`）。也可用等价 `dict`，任务类字段校验更明确。

函数清单：

- `set_config_path`
- `get_client`
- `get_stock_code_name`
- `get_stock_concepts`
- `get_etf_code_name`
- `get_all_future_list`
- `StockKlineTask` / `IndexKlineTask` / `BlockKlineTask`（`zsdtdx.kline_task`）
- `get_stock_kline`
- `get_index_kline`
- `get_block_names`
- `get_block_kline`
- `prewarm_parallel_fetcher`
- `restart_parallel_fetcher`
- `destroy_parallel_fetcher`
- `get_future_kline`
- `get_company_info`
- `get_stock_stat`
- `get_future_latest_price`
- `get_runtime_failures`
- `get_runtime_metadata`

## 运行环境与依赖

- Python: `>=3.10`
- 依赖：`numpy`、`pandas`、`PyYAML`、`six`、`psutil`



## 许可证与来源

- 许可证：`MIT`（见 `LICENSE`）
- 第三方归属：见 `THIRD_PARTY_NOTICES.md`（声明上游参考 `pytdx`；同时说明部分协议请求/解析已重新实现，非直接二次封装）



## 快速开始



#### 对外入口说明

- **调用入口**：`zsdtdx.simple_api`（或 `from zsdtdx import get_stock_kline, ...`）。
- **K 线任务构造入口**：`zsdtdx.kline_task`（或 `from zsdtdx import StockKlineTask, IndexKlineTask, BlockKlineTask`）。
- `get_stock_kline` / `get_index_kline` / `get_block_kline` 的 `task` 参数接受任务类实例或等价 `dict`；
  推荐任务类，字段与校验更清晰。期货 `get_future_kline` 仍用 `codes/freq/start_time/end_time`，无任务类。



#### StockKlineTask / IndexKlineTask / BlockKlineTask

定义于 `zsdtdx.kline_task`，是用户构造 K 线任务列表的类型入口；也可从 `zsdtdx` 直接导入。

**字段:**

- `StockKlineTask`: `code`, `freq`, `start_time`, `end_time`
- `IndexKlineTask`: `index_name`, `freq`, `start_time`, `end_time`
- `BlockKlineTask`: `block_name`, `freq`, `start_time`, `end_time`（`block_name` 取自 `get_block_names`）

**时间窗口:**

- 仅传日期时补齐为 `09:30:00` / `16:00:00`（与期货的 `09:00` / `15:00` 不同）。

**调用示例:**

```python
from zsdtdx import BlockKlineTask, IndexKlineTask, StockKlineTask

stock_tasks = [
    StockKlineTask(code="600000", freq="d", start_time="2026-02-13", end_time="2026-02-13"),
    {"code": "000001", "freq": "60", "start_time": "2026-02-13", "end_time": "2026-02-14"},  # dict 亦可
]
index_tasks = [
    IndexKlineTask(index_name="中证1000", freq="d", start_time="2026-03-01", end_time="2026-03-31"),
]
block_tasks = [
    BlockKlineTask(block_name="芯片", freq="d", start_time="2026-03-01", end_time="2026-03-31"),
]
```



#### set_config_path

设置全局配置路径（主进程与并行 worker 统一生效），如不调用则后续函数使用包内默认配置(见文档最后的示例)。

用户侧 YAML **允许不完整**：运行时以包内默认 `config.yaml` 为底，对用户文件中与内置**同名的键**做深合并覆盖；内置不存在的键会被丢弃；列表字段（如 `hosts.standard`）整段替换，不做元素并集。

TCP 可用地址探测在后台或首次建连前由 `_ensure_availability_hosts_cache` 统一写入进程内缓存；默认 `async_background_probe=True` 时函数立即返回，不阻塞启动。

**输入:**

- `config_path`: 配置文件路径（可只写需要覆盖的字段）。
- `async_background_probe`: 缓存不可用时是否在后台线程预热（默认 True）；设为 False 则同步探测后再返回。

**调用示例:**

```python
from zsdtdx import set_config_path

set_config_path(r"D:\\configs\\zsdtdx.yaml")
# 或启动阶段同步等待探测完成：
# set_config_path(r"D:\\configs\\zsdtdx.yaml", async_background_probe=False)
call other functions...
```

**不完整配置示例:**

```yaml
# 仅覆盖连接超时；hosts / parallel 等其余项沿用包内默认
pool:
  connect_timeout: 3.0
```



#### get_client

获取客户端实例。

**输入:**

- separate_instance: 是否强制新建独立客户端实例；为 True 时忽略当前 with 上下文并创建新实例。默认False。

**调用示例:**

```python
from zsdtdx import get_client

# 一个 with 中复用同一 client，避免重复建连。
with get_client():
    stock_map = get_stock_code_name()
    board = get_stock_stat()
```

**作用边界:**

- 该 client 仅管理"当前主进程"上下文中的连接生命周期（进入 with 预连接，退出 with 自动 close）。
- `get_stock_kline` / `get_index_kline` / `get_block_kline` 的 sync/async 抓取连接由并行层进程内常驻 client 管理，
  不与此处 with 返回的主进程 client 共用；with 结束也不会关掉它们。
- 市场列表不再有独立 `get_*` 封装；需要时在 with 内调用客户端方法：
  `with get_client() as client: client.get_supported_markets(return_df=True)`。



#### get_stock_code_name

获取统一股票代码名称字典。

**调用前置约定:**

- 可直接调用；连续调用多个主进程 API 时建议进入 `with get_client():` 复用连接；

**输入:**

- 无缓存开关；自动复用有效缓存，缺失、损坏或过期时重建。
- 本函数属于全量代码接口，返回范围由配置 `stock_scope.defaults_when_codes_none.get_stock_code_name` 控制（包内默认 `szsh+bj`；可增配 `hk` 港股通）。

**输出:**

- 返回 `Dict[str, str]`：key 为带市场前缀的股票代码（`sh./sz./bj./hk.`），
value 为股票名称；`hk.` 为港股通标的；不返回纯数字代码。

**调用示例:**

```python
from zsdtdx import get_client, get_stock_code_name

with get_client():
    stock_map = get_stock_code_name()
```

**返回示例:**

```json
{"sh.600000": "浦发银行", "sz.000001": "平安银行"}
```



#### get_stock_concepts

获取股票所属板块（成分归属）。概念、风格、指数来自 `infoharbor_block.dat`；通达信基础行业来自 `tdxhy.cfg` + `zhb.zip`/`tdxzs.cfg`。股票名称用 `get_stock_code_name` 当日码表。不写盘。

与 `get_block_names` 文件同源，但语义不同：本接口回答「股票属于哪些板块」；可请求 K 线的板块指数名请用 `get_block_names`。

**调用前置约定:**

- 可直接调用；连续调用多个主进程 API 时建议进入 `with get_client():` 复用连接；

**输出:**

- `names`: 有成分代码的板块名称（概念、风格、指数、基础行业），去重后按字排序。
- `map`: 股票名称到所属板块名称列表；列表去重且按字排序。码表中没有名称的代码不进入 `map`。

**调用示例:**

```python
from zsdtdx import get_client, get_stock_concepts

with get_client():
    concepts = get_stock_concepts()
```

**返回示例:**

```json
{"names": ["5G概念", "芯片"], "map": {"中信特钢": ["5G概念"]}}
```



#### get_etf_code_name

获取场内 ETF/LOF（本语境统称 etf）代码名称字典。成分来自 `spec/specetfdata.txt` / `spec/speclofdata.txt`；名称优先 `infoharbor_ex.name` 与 `zhb.zip`/`ilong.dat` 合并（同码以 ilong 为准），缺名回退 std 码表版面短名。当日快照写入 `etf_code_name.pkl`，无当日文件才下载。不改动 `get_stock_code_name` 口径，不读银河安装目录。

**调用前置约定:**

- 可直接调用；连续调用多个主进程 API 时建议进入 `with get_client():` 复用连接；

**输入:**

- 无缓存开关；自动复用当日磁盘/内存快照，缺失、损坏或过期时重建。
- 另纳入名称文件中命中 etf/lof 的代码；排除深指 `399*` 与 `market_rules.etf_name_drop_substr`。

**输出:**

- 返回 `Dict[str, str]`：key 为 `sz.`/`sh.` 前缀代码，value 为名称。

**调用示例:**

```python
from zsdtdx import get_client, get_etf_code_name

with get_client():
    etf_map = get_etf_code_name()
```

**返回示例:**

```json
{"sz.159915": "创业板ETF易方达", "sh.510050": "上证50ETF华夏", "sz.159105": "恒生生物科技ETF易方达"}
```



#### get_all_future_list

获取统一商品期货列表（郑州/大连/上海/广州）。

**调用前置约定:**

- 可直接调用；连续调用多个主进程 API 时建议进入 `with get_client():` 复用连接；

**输入:**

- return_df: 可选，是否返回 pandas.DataFrame；None 时跟随 `output.return_df_default`（包内默认 True）。
- 无缓存开关；自动复用有效缓存，缺失、损坏或过期时重建。

**调用示例:**

```python
from zsdtdx import get_client, get_all_future_list

with get_client():
    future_df = get_all_future_list(return_df=True)
```

**返回示例:**

```json
[{"code": "CU2603", "name": "沪铜2603", "market_name": "上海期货", "source": "ex"}]
```



#### get_stock_kline

获取股票 K 线任务结果（任务化输入，支持同步/异步与队列实时回传）。

**调用前置约定:**

- sync/async 均不要求前置 `with get_client()`；可选 with 以便同块调用其它主进程 `get_*`。
- 抓取连接由并行层进程内常驻 client 管理，不复用 with 内主进程 client。
- `mode="async"`：worker 子进程内抓取；`mode="sync"`：主进程 inproc 调度。

**输入:**

- task: `StockKlineTask` 或等价 dict，字段 `{code, freq, start_time, end_time}`（任务类见上文 `kline_task`）。
- queue: 可选，需 `put()`。sync 不传则仅返回值；传则返回值 + 队列双写。async 不传则自动创建并挂到 `job.queue`。
- preprocessor_operator: `f(payload)->dict|None`；返回 None 或空 dict 时丢弃该条。
- mode: `"sync"` 或 `"async"`，默认 `"async"`。
- qfq: 默认 `True` 前复权，`False` 不复权；A 股与港股共用。指数/期货无此参数。
- start_time/end_time: 仅日期时补齐 `09:30:00` / `16:00:00`（股票/指数/板块共用；期货为 `09:00`/`15:00`）。

**K 线** `datetime` **输出契约:**

- `rows` 中 `datetime` 为 `YYYY-MM-DD HH:MM:SS`，秒位固定 `:00`。

**调用示例（写法一：with + sync）:**

```python
import queue as py_queue
from zsdtdx import StockKlineTask, get_client, get_stock_kline, get_stock_stat

with get_client():
    board = get_stock_stat()
    q = py_queue.Queue()
    result = get_stock_kline(
        task=[
            StockKlineTask(code="600000", freq="d", start_time="2026-02-13", end_time="2026-02-13"),
            {"code": "000001", "freq": "60", "start_time": "2026-02-13", "end_time": "2026-02-14"},
        ],
        queue=q,
        mode="sync",
    )
```

**调用示例（写法二：async）:**

```python
from zsdtdx import destroy_parallel_fetcher, get_stock_kline, restart_parallel_fetcher

# 一般不必手动 prewarm；首次 async 会自动预热。
job = get_stock_kline(
    task=[{"code": "600000", "freq": "d", "start_time": "2026-02-13", "end_time": "2026-02-13"}],
    mode="async",
)
try:
    while True:
        event = job.queue.get(timeout=20)
        if event.get("event") == "done":
            break
    job.result()
except Exception:
    restart_parallel_fetcher(prewarm=True)
    raise
finally:
    destroy_parallel_fetcher()
```

**连接生命周期:**

- `mode="sync"`：主进程 inproc 调度，连接为抓取器进程内常驻 client；with 结束只关主进程 with client。
- `mode="async"`：worker 子进程内按任务侧（std/ex）懒建连接；进程池见 prewarm/restart/destroy。
- 同时在飞上限、地址配额与冷却等调度细节见下文默认配置 `parallel` 段说明。

**返回:**

- mode="sync": `list[task_payload]`（传 queue 时同时推送 data/done）。
- mode="async": `StockKlineJob`（从 `job.queue` 消费到 done）。
- task_payload:
`{"event":"data","task":{...},"rows":[...],"error":str|None,"worker_pid":int}`。
  > 示例：
  >
  > ```json
  > {"event": "data",
  >  "task": {"code": "600000", "freq": "d", "start_time": "2026-02-01 09:30:00", "end_time": "2026-02-02 16:00:00"},
  >  "rows": [{"code": "sh.600000", "freq": "d", "open": 10.07, "close": 10.06, "high": 10.25, "low": 10.03, "volume": 105771232, "amount": 1072786048, "datetime": "2026-02-02 15:00:00"}],
  >  "error": null, "worker_pid": 7200}
  > ```
- done: `{"event":"done","total_tasks":...,"success_tasks":...,"failed_tasks":...}`。



#### get_index_kline

获取指数 K 线任务结果（按指数名称输入，支持同步/异步与队列实时回传）。

**调用前置约定:**

- sync/async 均不要求前置 `with get_client()`；无 with 时名称路由解析会临时建连。
- 抓取连接由并行层常驻 client 管理（sync 主进程 inproc；async worker 子进程）。
- sync 走主进程 inproc chunk；async 走进程池 worker。

**输入:**

- task: `IndexKlineTask` 或等价 dict，字段 `{index_name, freq, start_time, end_time}`。
- queue / preprocessor_operator / mode：与 `get_stock_kline` 相同（空 dict 亦丢弃）。
- 无 `qfq`。
- start_time/end_time: 仅日期时补齐 09:30:00 / 16:00:00。
- task 缺省：async 且 `None`/空列表时自动展开（全量指数 × 日线 × 近 7 天）；sync 必须非空。

**K 线** `datetime` **输出契约:**

- 与 `get_stock_kline` 相同。

**调用示例:**

```python
from zsdtdx import IndexKlineTask, get_index_kline

result = get_index_kline(
    task=[
        IndexKlineTask(index_name="中证1000", freq="d", start_time="2026-03-01", end_time="2026-03-31"),
        {"index_name": "中证2000", "freq": "60", "start_time": "2026-03-01", "end_time": "2026-03-31"},
    ],
    mode="sync",
)
```

**名称匹配与报错:**

- 先精确匹配（支持别名，如 `上证综指 -> 上证指数`）。
- 未命中时抛错并给出名称片段候选。
- 路由来自标准 `get_security_list` + 扩展 `get_instrument_info`；失败会刷新路由后重试一次。



#### get_block_names

获取可请求 K 线的「全部板块」指数名称列表（概念、非统计风格、地区、研究行业中类）。

与 `get_stock_concepts` 共用三份命名文件（`infoharbor_block.dat`、`tdxhy.cfg`、`zhb.zip`），但本接口是板块指数名单，不是股票归属。与 `get_block_kline` 共用 6 小时本地缓存。

**调用前置约定:**

- 可直接调用；连续调用多个主进程 API 时建议进入 `with get_client():` 复用连接；

**输出:**

- 板块名称列表，不含指数代码；可直接作为 `get_block_kline` 的 `block_name`。历史区间没有行情时返回空 rows，不视为报错。

**调用示例:**

```python
from zsdtdx import get_client, get_block_names

with get_client():
    names = get_block_names()
```



#### get_block_kline

获取板块指数 K 线（按板块名称，支持 sync/async）。

**调用前置约定:**

- sync/async 均不要求前置 `with get_client()`；无 with 时名称解析会临时建连。
- 抓取连接由并行层常驻 client 管理（与 `get_stock_kline` 相同）。
- 必须显式传入非空 task；不会自动拉全部板块。
- `block_name` 使用 `get_block_names` 的返回值；与其共用 6 小时文件缓存。

**输入:**

- task: `BlockKlineTask` 或等价 dict，字段 `{block_name, freq, start_time, end_time}`。
- queue / preprocessor_operator / mode：与 `get_index_kline` 相同。
- 无 `qfq`。底层命令为板块指数 `0x0523`；回包 OHLC 为绝对价（与个股/指数 `0x052D` 差分不同）。

**调用示例:**

```python
from zsdtdx import BlockKlineTask, get_block_kline, get_block_names, get_client

with get_client():
    names = get_block_names()

result = get_block_kline(
    task=[
        BlockKlineTask(block_name=names[0], freq="d", start_time="2026-03-01", end_time="2026-03-31"),
    ],
    mode="sync",
)
```

**返回:**

- sync: `list[task_payload]`；async: `StockKlineJob`。
- rows 含 `block_name/freq/open/close/high/low/volume/amount/datetime`。



#### （一般无需手动调用）prewarm_parallel_fetcher

手动预热 async 并行抓取进程池。仅拉起 worker，不建 std/ex 业务连接。默认不要求全部 worker 成功；超时 60 秒、最多 3 轮，不读 YAML。首次 async 任务会自动预热，一般不必手动调用。

**输出:**

- 预热摘要字典（目标进程数、已预热进程数、pid 列表、耗时等）。

**什么时候调用:**

- 服务启动或压测前，希望把冷启动成本前移。

**边界条件:**

- 只校验目标 worker 是否已启动，不要求 std/ex 同时建连。
- 仅当 `require_all_workers=True` 且预热不足时抛 RuntimeError；默认 False，不足只记入摘要。



#### （一般无需手动调用）restart_parallel_fetcher

强制重启 async 并行抓取进程池（终止旧 worker 并按需预热）。

**输入:**

- prewarm: 重启后是否立即预热新池。
- prewarm_timeout_seconds: 重启后预热总超时（秒）。
- max_rounds: 重启后预热轮次上限。

**输出:**

- 重启摘要字典（旧 pid、终止结果、预热摘要、耗时等）。

**什么时候调用:**

- 出现连续 timeout/连接异常，怀疑 worker 状态异常时。
- 需要快速回收并重建 worker 连接状态时。

**边界条件:**

- 即便旧池不存在也会返回摘要，不抛错。



#### （一般无需手动调用）destroy_parallel_fetcher

销毁 async 并行抓取进程池并释放 worker 资源。
主进程正常退出时进程池会自行销毁。

**输入:**

- 无显式输入参数。

**输出:**

- 销毁摘要字典（是否存在旧池、旧 worker 数、销毁后版本号、耗时）。

**什么时候调用:**

- 长驻服务优雅停机前，主动释放 worker 与连接资源。
- 短脚本结束前，避免保留并行进程池到解释器退出阶段。

**边界条件:**

- 进程池不存在时安全返回，不抛错。



#### get_future_kline

获取商品期货 K 线（多周期批处理，返回合并 DataFrame）。

**调用前置约定:**

- 建议 `with get_client():` 以便同块调用其它主进程 API。
- 进程数 > 1 时 worker 并行（不占用主进程连接）；≤ 1 时串行并可复用 with 内连接。
- 与 `get_stock_kline` 的 task 路径不同。

**输入:**

- codes: 期货代码，支持 str/list/tuple/set；纯品种按码表「主连」补全（如 `AL` -> `ALL8`）。
为空时获取全部商品期货。带合约月或 `L7/L8/L9` 原样查询；无主连时抛错。
- freq: 周期，支持 str 或列表，如 `"d"` / `["d", "60", "30"]`。
支持周期: d/w/m/60min/30min/15min/5min 与 60/30/15/5。
- start_time/end_time: 闭区间；仅日期时补齐 09:00:00 / 15:00:00。
- 无 `qfq`。

**调用示例:**

```python
from zsdtdx import get_client, get_future_kline

with get_client():
    df = get_future_kline(
        codes=["CU", "AL"],
        freq=["d", "60"],
        start_time="2026-02-01",
        end_time="2026-02-13",
    )
```

**返回:**

- DataFrame 字段: code, freq, open, close, high, low, settlement_price, volume, datetime
- 大批量建议自行分批；无成交数据可能为空。



#### get_company_info

获取股票公司信息；默认并行，可选顺序。

**调用前置约定:**

- `mode="sync"`：可直接调用；进入 `with get_client():` 时可复用主进程连接。
- `mode="async"`（默认）：走进程池并行，不依赖主进程 with 连接；退出前建议 `destroy_parallel_fetcher()`。

**输入:**

- codes: 股票代码列表（一只也须写成 `["600000"]`）。
- category: 中文分类名列表；**sync/async 共用同一语义**——传入则只拉这些分类，为 None/空则拉全部分类。
- mode: `async`（默认）或 `sync`（无论 codes 长短均顺序跑）。
- queue: 可选事件队列（需 `put()`）。async 不传则自动创建。
- return_df: **仅 sync 最终返回**时生效（None 跟随 `output.return_df_default`）；中间传递始终为 `list[dict]`，不用 DataFrame。

**返回:**

- `mode="sync"`: `list[dict]` 或 DataFrame。
- `mode="async"`: `StockKlineJob`；从 `job.queue` 消费至 `event="done"`；`job.result()` 为全部行 list[dict]。

**调用示例:**

```python
from zsdtdx import destroy_parallel_fetcher, get_client, get_company_info

job = get_company_info(
    codes=["600000", "000001"],
    category=["最新提示", "公司概况"],
)
while True:
    event = job.queue.get()
    if event.get("event") == "done":
        break
destroy_parallel_fetcher()

with get_client():
    info_df = get_company_info(
        codes=["689009"],
        category=["最新提示", "公司概况"],
        mode="sync",
        return_df=True,
    )
```

**返回示例（sync list / 队列 data.rows 元素）:**

```json
[{"code": "689009", "category": "公司概况", "content": "......"}]
```



#### get_stock_stat

获取全市场股票统计宽表（实时价量 + 财务/股本 + 多日涨幅等）。

**调用前置约定:**

- 可直接调用；连续调用多个主进程 API 时建议进入 `with get_client():` 复用连接；

**输入:**

- 无。分页与财务批次由 `config.yaml` 的 `stock_stat` 控制
  （`page_size` / `finance_batch_size` / `finance_workers`）。
- 行情来自 `0x054B`；财务/股本来自批量 `0x0010`；多日涨幅与 PE 等来自
  `zhb.zip`（tdxstat）与 `tdxhy.cfg`，复用板块命名文件 6 小时缓存。
- 市值 / 市净 / 市销 / 市现用现价，现价非正则回退昨收。
- 市盈(TTM|静) / 股息率 / 多日涨幅 / 统计基准日按 `zhb.zip` 中 tdxstat 文件原值返回，不根据本机日期或实时价格折算。
- 不含港股；无 `codes` 入参。返回列为中文表头（含单位）。

**字段说明（仅最终返回列）:**

单位：金额万元，量为手，股本万股，价格元（价列表头不标单位）。

| 分组 | 列 | 含义 |
|---|---|---|
| 价量 | 现价/昨收/开盘/最高/最低/均价 | 行情价 |
| 价量 | 总量(手) / 现量(手) | 当日成交量 / 最近分笔成交量 |
| 价量 | 涨跌额 / 涨幅(%) | 现价−昨收；(现价−昨收)/昨收×100 |
| 价量 | 振幅(%) | (最高−最低)/昨收×100 |
| 价量 | 内盘(手) / 外盘(手) / 内外比 | 主动卖 / 主动买；内盘÷外盘 |
| 价量 | 换手(%) | 总量÷流通股本 |
| 估值 | 流通市值(万元) / 总市值(万元) | 流通股本×现价 / 总股本×现价 |
| 估值 | 市净率 / 市销率 / 市现率 | 现价÷每股净资；总市值÷营业收入；现价÷每股现金流 |
| 估值 | 市盈率(TTM) / 市盈率(静) / 股息率(%) | tdxstat 文件原值 |
| 财务 | 财报更新日期 / 上市日期 | 财务数据更新日期 / 上市交易日 |
| 财务 | 资产负债率(%) | (总资产−净资产−少数股东权益)/总资产×100 |
| 财务 | 税后利润(万元) / 净利润(万元) | 含少数股东损益 / 归母净利润 |
| 财务 | 净资产收益率(%) | 净利润÷净资产×100 |
| 统计 | 统计基准日 | tdxstat 文件记录的快照基准日 |
| 统计 | 贝塔系数 | 近60日相对大盘（沪→上证、深→深成指） |
| 统计 | 连涨天数 / n日涨幅(%) 等 | tdxstat 文件原值，不拼接实时涨幅 |

完整返回列名见 `zsdtdx.biz.stock_stat.STOCK_STAT_COLUMN_LABELS`。

**调用示例:**

```python
from zsdtdx import get_client, get_stock_stat

with get_client():
    df = get_stock_stat()
```

**返回示例（列节选）:**

```text
代码    现价  昨收  ...  市盈率(TTM)  5日涨幅(%)  行业
600000  9.98  9.90  ...  5.2         1.23        银行
```



#### get_future_latest_price

获取商品期货实时最新价字典。

**调用前置约定:**

- 可直接调用；连续调用多个主进程 API 时建议进入 `with get_client():` 复用连接；

**输入:**

- codes: 可选期货代码列表；纯品种代码按码表主连合约补全；为空时拉取全部商品期货。

**调用示例:**

- 1个code:

```python
from zsdtdx import get_client, get_future_latest_price

with get_client():
    one = get_future_latest_price("CU2603")
```

- 2个code:

```python
with get_client():
    two = get_future_latest_price(["AL", "CU2603"])
```

- 全部code:

```python
with get_client():
    all_prices = get_future_latest_price()
```

**返回示例:**

```json
{"ALL8": 23610.0, "CU2603": 102330.0}
```

服务端现价非正时回退昨收；现价与昨收均为空或非正时返回 `None`，并可通过
`get_runtime_failures()` 查看 `no_valid_quote` 明细。



#### get_runtime_failures

获取运行期失败/无数据明细。

**调用前置约定:**

- 可直接调用；连续调用多个主进程 API 时建议进入 `with get_client():` 复用连接；

**调用示例:**

```python
from zsdtdx import get_client, get_runtime_failures

with get_client():
    failures = get_runtime_failures()
```

**返回示例:**

```json
[{"task": "stock_kline", "code": "999999", "freq": "d", "reason": "code_not_found"}]
```



#### get_runtime_metadata

获取运行元数据快照。

**调用前置约定:**

- 可直接调用；连续调用多个主进程 API 时建议进入 `with get_client():` 复用连接；

**调用示例:**

```python
from zsdtdx import get_client, get_runtime_metadata

with get_client():
    meta = get_runtime_metadata()
```

**返回示例:**

```json
{"config_path": "<auto>", "std_active_host": "114.117.72.207:7709"}
```



### 默认配置文件内容（可复制）

以下示例的配置值与包内默认 `config.yaml` 一致，注释为文档精简版；可整份复制后修改，
也可只写需要覆盖的字段（见上文 `set_config_path` 合并说明）。

```yaml
# ---------------------------------------------------------------------------
# zsdtdx 封装配置（YAML 支持注释，注释使用 "#"）
# 完整 API 与可复制配置示例见项目 README.md
# ---------------------------------------------------------------------------
# 基本书写格式
# 1) 缩进必须用空格（建议 2 空格），不要用 Tab。
# 2) 布尔值用小写 true/false。
# 3) 列表写法：
#    key:
#      - "v1"
#      - "v2"
# 4) 字符串中包含 ":" 等特殊字符时建议加双引号。

client:
  # with get_client(...) 进入时是否预连接标准/扩展连接池。
  # 取值: true/false
  # 影响: true 启动即连接两侧；false 按实际 API 所需侧懒连接，减少冷启动时间与空闲 socket。
  preconnect_on_enter: false

logging:
  # 包级唯一日志阈值；仅支持 DEBUG/INFO/ERROR/OFF。
  # 每条日志的实际级别由代码固定：过程为 INFO、异常为 ERROR、高频诊断为 DEBUG。
  level: "INFO"
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

hosts:
  # 标准行情 IP 池（A股主站）。
  # 来源：银河证券海王星金融终端 connect.cfg [HQHOST]，端口均为 7709（已去掉 IPv6）。
  # 格式: "ip:port" 字符串列表
  standard:
    - "114.117.72.207:7709"  # 银河证券腾讯云行情
    - "123.125.108.101:7709"  # 银河证券上证云北京
    - "114.141.177.118:7709"  # 银河证券上证云上海一
    - "114.141.177.40:7709"  # 银河证券上证云上海二
    - "27.151.2.90:7709"  # 银河证券上证云福州
    - "202.100.166.12:7709"  # 银河证券上证云新疆
    - "182.118.8.9:7709"  # 银河证券上证云郑州
    - "183.201.231.85:7709"  # 银河证券上证云太原
    - "1.202.143.37:7709"  # 银河证券富丰电信
    - "111.203.134.118:7709"  # 银河证券富丰联通
    - "117.133.128.226:7709"  # 银河证券富丰移动
  # 扩展行情 IP 池（港股/期货）。
  # 来源：银河证券海王星金融终端 connect.cfg [DSHOST]，端口 7720/7730（已去掉 IPv6）。
  # 格式: "ip:port" 字符串列表
  extended:
    - "114.117.72.207:7720"  # 银河腾讯云扩展行情
    - "118.31.28.30:7730"  # 银河阿里云扩展行情

pool:
  # 连接超时（秒）。
  # 取值: 正浮点数
  # 影响: 值越小故障切换越快，但弱网下误判会增加。
  connect_timeout: 1.5
  # 单次 pool.call 固定恢复（无额外 YAML 开关）：
  # 每 host 三步：请求1 → 同连接再请求1 → 同host重连再请求1；
  # 多 host 时三步仍失败则 rotate 一次，在新 host 上重复同样三步；
  # 失败含抛错或 allow_none 时返回 None；最坏 3 次（单 host）或 6 次（多 host）底层请求。
  # TCP 探测单个 host 的超时（秒）；由 _ensure_availability_hosts_cache 在写缓存前使用。
  # 取值: 正浮点数（建议 0.5~2.0）
  # 影响: 值越大能探测到更多高延迟 host，但首次连接等待更久。
  probe_timeout: 0.8

parallel:
  # 并行进程数倍率：推荐进程数 = max(2, int(物理核心数 * 该倍率))。
  # 取值: 正浮点数（建议 0.5~3.0）
  # 影响: 倍率越大并发越高，吞吐可能提升，但 CPU/内存占用也会上升。
  process_count_core_multiplier: 1
  # 父进程同时提交的 bundle 窗口 = 进程数 × 本值。
  # 取值: 正整数
  # 影响: 标准侧地址配额按本值放大，用来把下一批 bundle 放进进程池队列；扩展侧不放大。
  task_chunk_max_inflight_multiplier: 2
  # 标准行情每个可达地址允许同时摊到的进程数。
  # 取值: 正整数
  # 影响: 地址不少于总进程时该侧用满总进程；进程多于地址时该侧上限 = min(总进程数, 地址数 × 本值)。
  adaptive_processes_per_host_std: 4
  # 扩展行情每个可达地址允许同时摊到的进程数；与标准侧分开配置。
  # 取值: 正整数
  # 影响: 扩展地址很少时把同时在飞进程压在 地址数 × 本值，避免 40 个进程打两个站。
  adaptive_processes_per_host_ex: 4
  # 重试耗尽后的连接不可用、超时或 watchdog 后保留的该地址进程配额比例。
  # 取值: 0~1 浮点数（建议 0.3~0.8）
  # 影响: chunk 重试成功或成功切到其他地址不降配额；只在最终仍失败时退避。
  adaptive_decrease_factor: 0.75
  # host 触发拥塞后的冷却秒数。
  # 取值: 非负浮点数
  # 影响: 冷却期间不向该 host 分配新 bundle；结束后保持降后的配额，成功时每次 +1 直到回到拥塞前上限。
  adaptive_cooldown_seconds: 1.5
  # 单次 chunk 抓取尝试墙钟上限（秒）：仅约束每一次 get_*_kline_rows_for_chunk_tasks（含该次建连与网络 IO）。
  # 不含入口归一化失败；不含 bundle 级预热建连；不含重试累计（重试由 chunk_retry_max_attempts 单独控制，每次尝试各算本上限）。
  # 取值: 正浮点数
  # 影响: 到点关闭本次尝试的 socket，阻塞中的 connect/recv 返回并按失败结束；重连同样受本上限约束。最坏墙钟约 chunk_timeout_seconds × (1 + chunk_retry_max_attempts)。
  # 调优指南（D4）: 5s 适合超低延迟内网；公网/弱网建议 15s+，避免短超时频繁触发重连放大尾延迟。
  chunk_timeout_seconds: 15
  # chunk 超时或报错后的最大重试次数。
  # 取值: 非负整数
  # 影响: 每个 chunk 最多重试 N 次（超时、连接不可用、其他异常均触发），避免无限重试拖慢整体吞吐。
  chunk_retry_max_attempts: 2

pagination:
  # 个股 K 线是否请求服务器前复权（A 股 reserved0；港股 extra）。
  # 取值: true=前复权，false=不复权。期货无复权，get_future_kline 不使用本项。
  standard_kline_qfq: true

catalog_cache:
  # 标准/扩展码表、ETF/LOF 名称板块按自然日缓存；板块三文件按 6 小时缓存。
  # 调用方无需控制缓存开关：有效即复用，缺失、损坏或过期即自动重建。
  # ETF 文件为 etf_code_name.pkl；当日已有则 get_etf_code_name 不再下载 0x02C5/0x06B9
  #（含 infoharbor_ex.name、zhb.zip/ilong.dat 与板块文件）。
  enabled: true
  # 可选：手动指定缓存目录（文件路径则取其父目录）。
  # 留空时自动选择用户可写目录；不可写会回退系统临时目录。
  path: ""

market_rules:
  # 场内 ETF/LOF 名称二次剔除子串（去空白后命中任一即丢弃）。
  # 作用对象: get_etf_code_name。etf/lof 初筛只用于名称文件中的额外代码，板块成分不走初筛。
  etf_name_drop_substr:
    - "债"
    - "货币"
    - "增强"
    - "红利"
    - "现金流"

stock_scope:
  # 当股票接口不传 codes（即 codes=None）时，默认抓取范围。
  # 作用对象: get_stock_code_name；
  # 以及客户端批路径 get_stock_kline(codes=None)。simple_api 任务化 get_stock_kline 必须显式传 task.code，不受本段影响。
  defaults_when_codes_none:
    # 取值支持:
    # - szsh: 标准市场（深圳+上海）
    # - bj:   北京股票（标准行情 market=2 且命中北京前缀）
    # - hk:   港股通（扩展行情市场名「港股通」，五位数字代码；不含香港主板）
    #
    # 推荐写法（列表）:
    # get_stock_kline:
    #   - "szsh"
    #   - "hk"
    #
    # 等价写法（字符串）:
    # get_stock_kline: "szsh+hk"
    # get_stock_kline: "szsh, hk"
    #
    # 注意:
    # 1) 大小写不敏感。
    # 2) 非法值会被忽略；若全非法会回退为 szsh。
    # 3) 显式传入 codes 时，不受这里配置影响。
    get_stock_code_name:
      - "szsh+bj"
    get_stock_kline:
      - "szsh+bj"

stock_stat:
  # 全市场股票统计宽表 get_stock_stat。
  page_size: 80
  finance_batch_size: 100
  # 0010 财务批次并发线程数；1=串行，内部限制为 1..8。
  finance_workers: 4

output:
  # 默认返回 DataFrame 还是 list[dict]。
  # 取值: true/false
  return_df_default: true
  # 批量接口默认 batch_size（按“代码数”分批，不是按K线行数）。
  # 取值: 正整数
  default_batch_size: 100
  # 是否过滤停牌/占位K线。
  # 取值: true/false
  filter_suspended_placeholder_bar: true

index_kline:
  # 指数别名：先将用户输入转换为标准名称，再执行精确匹配。
  aliases:
    上证综指: 上证指数
```

- `catalog_cache` 说明：
  - 标准行情（深沪京 `get_security_list`）、扩展行情（`get_instrument_info`）与 ETF/LOF 名称板块分文件按自然日缓存。
  - ETF/LOF 缓存文件为 `etf_code_name.pkl`；`get_etf_code_name` 仅在本地没有当日文件时下载 `0x02C5/0x06B9`。
  - 板块三文件缓存为 `block_named_files.pkl`，有效期 6 小时；板块名称、板块 K 线、股票板块归属与股票统计共享读取。
  - 股票 / 期货 / 指数在使用时从对应侧过滤；指数会同时确保 std 与 ex 当日缓存最新。
  - 同一缓存冷启动使用进程内与跨进程 single-flight，避免并发重复下载；缓存写失败只影响该次持久化，不影响已取得的业务数据和其它缓存。
  - 默认缓存位置会自动选择用户可写目录（Windows: `LOCALAPPDATA`，Linux: `XDG_CACHE_HOME` 或 `~/.cache`）。
  - 若目标目录不可写，会自动回退到系统临时目录；仍不可写时自动禁用磁盘缓存，不影响主流程。
- `logging` 说明：
  - `level` 是整个 `ZSDTDX` logger 唯一的输出阈值，仅支持 `DEBUG`、`INFO`、`ERROR`、`OFF`。
  - 过程日志固定为 `INFO`，异常固定为 `ERROR`，逐 chunk 派发/完成与重试细节固定为 `DEBUG`；配置不能改变单条日志所属级别。
  - `OFF` 仅关闭日志输出，不会删除 `get_runtime_failures()` 中的结构化失败。
- 常用并行配置位于 `parallel` 段：
  - `process_count_core_multiplier`
  - `task_chunk_max_inflight_multiplier`
  - `adaptive_processes_per_host_std`
  - `adaptive_processes_per_host_ex`
  - `adaptive_decrease_factor`
  - `adaptive_cooldown_seconds`
  - `chunk_timeout_seconds`
  - `chunk_retry_max_attempts`

