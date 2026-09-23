# zsdtdx

`zsdtdx` 是面向 A 股/期货行情场景的 Python 封装库，参考 pytdx 生态提供统一 API、连接池、重试和并行抓取能力。部分请求组包与回包解析已按实盘抓包重新实现，**并非**对 `pytdx` 的直接二次封装或运行时依赖。

## 安装

```bash
pip install zsdtdx
```

## API 概览

- `set_config_path`
- `get_client`
- `get_supported_markets`
- `get_stock_code_name`
- `get_stock_concepts`
- `get_etf_code_name`
- `get_all_future_list`
- `get_stock_kline`
- `get_index_kline`
- `prewarm_parallel_fetcher`
- `restart_parallel_fetcher`
- `destroy_parallel_fetcher`
- `get_future_kline`
- `get_company_info`
- `get_stock_latest_price`
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
    markets = get_supported_markets(return_df=True)
    stock_map = get_stock_code_name()
    prices = get_stock_latest_price(["600000", "000001"])
```

**作用边界:**

- 该 client 仅管理"当前主进程"上下文中的连接生命周期（进入 with 预连接，退出 with 自动 close）。
- `get_stock_kline(mode="async")` 的 worker 连接由并行抓取器在 worker 进程内独立维护，不与此处返回的主进程 client 共用连接对象。



#### get_supported_markets

获取标准+扩展行情支持的市场列表。

**调用前置约定:**

- 请先进入 `with get_client():`；

**输入:**

- return_df: 可选，是否返回 pandas.DataFrame；默认 True。

**调用示例:**

```python
from zsdtdx import get_client, get_supported_markets

with get_client():
    df = get_supported_markets(return_df=True)
```

**返回示例:**

```json
[{"market": 0, "name": "深圳", "source": "std"}, {"market": 1, "name": "上海", "source": "std"}]
```



#### get_stock_code_name

获取统一股票代码名称字典。

**调用前置约定:**

- 请先进入 `with get_client():`；

**输入:**

- use_cache: 是否使用股票缓存；默认值为True，置为 False 时强制刷新股票缓存。
- 本函数属于全量代码接口，返回范围由配置文件的stock_scope控制,默认szsh市场（可增配bj、hk；hk 为港股通）。

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

获取股票所属板块。概念、风格、指数来自 `infoharbor_block.dat`。钢铁、煤炭、水泥等通达信基础行业来自 `tdxhy.cfg`（股票→行业码）和 `zhb.zip` 内的 `tdxzs.cfg`（行业码→板块名）。主机用配置里的 `hosts.standard`，都走命名文件下载。股票名称用 `get_stock_code_name` 的当日码表。不写 pkl。

**调用前置约定:**

- 请先进入 `with get_client():`；

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

获取场内 ETF/LOF（本语境统称 etf）代码名称字典。成分来自标准行情 `7709` 板块文件（默认 `spec/specetfdata.txt` / `spec/speclofdata.txt`），名称优先 `infoharbor_ex.name`，缺名回退标准码表 `0x044D` 的 16 字节 GBK 名称。当日快照写入 `catalog_cache` 的 `etf_code_name.pkl`，仅本地没有当日文件时才下载。不改动 `get_stock_code_name` 口径，不读银河安装目录。

**调用前置约定:**

- 请先进入 `with get_client():`；

**输入:**

- use_cache: True 复用当日磁盘/内存快照；False 强制重新下载命名文件。
- 板块成分跳过 etf/lof 初筛，仍排除 `399*` 与 `etf_name_drop_substr`；名称文件中额外命中 etf/lof 的代码一并纳入。
- 远程文件为 `infoharbor_ex.name` 与 `spec/specetfdata.txt`、`spec/speclofdata.txt`；单页 30000 字节。名称剔除子串仍由 `market_rules.etf_name_drop_substr` 配置。

**输出:**

- 返回 `Dict[str, str]`：key 为 `sz.`/`sh.` 前缀代码，value 为名称（infoharbor 完整名或 16 字节回退名）；排除深指 `399*` 与名称命中剔除子串的品种。

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

- 请先进入 `with get_client():`；

**输入:**

- return_df: 可选，是否返回 pandas.DataFrame；默认 True。
- use_cache: 是否使用期货清单缓存；为 False 时强制刷新期货清单缓存。

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

- `mode="sync"`：可进入 `with get_client():` 在主进程复用连接并统一资源释放。
- `mode="async"`：可直接调用；async 抓取使用 worker 进程内独立连接。
若同一流程还要连续调用主进程 `get_*` 接口，可把这些主进程调用放在 with 块内执行。

**输入:**

- task: 任务列表，元素是 `StockKlineTask` 或 dict，模板字段:
`{code, freq, start_time, end_time}`。
- queue: 可选队列，需支持 `put()`。
  - `mode="sync"` 且不传 queue：仅通过返回值拿到结果。
  - `mode="sync"` 且传 queue：返回值仍是完整结果列表，同时会向 queue 增量写入 data/done 事件。
  - `mode="async"` 且不传 queue：函数会自动创建 queue 并挂到返回的 `job.queue`。
  - `mode="async"` 且传 queue：返回的 `job.queue` 即该 queue。
- preprocessor_operator: 可选钩子，签名 `f(payload)->dict|None`；
返回 None 或空 dict 时该条结果不入队也不进入返回值（默认 OHLC 两位小数、成交额/量为整数由协议解析层统一）。
- mode: `"sync"` 或 `"async"`，默认 `"async"`。sync 阻塞直到完成；async 立即返回句柄。
- start_time/end_time: 支持字符串/date/datetime。若仅传入日期（无时分秒），自动补齐为 `start_time="… 09:30:00"`、`end_time="… 16:00:00"`（股票/指数任务共用，与 `get_future_kline` 的 09:00/15:00 不同）。

**K 线** `datetime` **输出契约:**

- `rows` 中每条 K 线的 `datetime` 为 `YYYY-MM-DD HH:MM:SS`，秒位固定 `:00`（例如 `"2026-02-02 15:00:00"`）。

**复权:**

- `get_stock_kline` 增加参数 `qfq`（默认 `True`=前复权，`False`=不复权），接在原有参数之后；A 股与港股共用该开关。
- `get_index_kline` 不提供 `qfq`：指数无复权语义，标准行情 reserved0 固定为 0。
- `get_future_kline` 不提供 `qfq`：期货无复权，扩展行情 extra 固定为 0。
- task 字段与返回结构不变。

**调用示例（写法一：with 主进程上下文 + sync + 其它接口）:**

```python
import queue as py_queue
from zsdtdx import (
    StockKlineTask,
    get_client,
    get_stock_kline,
    get_stock_latest_price,
    get_supported_markets,
)

with get_client():
    markets = get_supported_markets(return_df=True)
    prices = get_stock_latest_price(["600000", "000001"])
    # sync 模式可传入队列，边产出边消费（也可不传，仅用返回值）。
    q = py_queue.Queue()
    result = get_stock_kline(
        task=[
            # 使用任务对象写法，字段校验更明确
            StockKlineTask(code="600000", freq="d", start_time="2026-02-13", end_time="2026-02-13"),
            # 也支持 dict 写法
            {"code": "000001", "freq": "60", "start_time": "2026-02-13", "end_time": "2026-02-14"},
        ],
        queue=q,
        mode="sync",
    )
    print(len(markets), prices)
    # result 为完整 payload 列表；q 中也会收到相同 data 事件和最终 done 事件
    print(result)
```

**调用示例（写法二：async 独立进程池调用 + prewarm/restart/destroy）:**

```python
from zsdtdx import (
    destroy_parallel_fetcher,
    get_stock_kline,
    prewarm_parallel_fetcher,
    restart_parallel_fetcher,
)
#不建议主动调用，在get_stock_kline()时会按yaml配置快速自动创建。
prewarm_parallel_fetcher()

job = get_stock_kline(
    task=[{"code": "600000", "freq": "d", "start_time": "2026-02-13", "end_time": "2026-02-13"}],
    mode="async",
)
try:
    while True:
        # 实时读取 data 事件，直到 done
        event = job.queue.get(timeout=20)
        if event.get("event") == "done":
            break
        # event="data" 时可按 task/rows/error 增量处理
        print(event.get("task"), event.get("error"))
    # 等待后台任务完全结束并传播异常
    job.result()
except Exception:
    # 任务执行链路出现持续异常时，可强制重启并重建 worker 连接
    restart_parallel_fetcher(prewarm=True, prewarm_timeout_seconds=60, max_rounds=3)
    raise
finally:
    # 服务停机或脚本结束前主动销毁进程池
    destroy_parallel_fetcher()
```

**连接生命周期说明:**

- `mode="sync"`：主要使用主进程连接；with 结束会关闭主进程 client 连接。
- `mode="async"`：父进程先固化每个股票/指数任务的 std/ex 路由；worker 只为当前 bundle 对应侧懒建连接，纯 std 不建立 ex 业务连接，纯 ex 不建立 std 业务连接。
- CPU 推导的进程数只是硬上限。某一侧同时在飞进程数按可达地址数 H 与总进程数 C 计算：地址不少于进程时用满 C；进程多于地址时上限为 `min(C, H × per_host)`。标准/扩展的 `per_host` 分开配置。父进程提交窗口为进程数 × `task_chunk_max_inflight_multiplier`，标准侧地址配额按该倍率放大以填满进程池队列；扩展侧不放大。
- 重试耗尽后的连接不可用、超时或 watchdog 降低对应地址配额并冷却；chunk 重试成功或成功切到其他地址不降配额。冷却结束后成功时每次 +1 回到拥塞前上限，不向外探测更高上限。多个同时运行的 async job 共用该容量状态。纯单侧任务不占另一侧连接；混合任务按两侧各自预算并行。

**返回:**

- mode="sync": 始终返回 `list[task_payload]`（无论是否传 queue）。
  - 未传 queue：结果仅在返回值中。
  - 传了 queue：结果既在返回值中，也会同步推送到 queue。
- mode="async": 始终返回 `StockKlineJob`（无论是否传 queue）。
  - 未传 queue：可从自动创建的 `job.queue` 消费事件。
  - 传了 queue：可从传入的 queue（即 `job.queue`）消费事件。
- task_payload 结构:
`{"event":"data","task":{...},"rows":[...],"error":str|None,"worker_pid":int}`。
  > 示例：
  >
  > ```json
  > {"event": "data",
  >  "task": {"code": "600000", "freq": "d", "start_time": "2026-02-01 09:30:00", "end_time": "2026-02-02 16:00:00"},
  >  "rows": [{"code": "sh.600000", "freq": "d", "open": 10.07, "close": 10.06, "high": 10.25, "low": 10.03, "volume": 105771232, "amount": 1072786048, "datetime": "2026-02-02 15:00:00"}],
  >  "error": null, "worker_pid": 7200}
  > ```
- 队列最终会额外推送 done 事件:
`{"event":"done","total_tasks":...,"success_tasks":...,"failed_tasks":...}`。



#### get_index_kline

获取指数 K 线任务结果（按指数名称输入，支持同步/异步与队列实时回传）。

**调用前置约定:**

- `mode="sync"`：走 `ParallelFetcher` 的主进程 inproc chunk 路径；若当前已进入 `with get_client():`，其它主进程 API 仍可继续复用该上下文连接。
- `mode="async"`：走 `ParallelFetcher` 的进程池 chunk 路径，worker 会独立创建并复用自己的连接，不依赖 with 上下文。

**输入:**

- task: 任务列表，元素是 `IndexKlineTask` 或 dict，字段:
`{index_name, freq, start_time, end_time}`。
- queue: 可选队列，需支持 `put()`。
- preprocessor_operator: 可选钩子，签名 `f(payload)->dict|None`（默认数值刻度由协议解析层统一）。
- mode: `"sync"` 或 `"async"`，默认 `"async"`。
- start_time/end_time: 规则与 `get_stock_kline` 相同（仅日期时补齐为 09:30:00 / 16:00:00）。
- task 缺省行为：`mode="async"` 且 `task` 为 `None` 或空列表时，自动构建默认任务（全量指数目录 × 日线 × 近 7 天）；`mode="sync"` 必须显式传入非空 task。

**K 线** `datetime` **输出契约:**

- 与 `get_stock_kline` 相同：`YYYY-MM-DD HH:MM:SS`，秒位固定 `:00`。

**调用示例:**

```python
import queue as py_queue
from zsdtdx import IndexKlineTask, get_index_kline

q = py_queue.Queue()
result = get_index_kline(
    task=[
        IndexKlineTask(index_name="中证1000", freq="d", start_time="2026-03-01", end_time="2026-03-31"),
        {"index_name": "中证2000", "freq": "60", "start_time": "2026-03-01", "end_time": "2026-03-31"},
    ],
    queue=q,
    mode="sync",
)
print(result)
```

**名称匹配与报错:**

- 先做精确匹配（支持别名标准化），例如 `上证综指 -> 上证指数`。
- 未命中时抛错并返回“名称片段候选”。
- 路由由后台动态发现：标准行情 `get_security_list`（深沪京）+ 扩展 `get_instrument_info`（中证等）。
- 抓取失败时，会自动刷新路由后重试一次。



#### （一般无需手动调用）prewarm_parallel_fetcher

手动预热 async 并行抓取进程池。预热只拉起 worker 进程，不建立 std/ex 行情连接；实际连接由 route-homogeneous bundle 按任务侧懒建。预热超时固定 60 秒、最多 3 轮，不从 YAML 读取。

**输出:**

- 预热摘要字典（目标进程数、已预热进程数、pid 列表、耗时等）。

**什么时候调用:**

- 服务启动阶段：希望把 async 首次冷启动成本前移。
- 压测/批跑前：希望先完成 Windows spawn 与模块加载成本。

**边界条件:**

- `require_all_workers=True` 时只校验目标 worker 进程是否已启动，不再要求 std/ex 同时建连成功。



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

获取商品期货 K 线（支持多周期并行获取，返回合并后的 DataFrame）。

**调用前置约定:**

- 请先进入 `with get_client():`；

**输入:**

- codes: 期货代码，支持 str/list/tuple/set；纯品种代码按码表名称含「主连」的合约补全（如 `AL` -> `ALL8`，`CU` -> `CUL8`）。
为空时获取全部商品期货。带 3~4 位合约月份或 `L7/L8/L9` 连续合约原样查询（如 `CU2603`、`CUL9` 加权）。
该品种码表中无主连时抛错。
- freq: 周期，支持 str 或列表，如 `"d"` / `["d", "60", "30"]`。
支持周期: d/w/m/60min/30min/15min/5min 与 60/30/15/5。
- start_time/end_time: 支持字符串/date/datetime，底层过滤按闭区间 `[start_time, end_time]` 执行。
若传入不带时分秒的日期字符串，自动补齐为 start=09:00:00、end=15:00:00。
例如 `2026-02-13` 等价于 `start_time="2026-02-13 09:00:00"`、`end_time="2026-02-13 15:00:00"`。
- 期货无复权，不提供 `qfq`。

**调用示例:**

```python
from zsdtdx import get_client, get_future_kline

with get_client():
    # 获取多个期货、多个周期的数据，返回一个合并 DataFrame
    df = get_future_kline(
        codes=["CU", "AL"], 
        freq=["d", "60"],
        start_time="2026-02-01", 
        end_time="2026-02-13"
    )
    # df 已按统一字段规范输出，可直接过滤 code/freq 继续处理
    print(df)
```

**返回:**

- pd.DataFrame: 包含所有获取的数据，字段:
code, freq, open, close, high, low, settlement_price, volume, datetime



#### get_company_info

获取股票公司信息；默认并行，可选顺序。

**调用前置约定:**

- `mode="sync"`：请先进入 `with get_client():`，主进程按 codes 顺序拉取。
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



#### get_stock_latest_price

获取股票实时最新价字典。

**调用前置约定:**

- 请先进入 `with get_client():`；

**输入:**

- codes: 可选股票代码列表；支持 `sh./sz./bj./hk.` 前缀；
为空时按 `config.yaml.stock_scope.defaults_when_codes_none.get_stock_latest_price`
拉取默认范围全量股票；显式传入代码时不受该范围开关影响。
停牌时回退昨收；未上市占位（现价与昨收都非正）为 None，随整批一次解析，不额外重试。
单票无有效报价不会把同批其它代码打成 None。

**调用示例:**

- 1个code:

```python
from zsdtdx import get_client, get_stock_latest_price

with get_client():
    one = get_stock_latest_price("600000")
```

- 2个code:

```python
with get_client():
    two = get_stock_latest_price(["600000", "09988"])
```

- 全部code:

```python
with get_client():
    all_prices = get_stock_latest_price()
```

**返回示例:**

```json
{"600000": 9.98, "09988": 158.5}
```



#### get_future_latest_price

获取商品期货实时最新价字典。

**调用前置约定:**

- 请先进入 `with get_client():`；

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



#### get_runtime_failures

获取运行期失败/无数据明细。

**调用前置约定:**

- 请先进入 `with get_client():`；

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

- 请先进入 `with get_client():`；

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

以下内容与包内默认 `config.yaml` 一致，可整份复制后修改，也可只写需要覆盖的字段（见上文 `set_config_path` 合并说明）。

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
  # 影响: true 启动更快暴露连接问题；false 首次调用接口时再连接。
  preconnect_on_enter: true

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
  # 标准/扩展码表与 ETF/LOF 名称板块的磁盘缓存：按自然日分文件保存，使用时再过滤。
  # ETF 文件为 etf_code_name.pkl；当日已有则 get_etf_code_name 不再下载 0x02C5/0x06B9。
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
  # 作用对象: get_stock_code_name / get_stock_latest_price / get_stock_kline
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
    get_stock_latest_price:
      - "szsh+bj"
    get_stock_kline:
      - "szsh+bj"

output:
  # 默认返回 DataFrame 还是 list[dict]。
  # 取值: true/false
  return_df_default: true
  # 批量接口默认 batch_size（按“代码数”分批，不是按K线行数）。
  # 取值: 正整数
  default_batch_size: 100
  # 股票实时行情批量查询每次请求代码数。
  # 取值: 正整数
  latest_quote_batch_size: 80
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
  - 股票 / 期货 / 指数在使用时从对应侧过滤；指数会同时确保 std 与 ex 当日缓存最新。
  - 默认缓存位置会自动选择用户可写目录（Windows: `LOCALAPPDATA`，Linux: `XDG_CACHE_HOME` 或 `~/.cache`）。
  - 若目标目录不可写，会自动回退到系统临时目录；仍不可写时自动禁用磁盘缓存，不影响主流程。
- 常用并行配置位于 `parallel` 段：
  - `process_count_core_multiplier`
  - `task_chunk_max_inflight_multiplier`
  - `adaptive_processes_per_host_std`
  - `adaptive_processes_per_host_ex`
  - `adaptive_decrease_factor`
  - `adaptive_cooldown_seconds`
  - `chunk_timeout_seconds`
  - `chunk_retry_max_attempts`

