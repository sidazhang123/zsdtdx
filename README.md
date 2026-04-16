# zsdtdx

`zsdtdx` 是面向 A 股/期货行情场景的 Python 封装库，基于 pytdx 生态提供统一 API、连接池、重试和并行抓取能力。

## 安装

```bash
pip install zsdtdx
```


## API 概览

- `set_config_path`
- `get_client`
- `get_supported_markets`
- `get_stock_code_name`
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
- 依赖：`pandas`、`PyYAML`、`six`、`psutil`



## 许可证与来源

- 许可证：`MIT`（见 `LICENSE`）
- 第三方归属：见 `THIRD_PARTY_NOTICES.md`


## 快速开始

#### set_config_path

设置全局配置路径（主进程与并行 worker 统一生效），如不调用则后续函数使用包内默认配置(见文档最后的示例)。

**调用示例:**
```python
from zsdtdx import set_config_path

set_config_path(r"D:\\configs\\zsdtdx.yaml") 
call other functions...
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
- 本函数属于全量代码接口，返回范围由配置文件的stock_scope控制,默认szsh市场（可增配bj、hk）。


**输出:**
- 返回 `Dict[str, str]`：key 为带市场前缀的股票代码（`sh./sz./bj./hk.`），
  value 为股票名称；不返回纯数字代码。

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
- preprocessor_operator: 可选预处理函数，签名 `f(payload)->dict|None`；
  返回 None 或空 dict 时该条结果不入队也不进入返回值。
- mode: `"sync"` 或 `"async"`，默认 `"async"`。sync 阻塞直到完成；async 立即返回句柄。

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
- `mode="async"`：主要使用 worker 连接；with 结束不会直接关闭 worker 连接，worker 连接由并行抓取器按进程池生命周期管理。

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
  > ```json
  > {"event": "data",
  >  "task": {"code": "600000", "freq": "d", "start_time": "2026-02-01 09:30:00", "end_time": "2026-02-02 16:00:00"},
  >  "rows": [{"code": "sh.600000", "freq": "d", "open": 10.07, "close": 10.06, "high": 10.25, "low": 10.03, "volume": 105771232.0, "amount": 1072786048.0, "datetime": "2026-02-02 15:00:00"}],
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
- preprocessor_operator: 可选预处理函数，签名 `f(payload)->dict|None`。
- mode: `"sync"` 或 `"async"`，默认 `"async"`。

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
- 路由由后台动态发现（标准 `get_security_list` + 扩展 `get_instrument_info`），并自动缓存。
- 名称路由与指数目录可落盘为 pickle（默认在用户缓存目录，见 `index_kline.disk_cache`）；写入采用临时文件 + 原子替换，加载失败会删除损坏文件。
- 修改 `index_kline` 配置段会改变指纹，旧缓存文件会被忽略或删除后重建。
- 缓存未命中或命中后抓取失败时，会自动刷新路由后重试一次。

#### （一般无需手动调用）prewarm_parallel_fetcher

手动预热 async 并行抓取进程池与 worker 常驻连接。使用 config 中的 `parallel.auto_prewarm_*` 参数（config 来源：用户 `set_config_path()` 后使用用户提供的配置文件，如果没有调用则使用包内默认配置文件 `config.yaml`）。若 config 读取失败，则使用内部兜底值。

**输出:**
- 预热摘要字典（目标进程数、已预热进程数、pid 列表、耗时等）。

**什么时候调用:**
- 服务启动阶段：希望把 async 首次冷启动成本前移。
- 压测/批跑前：希望先确认 worker 建连健康再开始任务。

**边界条件:**
- 默认 `require_all_workers=True`；若预热不足会抛 RuntimeError。

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
- codes: 期货代码，支持 str/list/tuple/set；代码不含数字会自动补 `L8`（如 `AL` -> `ALL8`）。
  为空时获取全部商品期货。
- freq: 周期，支持 str 或列表，如 `"d"` / `["d", "60", "30"]`。
  支持周期: d/w/m/60min/30min/15min/5min 与 60/30/15/5。
- start_time/end_time: 支持字符串/date/datetime，底层过滤按闭区间 `[start_time, end_time]` 执行。
  若传入不带时分秒的日期字符串，自动补齐为 start=09:00:00、end=15:00:00。
  例如 `2026-02-13` 等价于 `start_time="2026-02-13 09:00:00"`、`end_time="2026-02-13 15:00:00"`。

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

获取单只股票公司信息。

**调用前置约定:**
- 请先进入 `with get_client():`；


**输入:**
- code: 股票代码。
- category: 中文分类名列表；为空时返回全部分类。
- return_df: 可选，是否返回 pandas.DataFrame；默认 True。

**调用示例:**
```python
from zsdtdx import get_client, get_company_info

with get_client():
    info_df = get_company_info(code="689009", category=["最新提示", "公司概况"], return_df=True)
```

**返回示例:**
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
- codes: 可选期货代码列表；不含数字会自动补 `L8`（连续合约）；为空时拉取全部商品期货。

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
{"config_path": "<auto>", "std_active_host": "120.76.1.198:7709"}
```

### 默认配置文件内容

```yaml
# ---------------------------------------------------------------------------
# zsdtdx 封装配置（YAML 支持注释，注释使用 "#"）
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
  # 海王星金融终端
  # 标准行情 IP 池（A股主站）。
  # 格式: "ip:port" 字符串列表
  standard:
    - "120.76.1.198:7709"
    - "123.125.108.101:7709"
    - "114.141.177.118:7709"
    - "114.141.177.40:7709"
    - "27.151.2.113:7709"
    - "27.151.2.38:7709"
    - "202.100.166.12:7709"
    - "182.118.8.9:7709"
    - "203.231.201.85:7709"
    - "183.201.231.85:7709"
    - "120.76.4.28:7719"
    - "1.202.143.37:7709"
    - "111.203.134.118:7709"
    - "117.133.128.226:7709"
  # 扩展行情 IP 池（港股/期货/北京部分路由）。
  # 格式: "ip:port" 字符串列表
  extended:
    - "47.112.95.207:7720"
    - "118.31.28.30:7730"

pool:
  # 连接超时（秒）。
  # 取值: 正浮点数
  # 影响: 值越小故障切换越快，但弱网下误判会增加。
  connect_timeout: 1.5
  # 封装层单次调用总重试预算（含同连接重试/同host重连/轮换IP）。
  # 取值: 正整数，建议 >= 1
  max_retry: 3
  # 同一连接上的快速重试次数（不重连）。
  # 取值: 非负整数
  same_connection_retry_times: 2
  # 同一连接快速重试间隔（毫秒）。
  # 取值: 非负整数
  same_connection_retry_interval_ms: 800
  # 同一 host 重连尝试次数（disconnect -> connect 同 host）。
  # 取值: 非负整数
  same_host_reconnect_times: 3
  # 同一 host 重连间隔（毫秒）。
  # 取值: 非负整数
  same_host_reconnect_interval_ms: 50
  # 是否开启底层心跳线程。
  # 取值: true/false
  # 影响: true 可降低长时间空闲断连概率。
  heartbeat: true

parallel:
  # 并行进程数倍率：推荐进程数 = max(2, int(物理核心数 * 该倍率))。
  # 取值: 正浮点数（建议 0.5~3.0）
  # 影响: 倍率越大并发越高，吞吐可能提升，但 CPU/内存占用也会上升。
  process_count_core_multiplier: 1.5
  # [仅供 get_future_kline 旧路径 _fetch_parallel 使用] 单次并行抓取的总超时（秒）。
  # 取值: 正浮点数
  # 影响: 超时后会回收未完成 future，并按策略触发强制回收与串行补拉。
  parallel_total_timeout_seconds: 300
  # [仅供 get_future_kline 旧路径 _fetch_parallel 使用] 单个 future.result 的超时（秒）。
  # 取值: 正浮点数
  # 影响: 防止 worker 返回阶段异常阻塞。
  parallel_result_timeout_seconds: 600
  # [仅供 get_future_kline 旧路径 _fetch_parallel 使用] 总超时后是否触发并行进程强制回收。
  # 取值: true/false
  force_recycle_on_timeout: true
  # [仅供 get_future_kline 旧路径 _fetch_parallel 使用] 总超时后是否对未完成任务回退串行补拉。
  # 取值: true/false
  timeout_fallback_to_serial: true
  # chunk 级缓存启用阈值（以 get_stock_kline(task) 任务链路计）。
  # 取值: 正整数
  # 影响: 当 code+freq 的 chunk 任务数达到阈值时启用轻量缓存。
  task_chunk_cache_min_tasks: 2
  # worker 进程内 chunk 级 future 并发数。
  # 取值: 正整数
  # 影响: 每个进程一次最多并发执行的 chunk 数。
  task_chunk_inproc_future_workers: 3
  # 父进程提交 chunk 批次的在飞窗口倍率。
  # 取值: 正整数
  # 影响: 在飞 future 上限 = 进程数 × 该倍率。
  task_chunk_max_inflight_multiplier: 2
  # chunk 重试循环中，若错误匹配连接不可用关键词，重试前自动尝试重建连接。
  # 取值: true/false
  # 影响: true 时可降低长任务尾部因连接失效导致的批量失败。
  chunk_reconnect_on_unavailable: true
  # 单个 chunk 执行超时（秒），适用于 get_stock_kline / get_index_kline 的 async/sync mode。
  # 取值: 正浮点数
  # 影响: 超时后进入重试循环，由 _fetch_one_task_chunk 内部处理。
  chunk_timeout_seconds: 30
  # chunk 超时或报错后的最大重试次数，适用于 get_stock_kline / get_index_kline 的 async/sync mode。
  # 取值: 非负整数
  # 影响: 每个 chunk 最多额外重试 N 次，重试耗尽则标记失败。
  chunk_retry_max_attempts: 2
  # async 调用时是否自动预热进程池与 worker 常驻连接。
  # 取值: true/false
  # 影响: true 时 async 首次调用会自动触发 prewarm，降低冷启动抖动。
  auto_prewarm_on_async: true
  # 自动预热时是否要求所有 worker 都完成预热。
  # 取值: true/false
  # 影响: true 时只要有 worker 预热不足会直接失败并抛错。
  auto_prewarm_require_all_workers: true
  # 自动预热总超时（秒）。
  # 取值: 正浮点数
  # 影响: 控制 async 自动预热最多等待时长。
  auto_prewarm_timeout_seconds: 60
  # 自动预热最大轮次。
  # 取值: 正整数
  # 影响: 每轮会向进程池提交探针任务以拉齐 worker 预热状态。
  auto_prewarm_max_rounds: 3
  # async 自动预热时是否让各 worker 尽量分散连接到不同标准行情有效 IP。
  # 取值: true/false
  # 影响: true 时会先探测标准行情有效 IP，再按 worker 轮询分配，降低单 IP 集中压力。
  auto_prewarm_spread_standard_hosts: false

pagination:
  # 标准行情 get_security_list 单页数量。
  # 取值: 正整数
  standard_security_list_page_size: 800
  # 扩展行情 get_instrument_info 单页数量。
  # 取值: 正整数
  extended_instrument_info_page_size: 800
  # 标准行情 K线单页数量（get_security_bars）。
  # 取值: 正整数，常用 800
  standard_kline_page_size: 800
  # 扩展行情 K线单页数量（get_instrument_bars）。
  # 取值: 正整数，常用 700
  extended_kline_page_size: 700
  # 公司信息正文分块读取长度。
  # 取值: 正整数
  company_info_chunk_size: 30000
  # K线最大分页次数上限，防止异常场景下无限循环。
  # 取值: 正整数
  max_kline_pages: 400

market_rules:
  # 北京股票代码前缀集合（扩展市场 market=44 下匹配）。
  include_beijing_prefixes:
    - "92"
  # 识别港股市场使用的扩展市场名称集合。
  include_hk_market_names:
    - "港股通"
  # 深圳 A 股前缀集合（标准市场）。
  stock_prefix_sz:
    - "000"
    - "001"
    - "002"
    - "003"
    - "300"
    - "301"
  # 上海 A 股前缀集合（标准市场）。
  stock_prefix_sh:
    - "600"
    - "601"
    - "603"
    - "605"
    - "688"
    - "689"
  # 纳入“商品期货”集合的扩展市场名称。
  future_market_names:
    - "郑州商品"
    - "大连商品"
    - "上海期货"
    - "广州期货"

stock_scope:
  # 当股票接口不传 codes（即 codes=None）时，默认抓取范围。
  # 作用对象: get_stock_code_name / get_stock_latest_price / get_stock_kline
  defaults_when_codes_none:
    # 取值支持:
    # - szsh: 标准市场（深圳+上海）
    # - bj:   北京股票（扩展 market=44 且命中北京前缀）
    # - hk:   港股（扩展市场名命中 include_hk_market_names）
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
      - "szsh"
    get_stock_latest_price:
      - "szsh"
    get_stock_kline:
      - "szsh"

index_kline:
  disk_cache:
    enabled: true
    directory: null
    filename: index_route_cache.pkl
  # 动态发现扩展指数时优先考虑的 ex 市场列表。
  prefer_ex_markets:
    - 62
    - 102
    - 37
    - 27
  aliases:
    上证综指: 上证指数
  lookup:
    max_candidates: 10
    normalize_whitespace: true

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
  # 占位K线成交量/成交额“极小值”阈值。
  # 取值: 非负浮点数（科学计数法可用）
  suspended_placeholder_eps: 1.0e-20
```

- 常用并行配置位于 `config.yaml.parallel`：
  - `process_count_core_multiplier`
  - `task_chunk_cache_min_tasks`
  - `task_chunk_inproc_future_workers`
  - `task_chunk_max_inflight_multiplier`
  - `chunk_reconnect_on_unavailable`
  - `chunk_timeout_seconds`
  - `chunk_retry_max_attempts`
  - `auto_prewarm_on_async`
  - `auto_prewarm_require_all_workers`
  - `auto_prewarm_timeout_seconds`
  - `auto_prewarm_max_rounds`
  - `auto_prewarm_spread_standard_hosts`


```

