# Changelog

## Unreleased

## v2.0.3 - 2026-09-20

### Summary
1. 默认并行进程倍率 `parallel.process_count_core_multiplier` 由 5 调整为 1，降低默认进程池规模与行情站并发压力。
2. 版本号：`pyproject.toml` 与 `__init__.__version__` 对齐为 `2.0.3`。

## v2.0.2 - 2026-09-20

### Summary
1. 新增 `get_etf_code_name`：从标准行情深/沪码表按名称过滤场内 ETF/LOF（语境含两者），返回 `sz.`/`sh.` 前缀字典；不改动 `get_stock_code_name` 与其它股票宇宙。
2. 配置：`market_rules.etf_name_drop_substr`（默认债/货币/增强/红利/现金流）；名称初筛 etf/lof 写死在代码中。
3. 版本号：`pyproject.toml` 与 `__init__.__version__` 对齐为 `2.0.2`。

## v2.0.1 - 2026-09-20

### Summary
1. TCP 探测裁剪：配置侧地址数 ≤ 3 的短池只剔除不可达、保留全部可达站，不再去掉最慢节点；长池（>3）行为不变。短池阈值写死为 3。
2. 文档：补充 `docs/ex_std_host_concurrency_plan.md`（std/ex 并发亲和后续方案与残留问题）。
3. 版本号：`pyproject.toml` 与 `__init__.__version__` 对齐为 `2.0.1`。

## v2.0.0 - 2026-09-20

### Summary
1. **大版本**：标准/扩展行情协议按银河海王星与实盘抓包重解析，默认 hosts 切到银河节点；不再是对 `pytdx` 的直接二次封装（见 `THIRD_PARTY_NOTICES.md`）。
2. 标准行情握手对齐银河海王星客户端：292 字节首包 + 13 字节第二包（末字节 02）+ GBK「银河证券」身份包（float 11.63）；扩展行情 92 字节 0x6548 握手同步对齐银河 7720 抓包。
3. 默认 hosts 切到银河 `connect.cfg`（标准 7709 / 扩展 7720·7730，去掉不可达 IPv6）；通达信官方地址注释保留备查；README 可复制 YAML 与包内 `config.yaml` 对齐。
4. 公司信息 F10：目录带 `index`；正文请求写入 `category_index`，请求 length 为剩余总字节，单页上限 30720；北交所 F10 走 market=0；正文各页 bytes 拼接后整包 GBK 解码。
5. 个股/指数 K 线请求为 54 字节 0x052D；默认单页 800 条（服务端硬上限）；`start=0` 与翻页使用不同 inner 字段。
6. 个股标准行情 K 线默认请求服务器前复权（`pagination.standard_kline_qfq: true`，reserved0=1）；指数 reserved0 固定为 0。
7. 官方周期号：1 分钟 category=7，日线 category=4（高层 `d` 本已映射为 4）。
8. `get_stock_kline` 增加 `qfq`（默认 True）：A 股写 reserved0，港股写扩展行情 extra；`get_index_kline` / `get_future_kline` 不暴露该参数，reserved0/extra 固定 0。
9. 扩展行情握手与 K 线：92 字节 0x6548 握手对齐银河海王星（7720）；K 线请求 64 字节 0xD808/0xD908；回包 42 字节前缀 + 32 字节记录。默认单页 700 条（服务端硬上限）。
10. 移除空闲心跳：删除 `heartbeat.py`、`do_heartbeat`、`BaseSocketClient(heartbeat=...)` 与 `pool.heartbeat`。官方客户端只在选站时发 12 字节探测（标准 0x15 / 扩展 0x2455）；空闲断连由连接池重连处理。
11. 北交所个股改走标准行情 market=2（reserved0 前复权）；扩展行情 market=44（股转系统）不再承担北交所 K 线。
12. 标准行情码表使用 0x044D（单页 1600、记录 37 字节），`get_security_list` 覆盖深沪京；HQ 指数目录扫描 market=0/1/2（含北证50）；中证等扩展指数仍走 `get_instrument_info`。
13. 扩展行情码表使用 0x2422 文本页（`市场#代码|名称`）；`get_instrument_info` 覆盖港股、期货与中证等扩展品种。
14. 期货纯品种代码按码表名称含「主连」的合约补全（不再硬编码 `L8`）；`L9` 为加权连续，不当作主连。带 3~4 位合约月份的代码原样查询。
15. 码表改为统一缓存聚合层 `ensure_code_catalog`：std/ex 分文件按自然日缓存未过滤原文，使用时再过滤。删除 `index_kline.route_cache` 与 `index_route_disk_cache.py`。
16. 港股市场识别默认改为扩展行情「香港主板」（港股通股票均在主板，不含创业板）。
17. `get_future_kline` 并行分流按合约形态识别期货：`CUL8` 等 `L+数字` 连续合约不再因字母 `L` 被误判为股票。
18. `get_stock_latest_price`：未上市占位码（五档全 0）解析不再抛错，一只票不会把整批打成 None；现价非正回退昨收，占位票记 None 且不拆单重试。
19. `get_company_info(codes, category=..., mode="async"|"sync", queue=...)`：仅接受 `codes` 列表；默认 `async` 返回 `StockKlineJob`+队列流式推送；`sync` 无论只数均顺序跑，`return_df` 仅作最终可选 DataFrame。配置项见 `parallel.company_info_*`。
20. 文档与归属：`THIRD_PARTY_NOTICES.md` / README / AGENTS / `pyproject.toml` description 标明部分协议请求已重解析；收录银河验收与周/日线对照手工脚本。
21. 版本号：`pyproject.toml` 与 `__init__.__version__` 对齐为 `2.0.0`。

## v1.4.9 - 2026-05-25

### Summary
1. 文档：README 嵌入与包内一致的完整 `config.yaml` 可复制示例；补充股票/指数任务日期补齐（09:30/16:00）、K 线 `datetime` 契约（`YYYY-MM-DD HH:MM:SS`，秒位 `:00`）及 `get_index_kline` async 默认任务说明；`simple_api` docstring 同步。
2. 测试：移除冗余 `tests/test_pytdx_alignment.py` 与 `tests/manual/test_chunk_timeout_retry.py`；保留 `tests/manual/` 基准与冒烟脚本。
3. 代码质量：`src/` 执行 `ruff format` 与 lint 修复；parser/unified_client 注释与 datetime 说明对齐固定 `:00` 秒输出。
4. 版本号：`pyproject.toml` 与 `__init__.__version__` 对齐为 `1.4.9`。

## v1.4.8 - 2026-05-22

### Summary
1. TCP 可用地址探测统一由 `_ensure_availability_hosts_cache` 写入进程内缓存；移除 `pool.probe_on_init`、包 `import` 阶段预热及连接池内探测/shuffle。
2. `set_config_path` 新增 `async_background_probe`（默认 True）：校验阶段不再构造会触发探测的 `UnifiedTdxClient`；后台或首次建连前完成缓存写入。
3. 并行 worker 建池前按槽位随机起始下标旋转地址列表；spawn 子进程经 `_seed_probe_result_cache_from_snapshot` 复用槽位列表，禁止 worker 内重复探测。
4. 连接池恢复策略收敛为固定三步（同连接再试 → 同 host 重连 → rotate 新 host 重复三步），删除 `max_retry` / `same_connection_retry_*` / `same_host_reconnect_*` 等 YAML 开关。
5. worker/主进程 chunk 并发由 `ThreadPoolExecutor` 改为 `asyncio` 协程 + `to_thread`；配置键 `task_chunk_inproc_future_workers` 更名为 `task_chunk_inproc_coroutine_workers`。
6. `chunk_timeout_seconds` 语义明确为单次抓取尝试墙钟上限（默认 15s）；`get_future_kline` 遗留并行项标注 `[DEPRECATED-E5]`。
7. `get_volume` 与 pytdx 按位对齐（含 `dwEdx < 0` 历史分支），修复量价解码偏差；socket 层启用 `TCP_NODELAY`；`TdxFunctionCallError` 携带方法名与端点信息。
8. 测试布局：`examples/` 大体积基准与演示脚本移除；新增 `tests/test_*.py` 离线验收与 `tests/manual/` 手工脚本；`pyproject.toml` 配置 pytest 与显式 `numpy` 依赖。

## v1.4.7 - 2026-04-20

### Summary
1. 修正 `resolve_index_name` 运行时缓存流程：调用时优先使用内存缓存，内存缺失时再检查磁盘缓存。
2. 当磁盘缓存不存在、损坏、跨日或无效时，调用链路内立即触发全量重建并回填内存缓存。
3. 补充运行时缓存流程回归测试，覆盖“内存命中 / 磁盘命中 / 磁盘失效触发重建”三种核心路径。

## v1.4.6 - 2026-04-20

### Summary
1. 指数名称路由新增按自然日刷新磁盘缓存：当天首次运行全量更新映射，日内复用缓存，跨日自动重建。
2. `resolve_index_name` 增加名称级映射缓存复用，减少重复扫描全市场目录带来的耗时。
3. 缓存路径新增跨平台可写性探测与回退：优先用户缓存目录，不可写时回退系统临时目录，仍不可写则自动禁用磁盘缓存。

## v1.4.5 - 2026-04-17

### Summary
1. 指数名称识别补充 `沪深` 关键字，减少标准指数在名称过滤阶段被误排除的概率。
2. 指数识别新增标准代码兜底：上证市场 `000***` 与深证市场 `399***` 可在名称不命中关键字时继续进入指数候选集合。
3. 提升 `get_stock_code_name(..., include_index=True)` 返回完整性，降低后续指数路由与 K 线拉取的漏数风险。

## v1.4.4 - 2026-04-17

### Summary
1. 指数 chunk 执行新增批量任务入口：在 worker 侧按 `index_name+freq` 聚合后，通过 `get_index_kline_rows_for_chunk_tasks()` 统一执行，减少重复调用开销。
2. 指数 chunk 级缓存能力补齐：支持在同一 chunk 内复用已拉取的原始 bar，按任务时间窗口切片返回，降低分页请求次数。
3. 指数并行链路补充命中统计：`chunk_hit_tasks` 与 `chunk_network_page_calls` 在 index 任务路径中按真实执行结果回填，便于性能观测与调优。

## v1.4.3 - 2026-04-17

### Summary
1. 指数名称路由增加进程内内存复用：`resolve_index_name(refresh=False)` 命中客户端内缓存时直接返回，避免同一客户端重复全市场目录扫描。
2. `get_index_kline` 增加主进程预解析：在任务分发前统一解析 `index_name -> {source, market, code}`，并将路由写入任务负载，降低 worker 侧重复目录发现开销。
3. worker 执行链路支持消费预解析路由：`get_index_kline_rows_for_chunk_tasks` 优先使用任务路由字段，缺失时再回退原有名称解析逻辑，保持兼容性。
4. 指数路由配置默认收敛：`prefer_ex_markets` 默认由 `[62,102,37,27]` 调整为 `[62]`，减少扩展市场索引候选扫描范围。

## v1.4.2 - 2026-04-16

### Summary
1. 移除指数名称路由缓存机制：删除内存/磁盘缓存命中与持久化逻辑，避免缓存文件权限问题与历史缓存污染路由结果。
2. `get_index_kline` 新增无 task 默认行为：支持 `task=None` 或空列表时自动构建默认指数任务（近 7 天日线）。
3. 清理配置与文档中的 `index_kline.disk_cache` 残留说明，并更新相关示例与交接文档。

## v1.4.1 - 2026-04-16

### Summary
1. 修复 Windows 场景指数路由磁盘缓存目录可能 `Access is denied` 的问题：初始化时先探测目录可写性，不可写则自动降级到系统临时目录。
2. 当首选目录与临时目录都不可写时，自动禁用指数路由磁盘缓存并记录告警，避免影响主业务请求链路。

## v1.3.0 - 2026-03-27

### Summary
1. 新增 TCP Connect 延迟探测：首次连接前并发对地址池所有 host 做 TCP 握手测速，按延迟升序排列后优先连接最快节点，显著降低首次连接延迟。
2. 探测结果自动传递给 worker 子进程：主进程探测一次，通过 `initargs` 传递排序结果，避免每个 worker 重复探测。
3. 新增配置项 `pool.probe_on_init`（开关）和 `pool.probe_timeout`（单 host 超时秒数），可通过配置关闭恢复原行为。
4. 不可达 host 不丢弃，排至末尾保留作为 failover 后备。

## v1.2.0 - 2026-03-16

### Summary
1. 并行任务链路重构：`get_stock_kline` async/sync chunk 路径移除整体超时与 bundle 超时，统一为 chunk 级超时 + 通用重试机制。
2. 修复 chunk 超时实现阻塞问题：超时后使用非阻塞 executor 回收，确保单个 chunk 卡死不会拖住整个链路。
3. 修复异常吞错导致全量 `no_data` 问题：`allow_none=True` 不再吞掉真实调用失败，断网/连接异常可正确进入重试与失败分支。
4. 配置与文档更新：新增 `chunk_timeout_seconds`、`chunk_retry_max_attempts`，并将旧版整体超时参数标注为仅供 `get_future_kline` 旧路径使用。

## v1.1.4 - 2026-03-13

### Summary
1. 修复 Windows 下并行抓取 worker 进程无法继承 `set_config_path()` 用户配置路径的问题，避免 worker 意外回退到包内默认 `config.yaml`。
2. 当活动配置路径发生变化时，主动销毁旧进程池，确保后续重建的 worker 全部使用最新配置。

## v1.1.3 - 2026-03-13

### Summary
1. 新增深圳 A 股 `302` 前缀支持，确保 `302***` 代码会被纳入标准市场股票路由。
2. 校验 `get_stock_kline` 对 `sh.` / `sz.` / `bj.` 前缀代码的容错查询逻辑，带前缀与不带前缀输入都会在底层统一转换为源端所需的纯代码格式。

## v1.1.1 - 2026-03-06

### Summary
1. `prewarm_parallel_fetcher` 函数签名简化：移除参数要求，内部直接从 config 读取 `parallel.auto_prewarm_*` 参数（优先使用用户 `set_config_path()` 设置的配置文件，否则使用包内默认 `config.yaml`）。若 config 读取失败，则使用内部兜底值。
2. README 更新：移除函数参数描述，强调 config 驱动调用方式。

## v1.1.0 - 2026-03-02

### Summary
1. 关键修复：修正“每个 chunk 都触发线程级新建连接”的问题，改为复用 worker 进程级常驻连接与进程级 chunk 线程池连接，显著降低连接抖动与失败率。
2. 并行链路新增 chunk 级连接自愈参数（`chunk_reconnect_on_unavailable`、`chunk_reconnect_max_attempts`），在命中连接不可用时可在当前线程重建连接后重试。
3. `_build_task_chunks` 热点路径优化：降低时间解析与分片构建 CPU 开销，保持排序与返回契约不变。
4. 统一客户端从 `src/zsdtdx/wrapper/unified_client.py` 迁移到 `src/zsdtdx/unified_client.py`，并清理 `wrapper/` 目录冗余文件。
5. `BaseSocketClient` 清理双重重试冗余逻辑，重试策略统一上收至封装层连接池。
6. 配置默认值与文档对齐：并行缓存阈值、进程内并发、连接池重试参数与分页参数统一到 `config.yaml` 当前默认语义。
