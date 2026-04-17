# Changelog

## v1.4.4 - 2026-04-17

### Summary
1. 指数 chunk 执行新增批量任务入口：在 worker 侧按 `index_name+freq` 聚合后，通过 `get_index_kline_rows_for_chunk_tasks()` 统一执行，减少重复调用开销。
2. 指数 chunk 级缓存能力补齐：支持在同一 chunk 内复用已拉取的原始 bar，按任务时间窗口切片返回，降低分页请求次数。
3. 指数并行链路补充命中统计：`chunk_hit_tasks` 与 `chunk_network_page_calls` 在 index 任务路径中按真实执行结果回填，便于性能观测与调优。

## v1.4.3 - 2026-04-17

### Summary
1. 指数名称路由增加进程内内存复用：`resolve_index_name(refresh=False)` 命中客户端内缓存时直接返回，避免同一客户端重复全市场目录扫描。
2. `get_index_kline` 增加主进程预解析：在任务分发前统一解析 `index_name -> {source, market, code}`，并将路由写入任务负载，降低 worker 侧重复目录发现开销。
3. worker 执行链路支持消费预解析路由：`get_index_kline_rows_for_task` 优先使用任务路由字段，缺失时再回退原有名称解析逻辑，保持兼容性。
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
