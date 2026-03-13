# Changelog

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
