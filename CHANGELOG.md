## 未发布

- 审查修复：完整缓存与缺文件容错共用股票归属规范化，保持原文别名优先级及同分类去重，未知分类一致跳过；映射缓存格式升至v3。股票归属返回仅复制可变容器，避免整图深拷贝；清理重复获取空码表和无用状态字段；容错和状态清理纳入同一锁，避免并发清空部分来源，码表获取抛错也保证清理。

- 板块身份统一为（中文分类名称，板块名称）二元组。`get_block_names()`、`BlockKlineTask.block_name`、K 线结果的 `block_name`，以及 `get_stock_concepts()` 的 `names/map` 使用同一结构；不再接受裸名称板块任务。JSON 序列化为二元素数组，任务输入也接受该数组。
- 目录覆盖概念 GN、风格 FG、指数成分 ZS、通达信基础行业 HY、研究行业中类 RS、补充成分 SP 六类；纳入统计风格、空成分和无代码板块，不纳入地区。基础行业与研究行业同名时分别路由，不再跨分类合并股票归属。
- 板块无可确定代码或服务端合法空页/区间无行情时返回空 `rows`、`error=None`，计入成功任务；真实网络/解析失败仍保留错误。补充六类目录、重名隔离、空行情和公开接口全量网络验收脚本。
- 在 `cache/block_catalog_cache.py` 增加 `block_catalog.pkl` 完整映射磁盘缓存，三个板块接口共同读取；包含目录、路由、原始成分、股票码名和带分类的股票归属，与原始三文件共用6小时生命周期。优先使用有效原文，按原文下载时间到期，过期先刷新原文再建图；原文提前更新会使内存和磁盘旧图失效。目录保留全部身份；股票归属维持同分类内相同成分仅保留最长名称的规则；首次建图按进程内锁和跨进程锁串行，校验后原子写入，空码表及部分源文件不落盘。

## v3.0.5 - 2026-10-29

- `get_stock_stat(quotes_only=False)` 默认仍返回行情、财务和 zhb 宽表。`quotes_only=True` 只返回 0x054B 实时列，不拉取、不解析 0x0010 与 zhb/tdxhy。
- 0x054B 改为同站 4 条连接并行翻页，条数写死在 `TDXParams.STOCK_STAT_QUOTE_WORKERS`，不进入 YAML。当前站整段失败才换下一台 szshbj。全量时 zhb/tdxhy 与行情同时开始；0x0010 等行情页结束后再按 `finance_workers` 并发。`quotes_only=True` 同样返回 DataFrame。
- 版本号：`pyproject.toml` 与 `__init__.__version__` 对齐为 `3.0.5`。

## v3.0.4 - 2026-10-26

- 异步公司信息只经队列交付正文。`job.result()` 返回 `total_codes`、`success_codes`、`failed_codes`；库内只保留代码去重和计数，不把整批正文放进聚合结果。同步公司信息仍返回全文。data 事件仍为 `code` 与 `rows` 中的 `code/category/content`，done 仍给出成功和失败计数。
- 异步 K 线分片在交出后立即放开引用，避免已交付行情在生成器等待下一次结果时继续驻留。
- 补充公司信息异步结果释放与超时回归，并同步文档及手工验收脚本。
- 版本号：`pyproject.toml` 与 `__init__.__version__` 对齐为 `3.0.4`。

## v3.0.3 - 2026-10-26

- 异步 K 线在带队列时只经队列交付行情。`job.result()` 返回 `total_tasks`、`success_tasks`、`failed_tasks`，不再保留已完成任务的行情正文。未带队列时 `result()` 仍返回 payload 列表。同步接口仍返回 payload 列表。
- 股票、指数、板块异步 K 线在后台正常或异常结束时统一向可写队列发送一次 done；异常终态附带 error，未完成任务计失败，`job.result()` 仍保留原异常。结束通知失败不覆盖此前的任务异常，有界队列继续阻塞等待消费。
- 股票所属板块增加研究行业中类和 spblock 补充成分；按板块代码使用目录全称，同代码或同名成分取并集，可发布股票集合相同的板块保留最长名称。返回结构保持 `{names, map}`。
- 补充异步结果释放、异常终态、队列故障和背压回归，并同步股票、指数、板块文档及手工验收脚本。

## v3.0.2 - 2026-10-22

- 修复 F10 公司资料冷启动并发竞争，以及连接重获后目录与正文跨站错配导致的正文错位。
- 增加正文身份校验、同站重取与换站重试，补充逐帧取证工具和远端全量验收记录。

- 修复公司资料公开异步接口冷启动竞争：整批提交前单次准备标准股票路由，单股缺失时复用缓存并二次检查，不再触发多线程 `refresh=True` 全量重抓。
- F10 目录与每页正文在整个 IO/重连阶段钉住同一物理站；修复连接丢失后 `call_current_host` 可能重新租到其它站的路径。历史 5 篇错位全文均可用 A 站目录偏移读取 B 站合并文件布局复现，完整 SHA-256 一致。
- 正文起始标题校验精确股票代码与栏目；错位整股丢弃，同站重取一次目录与正文，再按能力换站；取消、GBK ignore 与候选栏目过滤语义保留。
- 补充冷启动、连接丢失、错位、同站重取、换站及终态回归，以及公开接口逐帧取证工具；证据和历史事件的可归因边界见 `docs/company_info_cold_start_identity_2026-10-22.md`。

## v3.0.1 - 2026-10-21

- 公司信息按真实市场能力候选集取得可用站点准入，准入/建连失败不再转换为无原因空成功；目录与正文分页继续同站。
- 公司信息逐股限时重试，统一同步/异步终态与去重计数；空结果明确报告 no_data，取消与重试耗尽均可对账，成功股不重复抓取。
- 全部换站后仍短页或中断的正文不作为完整成功交付；长度完整但夹有源端非法 GBK 字节时忽略坏字节并保留其余正文；并发正文归还目录许可并独立回收各线程许可。
- 分类参数保持候选过滤语义；命中部分候选时正常成功，不再为未出现的可选栏目写运行态失败。
- 标准码表 0x044D 的昨收按真实协议改为小端 `float32`，缓存格式升至 v4，使旧错误值失效重抓；全目录 52,448 个身份、11 个标准站均未发现其它编码方式。
- 标准 0x052D K 线量额改按真实小端 IEEE `float32` 位型解码，除修正 511920.SH 的 `0x80` 边界外，也修正低量周线把 12/50/59 误解为 1,032/320/464 的 pytdx 负指数分支。
- 标准 K 线成交量统一为与日线相同的最小成交单位：指数 5/15/30/60 分钟除以 100，股票/基金/可转债/指数周月线乘以 100；7,777 标的的 602,439 根周线、135,785 根月线和全部 94 个指数的 24,816 根 5→15 分钟聚合均无倍数错误。
- 实时报价支持手工固定品种精度：股票/指数 2 位、基金 3 位、可转债 4 位；同品种批次可传 `security_type`，混合批次可逐项传小数位，旧两元组保持 2 位兼容。收盘后 7,680 个正成交标的的实时量额与 TdxQuant 无倍数差异，因此成交额保持现实现；板块 0x0523 仍需独立取证。
- 版本号：`pyproject.toml` 与 `__init__.__version__` 对齐为 `3.0.1`。

## v3.0.0 - 2026-10-17

- 并行抓取统一为有界线程执行器，标准/扩展连接采用按站独占租约，保留重试、累计超时和拥塞恢复。
- K 线采用 numba 解码和只读列式内核，标准差分、指数、板块绝对价及扩展格式统一覆盖，交付层保留 rows / DataFrame。
- 新增 thread_workers、threads_per_host_std/ex，移除旧进程配置；行情事件移除 worker_pid，预热和回收改报线程状态。
- 将列式内核与交付打包分离，为后续用户侧格式转换保留明确边界。
- 标准行情地址按 `szshbj` / `szsh_only` 静态能力分组；北交所行情、码表、F10 及 `get_stock_stat` 的 054B/0010 只在支持北交所的站内重连换站，合法空页不触发换站。
- 版本号：`pyproject.toml` 与 `__init__.__version__` 对齐为 `3.0.0`。

# Changelog

## v2.4.0 - 2026-10-09

### Summary
1. 完成业务、网络、缓存、解析器与并行调度分层重梳理，股票/指数/板块 K 线共用任务校验、分发与 chunk 调度链路。
2. 以 `get_stock_stat` 替换旧股票最新价统计入口：整合 054B 实时行情、0010 批量财务与 zhb/tdxstat 静态统计，输出全市场股票宽表。
3. 新增板块名称与板块指数 K 线能力，统一复用板块文件缓存；补充 ETF/LOF 名称与成分聚合、运行态失败和元数据 introspection。
4. 收敛连接池重试、TCP 探测、worker 预热、自适应并发与 chunk 超时语义，修复期货路由、港股成交额、历史逐笔日期组包等问题。
5. 期货 K 线批处理固定走扩展行情；worker 级异常逐任务写入运行态失败，不再执行串行补拉，并删除无效的单 future 超时。
6. 删除 Python 2 解压、旧签名测试回退、期货白名单、分区缓存兼容别名及其它确认无调用的死代码。

## v2.3.1 - 2026-09-30

### Summary
1. 以 `get_stock_stat` 替换 `get_stock_latest_price`：返回全市场宽表 DataFrame（054B 实时 + 批量 0010 财务 + zhb/tdxstat 多日统计）。
2. 新增解析器 `get_board_quote_page` / `get_finance_info_batch` / `tdxstat`，经 `net/hq.py` 与 `biz/stock_stat` 分层接入。
3. D 类文件复用 `block_file_cache` 中的 `zhb.zip` / `tdxhy.cfg`（6 小时 TTL），不另起日缓存。
4. 配置新增 `stock_stat`；删除 `stock_scope...get_stock_latest_price` 与 `output.latest_quote_batch_size`。
5. 删除未使用的单票 `get_finance_info`（pytdx 旧字段名/量纲）；批量 0x0010 仅保留 `get_finance_info_batch`（对齐看板标定）。
6. 版本号：`pyproject.toml` 与 `__init__.__version__` 对齐为 `2.3.1`。

## v2.3.0 - 2026-09-29

### Summary
1. 新增 `get_block_names` / `get_block_kline` / `BlockKlineTask`：按板块名称抓取板块指数 K 线（协议 `0x0523`），与 `get_stock_concepts` 共用命名文件并提供 6 小时本地缓存。
2. 对外不再导出 `get_supported_markets`（仍保留 `UnifiedTdxClient.get_supported_markets`）；`StockKlineTask` / `IndexKlineTask` / `BlockKlineTask` 聚合在 `kline_task.py` 并经包入口导出。
3. 股票/指数/板块/期货 K 线与公司信息业务门面迁入 `biz/`；传输/编排/缓存拆为 `net/`、`engine/`、`cache/`；异常/工具/日志迁入 `util/`。
4. `params` 增补港股通市场名、`tdxzs3.cfg`、股票/期货日期默认时分秒、`FUTURE_PATTERNS`。
5. 版本号：`pyproject.toml` 与 `__init__.__version__` 对齐为 `2.3.0`。

## v2.2.2 - 2026-09-23

### Summary
1. `get_etf_code_name` 名称表并入 `zhb.zip` 内 `ilong.dat`；与 `infoharbor_ex.name` 同码时以 ilong 为准；板块缺名仍回退本库 std/`0x044D` 16 字节短名（与客户端版面一致）。名称文件中未列入板块的代码须名称含 etf/lof。
2. 码表磁盘缓存 `format_version` 升至 3，使旧 ETF 快照失效并重拉。
3. 版本号：`pyproject.toml` 与 `__init__.__version__` 对齐为 `2.2.2`。

## v2.2.1 - 2026-09-23

### Summary
1. 去掉 `index_kline.prefer_ex_markets`。扩展行情指数按市场名含「指数」或名称关键字纳入，同名路由仍由 `_index_route_priority` 决定。
2. 默认 `parallel.process_count_core_multiplier` 由 5 改为 1。
3. 版本号：`pyproject.toml` 与 `__init__.__version__` 对齐为 `2.2.1`。

## v2.2.0 - 2026-09-23

### Summary
1. 新增 `get_stock_concepts`：结构与概念 pkl 的 `{names, map}` 一致（股票名称 → 板块名列表）。
2. 概念/风格/指数来自标准行情命名文件 `infoharbor_block.dat`（0x02C5 + 0x06B9）。
3. 通达信基础行业来自 `tdxhy.cfg` 与 `zhb.zip` 内 `tdxzs.cfg`；只输出个股挂上的最下级行业名。
4. 股票名称复用 `get_stock_code_name` 当日码表；解析落在 `parser/infoharbor_block.py`。
5. 版本号：`pyproject.toml` 与 `__init__.__version__` 对齐为 `2.2.0`。

## v2.1.0 - 2026-09-22

### Summary
1. 股票/指数 async 的 K 线进程按行情站拆开：进程创建时绑定自家站，该站的任务只进入这些进程。可用连接不会因为下一批任务被拆掉。父进程准入抽到 `adaptive_scheduler.py`，跨 async job 共享。
2. 地址配额只在 chunk 报告标记拥塞时下降。`timed out` / 「超时」在重试耗尽后记为拥塞；任务错误文本里的 timeout 不再单独降配额。
3. 某一侧已经没有可选地址且没有在飞 bundle 时，准入立即失败，不再空等。
4. 版本号：`pyproject.toml` 与 `__init__.__version__` 对齐为 `2.1.0`。

## v2.0.6 - 2026-09-21

### Summary
1. 港股路由改为扩展行情「港股通」（`0x2422` 市场名写死，五位数字代码）；不再识别香港主板，去掉 `market_rules.include_hk_market_names`。
2. `get_etf_code_name` 当日快照与 std/ex 码表一样走 `catalog_cache` 落盘（`etf_code_name.pkl`）；仅本地没有当日文件时才下载命名文件。名称或任一块板块文件为空则不写盘、不把空列表当成当天成功。
3. 命名文件下载只走 `TdxHq_API.get_report_file_by_size`（0x02C5 + 0x06B9），去掉客户端内第二套翻页循环。
4. 配置加载：用户 YAML 以包内默认为底深合并；仅覆盖内置已有键，丢弃未知字段；列表整段替换。
5. 版本号：`pyproject.toml` 与 `__init__.__version__` 对齐为 `2.0.6`。
6. 股票/指数 async 改为父进程固化 std/ex 路由、worker 按侧懒连接，并用可达地址数与总进程数计算两侧同时在飞上限；预热只拉起任务所需进程，不再同时预连接两侧。
7. 恢复 `task_chunk_max_inflight_multiplier`（默认 2）拉大提交窗口，标准侧地址配额随倍率放大，扩展侧不放大。拥塞只认重试耗尽后的连接/超时或 watchdog；chunk 重试成功或成功切站不降配额。
8. chunk 单次尝试或重连超过 `chunk_timeout_seconds` 时关闭本次 socket，让阻塞中的 connect/recv 返回，并把该 chunk 按失败结束。
9. 协议收发改为 `sendall` 与按 `zipsize` 读满包体。响应头、未压缩包体和 zlib 包体的解析结果保持原样。
10. 配置只保留地址、并发旋钮、复权、码表缓存、股票默认范围、占位 K 线开关和 ETF 名称剔除。协议页大小、交易所前缀、期货市场名和 ETF 远程文件名改到 `params.py`；内部调度常数写死。

## v2.0.5 - 2026-09-21

### Summary
1. 标准行情 `0x044D` 码表按银河抓包解析：37 字节记录中名称为 **16 字节 GBK**。`get_stock_code_name` 使用该 16 字节名；更长的场内基金全名只在 `get_etf_code_name` 经 `infoharbor_ex.name` 提供。
2. 码表磁盘缓存 `format_version` 升至 2，版本不符的旧缓存直接丢弃。
3. 版本号：`pyproject.toml` 与 `__init__.__version__` 对齐为 `2.0.5`。

## v2.0.4 - 2026-09-21

### Summary
1. `get_etf_code_name` 改为标准行情 `7709` 拉取命名文件：`infoharbor_ex.name` 提供完整名，`spec/specetfdata.txt`/`spec/speclofdata.txt` 提供板块成分（0x02C5 元数据 + 0x06B9 分页，内存 zlib 解压）；缺名回退 std 码表。不读银河安装目录。
2. 配置：`market_rules.etf_name_remote_file`、`market_rules.etf_board_remote_files`、`pagination.named_file_chunk_size`（默认 30000）。
3. 版本号：`pyproject.toml` 与 `__init__.__version__` 对齐为 `2.0.4`。

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
