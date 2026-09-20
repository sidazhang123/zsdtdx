# 标准/扩展行情：短地址池与并发亲和（后续方案）

本文记录针对 `extended 无可用连接`（及对称的标准侧过载风险）的成因结论、已落地改动、待做方案与残留问题。  
**当前仅落地短池探测裁剪；进程亲和/配额调度尚未实现。**

## 1. 问题与根因（已验证）

### 1.1 现象

全市场（深沪京 + 港股）一次 `get_stock_kline(mode="async")` 大任务时：

- A 股（标准行情）基本成功；
- 港股大量任务失败，错误原文为 **`extended 无可用连接`**；
- 同批次仍有部分港股成功 → 并非本机到对方的连接被整批掐断。

### 1.2 抛错点（精确）

`PersistentFailoverPool.call()` 在扩展池上：

- `_ensure_connected()` 返回 `False` 时直接 `raise RuntimeError(f"{self.name} 无可用连接")`；
- 含义是：**当前 worker 线程在发业务请求前，对配置内所有 extended host 建连（TCP + setup）全部失败**；
- 不是 `extended.<method> 调用失败: ...`（那是已有连接后的业务调用失败）。

### 1.3 结构性原因

1. 默认 `hosts.extended` 只有少数银河站；旧探测逻辑在可达 ≥ 2 时还会**去掉最慢 1 个**，常裁成 **1 站**。
2. 并行进程数由 `cpu × process_count_core_multiplier` 决定（常见约 40）；A/H **混在同一 job、同一进程池**，港股与 A 股按代码路由分别走 `ex_pool` / `std_pool`，**不会**自动拆成两次 `get_stock_kline`。
3. 预热会尝试 std/ex，但进程级预热成功只统计 `pid > 0`，**不以 `ex_ok` 为门槛** → 预热可通过，拉港股时才爆。
4. stock chunk 遇「无可用连接」时的自愈路径偏 **standard**，扩展侧重连不对称。

网卡层面连接数翻倍通常**不是**主因；瓶颈更贴近 **少数远端 ex 站上的并发建连/握手**。

## 2. 已落地（本轮）

### 2.1 短地址池不再剔最慢站

文件：`src/zsdtdx/unified_client.py` → `_tcp_probe_and_trim_available_hosts`

- 常量写死：`_SHORT_HOST_POOL_NO_DROP_SLOWEST_MAX = 3`（**不提供 YAML 配置**）。
- 规则：
  - 始终剔除不可达，可达按延迟升序；
  - **仅当配置侧 `len(hosts) > 3` 且可达数 ≥ 2** 时，去掉最慢 1 个；
  - **配置侧 ≤ 3**：保留全部可达站，便于 rotate。

意图：避免 extended（常见 2 站）被裁成 1 站，先抬高可用地址余量。

## 3. 待做方案：按地址数限制两侧占用的进程亲和

### 3.1 目标态思路

在 **std/ex 任务混合存在于同一并行 job** 时预先计算：

1. **总进程数**（由更宽裕一侧撑满池子，并仍受 CPU 倍率上限约束），示意：

   ```text
   total = max(
     min(std_host_count * 5, cpu_core * multiplier),
     min(ex_host_count * 5, cpu_core * multiplier),
   )
   ```

   （`*5` 与现网 `process_count_core_multiplier` 量级对齐，实施时可再标定。）

2. **窄侧配额**：地址少的市场只占用上述进程中的有限子集，例如  
   `side_workers = min(side_host_count * 5, total)`。  
   该侧任务**只平铺进这些进程**；宽侧任务平铺进全部 `total` 进程。

3. 例：`std≈10、ex=1、cpu×mult≈40` → 总进程 40；ex 任务只进约 5 个进程；std 进全部 40。

4. A/H **仍可同一次** `get_stock_kline`；拆分在调度层完成，不必强迫调用方拆两次 API。

### 3.2 为何认为能对症

对准的是根因「**少站被过多客户端同时建连**」。若任意时刻对每个 ex 站的并发占用被压到与站数相关的上限，上次那种大批 `extended 无可用连接` 有望大幅下降。

## 4. 仍存在的问题 / 实施闭环缺口（未做前必须处理）

下列问题**不会**仅靠「短池不剔最慢」自动消失；做亲和方案时必须一并设计。

### 4.1 进程内并发（inproc）会放大真实连接数

配置项 `parallel.task_chunk_inproc_coroutine_workers`（默认 3）使每进程可有多条线程本地连接。  
若只限「5 个进程」却不限 inproc，峰值可能是 `5 × 3` 而非 5。  
**配额公式必须计入：占用进程数 × 会建该侧连接的线程数。**

### 4.2 预热会拆台

当前全量 worker 预热都会碰 ex。若仍 40 进程全预热 ex，亲和配额未生效前启动瞬间又是满扇出。  
需要：**ex 懒建连**，或**仅 ex 配额进程预热 ex**；进程级预热是否应以 `ex_ok` 为门槛需单独决策（过严会导致有港股任务时启动失败）。

### 4.3 Worker 亲和在现有进程池上不是免费能力

`ProcessPoolExecutor` 默认轮询分发，**不能保证**某类 chunk 只落到某几个 PID。  
需要显式机制（指定 worker 子集、双队列、分调度器等），否则纸面配额落不到实处。

### 4.4 扩展连接自愈不对称

stock 路径「无可用连接」重建偏 std。窄侧进程上偶发断线后，若不能对等重建 ex，错误会残留。亲和落地时应补 **extended 侧重连/rotate**。

### 4.5 服务器硬限额未知

即使压到「站数×系数」，对方若对单客户端更严，仍会失败 → 需可调系数、加站、或失败码分时重试。  
加站（例如恢复更多 extended 备查站）是抬高容量上限的另一条杠杆，与配额互补。

### 4.6 本方案不覆盖的问题

- 港股 K 线 bar 奇异值（负成交额、高低价与开收不一致等）；
- `no_data`（窗口内无行情）；
- 期货路径（`get_future_kline` 无 async task 亲和模型，另议）。

## 5. 建议实施顺序（以后做）

1. 短池裁剪（**已完成**）。
2. 设计并可测的 **std/ex chunk 亲和调度**（含 inproc 计入配额）。
3. 预热策略与亲和一致（懒建连 / 子集预热）。
4. extended 与 standard 对称的连接自愈。
5. （可选）扩充 `hosts.extended` 默认/备查站。
6. 用「京沪深 + 全港股」多周期回归，对比 `extended 无可用连接` 率与总耗时。

## 6. 相关代码入口

| 主题 | 入口 |
|------|------|
| 探测裁剪 | `unified_client._tcp_probe_and_trim_available_hosts` |
| 并行进程数 | `parallel_fetcher` + `process_count_core_multiplier` |
| 预热 | `prewarm_parallel_fetcher` / `_worker_warmup_probe` |
| 无可用连接抛错 | `PersistentFailoverPool.call` → `_ensure_connected` |
| stock 重连目标推断 | `_infer_recover_target_from_chunk`（stock 现为 `"std"`） |
