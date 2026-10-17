"""模块：`manual/render_thread_refactor_report.py`。

职责：把已完成的基准、真实对拍和故障注入结果汇总为独立 HTML。
边界：只读取本地验收产物；不执行网络请求，不引用外部静态资源。
"""

from __future__ import annotations

from collections import defaultdict
import html
import json
from pathlib import Path
import statistics

import yaml

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]


def read_json(path):
    """输入本地路径，输出 JSON 值；缺少验收产物时明确失败。"""
    return json.loads(path.read_text(encoding="utf-8"))


def median(values):
    """输入非空数列，输出五轮中位数；不把单次最快结果作为结论。"""
    return statistics.median(values)


def main():
    """输入相邻验收配置，输出独立 HTML 与数值 JSON；要求全部验收记录完整。"""
    cfg = yaml.safe_load(
        (HERE / "thread_columnar_benchmark.yaml").read_text(encoding="utf-8")
    )
    results = read_json(HERE / cfg["artifact_dir"] / "summary.json")
    contracts = read_json(
        HERE / "artifacts/refactor/benchmark_final/contract_summary.json"
    )
    fault = read_json(HERE / "artifacts/refactor/thread_fault_injection.json")
    packet_dir = HERE / "artifacts/refactor/benchmark_final/captured_kline_frames"
    parser_types = {
        read_json(path)["parser"] for path in packet_dir.glob("*/request.json")
    }
    assert parser_types == {
        "GetSecurityBarsCmd",
        "GetIndexBarsCmd",
        "GetBlockBarsCmd",
        "GetInstrumentBars",
    }
    assert all(item["passed"] for item in contracts.values()) and fault["passed"]
    assert not fault["residual_fetch_threads"]
    groups = defaultdict(list)
    for item in results:
        groups[item["workload"]].append(item)
    assert len(groups) == 3 and all(len(group) == 5 for group in groups.values())
    names = {
        "demo_10k": "一万任务 / 250 标的 / 多窗口长历史",
        "full_market_daily": "全市场 / 每标的一个日线窗口",
        "mixed_periods": "12 标的 × 七周期 / 固定单 host",
    }
    sections, data = [], {}
    for name, rounds in groups.items():
        metrics = {}
        for engine in ("baseline", "thread"):
            warm = [item["engines"][engine][1] for item in rounds]
            metrics[engine] = {
                field: median([item[field] for item in warm])
                for field in ("wall_seconds", "cpu_seconds", "peak_rss_bytes")
            }
        old, new = metrics["baseline"], metrics["thread"]
        speed = old["wall_seconds"] / new["wall_seconds"]
        memory_saved = 1 - new["peak_rss_bytes"] / old["peak_rss_bytes"]
        cpu_remaining = new["cpu_seconds"] / old["cpu_seconds"]
        for item in rounds:
            for a, b in zip(item["engines"]["baseline"], item["engines"]["thread"]):
                assert all(
                    a[field] == b[field]
                    for field in ("tasks", "rows", "failures", "digest")
                )
        sample = rounds[0]["engines"]["thread"][1]
        metrics.update(
            speedup=speed,
            memory_saved=memory_saved,
            cpu_remaining=cpu_remaining,
            tasks=sample["tasks"],
            rows=sample["rows"],
            error_kinds=sample["error_kinds"],
        )
        data[name] = metrics
        rows = ""
        for field, label, divisor, unit in (
            ("wall_seconds", "耗时", 1, "秒"),
            ("cpu_seconds", "进程树 CPU 总时间", 1, "CPU 秒"),
            ("peak_rss_bytes", "进程树峰值 RSS", 1024**2, "MiB"),
        ):
            rows += f"<tr><th>{label}</th><td>{old[field] / divisor:.2f} {unit}</td><td>{new[field] / divisor:.2f} {unit}</td></tr>"
        sections.append(
            f"<section><h2>{names[name]}</h2><p class='metric'>{speed:.2f}× 耗时加速比 <span>内存下降 {memory_saved:.1%} · CPU 总时间为旧版 {cpu_remaining:.1%}</span></p><table><thead><tr><th>热批次五轮中位数</th><th>旧多进程</th><th>最终线程版</th></tr></thead><tbody>{rows}</tbody></table><p>{sample['tasks']:,} 项任务、{sample['rows']:,} 行；错误类型 {html.escape(str(sample['error_kinds']))}。每轮冷/热完整指纹一致。</p></section>"
        )
    cold = [item["engines"]["thread"][0]["wall_seconds"] for item in groups["demo_10k"]]
    stage = {}
    for name in ("columnar_attempt", "materialization"):
        stage[name] = median(
            [
                item["engines"]["thread"][1]["pipeline"][name]["cpu_seconds"]
                for item in groups["demo_10k"]
            ]
        )
    contract_rows = "".join(
        f"<li>{html.escape(name)}：{item['thread']['rows']} 行，字段摘要一致</li>"
        for name, item in contracts.items()
    )
    document = f"""<!doctype html><html lang="zh-CN"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>zsdtdx 线程与列式改造验收</title>
<style>body{{font:16px/1.7 system-ui,'Microsoft YaHei',sans-serif;background:#f3f6fa;color:#172d42;margin:0}}main{{max-width:1060px;margin:auto;padding:38px 22px}}section,header{{background:#fff;border:1px solid #dbe4ef;border-radius:14px;padding:25px;margin-bottom:20px}}h1{{font-size:30px;line-height:1.3}}h2{{font-size:21px}}.tag{{color:#16745b;font-weight:700}}.metric{{font-size:24px;font-weight:700}}.metric span{{display:block;font-size:15px;font-weight:400;color:#50647c}}table{{width:100%;border-collapse:collapse}}td,th{{text-align:left;padding:11px;border-bottom:1px solid #e6edf4}}thead{{background:#f1f5fa}}code{{background:#f0f4f8;padding:2px 5px}}li{{margin:7px 0}}.note{{border-left:4px solid #da9b2c;padding-left:16px}}small{{color:#526980}}@media(max-width:600px){{main{{padding:15px}}section,header{{padding:18px}}h1{{font-size:24px}}td,th{{font-size:13px;padding:7px}}}}</style>
<main><header><p class="tag">2026-10-17 · 基线 143da95 · Windows / 系统 Python 3.10</p><h1>线程抓取 + numba 解析 + 列式内核</h1><p>已移除多进程执行路径。准确性与故障恢复验收通过；性能结果按任务形态分别报告，未达到所有软目标。</p><p>完整回归 <b>389 项通过</b>。核心新增依赖 numba；现有 simple_api 的 rows、DataFrame 和队列 data/done 继续保留。</p></header>
{"".join(sections)}
<section><h2>测量口径与边界</h2><ul><li>先完成三组各五轮旧新交替测量；运行时复查后的最终代码再分别复测五轮，表中旧版指标引用已完成的原五轮，最终版与旧版不是同轮交替执行。未与测试或构建并行。</li><li>仅采样本次子进程及后代；CPU 为累计 CPU 秒，不是全机占用百分比。RSS 为各进程工作集之和，含重复映射页，并非独占物理内存。单位 MiB。</li><li>探测与配置加载在计时前；cold 含预热、JIT 编译/缓存加载和首次建连，warm 复用执行器、目录与连接。最终版一万任务首轮 cold 为 {cold[0]:.2f} 秒；后四轮缓存 cold 中位数 {median(cold[1:]):.2f} 秒。部分冷批次发生 chunk 重试，不能把全部差额视为 JIT 编译耗时，也不能据热启动结果承诺首次调用同样耗时。</li><li>一万任务热批次，列式抓取阶段累计线程 CPU 中位数约 {stage["columnar_attempt"]:.2f} 秒，行对象打包约 {stage["materialization"]:.2f} 秒；这些是线程累计值，不等于端到端墙钟，也不包含全部入口调度开销。</li></ul><p class="note">单 host 小批次热启动耗时退步：新单站参数约束实际工作连接，旧进程配额还可叠加进程内并发。没有为追求速度放宽该保护。结果仍保留行对象，因此一万任务内存降幅未达一半；CPU 总时间也未达旧版 10%。</p></section>
<section><h2>准确性与稳健性证据</h2><ul>{contract_rows}<li>一万任务故障注入：三轮关闭 {sum(fault["closed_connections"])} 条连接，恢复后 {fault["fault"]["rows"]:,} 行、全部任务指纹一致；销毁待处理数为零，无残留抓取线程。</li><li>固定二进制覆盖标准差分、指数家数、板块绝对价、扩展固定记录和历史时区；10,003 个量能值与旧函数按浮点位比较。覆盖截断、越界、累计滴流超时、复用连接重新登记预算、取消与连接归还竞态、单桶超时及并发作业隔离。</li><li>314 / 13 / 24 项无数据分别属于三组输入的合法空窗口，旧新完全一致；不将未取得合法回包的传输失败当成 no_data。</li></ul><p>跨 API 对拍对标准、指数、板块和扩展协议的 11 份实际回包固定重放，比较完整公开入口的解析、分页和交付；公司信息实时对拍。混合周期性能基准固定同一 host。初测发现不同扩展 host 的港股成交量、不同标准 host 的历史分钟线切分存在差异，固定同地址也观察到旧版波动；本次保留该证据，未擅自做跨站合成或修正。</p></section>
<section><h2>ZIP 思路的工程落地</h2><ul><li>借鉴释放 GIL 的有界解码、零拷贝输入、NumPy 列、覆盖即停分页、去重与闭区间切片；分钟线、指数、板块、港股和期货按本工程协议补齐。</li><li>保留原任务分桶与稳定排序、host 探测排序、多站均衡、请求恢复阶梯、chunk 重试、自适应降配额和冷却恢复。收发失败先废弃连接，完整空响应与传输失败分开处理。</li><li>没有照搬 Demo 的日线专用格式、UTC+8 假设、环境版本锁定、简化冷却策略及同站超配。旧进程 YAML 字段移除，使用 thread_workers 与 threads_per_host_std/ex；worker_pid 移除。</li><li>预热集中编译且不连业务站；重启先中断旧 socket 并等待真实回收。原生扩展或解释器死锁仍需外部重启，无法安全强杀 Python 线程。</li></ul></section>
<section><h2>后续 simple_api 列式交付</h2><p>当前正式内核产物为只读 NumPy 列页，分页合并与窗口切片持有有效数组引用；现有行格式只在独立交付适配层构造。没有提前改变公开返回格式。</p><p>后续队列可直接交付列式批次，把 DataFrame、字典和其他格式打包留给用户侧。启用前需明确 schema 版本、字段/dtype、有效行数、时间单位与时区、缺失值、数组所有权/生命周期及回调协议，再单独修改公开契约。</p><p>现有验证覆盖所列样本和故障场景，不能证明所有服务器回包和所有 Python/操作系统组合永远无缺陷。wheel/sdist 构建及提取 wheel 的导入、解码预热、执行器回收冒烟已通过；未提交、推送或发布。</p></section>
<small>原始证据：tests/manual/artifacts/refactor/benchmark_final（交替五轮、契约对拍），benchmark_verified（最终代码五轮），thread_fault_injection.json。方案：docs/parallel_thread_columnar_plan.md。</small></main></html>"""
    destination = ROOT / "docs/parallel_thread_columnar_results.html"
    destination.write_text(document, encoding="utf-8")
    (ROOT / "docs/parallel_thread_columnar_metrics.json").write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(data, ensure_ascii=False, indent=2))
    print(destination)


if __name__ == "__main__":
    main()
