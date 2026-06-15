#!/usr/bin/env python3
"""降量随规模扫描：对一组节点数跑 random vs scored_reduce，画「降幅 vs 节点数」曲线。

对接 4.4.3：考核在 100 节点规模测「全局传输总量降 30%」。本扫描在 localhost 取若干
规模点，外推趋势，验证降量随节点数增长并跨过 30%。

用法:
    python3 research/harness/sweep.py --sizes 6 12 18 24 30 --rows 10
    # 产出 research/harness/reduction_vs_nodes.png + 控制台表格
"""

import argparse
import os

import matplotlib
matplotlib.use("Agg")  # 无显示环境
import matplotlib.pyplot as plt

from run import run_once  # 复用单次实验逻辑

HERE = os.path.dirname(os.path.abspath(__file__))


def drop_pct(cur, ref):
    return (1 - cur / ref) * 100 if ref else 0.0


def mean(xs):
    return sum(xs) / len(xs) if xs else 0.0


def measure(n, rows, repeats):
    """重复 repeats 次跑 random+scored_reduce，返回各次降幅列表 + 均值。

    单次 localhost 实验方差大(gossip 时机/重传/anti-entropy 兜底非确定)，
    取多次均值提取信号、用 min~max 作误差带。
    """
    push_drops, total_drops = [], []
    for _ in range(repeats):
        base = run_once(n, rows, "random")
        red = run_once(n, rows, "scored_reduce")
        if not base or not red:
            continue
        bm, rm = base["metrics"], red["metrics"]
        b_push = bm["corro.broadcast.sent.bytes"]
        r_push = rm["corro.broadcast.sent.bytes"]
        b_total = b_push + bm["corro.sync.chunk.sent.bytes"]
        r_total = r_push + rm["corro.sync.chunk.sent.bytes"]
        push_drops.append(drop_pct(r_push, b_push))
        total_drops.append(drop_pct(r_total, b_total))
    if not push_drops:
        return None
    return {"push": push_drops, "total": total_drops,
            "push_mean": mean(push_drops), "total_mean": mean(total_drops)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sizes", type=int, nargs="+", default=[6, 12, 18, 24, 30])
    ap.add_argument("--rows", type=int, default=20)
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--out", default=os.path.join(HERE, "reduction_vs_nodes.png"))
    args = ap.parse_args()

    print(f"== 降量规模扫描: sizes={args.sizes}, rows={args.rows}/表, "
          f"repeats={args.repeats} ==\n")
    sizes, push_means, total_means = [], [], []
    push_err, total_err = [[], []], [[], []]  # [下误差, 上误差]
    print(f"{'节点数':<8}{'推送降幅%(均值[min~max])':<32}{'总传输降幅%(均值[min~max])':<32}")
    for n in args.sizes:
        r = measure(n, args.rows, args.repeats)
        if not r:
            print(f"{n:<8}跳过(节点未就绪/超时)")
            continue
        sizes.append(n)
        pm, tm = r["push_mean"], r["total_mean"]
        push_means.append(pm)
        total_means.append(tm)
        push_err[0].append(pm - min(r["push"]))
        push_err[1].append(max(r["push"]) - pm)
        total_err[0].append(tm - min(r["total"]))
        total_err[1].append(max(r["total"]) - tm)
        print(f"{n:<8}{pm:>6.1f} [{min(r['push']):.0f}~{max(r['push']):.0f}]"
              f"{'':<14}{tm:>6.1f} [{min(r['total']):.0f}~{max(r['total']):.0f}]")

    if not sizes:
        print("无有效数据点，无法绘图")
        return

    # 图内用英文标签(matplotlib 默认字体缺中文会显示豆腐块)；控制台表格仍中文。
    plt.figure(figsize=(7, 4.5))
    plt.errorbar(sizes, push_means, yerr=push_err, fmt="o-", capsize=4,
                 label="push bytes reduction", color="#2563eb")
    plt.errorbar(sizes, total_means, yerr=total_err, fmt="s--", capsize=4,
                 label="total transfer reduction", color="#16a34a")
    plt.axhline(30, color="#dc2626", ls=":", label="target 30%")
    plt.axhline(0, color="#999", lw=0.8)
    plt.xlabel("number of nodes")
    plt.ylabel("reduction vs broadcast baseline (%)")
    plt.title(f"scored_reduce: reduction vs cluster size "
              f"(mean of {args.repeats}, error=min~max)")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(args.out, dpi=130)
    print(f"\n趋势图已保存: {args.out}")


if __name__ == "__main__":
    main()
