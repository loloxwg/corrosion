"""交付证据：score heatmap + 方差感知 + 消融(里程碑收口, 对接 4.3.3 技术报告)。

产出 3 张图 + 1 张消融表，证明：
  ① GNN 学到了"适配度评分函数"(数据-平台 score heatmap)；
  ② RL 微调学会避开高方差链路(placement 概率随 link_var 下降，监督 GNN 不会)；
  ③ 提升归因(消融：random / rule / GNN监督 / GNN+RL)。

依赖 torch + matplotlib。用法: python3 research/rl/evaluate.py
"""
from __future__ import annotations

import copy
import numpy as np
import torch

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from env import Situation
from baselines import rule_placement, random_placement, greedy_placement
from model import BipartiteGNN, situation_tensors, scores_to_placement
from rl import pretrain, rl_finetune, robust_cost

OUT = __import__("os").path.dirname(__import__("os").path.abspath(__file__))


def train_models(train_sits):
    sup = BipartiteGNN(h=32, rounds=2)
    pretrain(sup, train_sits)
    rl = copy.deepcopy(sup)        # RL 从监督模型出发微调
    rl_finetune(rl, train_sits)
    return sup, rl


def heatmap(model, sit, fname):
    """数据-平台 适配度 score(sigmoid) 热图 = 学到的评分函数可视化。"""
    with torch.no_grad():
        prob = torch.sigmoid(model(situation_tensors(sit))).numpy()
    fig, ax = plt.subplots(figsize=(8, 3.2))
    im = ax.imshow(prob, aspect="auto", cmap="viridis", vmin=0, vmax=1)
    ax.set_yticks(range(sit.D))
    ax.set_yticklabels([d["name"] for d in sit.data])
    ax.set_xticks(range(sit.n))
    ax.set_xticklabels([f"{p}\n{r[:2]}" for p, r in enumerate(sit.roles)], fontsize=7)
    ax.set_xlabel("platform (id / role)")
    ax.set_title("Learned fitness score(data, platform)  [GNN+RL]")
    fig.colorbar(im, ax=ax, label="placement prob")
    fig.tight_layout()
    fig.savefig(f"{OUT}/{fname}", dpi=130)
    plt.close(fig)


def variance_plot(sup, rl, sit, fname):
    """对一个高写数据类型，画 placement 概率 vs 平台 link_var：RL 应随方差下降。"""
    d = 0  # telem_recon(高写)
    needers = sorted(sit.data[d]["needers"])
    with torch.no_grad():
        ps = torch.sigmoid(sup(situation_tensors(sit)))[d].numpy()
        pr = torch.sigmoid(rl(situation_tensors(sit)))[d].numpy()
    xs = [sit.link_var[p] for p in needers]
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.scatter(xs, [ps[p] for p in needers], label="GNN supervised", marker="o")
    ax.scatter(xs, [pr[p] for p in needers], label="GNN+RL", marker="s")
    ax.set_xlabel("platform link variance (unreliability)")
    ax.set_ylabel(f"placement prob for '{sit.data[d]['name']}'")
    ax.set_title("RL avoids high-variance links (supervised does not)")
    ax.legend(); ax.grid(alpha=0.3)
    fig.tight_layout(); fig.savefig(f"{OUT}/{fname}", dpi=130); plt.close(fig)


def ablation(sup, rl, test_sits, fname):
    def cost(strategy):
        out = []
        for sit in test_sits:
            if strategy == "random":
                pl = random_placement(sit, seed=1)
            elif strategy == "rule":
                pl = rule_placement(sit)
            elif strategy == "greedy":
                pl = greedy_placement(sit)
            elif strategy == "gnn_sup":
                pl = scores_to_placement(sit, sup(situation_tensors(sit)))
            elif strategy == "gnn_rl":
                pl = scores_to_placement(sit, rl(situation_tensors(sit)))
            out.append(robust_cost(sit, pl))
        return float(np.mean(out))

    names = ["random", "rule\n(human interest)", "greedy\n(mean-only)",
             "GNN\nsupervised", "GNN+RL\n(ours)"]
    keys = ["random", "rule", "greedy", "gnn_sup", "gnn_rl"]
    with torch.no_grad():
        vals = [cost(k) for k in keys]
    fig, ax = plt.subplots(figsize=(7, 4))
    bars = ax.bar(names, vals, color=["#999", "#c44", "#e90", "#48c", "#2a2"])
    ax.set_ylabel("robust cost (lower=better)")
    ax.set_title("Ablation: robust transfer cost under link uncertainty")
    for b, v in zip(bars, vals):
        ax.text(b.get_x() + b.get_width() / 2, v, f"{v:.0f}", ha="center", va="bottom")
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout(); fig.savefig(f"{OUT}/{fname}", dpi=130); plt.close(fig)
    return dict(zip(keys, vals))


def main():
    torch.manual_seed(0)
    train_sits = [Situation(n_platforms=12, seed=s) for s in range(40)]
    test_sits = [Situation(n_platforms=12, seed=s) for s in range(100, 130)]
    print("训练 监督 + RL ...")
    sup, rl = train_models(train_sits)

    sit = test_sits[0]
    heatmap(rl, sit, "fig_score_heatmap.png")
    variance_plot(sup, rl, sit, "fig_variance_aware.png")
    vals = ablation(sup, rl, test_sits, "fig_ablation.png")

    print("\n== 消融(扰动/风险态势下鲁棒成本均值,越低越好)==")
    for k in ["random", "rule", "greedy", "gnn_sup", "gnn_rl"]:
        print(f"  {k:<10} {vals[k]:.1f}")
    print(f"\nGNN+RL 相对 rule 省 {(1-vals['gnn_rl']/vals['rule'])*100:.1f}%、"
          f"相对 greedy 省 {(1-vals['gnn_rl']/vals['greedy'])*100:.1f}%")
    print("图已存: fig_score_heatmap.png / fig_variance_aware.png / fig_ablation.png")


if __name__ == "__main__":
    main()
