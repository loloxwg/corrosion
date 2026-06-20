"""placement 基线策略 + 对照跑分(里程碑 2/3)。

- rule   : 存给所有需要者(= 当前人工 interest 规则)。push 高、query=0。
- random : 随机 placement(经 repair 合法)。
- greedy : 启发式权衡——每类数据按"多存省查 vs 少存省推"逐平台增量决定(RL 的目标参照)。

跑分证明：高写低查数据上 rule 次优，greedy 更省 → RL 有可赢空间。
用法: python3 research/rl/baselines.py
"""
from __future__ import annotations

import numpy as np

from env import Situation


def rule_placement(sit: Situation):
    """存给所有需要者(人工 interest 规则)。"""
    return [set(d["needers"]) for d in sit.data]


def random_placement(sit: Situation, seed=0):
    rng = np.random.default_rng(seed)
    out = []
    for _ in range(sit.D):
        k = rng.integers(0, sit.n + 1)
        out.append(set(rng.choice(sit.n, size=k, replace=False).tolist()))
    return out


def greedy_placement(sit: Situation):
    """逐类数据：从 critical 起，只要"多加一个需要者作持有者"能降总成本就加。
    本质：写量大→少存(查便宜),查询量大→多存(避免路由)。这是 RL 应逼近/超过的参照。"""
    placement = [set(d["critical"]) for d in sit.data]
    placement = sit.repair(placement)
    for d, spec in enumerate(sit.data):
        candidates = list(spec["needers"] - placement[d])
        improved = True
        while improved and candidates:
            improved = False
            best, best_gain = None, 0.0
            base, _ = sit.cost(placement)
            for c in candidates:
                trial = [set(h) for h in placement]
                trial[d].add(c)
                t, _ = sit.cost(trial)
                gain = base - t
                if gain > best_gain:
                    best, best_gain = c, gain
            if best is not None:
                placement[d].add(best)
                candidates.remove(best)
                improved = True
    return placement


def run(strategy_fn, sit, **kw):
    pl = strategy_fn(sit, **kw) if "seed" in kw else strategy_fn(sit)
    total, parts, repaired = sit.evaluate(pl)
    avg_holders = np.mean([len(h) for h in repaired])
    feasible = sit.feasible(repaired)
    return total, parts, avg_holders, feasible


def main():
    print("态势: 12 平台(侦察/打击/干扰) × 5 类数据(遥测高写低查 / 目标中写高查)\n")
    sits = [Situation(n_platforms=12, seed=s) for s in range(5)]
    strategies = {"rule(人工interest)": rule_placement,
                  "random": lambda s: random_placement(s, seed=1),
                  "greedy(启发式)": greedy_placement}
    print(f"{'策略':<22}{'总成本(均值)':<14}{'push':<10}{'query':<10}{'平均持有者':<12}{'合法':<6}")
    for name, fn in strategies.items():
        totals, pushes, queries, holders, feas = [], [], [], [], []
        for sit in sits:
            t, parts, ah, ok = run(fn, sit)
            totals.append(t); pushes.append(parts["push"])
            queries.append(parts["query"]); holders.append(ah); feas.append(ok)
        print(f"{name:<22}{np.mean(totals):<14.1f}{np.mean(pushes):<10.1f}"
              f"{np.mean(queries):<10.1f}{np.mean(holders):<12.2f}{all(feas)!s:<6}")

    rule_cost = np.mean([run(rule_placement, s)[0] for s in sits])
    greedy_cost = np.mean([run(greedy_placement, s)[0] for s in sits])
    print(f"\ngreedy 相对 rule 降成本: {(1 - greedy_cost/rule_cost)*100:.1f}%"
          f"  → 规则基线有可赢空间，RL 目标是逼近/超过 greedy")


if __name__ == "__main__":
    main()
