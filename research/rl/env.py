"""态势数据智能推送 —— 仿真环境(里程碑 1/3, 对接 4.3.3)。

把"placement(每类数据存在哪些平台)"映射到全局传输成本 + 硬约束，作为 RL 的环境。
成本模型(可校准)：
  push_cost(d)  = 写量(d) × 复制因子(|placement(d)|)   # 存得多→复制贵(gossip 放大近似)
  query_cost(d) = Σ 需要 d 但不本地存的平台: 查询量 × 路由开销  # 存得少→查询贵
  total = Σ_d (push + query)
RL 要学的就是这个权衡：高写低查的数据少存(让需要者去查)，高查低写的数据多存。
规则基线"存给所有需要者"在高写低查数据上次优 → RL 有可赢空间。

硬约束(防奖励 hacking)：critical 消费者必须本地存；每类数据 ≥min_replicas 个持有者。
repair() 保证任何 placement 都先满足约束，再比成本。

仅依赖 numpy。校准(逐分量拟合 harness)留后续(见 rl-design §7.5)。
"""
from __future__ import annotations

import numpy as np

ROLES = ["recon", "strike", "jam"]


class Situation:
    """一个态势：N 个平台 + D 类数据 + 各自的写/查/需要者/critical。"""

    def __init__(self, n_platforms=12, seed=0, min_replicas=1,
                 push_amp=1.0, route_cost=1.0):
        rng = np.random.default_rng(seed)
        self.rng = rng
        self.n = n_platforms
        self.min_replicas = min_replicas
        self.push_amp = push_amp        # gossip 复制放大系数(校准点)
        self.route_cost = route_cost    # 单次跨平台查询路由开销(校准点)

        # 平台角色(轮转)
        self.roles = [ROLES[i % len(ROLES)] for i in range(self.n)]
        # 每平台链路/存储成本：差链路(远/丢包/低带宽)上存数据更贵。
        # 真实来源 500Kbps + RTT 差异;这让"存在哪个平台"有差别(RL 该挑便宜链路)。
        self.link_cost = rng.uniform(0.5, 2.5, self.n)

        # 数据类型：(名字, 写量, 单平台查询量, 需要者角色, critical 比例)
        # 遥测=高写低查、窄需求;目标=低写高查、宽需求 —— 制造权衡。
        # critical = 需要者里的"硬 SLA 子集"(其余可容忍查询延迟,可被 RL 丢)。
        specs = [
            ("telem_recon", 20.0, 0.3, ["recon"], 0.34),
            ("telem_strike", 20.0, 0.3, ["strike"], 0.34),
            ("telem_jam",    20.0, 0.3, ["jam"], 0.34),
            ("battlefield",   6.0, 2.5, ["strike"], 0.5),
            ("target",        3.0, 5.0, ["strike", "jam"], 0.34),
        ]
        self.data = []
        for name, wv, qv, need_roles, crit_frac in specs:
            needers = [p for p in range(self.n) if self.roles[p] in need_roles]
            # critical = 需要者中 crit_frac 比例(至少 1)；其余需要者可被丢去查询
            n_crit = max(1, int(round(len(needers) * crit_frac)))
            critical = set(sorted(needers)[:n_crit])
            self.data.append({
                "name": name, "write_vol": wv, "query_vol": qv,
                "needers": set(needers), "critical": critical,
            })
        self.D = len(self.data)
        # 加一点平台级写量扰动(让态势有差异)
        self.write_jitter = rng.uniform(0.8, 1.2, self.D)

    def repair(self, placement):
        """把 placement 修成合法：含 critical + 至少 min_replicas 个持有者(从需要者里补)。"""
        fixed = []
        for d, holders in enumerate(placement):
            spec = self.data[d]
            h = set(holders) | spec["critical"]
            need = spec["needers"]
            # 补到 min_replicas(优先从需要者里补，再从全体补)
            pool = list(need - h) + [p for p in range(self.n) if p not in h and p not in need]
            i = 0
            while len(h) < self.min_replicas and i < len(pool):
                h.add(pool[i]); i += 1
            fixed.append(h)
        return fixed

    def cost(self, placement):
        """给定(已 repair 的)placement，返回 (总成本, 分项 dict)。"""
        push = query = 0.0
        for d, holders in enumerate(placement):
            spec = self.data[d]
            wv = spec["write_vol"] * self.write_jitter[d]
            # 推送/复制成本：存在哪些平台都要付该平台链路成本(差链路更贵)。
            # gossip 放大用 |holders|^(push_amp-1) 的群体因子近似(push_amp=1→无额外放大)。
            link_sum = sum(self.link_cost[p] for p in holders)
            push += wv * link_sum * (max(len(holders), 1) ** (self.push_amp - 1.0))
            # 查询成本：需要但不本地存的平台 → 路由
            routed = spec["needers"] - set(holders)
            query += len(routed) * spec["query_vol"] * self.route_cost
        return push + query, {"push": push, "query": query}

    def feasible(self, placement):
        """硬约束检查：critical 都在、持有者数 ≥min_replicas。"""
        for d, holders in enumerate(placement):
            spec = self.data[d]
            if not spec["critical"].issubset(holders):
                return False
            if len(holders) < self.min_replicas:
                return False
        return True

    def evaluate(self, placement):
        """先 repair 保证合法，再算成本。返回 (total, parts, repaired)。"""
        repaired = self.repair(placement)
        total, parts = self.cost(repaired)
        return total, parts, repaired
