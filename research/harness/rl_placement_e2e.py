#!/usr/bin/env python3
"""4.3.3 × 4.4.3 真跑：RL 学出的 placement 直接驱动真 corrosion 推送，量全局降量。

此前 RL 只在抽象 env 验证成本、在 rl_e2e_latency 验证单 holder 查询延迟。本脚本让 RL 的
**placement 决策真正驱动 corrosion 的推送目标平台**（合同 4.3.3「智能决策推送的目标平台」
在真系统兑现），并量它相对广播式的全局传输降幅（合同 4.4.3 ≥30%）：

  RL 模型（pretrain 监督 greedy + rl_finetune 加 link_var 风险）输出 placement（每张表放哪些节点）
    → 翻译成每节点 interest 配置（node p 关心表 d ⟺ p ∈ holders(d)）
    → 起真 corrosion 集群按它推送（selector 照旧按 interest 查表推，RL 不进热路径）
    → 量全局传输字节。

三策略对照（除 strategy/interest 外拓扑/行数/settle/超时全同，多轮取均值排 localhost 方差）：
  broadcast : strategy=random        —— 全发，合同「广播式」基线
  rule      : scored_reduce + 所有需要者都持有   —— 人工 interest，现状智能基线
  rl        : scored_reduce + RL 稀疏 placement  —— RL 决定目标平台，非 critical 需要者靠查询路由(4.4.2)按需取

关键：placement ≡ interest_routing。RL 决定 placement 就是决定 selector 推给谁。selector 一行不改。

诚实边界见文末。用法: python3 research/harness/rl_placement_e2e.py --nodes 9 --rows 30 --repeats 3
"""
import argparse
import os
import shutil
import signal
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "..", "rl"))
import run as H  # 复用 start/wait_active/scrape/count_rows/METRICS/BIN/WORK

# ── RL 侧：训练 + 任务态势 + placement → interest ──────────────────────────────

# corrosion 3 表任务规格（镜像 run.py 的 ROLE_TABLES；写/查量制造读写权衡）：
#   flight       高写低查、窄需求(recon)      → RL 该少存(让 recon 非 critical 去查)
#   battlefield  中写中查(strike)
#   target       低写高查、宽需求(strike+jam) → RL 该多存(查贵，避路由)
MISSION_SPECS = [
    ("flight", 20.0, 0.3, ["recon"], 0.34),
    ("battlefield", 6.0, 2.5, ["strike"], 0.5),
    ("target", 3.0, 5.0, ["strike", "jam"], 0.34),
]


def build_mission_situation(n, seed=0):
    """构造镜像 corrosion 3 表任务的 Situation：角色 i%3 轮转(=corrosion role_of)，
    数据=flight/battlefield/target，节点索引与 corrosion 节点一一对应。"""
    from env import Situation
    sit = Situation(n_platforms=n, seed=seed)  # 复用其 roles/link_cost/link_var
    sit.data = []
    for name, wv, qv, need_roles, crit_frac in MISSION_SPECS:
        needers = [p for p in range(n) if sit.roles[p] in need_roles]
        n_crit = max(1, int(round(len(needers) * crit_frac)))
        critical = set(sorted(needers)[:n_crit])
        sit.data.append({"name": name, "write_vol": wv, "query_vol": qv,
                         "needers": set(needers), "critical": critical})
    sit.D = len(sit.data)
    sit.write_jitter = sit.rng.uniform(0.8, 1.2, sit.D)
    return sit


def train_rl_model(n, epochs_pre=50, iters_rl=60):
    """pretrain(监督 greedy) + rl_finetune(REINFORCE 加 link_var 风险)。
    在 n 节点的通用态势集上训练；GNN 跨 D/P 泛化，用于任务态势推理。"""
    import torch
    from model import BipartiteGNN
    from rl import pretrain, rl_finetune
    from env import Situation
    torch.manual_seed(0)
    train_sits = [Situation(n_platforms=n, seed=s) for s in range(40)]
    model = BipartiteGNN(h=32, rounds=2)
    pretrain(model, train_sits, epochs=epochs_pre)
    rl_finetune(model, train_sits, iters=iters_rl)
    model.eval()
    return model


def rl_placement(model, sit):
    """RL 推理 → placement(每表 holder 节点集，经 repair 保证含 critical + ≥min_replicas)。"""
    from model import situation_tensors, scores_to_placement
    return scores_to_placement(sit, model(situation_tensors(sit)))


# 路由哨兵表：corrosion 语义「空 interest = 隐式全量节点，本地答不路由」(public/mod.rs:809)。
# 稀疏 placement 会让某些需要者什么都不持有→interest 空→被当全量→不路由(还全量对账泄漏)。
# 给这类「route-only」需要者一个非空哨兵 interest(一张恒空的 marker 表)，使其:
#   ① 对真实表 ∉interest → 真走查询路由;② interest_for_sync 非 None → 对账按表过滤不泄漏。
ROUTE_MARKER = "__route_only__"


def fill_route_only(interest):
    """holds 什么都没有的节点 → 给哨兵 interest(否则空=隐式全量,不路由+全量对账)。"""
    return {i: (v if v else [ROUTE_MARKER]) for i, v in interest.items()}


def placement_to_interest(sit, placement):
    """placement(每表 holder 集) → 每节点 interest 表名列表(空的填哨兵→可路由)。"""
    interest = {i: [] for i in range(sit.n)}
    for d, holders in enumerate(placement):
        name = sit.data[d]["name"]
        for p in holders:
            interest[p].append(name)
    return fill_route_only(interest)


def rule_interest(n):
    """规则式：所有需要者都持有(= run.py 的 ROLE_TABLES，现状智能基线)。"""
    return {i: list(H.ROLE_TABLES[H.role_of(i)]) for i in range(n)}


def random_cut_interest(sit, placement, seed):
    """同副本预算对照(Codex #3)：每张表砍到与 RL 相同的 holder 数，但**随机**选
    (必含 critical + ≥min_replicas 保正确)。若 RL 的字节优势只是"少副本"平凡收益，
    random_cut 会与 RL 相当；RL 唯有在同等稀疏度下仍更省，才证明学到读写权衡/方差感知。"""
    import numpy as np
    rng = np.random.default_rng(seed)
    interest = {i: [] for i in range(sit.n)}
    for d, holders in enumerate(placement):
        spec = sit.data[d]
        k = len(holders)                      # 与 RL 同 holder 数
        chosen = set(spec["critical"])        # 必含 critical
        pool = [p for p in range(sit.n) if p not in chosen]
        rng.shuffle(pool)
        for p in pool:
            if len(chosen) >= max(k, sit.min_replicas):
                break
            chosen.add(p)
        for p in chosen:
            interest[p].append(spec["name"])
    return fill_route_only(interest)


# ── corrosion 侧：按自定义 interest 起集群、量传输 ──────────────────────────────

def write_configs_custom(n, strategy, interest_map, base=(7500, 8500, 9500)):
    """同 run.py::write_configs，但每节点 interest 由 interest_map 指定(RL 或 rule)。"""
    bg, ba, bp = base
    schema_dir = os.path.join(H.WORK, "schema")
    os.makedirs(schema_dir, exist_ok=True)
    with open(os.path.join(schema_dir, "mission.sql"), "w") as f:
        for t in list(H.TABLES) + [ROUTE_MARKER]:
            f.write(f"CREATE TABLE {t} (id BLOB NOT NULL PRIMARY KEY, "
                    f"data TEXT NOT NULL DEFAULT '');\n")
        f.write("CREATE TABLE node_interest (actor_id BLOB NOT NULL, "
                "table_name TEXT NOT NULL, active INTEGER NOT NULL DEFAULT 1, "
                "PRIMARY KEY (actor_id, table_name));\n")
    nodes = []
    for i in range(n):
        gossip, api, prom = bg + i, ba + i, bp + i
        boot = "" if i == 0 else f'"[::1]:{bg}"'
        interest = ", ".join(f'"{t}"' for t in interest_map[i])
        cfg = os.path.join(H.WORK, f"node{i}.toml")
        with open(cfg, "w") as f:
            f.write(f"""[db]
path = "{H.WORK}/node{i}.db"
schema_paths = ["{schema_dir}"]
[gossip]
addr = "[::]:{gossip}"
external_addr = "[::1]:{gossip}"
bootstrap = [{boot}]
plaintext = true
broadcast_strategy = "{strategy}"
interest = [{interest}]
[api]
addr = "127.0.0.1:{api}"
[admin]
path = "{H.WORK}/node{i}-admin.sock"
[telemetry]
prometheus.addr = "127.0.0.1:{prom}"
""")
        nodes.append({"i": i, "role": H.role_of(i), "cfg": cfg, "api": api,
                      "prom": prom, "db": os.path.join(H.WORK, f"node{i}.db"),
                      "log": os.path.join(H.WORK, f"node{i}.log"),
                      "interest": list(interest_map[i])})
    return nodes


def converged_by_interest(nd, prefix, rows):
    """节点是否已能查到它 interest 的**真实业务表**的全部 rows(经 API，含查询路由)。
    排除哨兵表 ROUTE_MARKER(恒空，不是业务表；Codex 复核#1:否则永远等不到 15 行=超时污染)。"""
    tabs = [t for t in (nd["interest"] or H.ROLE_TABLES[nd["role"]])
            if t != ROUTE_MARKER and t in H.TABLES]
    if not tabs:  # route-only 节点(只持哨兵)→ 无业务表要收齐，收敛靠查询路由(另测)
        return True
    return all(H.count_rows(nd, t, prefix) == rows for t in tabs)


def run_once_custom(n, rows, strategy, interest_map, settle):
    if os.path.exists(H.WORK):
        shutil.rmtree(H.WORK)
    os.makedirs(H.WORK)
    nodes = write_configs_custom(n, strategy, interest_map)
    procs = H.start(nodes)
    try:
        if not H.wait_active(nodes):
            print(f"  [{strategy}] 节点未全 ACTIVE，跳过")
            return None
        # corrosion 启动自写 node_interest(run_root.rs::write_own_interest 读 gossip.interest)，
        # 无需 harness 再 exec 写。等 interest 复制 + selector 3s 缓存刷新采纳。
        time.sleep(settle)
        before = {nd["i"]: H.scrape(nd, H.METRICS) for nd in nodes}

        prefix = f"r{int(time.time())}_"
        t0 = time.time()
        for t in H.TABLES:
            for j in range(rows):
                H.sh([H.BIN, "-c", nodes[0]["cfg"], "exec",
                      "--param", f"{prefix}{j}", "--param", f"{t}-payload-{j}",
                      f"INSERT INTO {t} (id,data) VALUES (?,?)"])

        conv = None
        deadline = time.time() + max(30, n)
        while time.time() < deadline:
            if all(converged_by_interest(nd, prefix, rows) for nd in nodes):
                conv = time.time() - t0
                break
            time.sleep(0.1)

        after = {nd["i"]: H.scrape(nd, H.METRICS) for nd in nodes}
        agg = {m: sum(after[nd["i"]][m] - before[nd["i"]][m] for nd in nodes)
               for m in H.METRICS}
        # 验证真部分复制：统计非 interest 表本地=0 的节点数(证明 RL/rule 真裁了，非全存)。
        # 排除种子节点 0：它写了所有表，本地必然有，不算泄漏。
        local0 = 0
        checks = 0
        for nd in nodes:
            if nd["i"] == 0:
                continue
            for t in H.TABLES:
                if t not in (nd["interest"] or []):
                    checks += 1
                    if H.count_rows_local(nd, t, prefix) == 0:
                        local0 += 1
        push = int(agg["corro.broadcast.sent.bytes"])
        sync = int(agg["corro.sync.chunk.sent.bytes"])
        return {"strategy": strategy, "conv": conv, "push": push, "sync": sync,
                "total": push + sync, "partial": (local0, checks)}
    finally:
        for p in procs:
            p.send_signal(signal.SIGTERM)
        for p in procs:
            try:
                p.wait(timeout=5)
            except Exception:
                p.kill()


def mean(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else 0.0


def probe_resolve_behavior(n, settle, fast_ms=20, slow_ms=220, kq=12):
    """合成对照(Codex #2):证 resolve_table_holder 选「首个」holder 而非低方差 holder。
    target 分别只放 {快 holder}/{慢 holder}/{快+慢};route-only consumer 路由查 target。
    若 {快+慢} 的 P50 ≈ 慢(而非 min=快)→ resolve 不认方差(RL 挑的好 holder 路由选不到);
    若 {快+慢} ≈ 快 → resolve 恰好选中快的(需看是否稳定/由 SQL 顺序决定,非方差感知)。"""
    import json
    import subprocess
    FAST, SLOW, CONSUMER = 1, 2, 3  # node1=快 holder, node2=慢 holder, node3=消费者(route-only)

    def one(holders, faults, label):
        if os.path.exists(H.WORK):
            shutil.rmtree(H.WORK)
        os.makedirs(H.WORK)
        os.environ["CORRO_LINK_FAULTS"] = json.dumps(faults)
        imap = {i: [ROUTE_MARKER] for i in range(n)}
        for h in holders:
            imap[h] = ["target"]
        # 从 holder 写 target(不让 node0 当无延迟持有者搭便车,否则 resolve 总选 0 延迟的它)。
        writer = holders[0]
        nodes = write_configs_custom(n, "scored_reduce", imap)
        procs = H.start(nodes)
        try:
            if not H.wait_active(nodes):
                print(f"    [{label}] 未 ACTIVE"); return None
            time.sleep(settle)
            prefix = f"p{int(time.time())}_"
            for j in range(15):
                H.sh([H.BIN, "-c", nodes[writer]["cfg"], "exec", "--param", f"{prefix}{j}",
                      "--param", f"v{j}", "INSERT INTO target (id,data) VALUES (?,?)"])
            # 等 holder 本地就绪(直读 sqlite)
            dl = time.time() + 40
            while time.time() < dl:
                if all(H.count_rows_local(nodes[h], "target", prefix) == 15 for h in holders):
                    break
                time.sleep(0.3)
            lat = []
            for _ in range(kq):
                r = subprocess.run(
                    ["curl", "-s", "-o", "/dev/null", "-w", "%{time_total}", "-X", "POST",
                     f"http://127.0.0.1:{nodes[CONSUMER]['api']}/v1/queries",
                     "-H", "content-type: application/json",
                     "-d", '"SELECT count(*) FROM target"'], capture_output=True, text=True)
                try:
                    lat.append(float(r.stdout.strip()) * 1000)
                except ValueError:
                    pass
            if not lat:
                return None
            lat.sort()
            p50 = lat[len(lat) // 2]
            print(f"    {label}: consumer 路由 target P50={p50:.0f}ms (n={len(lat)})")
            return p50
        finally:
            os.environ.pop("CORRO_LINK_FAULTS", None)
            for p in procs:
                p.send_signal(signal.SIGTERM)
            for p in procs:
                try:
                    p.wait(timeout=5)
                except Exception:
                    p.kill()

    # 两种角色分配:A) node1=快,node2=慢;B) node1=慢,node2=快(对调)。
    # 若两组的 {both} 都 ≈ node1 的延迟(A→快、B→慢)→ resolve 按 SQL 顺序选 node1,不认方差(证死)。
    fa = {node_addr(FAST): {"delay_ms": fast_ms}, node_addr(SLOW): {"delay_ms": slow_ms}}
    fb = {node_addr(FAST): {"delay_ms": slow_ms}, node_addr(SLOW): {"delay_ms": fast_ms}}
    print(f"  组A:node{FAST}={fast_ms}ms(快) node{SLOW}={slow_ms}ms(慢)")
    a_both = one([FAST, SLOW], fa, f"  A {{node{FAST}+node{SLOW}}}")
    time.sleep(2)
    print(f"  组B(对调):node{FAST}={slow_ms}ms(慢) node{SLOW}={fast_ms}ms(快)")
    b_both = one([FAST, SLOW], fb, f"  B {{node{FAST}+node{SLOW}}}")
    if a_both is not None and b_both is not None:
        mid = (fast_ms + slow_ms) / 2
        picks_first = a_both < mid and b_both > mid   # 两组都命中 node1(A=快/B=慢)→ 选首个
        picks_best = a_both < mid and b_both < mid    # 两组都命中快的 → 按 RTT 选最优
        print(f"  判定:组A {{both}}={a_both:.0f}ms(node{FAST}=快), 组B {{both}}={b_both:.0f}ms(node{FAST}=慢)")
        if picks_best:
            print("  → ✅两组 {both} 都命中「快」holder(与节点顺序无关)"
                  "→ resolve 按 RTT(ring)选最优 holder = **RL placement 集群价值解锁**")
        elif picks_first:
            print(f"  → 两组 {{both}} 都命中 node{FAST}(与快慢无关)"
                  "→ resolve 按 SQL 顺序选首个,不认方差(旧行为)")
        else:
            print("  → 未稳定,需更多轮/换拓扑再验")
    return a_both, b_both


# ── 查询延迟阶段(Codex #4)：RL 方差感知 placement 的真价值 ──────────────────────
DELAY_PER_VAR_MS = 120  # link_var(0~2) → 注入延迟:link_var×120ms(高方差=不稳/慢链路)


def node_addr(i, bg=7500):
    return f"[::1]:{bg + i}"


def measure_latency(sit, interest_map, label, settle, kq=12):
    """给每节点按 link_var 注入链路延迟(CORRO_LINK_FAULTS),收敛后从**非 holder 的需要者**
    路由查询它需要但不持有的表,量 P50/P95。RL 把查询密集数据放低方差 holder → 路由查询更快;
    random_cut 随机砍可能把 holder 落在高方差节点 → 更慢。同副本预算 → 隔离 RL 方差感知价值。
    Codex #2 修正:holder 就绪用直读 sqlite 验(不靠路由),区分「本地存」与「路由可查」。"""
    import json
    import subprocess
    n = sit.n
    if os.path.exists(H.WORK):
        shutil.rmtree(H.WORK)
    os.makedirs(H.WORK)
    faults = {node_addr(i): {"delay_ms": int(sit.link_var[i] * DELAY_PER_VAR_MS)}
              for i in range(n) if sit.link_var[i] > 0.01}
    os.environ["CORRO_LINK_FAULTS"] = json.dumps(faults)
    nodes = write_configs_custom(n, "scored_reduce", interest_map)
    procs = H.start(nodes)
    try:
        if not H.wait_active(nodes):
            print(f"  [{label}] 未全 ACTIVE"); return None
        time.sleep(settle)
        prefix = f"q{int(time.time())}_"
        for t in H.TABLES:
            for j in range(15):
                H.sh([H.BIN, "-c", nodes[0]["cfg"], "exec", "--param", f"{prefix}{j}",
                      "--param", f"{t}-{j}", f"INSERT INTO {t} (id,data) VALUES (?,?)"])

        # holder 就绪:直读 sqlite 验每个 holder 本地真收齐(绕路由，Codex #2)。
        def holders_ready():  # 只验真实业务表 holder 就绪，排除哨兵(Codex #1)
            for i in range(n):
                for t in interest_map[i]:
                    if t == ROUTE_MARKER or t not in H.TABLES:
                        continue
                    if H.count_rows_local(nodes[i], t, prefix) != 15:
                        return False
            return True
        deadline = time.time() + 40
        while time.time() < deadline and not holders_ready():
            time.sleep(0.3)

        # 诊断(Codex #2 核心):非 holder 需要者是否真需要路由,还是数据已泄漏到本地?
        leaked = genuine = 0
        for i in range(n):
            for t in H.ROLE_TABLES[nodes[i]["role"]]:
                if t not in interest_map[i]:
                    if H.count_rows_local(nodes[i], t, prefix) > 0:
                        leaked += 1
                    else:
                        genuine += 1
        print(f"  [{label}] 非持有需要者本地状态: 已泄漏={leaked} 真需路由={genuine} "
              f"(泄漏=epidemic 补到本地→查询不路由→注入延迟失效)")

        # 读 workload:非 holder 的需要者路由查它需要但不持有的表。
        lat = []
        probes = 0
        for i in range(n):
            routed = [t for t in H.ROLE_TABLES[nodes[i]["role"]]
                      if t not in interest_map[i]]
            for t in routed:
                probes += 1
                for _ in range(kq):
                    r = subprocess.run(
                        ["curl", "-s", "-o", "/dev/null", "-w", "%{time_total}",
                         "-X", "POST", f"http://127.0.0.1:{nodes[i]['api']}/v1/queries",
                         "-H", "content-type: application/json",
                         "-d", f'"SELECT count(*) FROM {t}"'],
                        capture_output=True, text=True)
                    try:
                        lat.append(float(r.stdout.strip()) * 1000)
                    except ValueError:
                        pass
        if not lat:
            print(f"  [{label}] 无路由查询(该 placement 无非持有需要者)"); return None
        lat.sort()
        p50 = lat[len(lat) // 2]
        p95 = lat[min(len(lat) - 1, int(len(lat) * 0.95))]
        print(f"  {label}: 路由点 {probes} 查询 n={len(lat)} "
              f"P50={p50:.0f}ms P95={p95:.0f}ms")
        return {"p50": p50, "p95": p95, "n": len(lat)}
    finally:
        os.environ.pop("CORRO_LINK_FAULTS", None)
        for p in procs:
            p.send_signal(signal.SIGTERM)
        for p in procs:
            try:
                p.wait(timeout=5)
            except Exception:
                p.kill()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", type=int, default=9)
    ap.add_argument("--rows", type=int, default=30)
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--settle", type=int, default=int(os.environ.get("SETTLE", "12")))
    args = ap.parse_args()

    if not os.path.exists(H.BIN):
        sys.exit(f"找不到 binary {H.BIN}，先 cargo build -p corrosion")

    print(f"== RL placement 真跑：{args.nodes} 节点 × {args.rows} 行/表 × "
          f"{len(H.TABLES)} 表，{args.repeats} 轮均值，settle={args.settle}s ==\n")

    print("训练 RL 模型(pretrain 监督 greedy + rl_finetune 加 link_var 风险)...")
    model = train_rl_model(args.nodes)
    sit = build_mission_situation(args.nodes)
    pl = rl_placement(model, sit)
    rl_map = placement_to_interest(sit, pl)
    rule_map = rule_interest(args.nodes)

    print("\nRL 学出的 placement(每表 holder 节点)：")
    for d, holders in enumerate(pl):
        spec = sit.data[d]
        print(f"  {spec['name']:<12} holders={sorted(holders)}  "
              f"(需要者={sorted(spec['needers'])}, critical={sorted(spec['critical'])})")
    print("对比 rule(所有需要者都持有)：")
    for t in H.TABLES:
        holders = sorted(i for i in range(args.nodes) if t in rule_map[i])
        print(f"  {t:<12} holders={holders}")

    # RL 相对 rule 的持有者总数(placement 稀疏度 = 降量的结构来源)
    rl_holders = sum(len(v) for v in rl_map.values())
    rule_holders = sum(len(v) for v in rule_map.values())
    print(f"\n持有者总数(表副本数): rule={rule_holders}  rl={rl_holders}  "
          f"(RL {'更稀疏' if rl_holders < rule_holders else '更密'})")

    agg = {}
    partials = {}
    for r in range(args.repeats):
        print(f"\n--- 第 {r+1}/{args.repeats} 轮 ---")
        # random_cut 每轮换 seed(测同预算随机砍的分布);策略顺序每轮轮转排热缓存/端口残留偏差。
        plans = [
            ("broadcast", "random", rule_map),               # 合同「广播式」基线
            ("rule", "scored_reduce", rule_map),             # 现状智能基线(全需要者)
            ("random_cut", "scored_reduce",
             random_cut_interest(sit, pl, seed=100 + r)),    # 同副本预算随机砍(Codex #3)
            ("rl", "scored_reduce", rl_map),                 # RL 学出的 placement
        ]
        order = plans[r % len(plans):] + plans[:r % len(plans)]
        for name, strat, imap in order:
            res = run_once_custom(args.nodes, args.rows, strat, imap, args.settle)
            if res:
                agg.setdefault(name, []).append(res)
                p = res["partial"]
                print(f"  {name:<11} push={res['push']:<9} sync={res['sync']:<9} "
                      f"total={res['total']:<9} 收敛={res['conv']}  "
                      f"本地裁剪={p[0]}/{p[1]}")
                partials[name] = p
    plan_names = ["broadcast", "rule", "random_cut", "rl"]

    print("\n== 均值对照(全局求和，push/sync 分列排除归因混淆 Codex #1) ==")
    print(f"{'策略':<12}{'推送字节':<12}{'对账字节':<12}{'总传输':<12}{'本地裁剪':<10}")
    tot = {}
    for name in plan_names:
        rs = agg.get(name, [])
        push = mean([x["push"] for x in rs])
        sync = mean([x["sync"] for x in rs])
        total = mean([x["total"] for x in rs])
        tot[name] = {"push": push, "sync": sync, "total": total}
        p = partials.get(name, (0, 0))
        print(f"{name:<12}{push:<12.0f}{sync:<12.0f}{total:<12.0f}{p[0]}/{p[1]}")

    base = tot.get("broadcast")
    if base and base["total"]:
        print("\n== 相对广播式基线降幅(合同 4.4.3 目标 ↓30%；含 sync 过滤+分组，非纯 RL 归因) ==")
        for name in ("rule", "random_cut", "rl"):
            if tot.get(name, {}).get("total"):
                d = (1 - tot[name]["total"] / base["total"]) * 100
                print(f"  {name}: 总传输 ↓{d:.1f}%")
    # RL 归因:同 scored_reduce 语义下,RL vs 同副本预算随机砍(Codex #3 的关键判据)
    if tot.get("random_cut", {}).get("total") and tot.get("rl", {}).get("total"):
        dr = (1 - tot["rl"]["total"] / tot["random_cut"]["total"]) * 100
        dp = (1 - tot["rl"]["push"] / tot["random_cut"]["push"]) * 100 if tot["random_cut"]["push"] else 0
        print(f"\n== ★RL 归因判据:RL vs random_cut(同副本预算随机砍) ==")
        print(f"  总传输 ↓{dr:.1f}%   推送字节 ↓{dp:.1f}%")
        print("  >0=RL 在同等稀疏度下更省(学到读写权衡该砍哪些)；≈0=字节降幅只是平凡少副本，"
              "RL 真价值在别处(查询延迟，见 rl_e2e_latency.py)。")

    # ── 查询延迟阶段:RL vs random_cut 同副本预算,注入 link_var 延迟(Codex #4)──
    print("\n== ★查询延迟对照(注入 per-node link_var 延迟,RL 方差感知真价值) ==")
    print("  RL 把查询密集数据放低方差 holder → 路由查询更快;random_cut 随机砍可能踩高方差 holder。")
    lat_rl, lat_rc = [], []
    for r in range(args.repeats):
        rc_map = random_cut_interest(sit, pl, seed=200 + r)
        # 交错跑,减少顺序偏差
        a = measure_latency(sit, rl_map, f"[{r+1}] rl        ", args.settle)
        time.sleep(2)
        b = measure_latency(sit, rc_map, f"[{r+1}] random_cut", args.settle)
        time.sleep(2)
        if a:
            lat_rl.append(a["p50"])
        if b:
            lat_rc.append(b["p50"])
    if lat_rl and lat_rc:
        mr, mc = mean(lat_rl), mean(lat_rc)
        print(f"\n  RL placement    路由查询 P50 均值 = {mr:.0f}ms")
        print(f"  random_cut      路由查询 P50 均值 = {mc:.0f}ms")
        verdict = ("✅ RL 更快(方差感知 placement 端到端降低查询延迟)"
                   if mc > mr * 1.15 else "差异不显著(见下方合成对照:根因=resolve 选首个 holder)")
        print(f"  判定:RL vs random_cut 同副本预算 → {verdict}")

    # 合成对照:证死"延迟无差"的根因是 resolve 不认方差(Codex #2)
    print("\n== ★合成对照:resolve_table_holder 是否按方差选 holder ==")
    probe_resolve_behavior(args.nodes, args.settle)

    print("""
诚实边界(Codex 复核后收紧措辞):
  - localhost N 节点、多轮均值排方差;非 100 节点半实物(合同考核环境,里程碑2 补)。
  - 字节:在**当前单表写入、同 per-table holder 数、无丢包/无限速**条件下,RL 与 random_cut
    推送字节相同 → 降量为 **cardinality 主导(副本数)**,非 RL 读写权衡;非"数学必然"的泛化。
  - 延迟:link_var→延迟为线性合成映射(app 层注入,非 QUIC 重传)。RL vs random_cut 无差的根因由
    合成对照证死:resolve_table_holder 选**首个** holder(SQL 无 ORDER BY),不认方差 →
    RL 挑的低方差非 critical holder 路由选不到;target 的 critical holder 硬约束必含且可能高方差。
  - 哨兵表 ROUTE_MARKER 已从收敛/holder-ready/裁剪判定中排除(Codex #1),收敛须 conv≠None 才采信。
  - RL 单 holder 受控优势(13×)见 rl_e2e_latency.py;集群解锁需 resolve 改为方差/负载感知(路由层一处改)。
""")


if __name__ == "__main__":
    main()
