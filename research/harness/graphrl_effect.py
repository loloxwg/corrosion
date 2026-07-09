#!/usr/bin/env python3
"""诚实测真效果:活模型(rl+GNN)vs scored,在传输量/收敛上到底有没有改善。

演示证明了 GNN 活着决策;本脚本量它的**系统效果**——开 GNN 的 rl 策略 vs scored,
同任务、同写入、注入链路 jitter(给 GNN 的链路感知留作用空间),多轮均值对照:
  全局传输字节(broadcast+sync)、收敛时间。

预期(基于三处吸收:字节被 cardinality、延迟被 resolve、选路被 ring 吸收):差异不显著。
但有这个诚实的数,比不测强——评审问"它到底有没有用",手里得有答案。
用法: python3 research/harness/graphrl_effect.py --repeats 3
"""
import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, HERE)
import run as H

WEIGHTS = os.path.join(REPO, "research", "rl", "gnn_weights.json")
WORK = H.WORK
BG, BA, BP = 7600, 8600, 9600
ROLE_TABLES = {"recon": ["flight"], "strike": ["battlefield", "target"], "jam": ["target"]}
ROLES = ["recon", "strike", "jam"]
TABLES = ["flight", "battlefield", "target"]

GRAPHRL_BLOCK = f"""[gossip.graphrl]
weights_path = "{WEIGHTS}"
critical_tables = ["target"]
[[gossip.graphrl.tables]]
name = "flight"
write_vol = 20.0
query_vol = 0.3
[[gossip.graphrl.tables]]
name = "battlefield"
write_vol = 6.0
query_vol = 2.5
[[gossip.graphrl.tables]]
name = "target"
write_vol = 3.0
query_vol = 5.0
[[gossip.graphrl.roles]]
name = "recon"
tables = ["flight"]
[[gossip.graphrl.roles]]
name = "strike"
tables = ["battlefield", "target"]
[[gossip.graphrl.roles]]
name = "jam"
tables = ["target"]
"""


def write_cfg(i, strategy, with_gnn, schema_dir):
    role = ROLES[i % 3]
    interest = ", ".join(f'"{t}"' for t in ROLE_TABLES[role])
    boot = "" if i == 0 else f'"[::1]:{BG}"'
    cfg = os.path.join(WORK, f"node{i}.toml")
    with open(cfg, "w") as f:
        f.write(f"""[db]
path = "{WORK}/node{i}.db"
schema_paths = ["{schema_dir}"]
[gossip]
addr = "[::]:{BG + i}"
external_addr = "[::1]:{BG + i}"
bootstrap = [{boot}]
plaintext = true
broadcast_strategy = "{strategy}"
interest = [{interest}]
{GRAPHRL_BLOCK if with_gnn else ""}[api]
addr = "127.0.0.1:{BA + i}"
[admin]
path = "{WORK}/node{i}-admin.sock"
[telemetry]
prometheus.addr = "127.0.0.1:{BP + i}"
""")
    return {"i": i, "role": role, "cfg": cfg, "api": BA + i, "prom": BP + i,
            "db": os.path.join(WORK, f"node{i}.db"),
            "log": os.path.join(WORK, f"node{i}.log")}


def run_once(n, rows, strategy, with_gnn, jittery, settle):
    if os.path.exists(WORK):
        shutil.rmtree(WORK)
    os.makedirs(WORK)
    schema_dir = os.path.join(WORK, "schema")
    os.makedirs(schema_dir)
    with open(os.path.join(schema_dir, "m.sql"), "w") as f:
        for t in TABLES:
            f.write(f"CREATE TABLE {t} (id BLOB NOT NULL PRIMARY KEY, data TEXT NOT NULL DEFAULT '');\n")
        f.write("CREATE TABLE node_interest (actor_id BLOB NOT NULL, table_name TEXT NOT NULL, "
                "active INTEGER NOT NULL DEFAULT 1, PRIMARY KEY (actor_id, table_name));\n")
    # 注入 jitter(给 GNN/ring 的链路感知留作用空间):后半 peer 高 jitter
    faults = {f"[::1]:{BG + j}": {"delay_ms": 5, "jitter_ms": 200} for j in jittery}
    os.environ["CORRO_LINK_FAULTS"] = json.dumps(faults)

    nodes = [write_cfg(i, strategy, with_gnn, schema_dir) for i in range(n)]
    procs = []
    for nd in nodes:
        with open(nd["log"], "w") as log:
            procs.append(subprocess.Popen([H.BIN, "-c", nd["cfg"], "agent"],
                                          stdout=log, stderr=subprocess.STDOUT))
    try:
        t0 = time.time()
        while time.time() - t0 < 30:
            if sum("considered ACTIVE" in open(nd["log"]).read()
                   for nd in nodes if os.path.exists(nd["log"])) == n:
                break
            time.sleep(0.5)
        time.sleep(settle)
        before = {nd["i"]: H.scrape(nd, H.METRICS) for nd in nodes}
        prefix = f"e{int(time.time())}_"
        tw = time.time()
        for t in TABLES:
            for j in range(rows):
                H.sh([H.BIN, "-c", nodes[0]["cfg"], "exec", "--param", f"{prefix}{j}",
                      "--param", f"{t}-{j}", f"INSERT INTO {t} (id,data) VALUES (?,?)"])
        # 收敛:每节点看到它关心表的全部 rows(经 API,含路由)
        conv = None
        deadline = time.time() + max(30, n)
        while time.time() < deadline:
            ok = all(
                all(H.count_rows(nd, t, prefix) == rows for t in ROLE_TABLES[nd["role"]])
                for nd in nodes)
            if ok:
                conv = time.time() - tw
                break
            time.sleep(0.1)
        after = {nd["i"]: H.scrape(nd, H.METRICS) for nd in nodes}
        agg = {m: sum(after[nd["i"]][m] - before[nd["i"]][m] for nd in nodes) for m in H.METRICS}
        push = int(agg["corro.broadcast.sent.bytes"])
        sync = int(agg["corro.sync.chunk.sent.bytes"])
        return {"push": push, "sync": sync, "total": push + sync, "conv": conv}
    finally:
        os.environ.pop("CORRO_LINK_FAULTS", None)
        for p in procs:
            p.send_signal(signal.SIGTERM)
        for p in procs:
            try:
                p.wait(timeout=5)
            except Exception:
                p.kill()


def mean(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", type=int, default=6)
    ap.add_argument("--rows", type=int, default=25)
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--settle", type=int, default=14)
    args = ap.parse_args()
    if not os.path.exists(WEIGHTS):
        sys.exit(f"缺权重 {WEIGHTS},先 python3 research/rl/export_weights.py")

    n = args.nodes
    jittery = list(range(n // 2, n))
    print(f"== 活模型真效果对照:{n} 节点,{args.rows} 行/表,{args.repeats} 轮,jitter peer={jittery} ==")
    print("   scored(方差盲基线) vs rl+GNN(活模型决策推送目标)\n")
    subprocess.run(["pkill", "-9", "-f", "target/debug/corrosion"], capture_output=True)
    time.sleep(2)

    # 三臂隔离 GNN 效果(避 Codex 归因陷阱):
    #   scored        = 不减量基线
    #   scored_reduce = 减量(interest 部分复制),无 GNN   ← GNN 效果的正确基线
    #   rl+GNN        = 减量 + GNN 选目标                  ← 差值=GNN 真效果
    plans = [("scored", "scored", False),
             ("scored_reduce", "scored_reduce", False),
             ("rl+GNN", "rl", True)]
    agg = {name: [] for name, _, _ in plans}
    for r in range(args.repeats):
        print(f"--- 第 {r+1}/{args.repeats} 轮 ---")
        for name, strat, gnn in plans:
            res = run_once(n, args.rows, strat, gnn, jittery, args.settle)
            agg[name].append(res)
            c = f"{res['conv']:.2f}s" if res["conv"] else "超时"
            print(f"  {name:<8} push={res['push']:<8} sync={res['sync']:<8} "
                  f"total={res['total']:<8} 收敛={c}")
            time.sleep(2)

    print("\n== 均值对照 ==")
    out = {}
    for name, _, _ in plans:
        rs = agg[name]
        out[name] = {
            "total": mean([x["total"] for x in rs]),
            "conv": mean([x["conv"] for x in rs]),
        }
        c = f"{out[name]['conv']:.2f}s" if out[name]["conv"] else "超时"
        print(f"  {name:<8} 总传输={out[name]['total']:.0f}  收敛={c}")

    # ① 减量的贡献(scored → scored_reduce):已知大头,非 GNN
    s, sr, g = out["scored"], out["scored_reduce"], out["rl+GNN"]
    if s["total"] and sr["total"]:
        d1 = (1 - sr["total"] / s["total"]) * 100
        print(f"\n① 减量贡献 scored→scored_reduce: 总传输 ↓{d1:.1f}%(部分复制,非 GNN)")
    # ② ★GNN 的隔离效果(scored_reduce → rl+GNN):同样减量,只差 GNN 选目标
    if sr["total"] and g["total"]:
        d2 = (1 - g["total"] / sr["total"]) * 100
        print(f"② ★GNN 隔离效果 scored_reduce→rl+GNN: 总传输 {'↓' if d2>=0 else '↑'}{abs(d2):.1f}%", end="")
        if sr["conv"] and g["conv"]:
            dc = (1 - g["conv"] / sr["conv"]) * 100
            print(f"   收敛 {'↓' if dc>=0 else '↑'}{abs(dc):.1f}%")
        else:
            print()
        if abs(d2) < 10:
            print("   → 差异不显著(<10%):GNN 活着决策,但增益被 corrosion 现成机制"
                  "(cardinality/ring)吸收——与三处吸收结论一致。真增益待半实物校准。")
        elif d2 > 0:
            print("   → GNN 带来可测增益(超噪声)。")
        else:
            print("   → GNN 当前反而不利(评分未校准/塌缩 critical),诚实记录。")

    print("\n诚实边界:localhost 小规模、多轮均值;评分尺度名义;GNN 输出塌缩 critical(见 §4.4)。")
    print("★关键:大头降量来自减量(部分复制),非 GNN;GNN 隔离效果看②(同减量下)。")


if __name__ == "__main__":
    main()
