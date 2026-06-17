#!/usr/bin/env python3
"""主动推送研究：实验 harness（mission 拓扑 + 降量对照）。

对接考核 4.4.3：构建 侦察/打击/干扰 3 类异构节点 + 飞行状态/战场环境/目标 3 类数据，
分别统计「广播式」(random) 与「智能主动推送」(scored_reduce) 的全局数据传输总量，算降幅。

任务→数据需求映射（数据需求模版，对应 4.2.3）：
    侦察 recon  → 飞行状态 flight
    打击 strike → 战场环境 battlefield + 目标 target
    干扰 jam    → 目标 target
节点按 i%3 轮转分配角色；interest_routing(表→关心它的 peer)据此生成，写进每个节点配置。

用法:
    python3 run.py --nodes 6 --rows 50 --strategies random scored_reduce

注意:
    1a 只降「推送轴」(broadcast)。对账轴(anti-entropy)仍全量兜底，故「对账字节」
    在两策略下相近、甚至补回推送省下的量——全局总量真正逼近 30% 需 1b(对账按 interest 过滤)。
    本 harness 已分列 推送字节 / 对账字节 / 总传输，便于看清两轴各自的降量。
"""

import argparse
import os
import shutil
import signal
import subprocess
import sys
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
BIN = os.path.join(REPO, "target", "debug", "corrosion")
WORK = "/tmp/corro-harness"

# 任务角色 → 关心的数据表（数据需求模版）
ROLES = ["recon", "strike", "jam"]  # 侦察 / 打击 / 干扰
ROLE_TABLES = {
    "recon": ["flight"],                  # 侦察 → 飞行状态
    "strike": ["battlefield", "target"],  # 打击 → 战场环境 + 目标
    "jam": ["target"],                    # 干扰 → 目标
}
TABLES = ["flight", "battlefield", "target"]


def role_of(i):
    return ROLES[i % len(ROLES)]


def sh(cmd):
    return subprocess.run(cmd, capture_output=True, text=True)


def build_routing(n, base_gossip):
    """表 -> 关心该表的节点 external gossip 地址列表（推送端据此判断该发给谁）。"""
    routing = {t: [] for t in TABLES}
    for i in range(n):
        for t in ROLE_TABLES[role_of(i)]:
            routing[t].append(f"[::1]:{base_gossip + i}")
    return routing


def write_interest(nodes):
    """各节点自声明 interest：写入复制表 node_interest(本节点 crsql_site_id(), 关心的表)。
    经正常 exec 写入→走 crsqlite 复制路径→全集群可见；推送端聚合它选目标平台。"""
    for nd in nodes:
        for t in ROLE_TABLES[nd["role"]]:
            sh([BIN, "-c", nd["cfg"], "exec",
                "INSERT INTO node_interest (actor_id, table_name) "
                f"VALUES (crsql_site_id(), '{t}')"])


def write_configs(n, base_gossip, base_api, base_prom, strategy):
    """生成 n 个节点配置；节点 0 为种子，其余 bootstrap 到它。"""
    schema_dir = os.path.join(WORK, "schema")
    os.makedirs(schema_dir, exist_ok=True)
    with open(os.path.join(schema_dir, "mission.sql"), "w") as f:
        for t in TABLES:
            f.write(f"CREATE TABLE {t} (id BLOB NOT NULL PRIMARY KEY, "
                    f"data TEXT NOT NULL DEFAULT '');\n")
        # node_interest：每节点自声明 interest 的复制表(CRR)。
        # actor_id=本节点 crsql_site_id()，table_name=关心的表；推送端聚合它选目标。
        f.write("CREATE TABLE node_interest (actor_id BLOB NOT NULL, "
                "table_name TEXT NOT NULL, active INTEGER NOT NULL DEFAULT 1, "
                "PRIMARY KEY (actor_id, table_name));\n")

    nodes = []
    for i in range(n):
        gossip, api, prom = base_gossip + i, base_api + i, base_prom + i
        boot = "" if i == 0 else f'"[::1]:{base_gossip}"'
        # 本节点自声明 interest(对应任务角色)：用于对账握手按表过滤(Phase 2)。
        interest = ", ".join(f'"{t}"' for t in ROLE_TABLES[role_of(i)])
        cfg = os.path.join(WORK, f"node{i}.toml")
        with open(cfg, "w") as f:
            f.write(f"""[db]
path = "{WORK}/node{i}.db"
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
path = "{WORK}/node{i}-admin.sock"
[telemetry]
prometheus.addr = "127.0.0.1:{prom}"
""")
        nodes.append({"i": i, "role": role_of(i), "cfg": cfg,
                      "api": api, "prom": prom,
                      "log": os.path.join(WORK, f"node{i}.log")})
    return nodes


def start(nodes):
    procs = []
    for nd in nodes:
        with open(nd["log"], "w") as log:
            p = subprocess.Popen([BIN, "-c", nd["cfg"], "agent"],
                                 stdout=log, stderr=subprocess.STDOUT)
        procs.append(p)
    return procs


def wait_active(nodes, timeout=30):
    t0 = time.time()
    while time.time() - t0 < timeout:
        ok = 0
        for nd in nodes:
            try:
                with open(nd["log"]) as f:
                    if "considered ACTIVE" in f.read():
                        ok += 1
            except FileNotFoundError:
                pass
        if ok == len(nodes):
            return True
        time.sleep(0.5)
    return False


def count_rows(nd, table, prefix):
    r = sh([BIN, "-c", nd["cfg"], "query",
            f"SELECT count(*) FROM {table} WHERE id LIKE '{prefix}%'"])
    try:
        return int(r.stdout.strip().split("|")[0])
    except (ValueError, IndexError):
        return -1


def node_converged(nd, prefix, rows):
    """节点是否已看到它「关心的表」的全部 rows（按任务定义相关收敛）。"""
    return all(count_rows(nd, t, prefix) == rows for t in ROLE_TABLES[nd["role"]])


def scrape(nd, names):
    """从节点 prometheus 抓指定指标(求和)。指标名里的 . 转 _。"""
    out = {n: 0.0 for n in names}
    try:
        txt = urllib.request.urlopen(
            f"http://127.0.0.1:{nd['prom']}/metrics", timeout=2).read().decode()
    except Exception:
        return out
    for line in txt.splitlines():
        if line.startswith("#") or " " not in line:
            continue
        key, _, val = line.partition(" ")
        metric = key.split("{")[0]
        for n in names:
            if metric == n.replace(".", "_"):
                try:
                    out[n] += float(val)
                except ValueError:
                    pass
    return out


METRICS = [
    "corro.broadcast.spawn",
    "corro.broadcast.sent.bytes",      # 推送轴字节(降量主代理)
    "corro.broadcast.duplicate.count",
    "corro.sync.chunk.sent.bytes",     # 对账轴字节
]


def run_once(n, rows, strategy):
    if os.path.exists(WORK):
        shutil.rmtree(WORK)
    os.makedirs(WORK)
    nodes = write_configs(n, 7400, 8400, 9400, strategy)
    procs = start(nodes)
    try:
        if not wait_active(nodes):
            print(f"  [{strategy}] 节点未全部 ACTIVE，跳过")
            return None
        write_interest(nodes)
        # 等 interest 复制到全集群 + 推送端 3s 缓存刷新采纳（在 before 快照之前，不计入度量）。
        # 随规模自适应：节点多时 interest 传播+成员收敛更慢，settle 太短会让早期写入走全发泄漏、压低降幅。
        time.sleep(int(os.environ.get("SETTLE", max(8, n // 3))))
        before = {nd["i"]: scrape(nd, METRICS) for nd in nodes}

        prefix = f"r{int(time.time())}_"
        t0 = time.time()
        # 三类数据各写 rows 行（飞行状态/战场环境/目标），均从种子节点写入。
        for t in TABLES:
            for j in range(rows):
                sh([BIN, "-c", nodes[0]["cfg"], "exec",
                    "--param", f"{prefix}{j}", "--param", f"{t}-payload-{j}",
                    f"INSERT INTO {t} (id,data) VALUES (?,?)"])

        # 轮询直到每个节点都看到「它关心的表」的全部 rows
        conv = None
        deadline = time.time() + max(30, n)  # 大规模收敛更慢，放宽超时
        while time.time() < deadline:
            if all(node_converged(nd, prefix, rows) for nd in nodes):
                conv = time.time() - t0
                break
            time.sleep(0.1)

        after = {nd["i"]: scrape(nd, METRICS) for nd in nodes}
        agg = {m: sum(after[nd["i"]][m] - before[nd["i"]][m] for nd in nodes)
               for m in METRICS}
        return {"strategy": strategy, "convergence_s": conv, "metrics": agg}
    finally:
        for p in procs:
            p.send_signal(signal.SIGTERM)
        for p in procs:
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", type=int, default=6)
    ap.add_argument("--rows", type=int, default=50)
    ap.add_argument("--strategies", nargs="+",
                    default=["random", "scored_reduce"])
    args = ap.parse_args()

    if not os.path.exists(BIN):
        sys.exit(f"找不到 binary {BIN}，先 cargo build -p corrosion")

    print(f"== 实验: {args.nodes} 节点(侦察/打击/干扰轮转), "
          f"{args.rows} 行/表 × {len(TABLES)} 表, 策略={args.strategies} ==\n")
    results = []
    for s in args.strategies:
        print(f"运行策略: {s} ...")
        r = run_once(args.nodes, args.rows, s)
        if r:
            results.append(r)

    print("\n== 对照结果（全局求和）==")
    hdr = (f"{'策略':<14}{'收敛(s)':<11}{'推送字节':<11}"
           f"{'对账字节':<11}{'总传输':<11}{'广播重复':<10}")
    print(hdr)
    totals = {}
    for r in results:
        m = r["metrics"]
        push = int(m["corro.broadcast.sent.bytes"])
        sync = int(m["corro.sync.chunk.sent.bytes"])
        total = push + sync
        totals[r["strategy"]] = {"push": push, "total": total}
        conv = f"{r['convergence_s']:.2f}" if r["convergence_s"] else "超时"
        print(f"{r['strategy']:<14}{conv:<11}{push:<11}{sync:<11}{total:<11}"
              f"{int(m['corro.broadcast.duplicate.count']):<10}")

    # 相对 random 基线算降幅（4.4.3 目标 ≥30%）
    base = totals.get("random")
    if base:
        print("\n== 相对广播式(random)基线的降幅 ==")
        for s, v in totals.items():
            if s == "random":
                continue
            def drop(cur, ref):
                return (1 - cur / ref) * 100 if ref else 0.0
            print(f"{s}: 推送字节 ↓{drop(v['push'], base['push']):.1f}%, "
                  f"总传输 ↓{drop(v['total'], base['total']):.1f}%  (目标总传输 ↓30%)")


if __name__ == "__main__":
    main()
