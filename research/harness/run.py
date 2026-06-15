#!/usr/bin/env python3
"""主动推送研究：实验 harness。

编排 N 个本地 Corrosion 节点 → 跑写入负载 → 测「收敛延迟」+ 抓 broadcast/gossip 指标，
对 random / scored 两种传播策略做对照，输出对比表。

用法:
    python3 run.py --nodes 5 --rows 50 --strategies random scored

注意:
    localhost 上各节点 RTT≈0、全为 ring0，scored 与 random 几乎无差异——
    这是预期的（无链路差异化）。要看出差距需注入链路延迟(见 --sim-latency 留作后续)
    或上 mission-aware 数据相关度。harness 本身的测量是通用的。
"""

import argparse
import json
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


def sh(cmd):
    return subprocess.run(cmd, capture_output=True, text=True)


def write_configs(n, base_gossip, base_api, base_prom, strategy):
    """生成 n 个节点配置；节点 0 为种子，其余 bootstrap 到它。"""
    schema_dir = os.path.join(WORK, "schema")
    os.makedirs(schema_dir, exist_ok=True)
    with open(os.path.join(schema_dir, "todo.sql"), "w") as f:
        f.write("CREATE TABLE todos (id BLOB NOT NULL PRIMARY KEY, "
                "title TEXT NOT NULL DEFAULT '', completed_at INTEGER);\n")
    nodes = []
    for i in range(n):
        gossip, api, prom = base_gossip + i, base_api + i, base_prom + i
        boot = "" if i == 0 else f'"[::1]:{base_gossip}"'
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
[api]
addr = "127.0.0.1:{api}"
[admin]
path = "{WORK}/node{i}-admin.sock"
[telemetry]
prometheus.addr = "127.0.0.1:{prom}"
""")
        nodes.append({"i": i, "cfg": cfg, "api": api, "prom": prom,
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


def count_rows(nd, prefix):
    r = sh([BIN, "-c", nd["cfg"], "query",
            f"SELECT count(*) FROM todos WHERE id LIKE '{prefix}%'"])
    try:
        return int(r.stdout.strip().split("|")[0])
    except (ValueError, IndexError):
        return -1


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


def run_once(n, rows, strategy):
    if os.path.exists(WORK):
        shutil.rmtree(WORK)
    os.makedirs(WORK)
    nodes = write_configs(n, 7400, 8400, 9400, strategy)
    procs = start(nodes)
    metric_names = ["corro.broadcast.spawn", "corro.broadcast.recv.count",
                    "corro.broadcast.duplicate.count", "corro.sync.chunk.sent.bytes"]
    try:
        if not wait_active(nodes):
            print(f"  [{strategy}] 节点未全部 ACTIVE，跳过")
            return None
        time.sleep(2)
        before = {nd["i"]: scrape(nd, metric_names) for nd in nodes}

        prefix = f"r{int(time.time())}_"
        t0 = time.time()
        for j in range(rows):
            sh([BIN, "-c", nodes[0]["cfg"], "exec",
                "--param", f"{prefix}{j}", "--param", f"payload-{j}",
                "INSERT INTO todos (id,title) VALUES (?,?)"])

        # 轮询直到所有节点都看到全部 rows
        conv = None
        deadline = time.time() + 30
        while time.time() < deadline:
            if all(count_rows(nd, prefix) == rows for nd in nodes):
                conv = time.time() - t0
                break
            time.sleep(0.1)

        after = {nd["i"]: scrape(nd, metric_names) for nd in nodes}
        agg = {m: sum(after[nd["i"]][m] - before[nd["i"]][m] for nd in nodes)
               for m in metric_names}
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
    ap.add_argument("--nodes", type=int, default=5)
    ap.add_argument("--rows", type=int, default=50)
    ap.add_argument("--strategies", nargs="+", default=["random", "scored"])
    args = ap.parse_args()

    if not os.path.exists(BIN):
        sys.exit(f"找不到 binary {BIN}，先 cargo build -p corrosion")

    print(f"== 实验: {args.nodes} 节点, {args.rows} 行写入, 策略={args.strategies} ==\n")
    results = []
    for s in args.strategies:
        print(f"运行策略: {s} ...")
        r = run_once(args.nodes, args.rows, s)
        if r:
            results.append(r)

    print("\n== 对照结果 ==")
    hdr = f"{'策略':<10}{'收敛(s)':<12}{'广播发送':<12}{'广播重复':<12}{'对账字节':<12}"
    print(hdr)
    for r in results:
        m = r["metrics"]
        conv = f"{r['convergence_s']:.2f}" if r["convergence_s"] else "超时"
        print(f"{r['strategy']:<10}{conv:<12}"
              f"{int(m['corro.broadcast.spawn']):<12}"
              f"{int(m['corro.broadcast.duplicate.count']):<12}"
              f"{int(m['corro.sync.chunk.sent.bytes']):<12}")


if __name__ == "__main__":
    main()
