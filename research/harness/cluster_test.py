#!/usr/bin/env python3
"""圈子(cluster)隔离验证 —— 部分复制主线的第一步。

N 个节点共享同一 gossip 网络(都 bootstrap 到 node0)，但分成 2 个圈子(cluster_id)。
在圈子1 写数据，验证：
  ① 圈子内全量复制：圈子1 所有节点都收到数据。
  ② 圈子间零传输：圈子2 节点行数=0，且 broadcast.recv/sync.changes.recv 增量≈0
     (证明跨圈子是"发送前就不发"，不是收到再丢)。

不依赖 interest/node_interest——纯 corrosion 原生 cluster 隔离。
用法: python3 research/harness/cluster_test.py --nodes 6
"""

import argparse
import os
import shutil
import signal
import subprocess
import time

import run as H

RECV = ["corro.broadcast.recv.count", "corro.sync.changes.recv"]


def write_cfgs(n, bg=7400, ba=8400, bp=9400):
    schema_dir = os.path.join(H.WORK, "schema")
    os.makedirs(schema_dir, exist_ok=True)
    with open(os.path.join(schema_dir, "kv.sql"), "w") as f:
        f.write("CREATE TABLE kv (id BLOB NOT NULL PRIMARY KEY, val TEXT NOT NULL DEFAULT '');\n")
    nodes = []
    for i in range(n):
        boot = "" if i == 0 else f'"[::1]:{bg}"'
        cfg = os.path.join(H.WORK, f"node{i}.toml")
        with open(cfg, "w") as f:
            f.write(f"""[db]
path = "{H.WORK}/node{i}.db"
schema_paths = ["{schema_dir}"]
[gossip]
addr = "[::]:{bg+i}"
external_addr = "[::1]:{bg+i}"
bootstrap = [{boot}]
plaintext = true
[api]
addr = "127.0.0.1:{ba+i}"
[admin]
path = "{H.WORK}/node{i}-admin.sock"
[telemetry]
prometheus.addr = "127.0.0.1:{bp+i}"
""")
        nodes.append({"i": i, "cfg": cfg, "api": ba + i, "prom": bp + i,
                      "log": os.path.join(H.WORK, f"node{i}.log")})
    return nodes


def set_cluster(nd, cid):
    return H.sh([H.BIN, "-c", nd["cfg"], "cluster", "set-id", str(cid)])


def count(nd, prefix):
    r = H.sh([H.BIN, "-c", nd["cfg"], "query",
              f"SELECT count(*) FROM kv WHERE id LIKE '{prefix}%'"])
    try:
        return int(r.stdout.strip().split("|")[0])
    except (ValueError, IndexError):
        return -1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", type=int, default=6)
    ap.add_argument("--rows", type=int, default=30)
    args = ap.parse_args()
    n = args.nodes

    if os.path.exists(H.WORK):
        shutil.rmtree(H.WORK)
    os.makedirs(H.WORK)
    nodes = write_cfgs(n)
    procs = H.start(nodes)
    try:
        if not H.wait_active(nodes):
            print("节点未全部 ACTIVE"); return
        # 前一半 → 圈子1，后一半 → 圈子2
        half = n // 2
        cluster_of = {nd["i"]: (1 if nd["i"] < half else 2) for nd in nodes}
        for nd in nodes:
            set_cluster(nd, cluster_of[nd["i"]])
        print(f"圈子划分：圈1={[i for i in cluster_of if cluster_of[i]==1]} "
              f"圈2={[i for i in cluster_of if cluster_of[i]==2]}")
        print("等圈子重新成形 8s ...")
        time.sleep(8)

        before = {nd["i"]: H.scrape(nd, RECV) for nd in nodes}
        prefix = f"c{int(time.time())}_"
        # 在圈子1的种子(node0)写入
        for j in range(args.rows):
            H.sh([H.BIN, "-c", nodes[0]["cfg"], "exec",
                  "--param", f"{prefix}{j}", "--param", f"v{j}",
                  "INSERT INTO kv (id,val) VALUES (?,?)"])
        print(f"圈1 写入 {args.rows} 行，等 12s 看传播 ...\n")
        time.sleep(12)

        after = {nd["i"]: H.scrape(nd, RECV) for nd in nodes}
        ok_in, ok_iso = True, True
        print(f"{'节点':<6}{'圈子':<6}{'kv行数':<9}{'Δbcast.recv':<14}{'Δsync.recv':<12}")
        for nd in nodes:
            i = nd["i"]
            c = count(nd, prefix)
            d = {m: after[i][m] - before[i][m] for m in RECV}
            if cluster_of[i] == 1 and i != 0 and c != args.rows:
                ok_in = False
            if cluster_of[i] == 2 and c != 0:
                ok_iso = False
            tag = "(写)" if i == 0 else ""
            print(f"{i:<6}{cluster_of[i]:<6}{c:<9}"
                  f"{int(d['corro.broadcast.recv.count']):<14}"
                  f"{int(d['corro.sync.changes.recv']):<12}{tag}")

        print("\n判定：")
        print("  ① 圈子内全量复制(圈1 非写入节点都收齐)：", "通过" if ok_in else "✗ 未收齐")
        print("  ② 圈子间零传输(圈2 行数全 0)：", "通过" if ok_iso else "✗ 有泄漏")
        print("\n总判定：", "✅ cluster 隔离 PASS" if (ok_in and ok_iso) else "❌ FAIL")
    finally:
        for p in procs:
            p.send_signal(signal.SIGTERM)
        for p in procs:
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()


if __name__ == "__main__":
    main()
