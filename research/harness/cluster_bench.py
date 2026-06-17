#!/usr/bin/env python3
"""圈子分群 降量对照 —— 部分复制主线第二步。

对照两种部署的全局总传输：
  - baseline(广播式)：所有节点同属一个圈子，每条数据复制到全部节点。
  - partitioned(智能)：按数据需求分圈子，每条数据只复制到需要它的圈子成员。

数据模型(按"谁需要"分圈子，成员=该数据的相关节点)：
  C_recon (圈1, 侦察节点)        ← 侦察遥测 telem_recon (高频，降量主力)
  C_strike(圈2, 打击节点)        ← 打击遥测 telem_strike + 战场环境 battlefield
  C_jam   (圈3, 干扰节点)        ← 干扰遥测 telem_jam
  C_target(圈4, 打击+干扰节点)   ← 目标 target (共享态势，需求面广)

一个物理节点属于几个圈子就跑几个 corrosion 实例。降量主要来自高频窄需求的遥测。

用法: python3 research/harness/cluster_bench.py --nodes 9 --repeats 3
"""

import argparse
import os
import shutil
import signal
import subprocess
import time

import run as H

ROLES = ["recon", "strike", "jam"]

# 每个圈子：cluster_id、成员角色、装的表
CLUSTERS = {
    "C_recon":  {"cid": 1, "roles": ["recon"],           "tables": ["telem_recon"]},
    "C_strike": {"cid": 2, "roles": ["strike"],          "tables": ["telem_strike", "battlefield"]},
    "C_jam":    {"cid": 3, "roles": ["jam"],             "tables": ["telem_jam"]},
    "C_target": {"cid": 4, "roles": ["strike", "jam"],   "tables": ["target"]},
}
ALL_TABLES = ["telem_recon", "telem_strike", "telem_jam", "battlefield", "target"]

# 每轮每张表写多少行(遥测占大头 → 降量主力)
LOAD = {"telem_recon": 20, "telem_strike": 20, "telem_jam": 20,
        "battlefield": 5, "target": 5}

RECV = ["corro.broadcast.sent.bytes", "corro.sync.chunk.sent.bytes"]


def role_of(i):
    return ROLES[i % len(ROLES)]


def schema_dir():
    d = os.path.join(H.WORK, "schema")
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, "mission.sql"), "w") as f:
        for t in ALL_TABLES:
            f.write(f"CREATE TABLE {t} (id BLOB NOT NULL PRIMARY KEY, val TEXT NOT NULL DEFAULT '');\n")
    return d


def build_instances(n, mode):
    """返回实例列表。每个实例 = 一个 corrosion 进程。
    baseline：每物理节点 1 实例，全在 cluster 0。
    partitioned：每物理节点按角色加入的圈子，各起 1 实例。"""
    sd = schema_dir()
    insts = []
    port = 0  # 实例序号 → 端口偏移
    for i in range(n):
        role = role_of(i)
        if mode == "baseline":
            membership = [("C_all", 0)]
        else:
            membership = [(name, c["cid"]) for name, c in CLUSTERS.items()
                          if role in c["roles"]]
        for (cname, cid) in membership:
            insts.append({
                "node": i, "role": role, "cluster": cname, "cid": cid,
                "gossip": 7400 + port, "api": 8400 + port, "prom": 9400 + port,
                "cfg": os.path.join(H.WORK, f"inst{port}.toml"),
                "log": os.path.join(H.WORK, f"inst{port}.log"),
            })
            port += 1
    # 写配置：同一圈子内 bootstrap 到该圈子第一个实例
    first_of_cluster = {}
    for it in insts:
        first_of_cluster.setdefault(it["cid"], it["gossip"])
    for it in insts:
        seed = first_of_cluster[it["cid"]]
        boot = "" if it["gossip"] == seed else f'"[::1]:{seed}"'
        with open(it["cfg"], "w") as f:
            f.write(f"""[db]
path = "{H.WORK}/inst{insts.index(it)}.db"
schema_paths = ["{sd}"]
[gossip]
addr = "[::]:{it['gossip']}"
external_addr = "[::1]:{it['gossip']}"
bootstrap = [{boot}]
plaintext = true
[api]
addr = "127.0.0.1:{it['api']}"
[admin]
path = "{H.WORK}/inst{insts.index(it)}-admin.sock"
[telemetry]
prometheus.addr = "127.0.0.1:{it['prom']}"
""")
    return insts


def tables_for_cluster(cid):
    for c in CLUSTERS.values():
        if c["cid"] == cid:
            return c["tables"]
    return ALL_TABLES  # baseline cluster 0 装全部


def run_once(n, mode):
    if os.path.exists(H.WORK):
        shutil.rmtree(H.WORK)
    os.makedirs(H.WORK)
    insts = build_instances(n, mode)
    procs = H.start(insts)
    try:
        if not H.wait_active(insts):
            print(f"  [{mode}] 实例未全 ACTIVE，跳过"); return None
        # 设 cluster_id(baseline 全 0 无需设；partitioned 各实例设各自圈子)
        if mode != "baseline":
            for it in insts:
                H.sh([H.BIN, "-c", it["cfg"], "cluster", "set-id", str(it["cid"])])
            time.sleep(8)  # 等圈子成形
        else:
            time.sleep(4)

        before = {idx: H.scrape(it, RECV) for idx, it in enumerate(insts)}
        prefix = f"b{int(time.time())}_"
        # 写负载：每张表写进"装它的那个圈子的某个实例"
        for t in ALL_TABLES:
            # 找一个装这张表的实例做写入端
            writer = next((it for it in insts
                           if t in tables_for_cluster(it["cid"])), None)
            if writer is None:
                continue
            for j in range(LOAD[t]):
                H.sh([H.BIN, "-c", writer["cfg"], "exec",
                      "--param", f"{prefix}{t}_{j}", "--param", f"{t}-{j}",
                      f"INSERT INTO {t} (id,val) VALUES (?,?)"])
        time.sleep(12)

        after = {idx: H.scrape(it, RECV) for idx, it in enumerate(insts)}
        agg = {m: sum(after[idx][m] - before[idx][m] for idx in range(len(insts)))
               for m in RECV}
        total = sum(agg.values())

        # 正确性校验：低传输必须是"正确分群"而非"数据没流"。
        # 抽查 telem_recon：C_recon 实例应收齐 20、非 C_recon 实例应为 0。
        def cnt(it, t):
            r = H.sh([H.BIN, "-c", it["cfg"], "query",
                      f"SELECT count(*) FROM {t} WHERE id LIKE '{prefix}{t}%'"])
            try:
                return int(r.stdout.strip().split("|")[0])
            except (ValueError, IndexError):
                return -1
        conv = "n/a(baseline)"
        if mode != "baseline":
            recon_insts = [it for it in insts if it["cid"] == CLUSTERS["C_recon"]["cid"]]
            other_insts = [it for it in insts if it["cid"] != CLUSTERS["C_recon"]["cid"]]
            in_ok = all(cnt(it, "telem_recon") == LOAD["telem_recon"] for it in recon_insts)
            iso_ok = all(cnt(it, "telem_recon") == 0 for it in other_insts)
            # 再查 target：C_target 实例应收齐 5
            tgt_insts = [it for it in insts if it["cid"] == CLUSTERS["C_target"]["cid"]]
            tgt_ok = all(cnt(it, "target") == LOAD["target"] for it in tgt_insts)
            conv = f"圈内收齐={in_ok} 圈间隔离={iso_ok} target收齐={tgt_ok}"
            if not (in_ok and iso_ok and tgt_ok):
                conv += "  ⚠ 校验未过——降量可能是假象"
        return {"mode": mode, "instances": len(insts), "metrics": agg,
                "total": total, "conv": conv}
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
    ap.add_argument("--nodes", type=int, default=9)
    ap.add_argument("--repeats", type=int, default=3)
    args = ap.parse_args()
    if not os.path.exists(H.BIN):
        raise SystemExit(f"找不到 binary {H.BIN}，先 cargo build -p corrosion")

    print(f"== 圈子分群降量对照：{args.nodes} 节点(侦察/打击/干扰), repeats={args.repeats} ==")
    print(f"   负载/轮: {LOAD}\n")
    results = {"baseline": [], "partitioned": []}
    for r in range(args.repeats):
        for mode in ["baseline", "partitioned"]:
            res = run_once(args.nodes, mode)
            if res:
                results[mode].append(res)
                print(f"  [{r+1}/{args.repeats}] {mode:<12} 实例数={res['instances']:<3} "
                      f"推送={int(res['metrics']['corro.broadcast.sent.bytes']):<9} "
                      f"对账={int(res['metrics']['corro.sync.chunk.sent.bytes']):<9} "
                      f"总={int(res['total']):<9} {res['conv']}")

    def mean(xs):
        return sum(xs) / len(xs) if xs else 0
    bt = mean([r["total"] for r in results["baseline"]])
    pt = mean([r["total"] for r in results["partitioned"]])
    print("\n== 均值 ==")
    print(f"  广播式基线 全局总传输: {bt:.0f}")
    print(f"  智能分群   全局总传输: {pt:.0f}")
    if bt:
        print(f"  降幅: {(1-pt/bt)*100:.1f}%   (目标 ≥30%)")


if __name__ == "__main__":
    main()
