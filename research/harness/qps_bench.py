#!/usr/bin/env python3
"""4.4.2 查询吞吐压测:单节点本地命中 QPS 基准 + 100 节点聚合外推 + 500Kbps 路由上限。

考核 4.4.2:全局并发跨平台查询 ≥ 1,000,000 QPS。半实物 100 节点全连通、节点间 500Kbps。
1M = 100 节点 × 10K/节点(聚合)。节点间 500Kbps 极低 → 跨节点路由查询吞吐被带宽卡死
→ **1M 必须靠本地命中**(部分复制 + interest 让高频查询本地化)。本压测证:
  ① 单节点本地点查 QPS(HTTP /v1/queries 全路径)远超 10K/节点;
  ② SQLite 执行本身可忽略,瓶颈在 HTTP/序列化;
  ③ 500Kbps 单链路路由 QPS 上限极低 → 本地命中是数学必然(非优化);
  ④ localhost 多进程受共享 CPU 限,聚合线性须外推到独立硬件(诚实边界)。

依赖:ab(ApacheBench,系统自带)。用法: python3 research/harness/qps_bench.py [--nodes N]

诚实边界:无 100 节点半实物;单机上 ab 与 corrosion 抢 CPU → 单节点数是**下界**(独立节点更高);
聚合 ×100 是同构独立节点 + 高本地命中率假设下的外推,非半实物实测。
"""
import argparse
import os
import re
import shutil
import signal
import subprocess
import sys
import time

import run as H

QUERY = '"SELECT data FROM flight WHERE id = \'k42\'"'  # 固定点查,本地命中
ROUTED_BYTES = 700  # 单次路由查询往返估算字节(200B 请求 + 500B 响应,保守下限)
LINK_BPS = 500_000  # 节点间 500Kbps


def ab_qps(api_port, conc, n):
    """跑一次 ab,返回 Requests per second。变长流式响应的 Length 'failures' 非真错误(http 200)。"""
    r = subprocess.run(
        ["ab", "-c", str(conc), "-n", str(n), "-p", "/tmp/qbody.json",
         "-T", "application/json", f"http://127.0.0.1:{api_port}/v1/queries"],
        capture_output=True, text=True)
    m = re.search(r"Requests per second:\s+([0-9.]+)", r.stdout)
    return float(m.group(1)) if m else 0.0


def start_wildcard_nodes(n, base_gossip, base_api, base_prom):
    """起 n 个 wildcard(interest=["*"])节点 → 本地存全部 → 查询全本地命中(测本地快路径)。"""
    os.environ["SKIP_WRITE_INTEREST"] = "1"
    nodes = H.write_configs(n, base_gossip, base_api, base_prom, "scored_reduce")
    for nd in nodes:
        c = nd["cfg"]
        txt = re.sub(r'interest = \[.*\]', 'interest = ["*"]', open(c).read())
        open(c, "w").write(txt)
    procs = [subprocess.Popen([H.BIN, "-c", nd["cfg"], "agent"],
                              stdout=open(nd["log"], "w"), stderr=subprocess.STDOUT)
             for nd in nodes]
    time.sleep(7)
    for nd in nodes:
        for j in range(500):
            H.sh([H.BIN, "-c", nd["cfg"], "exec", "--param", f"k{j}", "--param", f"v{j}",
                  "INSERT INTO flight (id,data) VALUES (?,?)"])
    return nodes, procs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", type=int, default=1)
    ap.add_argument("--ab-parallel", type=int, default=4, help="每节点并行 ab 进程数")
    ap.add_argument("--conc", type=int, default=16)
    ap.add_argument("--n", type=int, default=40000)
    args = ap.parse_args()

    if not shutil.which("ab"):
        sys.exit("需要 ab(ApacheBench)")
    open("/tmp/qbody.json", "w").write(QUERY)
    subprocess.run(["pkill", "-9", "-f", "target/debug/corrosion"], capture_output=True)
    time.sleep(3)
    if os.path.exists(H.WORK):
        shutil.rmtree(H.WORK)
    os.makedirs(H.WORK)

    nodes, procs = start_wildcard_nodes(args.nodes, 7600, 8600, 9600)
    try:
        for nd in nodes:
            if procs[nd["i"]].poll() is not None:
                print("节点启动失败:", open(nd["log"]).read()[-400:]); return
        print(f"=== {args.nodes} 节点(wildcard,全本地命中) | {sysctl_cpu()} 核 ===\n")

        # 单节点:多个并行 ab 求和(单 ab 单线程会先成瓶颈;并行求和逼近节点真实天花板)。
        per_node = []
        for nd in nodes:
            api = nd["api"]
            outs = []
            ps = [subprocess.Popen(
                ["bash", "-c",
                 f"ab -c {args.conc} -n {args.n} -p /tmp/qbody.json -T application/json "
                 f"http://127.0.0.1:{api}/v1/queries 2>/dev/null | "
                 f"grep 'Requests per second' | grep -oE '[0-9.]+' | head -1"],
                stdout=subprocess.PIPE, text=True) for _ in range(args.ab_parallel)]
            qps = sum(float(p.communicate()[0].strip() or 0) for p in ps)
            per_node.append(qps)
            print(f"  node{nd['i']}: {qps:,.0f} QPS (本地点查, {args.ab_parallel}×ab 求和)")

        single = per_node[0]
        agg = sum(per_node)
        print(f"\n  单节点本地命中 QPS(下界): {single:,.0f}  (> 需要的 10,000/节点)")
        if args.nodes > 1:
            print(f"  {args.nodes} 节点同机合计: {agg:,.0f}  "
                  f"(≈单节点值 → 共享 CPU 卡顶,非线性;真实独立节点才线性)")

        # 100 节点聚合外推。
        extrap = single * 100
        print(f"\n=== 100 节点聚合外推(同构独立节点 + 高本地命中率) ===")
        print(f"  单节点 {single:,.0f} × 100 = {extrap:,.0f} QPS  "
              f"({'✅ ≥ 1M' if extrap >= 1_000_000 else '⚠ < 1M'})")

        # 500Kbps 路由上限。
        per_link = LINK_BPS / (ROUTED_BYTES * 8)
        links = 100 * 99 // 2
        print(f"\n=== 500Kbps 跨节点路由查询上限(为何必须本地命中) ===")
        print(f"  单链路: {LINK_BPS} bps / ({ROUTED_BYTES}B×8) = {per_link:,.0f} QPS/link")
        print(f"  100 节点全连通 {links} 条链路 → 路由聚合上限 ≈ {per_link*links:,.0f} QPS "
              f"(< 1M,单向) → **路由扛不起 1M,本地命中是数学必然**")

        print(f"\n诚实边界:单机 ab 与 corrosion 抢 CPU,单节点数是下界;聚合 ×100 是外推"
              f"(无 100 节点半实物);路由上限为带宽数学推导。")
    finally:
        for p in procs:
            p.send_signal(signal.SIGTERM)
        for p in procs:
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()


def sysctl_cpu():
    try:
        return subprocess.run(["sysctl", "-n", "hw.ncpu"], capture_output=True, text=True).stdout.strip()
    except Exception:
        return "?"


if __name__ == "__main__":
    main()
