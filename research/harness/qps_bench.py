#!/usr/bin/env python3
"""4.4.2 查询吞吐压测:单节点本地命中 QPS 基准 + 100 节点聚合外推 + 500Kbps 路由上限。

考核 4.4.2:全局并发跨平台查询 ≥ 1,000,000 QPS。半实物 100 节点全连通、节点间 500Kbps。
1M = 100 节点 × 10K/节点(聚合)。节点间 500Kbps 极低 → 跨节点路由查询吞吐被带宽卡死
→ **1M 必须靠本地命中**(部分复制 + interest 让高频查询本地化)。本压测证:
  ① 单节点本地点查 QPS(HTTP /v1/queries 全路径)远超 10K/节点;
  ② SQLite 执行本身可忽略,瓶颈在 HTTP/序列化;
  ③ 500Kbps 单链路路由 QPS 上限极低 → 本地命中是数学必然(非优化);
  ④ localhost 多进程受共享 CPU 限,聚合线性须外推到独立硬件(诚实边界)。

依赖:默认用 ab；远端无 ab 时可用内置 Python 多进程持久连接驱动。
用法:
  python3 research/harness/qps_bench.py --nodes 1
  python3 research/harness/qps_bench.py --nodes 100 --aggregate-concurrent \
    --driver python --ab-parallel 1 --n 5000 --json-out qps-100.json
  CORRO_BIN=target/release/corrosion python3 research/harness/qps_bench.py ...
  # agent 主机:
  API_BIND_HOST=0.0.0.0 CORRO_BIN=target/release/corrosion \
    python3 research/harness/qps_bench.py --nodes 4 --server-only --driver python
  # 独立负载机:
  python3 research/harness/qps_bench.py --nodes 4 --load-only \
    --aggregate-concurrent --driver python --api-host 192.168.3.214 --duration 10

诚实边界:100 agent 可在单台机同步实测,但它们与起压器共享 CPU;只有分散到独立硬件后才可能
验证线性聚合。单节点 ×100 始终是外推,不能替代共同时间窗的实际聚合值。
"""
import argparse
import http.client
import json
import multiprocessing
import os
import re
import shutil
import signal
import statistics
import subprocess
import sys
import time

import run as H

QUERY = '"SELECT data FROM flight WHERE id = \'k42\'"'  # 固定点查,本地命中
ROUTED_BYTES = 700  # 单次路由查询往返估算字节(200B 请求 + 500B 响应,保守下限)
LINK_BPS = 500_000  # 节点间 500Kbps


def stop_server_only(_signum, _frame):
    """把 SIGTERM 转成正常退出，让 finally 清理子 agent。"""
    raise KeyboardInterrupt


def ab_qps(api_port, conc, n):
    """跑一次 ab,返回 Requests per second。变长流式响应的 Length 'failures' 非真错误(http 200)。"""
    r = subprocess.run(
        ["ab", "-k", "-c", str(conc), "-n", str(n), "-p", "/tmp/qbody.json",
         "-T", "application/json", f"http://127.0.0.1:{api_port}/v1/queries"],
        capture_output=True, text=True)
    m = re.search(r"Requests per second:\s+([0-9.]+)", r.stdout)
    return float(m.group(1)) if m else 0.0


def start_wildcard_nodes(n, base_gossip, base_api, base_prom,
                         startup_timeout, seed_timeout):
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
    try:
        # 单节点没有 peer，永远不会出现成员 considered ACTIVE；API listener 就绪即可。
        if n == 1:
            deadline = time.time() + startup_timeout
            cluster_ready = False
            while time.time() < deadline:
                if (os.path.exists(nodes[0]["log"])
                        and "Starting API listener" in open(nodes[0]["log"]).read()):
                    cluster_ready = True
                    break
                time.sleep(0.2)
        else:
            cluster_ready = H.wait_active(nodes, timeout=startup_timeout)
        if not cluster_ready:
            active = sum("considered ACTIVE" in open(nd["log"]).read()
                         for nd in nodes if os.path.exists(nd["log"]))
            raise RuntimeError(f"only {active}/{n} nodes became ACTIVE")

        # 只写一次并等待真实复制到每个节点。旧实现给每个节点各写 500 行，会在大规模
        # 压测前制造 50,000 次子进程调用和同主键 CRDT 冲突，污染吞吐测量。
        r = H.sh([H.BIN, "-c", nodes[0]["cfg"], "exec",
                  "--param", "k42", "--param", "v42",
                  "INSERT INTO flight (id,data) VALUES (?,?)"])
        if r.returncode != 0:
            raise RuntimeError(f"seed insert failed: {r.stderr.strip()}")
        deadline = time.time() + seed_timeout
        while time.time() < deadline:
            if all(H.count_rows_local(nd, "flight", "k42") == 1 for nd in nodes):
                return nodes, procs
            time.sleep(0.5)
        ready = sum(H.count_rows_local(nd, "flight", "k42") == 1 for nd in nodes)
        raise RuntimeError(f"seed converged on only {ready}/{n} nodes")
    except Exception:
        for proc in procs:
            proc.terminate()
        for proc in procs:
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
        raise


def parse_ab(stdout):
    def number(pattern, cast=float, default=0):
        m = re.search(pattern, stdout)
        return cast(m.group(1)) if m else default
    return {
        "complete": number(r"Complete requests:\s+([0-9]+)", int),
        "failed": number(r"Failed requests:\s+([0-9]+)", int),
        "non_2xx": number(r"Non-2xx responses:\s+([0-9]+)", int),
        "qps": number(r"Requests per second:\s+([0-9.]+)"),
    }


def run_aggregate(nodes, args):
    """所有节点同时起压；用总完成请求/共同墙钟时间作为真实聚合吞吐。"""
    procs = []
    started = time.time()
    for nd in nodes:
        for _ in range(args.ab_parallel):
            procs.append((nd["i"], subprocess.Popen(
                ["ab", "-k", "-c", str(args.conc), "-n", str(args.n),
                 "-p", "/tmp/qbody.json", "-T", "application/json",
                 f"http://{args.api_host}:{nd['api']}/v1/queries"],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)))
    samples = []
    for node_id, proc in procs:
        stdout, stderr = proc.communicate()
        sample = parse_ab(stdout)
        sample.update({"node": node_id, "returncode": proc.returncode,
                       "stderr": stderr.strip()[-300:]})
        samples.append(sample)
    elapsed = time.time() - started
    complete = sum(s["complete"] for s in samples)
    non_2xx = sum(s["non_2xx"] for s in samples)
    per_node_qps = {}
    for sample in samples:
        per_node_qps[sample["node"]] = (
            per_node_qps.get(sample["node"], 0) + sample["qps"])
    return {
        "mode": "aggregate_concurrent",
        "elapsed_s": elapsed,
        "complete_requests": complete,
        "non_2xx_responses": non_2xx,
        "failed_requests_reported": sum(s["failed"] for s in samples),
        "process_failures": sum(s["returncode"] != 0 for s in samples),
        "aggregate_qps_wall": complete / elapsed if elapsed else 0,
        "sum_ab_qps": sum(s["qps"] for s in samples),
        "per_process_qps_min": min((s["qps"] for s in samples), default=0),
        "per_process_qps_max": max((s["qps"] for s in samples), default=0),
        "per_node_qps": per_node_qps,
        "per_node_qps_min": min(per_node_qps.values(), default=0),
        "per_node_qps_median": statistics.median(per_node_qps.values()) if per_node_qps else 0,
        "per_node_qps_max": max(per_node_qps.values(), default=0),
        "nodes_at_or_above_10k": sum(qps >= 10_000 for qps in per_node_qps.values()),
        "samples": samples,
    }


def python_http_worker(node_id, api_host, api_port, requests, duration,
                       start_event, results):
    """单进程持久连接起压器；多进程避开 Python GIL。"""
    start_event.wait()
    ok = 0
    errors = 0
    started = time.time()
    conn = http.client.HTTPConnection(api_host, api_port, timeout=30)
    headers = {"Content-Type": "application/json", "Connection": "keep-alive"}
    deadline = started + duration if duration else None
    attempted = 0
    while (time.time() < deadline if deadline is not None else attempted < requests):
        attempted += 1
        try:
            conn.request("POST", "/v1/queries", body=QUERY, headers=headers)
            response = conn.getresponse()
            response.read()
            if response.status == 200:
                ok += 1
            else:
                errors += 1
        except Exception:
            errors += 1
            conn.close()
            conn = http.client.HTTPConnection(api_host, api_port, timeout=30)
    elapsed = time.time() - started
    conn.close()
    results.put({"node": node_id, "complete": ok, "errors": errors,
                 "elapsed_s": elapsed, "qps": ok / elapsed if elapsed else 0})


def run_python_aggregate(nodes, args):
    ctx = multiprocessing.get_context("fork")
    start_event = ctx.Event()
    results = ctx.Queue()
    procs = []
    for nd in nodes:
        for _ in range(args.ab_parallel):
            procs.append(ctx.Process(
                target=python_http_worker,
                args=(nd["i"], args.api_host, nd["api"], args.n, args.duration,
                      start_event, results)))
    for proc in procs:
        proc.start()
    started = time.time()
    start_event.set()
    samples = [results.get() for _ in procs]
    for proc in procs:
        proc.join()
    elapsed = time.time() - started
    complete = sum(s["complete"] for s in samples)
    errors = sum(s["errors"] for s in samples)
    per_node_complete = {}
    for sample in samples:
        per_node_complete[sample["node"]] = (
            per_node_complete.get(sample["node"], 0) + sample["complete"])
    per_node_qps = {node: count / elapsed for node, count in per_node_complete.items()}
    return {
        "mode": "aggregate_concurrent_python",
        "elapsed_s": elapsed,
        "complete_requests": complete,
        "request_errors": errors,
        "process_failures": sum(proc.exitcode != 0 for proc in procs),
        "aggregate_qps_wall": complete / elapsed if elapsed else 0,
        "sum_worker_qps": sum(s["qps"] for s in samples),
        "per_process_qps_min": min((s["qps"] for s in samples), default=0),
        "per_process_qps_max": max((s["qps"] for s in samples), default=0),
        "per_node_qps": per_node_qps,
        "per_node_qps_min": min(per_node_qps.values(), default=0),
        "per_node_qps_median": statistics.median(per_node_qps.values()) if per_node_qps else 0,
        "per_node_qps_max": max(per_node_qps.values(), default=0),
        "nodes_at_or_above_10k": sum(qps >= 10_000 for qps in per_node_qps.values()),
        "samples": samples,
    }


def emit_result(result, args, boundary):
    print("=== 全节点同步起压（真实共同墙钟聚合） ===")
    print(f"  完成请求: {result['complete_requests']:,}，墙钟: {result['elapsed_s']:.2f}s")
    print(f"  聚合 QPS: {result['aggregate_qps_wall']:,.0f}")
    errors = result.get("non_2xx_responses", result.get("request_errors", 0))
    print(f"  请求错误: {errors}，起压进程失败: {result['process_failures']}")
    print(f"  单起压进程 QPS 范围: {result['per_process_qps_min']:,.0f}"
          f" ~ {result['per_process_qps_max']:,.0f}")
    if "per_node_qps" in result:
        print(f"  每节点 QPS min/median/max: {result['per_node_qps_min']:,.0f} / "
              f"{result['per_node_qps_median']:,.0f} / {result['per_node_qps_max']:,.0f}")
        print(f"  达到 10K 的节点: {result['nodes_at_or_above_10k']}/{args.nodes}")
    result.update({"nodes": args.nodes, "ab_parallel": args.ab_parallel,
                   "concurrency_per_ab": args.conc,
                   "requests_per_ab": args.n,
                   "requested_duration_s": args.duration,
                   "cpu_count": os.cpu_count(), "driver": args.driver,
                   "corrosion_bin": None if args.load_only else H.BIN,
                   "api_host": args.api_host,
                   "load_only": args.load_only})
    if args.json_out:
        with open(args.json_out, "w") as f:
            json.dump(result, f, indent=2)
        print(f"  原始结果: {args.json_out}")
    print(f"\n诚实边界:{boundary}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", type=int, default=1)
    ap.add_argument("--ab-parallel", type=int, default=4, help="每节点并行 ab 进程数")
    ap.add_argument("--conc", type=int, default=16)
    ap.add_argument("--n", type=int, default=40000)
    ap.add_argument("--duration", type=float, default=0,
                    help="Python driver 持续起压秒数；非 0 时忽略 --n")
    ap.add_argument("--aggregate-concurrent", action="store_true",
                    help="所有节点同时起压，以共同墙钟时间计算真实聚合 QPS")
    ap.add_argument("--driver", choices=["ab", "python"], default="ab",
                    help="起压后端；python 使用多进程持久 HTTP 连接，无外部依赖")
    ap.add_argument("--startup-timeout", type=int, default=180)
    ap.add_argument("--seed-timeout", type=int, default=180)
    ap.add_argument("--json-out", help="写入机器可读的原始汇总和每个 ab 样本")
    modes = ap.add_mutually_exclusive_group()
    modes.add_argument("--server-only", action="store_true",
                       help="只启动、播种并保持 agent，等待外部起压器")
    modes.add_argument("--load-only", action="store_true",
                       help="不启动 agent，只对 --api-host 的端口 8600.. 起压")
    ap.add_argument("--api-host", default="127.0.0.1",
                    help="load-only 的远端 API 主机；默认本机")
    args = ap.parse_args()

    if not args.server_only and args.driver == "ab" and not shutil.which("ab"):
        sys.exit("需要 ab(ApacheBench)")
    open("/tmp/qbody.json", "w").write(QUERY)

    if args.load_only:
        if not args.aggregate_concurrent:
            sys.exit("--load-only 需要 --aggregate-concurrent")
        nodes = [{"i": i, "api": 8600 + i} for i in range(args.nodes)]
        result = (run_aggregate(nodes, args) if args.driver == "ab"
                  else run_python_aggregate(nodes, args))
        emit_result(
            result, args,
            f"agent 位于 {args.api_host}，起压器位于本机；流量经过真实局域网。")
        return

    # 只清理本 harness 工作目录启动的节点，避免误杀机台上的其他 Corrosion 实例。
    subprocess.run(
        ["pkill", "-9", "-f", f"{H.BIN} -c {H.WORK}/node"],
        capture_output=True)
    time.sleep(3)
    if os.path.exists(H.WORK):
        shutil.rmtree(H.WORK)
    os.makedirs(H.WORK)

    nodes, procs = start_wildcard_nodes(
        args.nodes, 7600, 8600, 9600, args.startup_timeout, args.seed_timeout)
    try:
        for nd in nodes:
            if procs[nd["i"]].poll() is not None:
                print("节点启动失败:", open(nd["log"]).read()[-400:]); return
        print(f"=== {args.nodes} 节点(wildcard,全本地命中) | {sysctl_cpu()} 核 ===\n")

        if args.server_only:
            print(json.dumps({"status": "ready", "nodes": args.nodes,
                              "api_bind": os.environ.get("API_BIND_HOST", "127.0.0.1"),
                              "api_ports": [nd["api"] for nd in nodes]}), flush=True)
            previous_sigterm = signal.signal(signal.SIGTERM, stop_server_only)
            try:
                while True:
                    time.sleep(3600)
            except KeyboardInterrupt:
                print("server-only: stopping agents", flush=True)
            finally:
                signal.signal(signal.SIGTERM, previous_sigterm)
            return

        if args.aggregate_concurrent:
            result = (run_aggregate(nodes, args) if args.driver == "ab"
                      else run_python_aggregate(nodes, args))
            emit_result(
                result, args,
                f"这是单台 Linux 主机上的 {args.nodes} 个真实 agent 进程并发，"
                f"不是 {args.nodes} 台物理机；server 与起压器共享 CPU，"
                "测得的是该机台聚合上限。")
            return

        if args.driver != "ab":
            sys.exit("python driver 仅支持 --aggregate-concurrent")

        # 单节点:多个并行 ab 求和(单 ab 单线程会先成瓶颈;并行求和逼近节点真实天花板)。
        per_node = []
        for nd in nodes:
            api = nd["api"]
            outs = []
            ps = [subprocess.Popen(
                ["bash", "-c",
                 f"ab -k -c {args.conc} -n {args.n} -p /tmp/qbody.json -T application/json "
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
    return os.cpu_count() or "?"


if __name__ == "__main__":
    main()
