#!/usr/bin/env python3
"""从独立负载机并发压测远端 Corrosion agent，汇总每节点 QPS/尾延迟。

agent 侧先用 qps_bench.py --server-only 启动；本脚本在另一台机器运行 oha，
避免 agent 与负载发生器争抢 CPU。
"""

import argparse
import json
import os
import shutil
import statistics
import subprocess
import sys
import time


QUERY = '"SELECT data FROM flight WHERE id = \'k42\'"'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", required=True)
    ap.add_argument("--nodes", type=int, required=True)
    ap.add_argument("--base-port", type=int, default=8600)
    ap.add_argument("--connections", type=int, default=64)
    ap.add_argument("--duration", default="10s")
    ap.add_argument("--oha", default="oha")
    ap.add_argument("--output-dir", required=True)
    ap.add_argument("--label", default="external-oha")
    args = ap.parse_args()

    if not os.path.exists(args.oha) and not shutil.which(args.oha):
        sys.exit(f"找不到 oha: {args.oha}")
    os.makedirs(args.output_dir, exist_ok=True)
    body_path = os.path.join(args.output_dir, "query-body.json")
    with open(body_path, "w") as f:
        f.write(QUERY)

    processes = []
    started = time.time()
    for node in range(args.nodes):
        result_path = os.path.join(args.output_dir, f"{args.label}-node{node}.json")
        cmd = [
            args.oha, "--no-tui", "-z", args.duration,
            "-c", str(args.connections), "-m", "POST",
            "-T", "application/json", "-D", body_path,
            "--output-format", "json", "-o", result_path,
            f"http://{args.host}:{args.base_port + node}/v1/queries",
        ]
        processes.append((node, result_path, subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)))

    results = []
    for node, result_path, proc in processes:
        stdout, stderr = proc.communicate()
        if proc.returncode != 0:
            results.append({"node": node, "returncode": proc.returncode,
                            "stderr": stderr[-500:], "stdout": stdout[-500:]})
            continue
        with open(result_path) as f:
            raw = json.load(f)
        metrics = raw["metrics"]
        results.append({
            "node": node,
            "returncode": proc.returncode,
            "qps": metrics["requests_per_sec"],
            "success_rate": metrics["success_rate"],
            "latency_ms": metrics["latency_ms"],
            "status_codes": raw.get("statusCodeDistribution", {}),
            "errors": raw.get("errorDistribution", {}),
        })
    wall = time.time() - started

    valid = [r for r in results if r["returncode"] == 0]
    qps = [r["qps"] for r in valid]
    summary = {
        "host": args.host,
        "nodes": args.nodes,
        "connections_per_node": args.connections,
        "duration": args.duration,
        "wall_s": wall,
        "process_failures": args.nodes - len(valid),
        "aggregate_qps": sum(qps),
        "per_node_qps_min": min(qps, default=0),
        "per_node_qps_median": statistics.median(qps) if qps else 0,
        "per_node_qps_max": max(qps, default=0),
        "nodes_at_or_above_10k": sum(v >= 10_000 for v in qps),
        "nodes_success_rate_100pct": sum(r["success_rate"] == 1.0 for r in valid),
        "per_node_p95_ms_max": max((r["latency_ms"]["p95"] for r in valid), default=0),
        "per_node_p99_ms_max": max((r["latency_ms"]["p99"] for r in valid), default=0),
        "results": results,
    }
    summary_path = os.path.join(args.output_dir, f"{args.label}-summary.json")
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)

    print(f"aggregate QPS: {summary['aggregate_qps']:,.0f}")
    print("per-node QPS min/median/max: "
          f"{summary['per_node_qps_min']:,.0f} / "
          f"{summary['per_node_qps_median']:,.0f} / "
          f"{summary['per_node_qps_max']:,.0f}")
    print(f">=10K nodes: {summary['nodes_at_or_above_10k']}/{args.nodes}")
    print(f"100% success nodes: {summary['nodes_success_rate_100pct']}/{args.nodes}")
    print(f"worst node P95/P99: {summary['per_node_p95_ms_max']:.3f} / "
          f"{summary['per_node_p99_ms_max']:.3f} ms")
    print(f"summary: {summary_path}")

    if (summary["process_failures"] or
            summary["nodes_success_rate_100pct"] != args.nodes):
        sys.exit(1)


if __name__ == "__main__":
    main()
