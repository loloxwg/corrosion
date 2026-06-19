#!/usr/bin/env python3
"""查询路由验证(4.4.2)——任意节点查任意数据。

部分复制后 recon 节点本地没有 battlefield/target 数据。验证：
  - 查本地表(flight) → 本地直接答(快路径)
  - 查非本地表(battlefield/target) → 经 QUIC 路由到持有者(strike/jam) → 拿回正确计数

用法: python3 research/harness/query_routing_test.py
"""
import os
import shutil
import signal
import subprocess
import time

import run as H


def query_count(nd, table, prefix):
    """在 nd 节点经 /v1/queries(corrosion query CLI)查计数；本地没有则触发路由。"""
    return H.count_rows(nd, table, prefix)


def main():
    if os.path.exists(H.WORK):
        shutil.rmtree(H.WORK)
    os.makedirs(H.WORK)
    nodes = H.write_configs(6, 7400, 8400, 9400, "scored_reduce")
    procs = H.start(nodes)
    try:
        if not H.wait_active(nodes):
            print("未全 ACTIVE"); return
        H.write_interest(nodes)
        time.sleep(12)

        prefix = f"q{int(time.time())}_"
        rows = 15
        for t in H.TABLES:
            for j in range(rows):
                H.sh([H.BIN, "-c", nodes[0]["cfg"], "exec",
                      "--param", f"{prefix}{j}", "--param", f"{t}-{j}",
                      f"INSERT INTO {t} (id,data) VALUES (?,?)"])
        time.sleep(12)

        # node3 = recon (interest=flight)：battlefield/target 本地应缺失，靠路由
        recon = next(nd for nd in nodes if nd["i"] == 3)
        print(f"在 recon 节点 node3(interest=flight)上查询(期望每个 ={rows}):\n")
        results = {}
        for t in H.TABLES:
            local_kind = "本地" if t in H.ROLE_TABLES["recon"] else "路由"
            c = query_count(recon, t, prefix)
            results[t] = c
            ok = "✓" if c == rows else "✗"
            print(f"  查 {t:<12}({local_kind}) → 计数 {c}   {ok}")

        all_ok = all(results[t] == rows for t in H.TABLES)
        routed_ok = all(results[t] == rows for t in H.TABLES
                        if t not in H.ROLE_TABLES["recon"])
        print("\n判定：")
        print("  本地表查询正确：", "✓" if results["flight"] == rows else "✗")
        print("  非本地表经路由查到正确结果：", "✓" if routed_ok else "✗")
        print("\n总判定：", "✅ 查询路由 PASS" if all_ok else "❌ FAIL")
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
