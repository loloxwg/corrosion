#!/usr/bin/env python3
"""定位泄漏：只写 battlefield，看非关心节点的数据从 push 还是 sync 来。

4 节点(0,3=recon 关心 battlefield；1=strike,2=jam 不关心)。只从 node0 写 battlefield。
对每个节点测 battlefield 行数 + Δbroadcast.recv(push 路) + Δsync.changes.recv(sync 路)
+ Δ对账过滤计数。非关心节点(1,2)若有 battlefield，看是哪条路送的。
"""
import shutil, signal, subprocess, time, os, sys
import run as H

NODES = int(sys.argv[1]) if len(sys.argv) > 1 else 4

PATHS = ["corro.broadcast.recv.count", "corro.sync.changes.recv",
         "corro.sync.interest.filtered.versions", "corro.sync.interest.kept.versions"]


def main():
    if os.path.exists(H.WORK):
        shutil.rmtree(H.WORK)
    os.makedirs(H.WORK)
    nodes = H.write_configs(NODES, 7400, 8400, 9400, "scored_reduce")
    procs = H.start(nodes)
    try:
        if not H.wait_active(nodes):
            print("节点未 ACTIVE"); return
        H.write_interest(nodes)
        time.sleep(6)
        before = {nd["i"]: H.scrape(nd, PATHS) for nd in nodes}

        prefix = f"d{int(time.time())}_"
        for j in range(20):
            H.sh([H.BIN, "-c", nodes[0]["cfg"], "exec",
                  "--param", f"{prefix}{j}", "--param", f"battlefield-{j}",
                  "INSERT INTO battlefield (id,data) VALUES (?,?)"])
        print("battlefield 写入完成，等 15s（push+sync）...\n")
        time.sleep(15)

        after = {nd["i"]: H.scrape(nd, PATHS) for nd in nodes}
        print(f"{'节点':<6}{'角色':<8}{'关心battlefield?':<12}{'battlefield行':<10}"
              f"{'Δbcast.recv':<14}{'Δsync.recv':<13}{'Δfiltered':<11}{'Δkept':<8}")
        for nd in nodes:
            i, role = nd["i"], nd["role"]
            cares = "battlefield" in H.ROLE_TABLES[role]
            fc = H.count_rows(nd, "battlefield", prefix)
            d = {m: after[i][m] - before[i][m] for m in PATHS}
            print(f"{i:<6}{role:<8}{str(cares):<12}{fc:<10}"
                  f"{int(d['corro.broadcast.recv.count']):<14}"
                  f"{int(d['corro.sync.changes.recv']):<13}"
                  f"{int(d['corro.sync.interest.filtered.versions']):<11}"
                  f"{int(d['corro.sync.interest.kept.versions']):<8}")
        print("\n看非关心节点(strike/jam)：battlefield行>0 且 Δbcast.recv 高 → push 泄漏；"
              "Δsync.recv 高 → sync 过滤失效")
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
