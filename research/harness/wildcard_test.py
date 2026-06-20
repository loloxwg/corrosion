#!/usr/bin/env python3
"""wildcard interest 验证:interest=["*"] 节点 = 全量节点(关心全部),角色节点仍部分复制。

3 节点:node0=recon(flight,写入者)、node1=jam(target)、node2=wildcard(*)。
node0 写满 3 张表。直读本地 db(绕查询路由)验证:
  - node2(*)   本地应有 **全部 3 张表**(flight+battlefield+target)——wildcard 收全。
  - node1(jam) 本地应只有 target,无 flight/battlefield——部分复制不被 wildcard 破坏。
corrosion 启动自写 node_interest(含 "*" 行),故用 SKIP_WRITE_INTEREST=1 跑。

用法: python3 research/harness/wildcard_test.py
"""
import os
import shutil
import signal
import subprocess
import time

import run as H


def main():
    os.environ["SKIP_WRITE_INTEREST"] = "1"  # 靠 corrosion 自写(含 "*" 行)
    if os.path.exists(H.WORK):
        shutil.rmtree(H.WORK)
    os.makedirs(H.WORK)

    nodes = H.write_configs(3, 7400, 8400, 9400, "scored_reduce")
    # write_configs 按 i%3 分角色:node0=recon, node1=strike, node2=jam。
    # 本测试要 recon/jam/wildcard 拓扑:① node1 strike→jam(仅 target) ② node2 jam→wildcard(*)。
    cfg1 = nodes[1]["cfg"]
    t1 = open(cfg1).read().replace('interest = ["battlefield", "target"]', 'interest = ["target"]')
    open(cfg1, "w").write(t1)
    nodes[1]["role"] = "jam"
    cfg2 = nodes[2]["cfg"]
    t2 = open(cfg2).read().replace('interest = ["target"]', 'interest = ["*"]')
    open(cfg2, "w").write(t2)
    nodes[2]["role"] = "wildcard"

    procs = H.start(nodes)
    try:
        if not H.wait_active(nodes):
            print("节点未全部 ACTIVE"); return
        time.sleep(18)  # 等 interest(含 "*")传播 + 推送端缓存刷新

        prefix = f"w{int(time.time())}_"
        for t in H.TABLES:
            for j in range(20):
                H.sh([H.BIN, "-c", nodes[0]["cfg"], "exec",
                      "--param", f"{prefix}{j}", "--param", f"{t}-{j}",
                      f"INSERT INTO {t} (id,data) VALUES (?,?)"])
        print("写入完成,等 anti-entropy 20s ...\n")
        time.sleep(20)

        expect = {
            0: {"flight": 20, "battlefield": 0, "target": 0},   # recon 写入者(本地写啥有啥,这里只判关心)
            1: {"flight": 0, "battlefield": 0, "target": 20},   # jam 仅 target
            2: {"flight": 20, "battlefield": 20, "target": 20}, # wildcard 全收
        }
        roles = {0: "recon", 1: "jam", 2: "wildcard(*)"}
        ok = True
        print(f"{'节点':<6}{'角色':<14}{'flight':<9}{'battlefield':<13}{'target':<9}{'判定'}")
        for nd in nodes:
            i = nd["i"]
            local = {t: H.count_rows_local(nd, t, prefix) for t in H.TABLES}
            # node0 是写入者,本地必有全部;只对 1/2 严格判定。
            if i == 0:
                verdict = "(写入者)"
            else:
                good = all(local[t] == expect[i][t] for t in H.TABLES)
                ok = ok and good
                verdict = "✓" if good else "✗"
            print(f"{i:<6}{roles[i]:<14}{local['flight']:<9}"
                  f"{local['battlefield']:<13}{local['target']:<9}{verdict}")

        print("\n判定:wildcard(*) 节点本地收全 3 表 + jam 节点仍只有 target:",
              "✅ PASS" if ok else "❌ FAIL")

        # 顺带验证:jam 节点查它本地没有的 flight,应经查询路由拿回(wildcard 可做持有者)。
        r = H.sh([H.BIN, "-c", nodes[1]["cfg"], "query",
                  f"SELECT count(*) FROM flight WHERE id LIKE '{prefix}%'"])
        routed = r.stdout.strip().split("|")[0]
        print(f"附:jam 节点经查询路由查 flight(本地无)→ 拿回 {routed} 行(期望 20,wildcard 持有)")
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
