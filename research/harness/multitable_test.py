#!/usr/bin/env python3
"""多表事务残留验证 —— 压实路线A第1步的已知边界。

step1 按"表"分组只对单表广播成立；单条事务若同时写多张表(一个 db_version 碰多表)，
按版本级仍会整发给 union 关心者 → 非关心表搭便车(文档 §6.5/§4.0 的版本级边界)。
本测确认：单表写仍隔离正常，多表事务才出现残留(且不比文档更糟)。

在 recon 节点(interest=flight)上检查：
  - 单表写的 battlefield → 应缺失(step1 隔离生效)
  - 多表事务(flight+battlefield 同事务)里的 battlefield → 残留(present)
用法: python3 research/harness/multitable_test.py
"""
import json
import os
import shutil
import signal
import subprocess
import time
import urllib.request

import run as H


def tx(node, statements):
    """打 /v1/transactions：statements 为 SQL 字符串数组 = 一个事务。"""
    data = json.dumps(statements).encode()
    req = urllib.request.Request(
        f"http://127.0.0.1:{node['api']}/v1/transactions",
        data=data, headers={"Content-Type": "application/json"}, method="POST")
    try:
        return urllib.request.urlopen(req, timeout=5).read().decode()
    except Exception as e:
        return f"ERR {e}"


def has(nd, table, idv):
    r = H.sh([H.BIN, "-c", nd["cfg"], "query",
              f"SELECT count(*) FROM {table} WHERE id = '{idv}'"])
    try:
        return int(r.stdout.strip().split("|")[0]) > 0
    except (ValueError, IndexError):
        return None


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
        time.sleep(10)

        # node0=recon(写入端)。单表写 battlefield；多表事务(flight+battlefield)。
        print("单表写 battlefield(st_b)：", H.sh([H.BIN, "-c", nodes[0]["cfg"], "exec",
              "INSERT INTO battlefield (id,data) VALUES ('st_b','x')"]).returncode)
        print("多表事务 flight+battlefield(mt_f/mt_b)：",
              tx(nodes[0], ["INSERT INTO flight (id,data) VALUES ('mt_f','x')",
                            "INSERT INTO battlefield (id,data) VALUES ('mt_b','x')"])[:80])
        time.sleep(12)

        # 在非写入的 recon 节点(node3, interest=flight)上检查
        recon = next(nd for nd in nodes if nd["i"] == 3)
        print(f"\n在 recon 节点 node3(interest=flight)上：")
        print(f"  单表 battlefield 'st_b'  本地存在? {has(recon,'battlefield','st_b')}  (期望 False=隔离生效)")
        print(f"  多表事务 flight  'mt_f'  本地存在? {has(recon,'flight','mt_f')}  (期望 True=关心表收到)")
        print(f"  多表事务 battlefield 'mt_b' 本地存在? {has(recon,'battlefield','mt_b')}  (期望 True=多表残留,已知边界)")
        st_b = has(recon, 'battlefield', 'st_b')
        mt_b = has(recon, 'battlefield', 'mt_b')
        print("\n判定：")
        print("  单表隔离正常：", "✓" if st_b is False else "✗")
        print("  多表残留(如文档所述)：", "✓ 确认存在" if mt_b else "未出现(更好/或未触发)")
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
