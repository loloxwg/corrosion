#!/usr/bin/env python3
"""critical 表传输优先级验证(4.3.3「优先将作战任务目标态势数据及时、精准推送」的"及时")。

机制:广播批路径(全局 + 每跳 rebroadcast)默认攒批,scope 不变时最多等 bcast_interval
(500ms)tick 才发。`gossip.critical_tables` 里的表触发缓冲立即 flush,当轮发送 →
多跳时延从 O(跳数×攒批间隔) 降到 O(跳数×RTT)。

实验(复用 multihop 场景 A 拓扑,把数据逼上 rebroadcast 批路径——攒批延迟住在那):
  - 6 节点全关心 flight+target;writer(node0)→needer(node5) 广播直连 drop_p=1.0;
    sync 拉长 60~120s 隔离 → needer 只能靠中继 rebroadcast 收到。
  - 逐行写→等 needer 直读可见→记单行到达时延(python sqlite3 细粒度轮询)。
  - 先写一块 flight(非 critical,每行吃一次中继攒批 tick),再写一块 target(critical,
    中继立即 flush)。分块避免交替写触发 scope-change flush 污染对照。
  - 判定:target 到达时延中位数 << flight(预期 flight ~几百 ms,target ~几十 ms)。

用法: python3 research/harness/critical_latency_test.py --nodes 6 --rows 15
"""
import argparse
import json
import os
import shutil
import signal
import sqlite3
import statistics
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import run as H

WORK = H.WORK
BG, BA, BP = 7900, 8900, 9900


def node_addr(i):
    return f"[::1]:{BG + i}"


def write_schema():
    schema_dir = os.path.join(WORK, "schema")
    os.makedirs(schema_dir, exist_ok=True)
    with open(os.path.join(schema_dir, "mission.sql"), "w") as f:
        for t in ("flight", "target"):
            f.write(f"CREATE TABLE {t} (id BLOB NOT NULL PRIMARY KEY, "
                    "data TEXT NOT NULL DEFAULT '');\n")
        f.write("CREATE TABLE node_interest (actor_id BLOB NOT NULL, "
                "table_name TEXT NOT NULL, active INTEGER NOT NULL DEFAULT 1, "
                "PRIMARY KEY (actor_id, table_name));\n")
    return schema_dir


def write_cfg(i, schema_dir):
    boot = "" if i == 0 else f'"[::1]:{BG}"'
    cfg = os.path.join(WORK, f"node{i}.toml")
    with open(cfg, "w") as f:
        f.write(f"""[db]
path = "{WORK}/node{i}.db"
schema_paths = ["{schema_dir}"]
[gossip]
addr = "[::]:{BG + i}"
external_addr = "[::1]:{BG + i}"
bootstrap = [{boot}]
plaintext = true
broadcast_strategy = "rl"
interest = ["flight", "target"]
critical_tables = ["target"]
[perf]
min_sync_backoff = 60
max_sync_backoff = 120
[api]
addr = "127.0.0.1:{BA + i}"
[admin]
path = "{WORK}/node{i}-admin.sock"
[telemetry]
prometheus.addr = "127.0.0.1:{BP + i}"
""")
    return {"i": i, "cfg": cfg, "api": BA + i, "prom": BP + i,
            "db": os.path.join(WORK, f"node{i}.db"),
            "log": os.path.join(WORK, f"node{i}.log")}


def row_visible(db, table, row_id):
    """python sqlite3 直读(WAL 并发读,绕查询路由),细粒度轮询用。"""
    try:
        conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=0.1)
        try:
            cur = conn.execute(f"SELECT 1 FROM {table} WHERE id = ?", (row_id,))
            return cur.fetchone() is not None
        finally:
            conn.close()
    except sqlite3.Error:
        return False


def measure_block(writer, needer, table, prefix, rows, timeout=3.0):
    """逐行:写→轮询 needer 可见→记时延(s)。同表连写,每行吃自己的中继攒批等待。"""
    lat = []
    for j in range(rows):
        row_id = f"{prefix}{table}_{j}"
        H.sh([H.BIN, "-c", writer["cfg"], "exec",
              "--param", row_id, "--param", f"v{j}",
              f"INSERT INTO {table} (id,data) VALUES (?,?)"])
        t0 = time.time()
        while time.time() - t0 < timeout:
            if row_visible(needer["db"], table, row_id):
                lat.append(time.time() - t0)
                break
            time.sleep(0.005)
        else:
            lat.append(None)
    return lat


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", type=int, default=6)
    ap.add_argument("--rows", type=int, default=15)
    args = ap.parse_args()
    if not os.path.exists(H.BIN):
        sys.exit(f"找不到 binary {H.BIN}(先 cargo build)")
    n, rows = args.nodes, args.rows

    subprocess.run(["pkill", "-9", "-f", "target/debug/corrosion"], capture_output=True)
    time.sleep(1)
    if os.path.exists(WORK):
        shutil.rmtree(WORK)
    os.makedirs(WORK)
    schema_dir = write_schema()
    nodes = [write_cfg(i, schema_dir) for i in range(n)]
    needer_addr = node_addr(n - 1)
    print(f"== critical 传输优先级:{n} 节点,writer=node0,needer=node{n-1}"
          f"(直连切死,数据必走中继 rebroadcast)==")
    print("   flight=普通(攒批,每跳最多等 500ms tick) vs target=critical(立即 flush)\n")

    procs = []
    for nd in nodes:
        env = dict(os.environ)
        env.pop("CORRO_LINK_FAULTS", None)
        if nd["i"] == 0:  # 只切 writer→needer 直连
            env["CORRO_LINK_FAULTS"] = json.dumps({needer_addr: {"drop_p": 1.0}})
        with open(nd["log"], "w") as log:
            procs.append(subprocess.Popen([H.BIN, "-c", nd["cfg"], "agent"],
                                          stdout=log, stderr=subprocess.STDOUT, env=env))
    try:
        if not H.wait_active(nodes, timeout=40):
            sys.exit("集群未全 ACTIVE")
        # ACTIVE 后重写 interest(成员网格已建,广播可达;sync 已被拉长,见 multihop_test)
        time.sleep(2)
        for nd in nodes:
            for t in ("flight", "target"):
                H.sh([H.BIN, "-c", nd["cfg"], "exec",
                      "INSERT OR REPLACE INTO node_interest (actor_id, table_name) "
                      f"VALUES (crsql_site_id(), '{t}')"])
        time.sleep(6)  # interest 传播 + 3s 刷新周期落一拍

        prefix = f"cl{int(time.time())}_"
        writer, needer = nodes[0], nodes[-1]
        print(f"写 {rows} 行 flight(非 critical)...")
        lat_flight = measure_block(writer, needer, "flight", prefix, rows)
        time.sleep(1)
        print(f"写 {rows} 行 target(critical)...")
        lat_target = measure_block(writer, needer, "target", prefix, rows)

        drops = sum(H.scrape(nd, ["corro.research.link.broadcast.dropped"])
                    ["corro.research.link.broadcast.dropped"] for nd in nodes)
        crit = sum(H.scrape(nd, ["corro.broadcast.critical.flush"])
                   ["corro.broadcast.critical.flush"] for nd in nodes)
    finally:
        for p in procs:
            p.send_signal(signal.SIGTERM)
        for p in procs:
            try:
                p.wait(timeout=5)
            except Exception:
                p.kill()

    def stats(lat, name):
        ok = [x for x in lat if x is not None]
        lost = len(lat) - len(ok)
        if not ok:
            print(f"  {name}: 全部超时")
            return None
        med = statistics.median(ok)
        print(f"  {name}: 中位 {med*1000:.0f}ms  P90 {sorted(ok)[int(len(ok)*0.9)-1]*1000:.0f}ms"
              f"  (n={len(ok)}{f', 超时 {lost}' if lost else ''})")
        return med

    print(f"\n== needer 单行到达时延(经中继 ≥2 跳)==")
    mf = stats(lat_flight, "flight(普通) ")
    mt = stats(lat_target, "target(critical)")
    print(f"  证据:直连丢弃计数={int(drops)}(>0=切断生效)  critical flush 计数={int(crit)}")
    if mf and mt and mt < mf * 0.5 and crit > 0:
        print(f"\n  ✅ critical 表多跳时延 {mt*1000:.0f}ms << 普通表 {mf*1000:.0f}ms"
              f"(↓{(1-mt/mf)*100:.0f}%)→「及时」落在传输层:立即 flush 砍掉每跳攒批等待。")
        sys.exit(0)
    print("\n  ❌ 未达预期(critical 应显著低于普通表)")
    sys.exit(1)


if __name__ == "__main__":
    main()
