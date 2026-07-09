#!/usr/bin/env python3
"""多跳路由机制验证(4.2.3「不确定链路通断下数据推送多跳路由机制」)。

corrosion 的多跳路由 = rebroadcast(疫情式多跳快路径,selector 每跳按 interest 过滤)
                     + anti-entropy sync(收敛兜底)。
本实验用 CORRO_LINK_FAULTS(应用层 per-peer 故障注入,只切广播数据面 send_uni,
SWIM/foca 走 send_datagram 不受影响 → 节点互知存活,是"链路断"不是"节点失联",归因干净)
验证两条路径在链路通断下的行为:

  场景 A(直连断 → 多跳快路径):
    writer(node0) → needer(nodeN-1) 的广播链路 drop_p=1.0 切死;
    全员 sync backoff 拉到 60~120s(隔离对账,60s 内到达的数据必然走广播多跳);
    写 R 行 → needer 直读本地 db 收齐 → 数据必经中继节点 rebroadcast(≥2 跳)到达。
    对照(无故障)给直连基准时延,Δ = 多跳绕行代价。

  场景 B(needer 广播全断 → sync 兜底):
    所有发送方 → needer 的广播链路全 drop_p=1.0(快路径整体失效);
    sync 用默认 backoff;needer 仍收齐 → anti-entropy 把漏收的补上(收敛兜底)。

证据链:发送方 corro.research.link.broadcast.dropped > 0(链路真在切、广播真在尝试)
       + needer count_rows_local 收齐(直读 db,绕开查询路由,见 run.py 注释)。

用法: python3 research/harness/multihop_test.py --nodes 6 --rows 20
"""
import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import run as H

WORK = H.WORK
BG, BA, BP = 7900, 8900, 9900
DROPPED = "corro.research.link.broadcast.dropped"


def node_addr(i):
    return f"[::1]:{BG + i}"


def write_schema():
    schema_dir = os.path.join(WORK, "schema")
    os.makedirs(schema_dir, exist_ok=True)
    with open(os.path.join(schema_dir, "mission.sql"), "w") as f:
        f.write("CREATE TABLE flight (id BLOB NOT NULL PRIMARY KEY, "
                "data TEXT NOT NULL DEFAULT '');\n")
        f.write("CREATE TABLE node_interest (actor_id BLOB NOT NULL, "
                "table_name TEXT NOT NULL, active INTEGER NOT NULL DEFAULT 1, "
                "PRIMARY KEY (actor_id, table_name));\n")
    return schema_dir


def write_cfg(i, schema_dir, sync_backoff=None):
    """全员关心 flight(候选池=全员,考察的是链路断时的绕行,不是减量)。
    sync_backoff=(min,max) 秒:拉长对账间隔以隔离广播快路径。"""
    boot = "" if i == 0 else f'"[::1]:{BG}"'
    perf = ""
    if sync_backoff:
        perf = (f"[perf]\nmin_sync_backoff = {sync_backoff[0]}\n"
                f"max_sync_backoff = {sync_backoff[1]}\n")
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
interest = ["flight"]
[api]
addr = "127.0.0.1:{BA + i}"
[admin]
path = "{WORK}/node{i}-admin.sock"
{perf}[telemetry]
prometheus.addr = "127.0.0.1:{BP + i}"
""")
    return {"i": i, "cfg": cfg, "api": BA + i, "prom": BP + i,
            "db": os.path.join(WORK, f"node{i}.db"),
            "log": os.path.join(WORK, f"node{i}.log")}


def interest_propagated(nodes, n):
    """每个节点直读本地 node_interest,须看到全员 n 行声明(interest 图完整)。"""
    for nd in nodes:
        r = H.sh(["sqlite3", nd["db"], "SELECT count(*) FROM node_interest"])
        try:
            if int(r.stdout.strip()) < n:
                return False
        except ValueError:
            return False
    return True


def start_cluster(n, faults_of, sync_backoff=None):
    """faults_of: i -> faults dict(该节点进程的 CORRO_LINK_FAULTS)或 None。"""
    if os.path.exists(WORK):
        shutil.rmtree(WORK)
    os.makedirs(WORK)
    schema_dir = write_schema()
    nodes = [write_cfg(i, schema_dir, sync_backoff) for i in range(n)]
    procs = []
    for nd in nodes:
        env = dict(os.environ)
        env.pop("CORRO_LINK_FAULTS", None)
        f = faults_of(nd["i"])
        if f:
            env["CORRO_LINK_FAULTS"] = json.dumps(f)
        with open(nd["log"], "w") as log:
            procs.append(subprocess.Popen([H.BIN, "-c", nd["cfg"], "agent"],
                                          stdout=log, stderr=subprocess.STDOUT, env=env))
    return nodes, procs


def stop_cluster(procs):
    for p in procs:
        p.send_signal(signal.SIGTERM)
    for p in procs:
        try:
            p.wait(timeout=5)
        except Exception:
            p.kill()


def run_scenario(label, n, rows, faults_of, sync_backoff, deliver_timeout):
    """起集群→等 interest 图完整→node0 写 rows 行→轮询 needer(nodeN-1)直读收齐。
    返回 (到达耗时 s 或 None, 全网 dropped 计数)。"""
    subprocess.run(["pkill", "-9", "-f", "target/debug/corrosion"], capture_output=True)
    time.sleep(1)
    nodes, procs = start_cluster(n, faults_of, sync_backoff)
    needer = nodes[-1]
    try:
        if not H.wait_active(nodes, timeout=40):
            print(f"  ⚠ [{label}] 集群未全 ACTIVE")
            return None, 0
        # 启动时 write_own_interest 发生在成员网格建立前(广播无候选),平时靠 sync 秒级补齐;
        # 场景 A 把 sync 拉到 60s+,须在 ACTIVE 后每节点重写一次自己的 interest 行,
        # 让声明经广播(此时成员已知)送达全网,不依赖 sync。
        time.sleep(2)
        for nd in nodes:
            H.sh([H.BIN, "-c", nd["cfg"], "exec",
                  "INSERT OR REPLACE INTO node_interest (actor_id, table_name) "
                  "VALUES (crsql_site_id(), 'flight')"])
        t0 = time.time()
        while time.time() - t0 < 60:
            if interest_propagated(nodes, n):
                break
            time.sleep(0.5)
        else:
            print(f"  ⚠ [{label}] interest 图 60s 未完整")
            return None, 0
        time.sleep(3)  # interest 刷新周期(3s tick)落一拍

        prefix = f"mh{int(time.time())}_"
        t_write = time.time()
        for j in range(rows):
            H.sh([H.BIN, "-c", nodes[0]["cfg"], "exec",
                  "--param", f"{prefix}{j}", "--param", f"v{j}",
                  "INSERT INTO flight (id,data) VALUES (?,?)"])
        t_deliver = None
        while time.time() - t_write < deliver_timeout:
            if H.count_rows_local(needer, "flight", prefix) >= rows:
                t_deliver = time.time() - t_write
                break
            time.sleep(0.2)
        dropped = sum(H.scrape(nd, [DROPPED])[DROPPED] for nd in nodes)
        got = H.count_rows_local(needer, "flight", prefix)
        print(f"  [{label}] needer 收到 {got}/{rows} 行"
              f"  耗时={f'{t_deliver:.1f}s' if t_deliver else f'超时(>{deliver_timeout}s)'}"
              f"  全网广播丢弃计数={int(dropped)}")
        return t_deliver, dropped
    finally:
        stop_cluster(procs)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", type=int, default=6)
    ap.add_argument("--rows", type=int, default=20)
    args = ap.parse_args()
    if not os.path.exists(H.BIN):
        sys.exit(f"找不到 binary {H.BIN}(先 cargo build)")
    n, rows = args.nodes, args.rows
    needer_addr = node_addr(n - 1)
    cut = {needer_addr: {"drop_p": 1.0}}
    slow_sync = (60, 120)  # 隔离对账:60s 内到达必然走广播多跳

    print(f"== 多跳路由验证:{n} 节点全关心 flight,writer=node0,needer=node{n-1} ==\n")

    print("对照(无故障,直连可用,sync 同样拉长——同条件基准):")
    t_ctrl, _ = run_scenario("对照", n, rows, lambda i: None, slow_sync, 30)

    print("\n场景 A(writer→needer 直连 drop_p=1.0,sync 60~120s 隔离 → 多跳快路径):")
    t_a, drop_a = run_scenario(
        "A", n, rows, lambda i: cut if i == 0 else None, slow_sync, 30)

    print("\n场景 B(全部发送方→needer drop_p=1.0,sync 默认 → anti-entropy 兜底):")
    t_b, drop_b = run_scenario(
        "B", n, rows, lambda i: cut if i != n - 1 else None, None, 60)

    print("\n== 结论 ==")
    ok_a = t_a is not None and drop_a > 0
    ok_b = t_b is not None and drop_b > 0
    if t_ctrl is not None:
        print(f"  直连基准:{t_ctrl:.1f}s")
    if ok_a:
        d = f"(+{t_a - t_ctrl:.1f}s 绕行代价)" if t_ctrl is not None else ""
        print(f"  ✅ A 直连切断仍 {t_a:.1f}s 收齐{d},且 sync 被隔离(60s+)"
              f" → 数据必经中继 rebroadcast 多跳(≥2跳)到达。")
    else:
        print("  ❌ A 失败:多跳快路径未在期限内送达。")
    if ok_b:
        print(f"  ✅ B 广播快路径全断仍 {t_b:.1f}s 收齐"
              f" → anti-entropy sync 兜底收敛(不确定链路下最终一致)。")
    else:
        print("  ❌ B 失败:sync 兜底未收敛。")
    sys.exit(0 if ok_a and ok_b else 1)


if __name__ == "__main__":
    main()
