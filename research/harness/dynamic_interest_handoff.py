#!/usr/bin/env python3
"""三节点动态 interest 摘除/handoff 回归。

1. node1 摘除 target 时只有 node0 一个其它 ready holder，min_replicas=2，必须失败且旧声明保留。
2. node2 扩大 interest，完整回填 target 历史并将 active 从 0 发布为 1。
3. node1 再次摘除成功；后续 target 新写入不再本地复制，但 API 会路由到 ready holder。

placement 变更按协议串行执行；此测试刻意不模拟两个节点并发摘除。
"""
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
BG, BA, BP = 7800, 8800, 9800
ROWS = 12


def write_schema():
    schema_dir = os.path.join(WORK, "schema")
    os.makedirs(schema_dir, exist_ok=True)
    with open(os.path.join(schema_dir, "mission.sql"), "w") as f:
        for table in ("flight", "target"):
            f.write(
                f"CREATE TABLE {table} (id BLOB NOT NULL PRIMARY KEY, "
                "data TEXT NOT NULL DEFAULT '');\n"
            )
        f.write(
            "CREATE TABLE node_interest (actor_id BLOB NOT NULL, "
            "table_name TEXT NOT NULL, active INTEGER NOT NULL DEFAULT 1, "
            "PRIMARY KEY (actor_id, table_name));\n"
        )
    return schema_dir


def cfg_path(i):
    return os.path.join(WORK, f"node{i}.toml")


def write_config(i, interests, schema_dir):
    boot = "" if i == 0 else f'"[::1]:{BG}"'
    interest = ", ".join(f'"{table}"' for table in interests)
    with open(cfg_path(i), "w") as f:
        f.write(
            f"""[db]
path = "{WORK}/node{i}.db"
schema_paths = ["{schema_dir}"]
[gossip]
addr = "[::]:{BG + i}"
external_addr = "[::1]:{BG + i}"
bootstrap = [{boot}]
plaintext = true
broadcast_strategy = "scored_reduce"
interest = [{interest}]
interest_min_replicas = 2
[api]
addr = "127.0.0.1:{BA + i}"
[admin]
path = "{WORK}/node{i}-admin.sock"
[telemetry]
prometheus.addr = "127.0.0.1:{BP + i}"
"""
        )
    return {
        "i": i,
        "cfg": cfg_path(i),
        "api": BA + i,
        "prom": BP + i,
        "db": os.path.join(WORK, f"node{i}.db"),
        "log": os.path.join(WORK, f"node{i}.log"),
    }


def start_one(node):
    with open(node["log"], "a") as log:
        return subprocess.Popen(
            [H.BIN, "-c", node["cfg"], "agent"],
            stdout=log,
            stderr=subprocess.STDOUT,
        )


def stop(proc):
    if proc.poll() is not None:
        return
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def scalar(node, sql):
    result = H.sh(["sqlite3", node["db"], sql])
    value = result.stdout.strip()
    return int(value) if value else 0


def wait_until(predicate, timeout=40, label="condition"):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(1)
    raise RuntimeError(f"timed out waiting for {label}")


def self_interest(node, table):
    return scalar(
        node,
        "SELECT count(*) FROM node_interest WHERE actor_id="
        "(SELECT site_id FROM crsql_site_id WHERE ordinal=0) "
        f"AND table_name='{table}' AND active=1",
    )


def main():
    subprocess.run(
        ["pkill", "-9", "-f", "target/debug/corrosion"], capture_output=True
    )
    time.sleep(2)
    if os.path.exists(WORK):
        shutil.rmtree(WORK)
    os.makedirs(WORK)
    schema_dir = write_schema()

    specs = {0: ["*"], 1: ["flight", "target"], 2: ["flight"]}
    nodes = {i: write_config(i, specs[i], schema_dir) for i in specs}
    procs = {i: start_one(nodes[i]) for i in nodes}

    try:
        wait_until(
            lambda: all(self_interest(nodes[i], specs[i][0]) == 1 for i in nodes),
            label="initial ready interests",
        )
        time.sleep(8)

        prefix = f"handoff_{int(time.time())}_"
        for row in range(ROWS):
            H.sh(
                [
                    H.BIN,
                    "-c",
                    nodes[0]["cfg"],
                    "exec",
                    "--param",
                    f"{prefix}{row}",
                    "--param",
                    f"target-{row}",
                    "INSERT INTO target (id,data) VALUES (?,?)",
                ]
            )
        wait_until(
            lambda: H.count_rows_local(nodes[1], "target", prefix) == ROWS,
            label="node1 initial target replication",
        )

        print("阶段1:只有 node0 一个其它 ready holder，node1 摘除 target 必须失败")
        stop(procs[1])
        write_config(1, ["flight"], schema_dir)
        procs[1] = start_one(nodes[1])
        wait_until(lambda: procs[1].poll() is not None, timeout=25, label="unsafe removal failure")
        unsafe_rc = procs[1].returncode
        retained = self_interest(nodes[1], "target")
        unsafe_log = open(nodes[1]["log"]).read()
        print(f"  exit={unsafe_rc}, target ready row retained={retained}")
        if unsafe_rc == 0 or retained != 1 or "unsafe interest removal" not in unsafe_log:
            raise RuntimeError("unsafe removal did not fail closed")

        # 先以旧配置恢复 node1，让它在线接收 node2 的 ready 声明。
        write_config(1, ["flight", "target"], schema_dir)
        procs[1] = start_one(nodes[1])
        wait_until(lambda: procs[1].poll() is None and self_interest(nodes[1], "target") == 1,
                   label="node1 recovery")

        print("阶段2:node2 扩大到 target，必须先回填历史再发布 active=1")
        stop(procs[2])
        write_config(2, ["flight", "target"], schema_dir)
        procs[2] = start_one(nodes[2])
        wait_until(
            lambda: self_interest(nodes[2], "target") == 1
            and H.count_rows_local(nodes[2], "target", prefix) == ROWS,
            timeout=50,
            label="node2 backfill and readiness",
        )
        node2_actor = H.sh(
            [
                "sqlite3",
                nodes[2]["db"],
                "SELECT hex(site_id) FROM crsql_site_id WHERE ordinal=0",
            ]
        ).stdout.strip()
        wait_until(
            lambda: scalar(
                nodes[1],
                "SELECT count(*) FROM node_interest "
                f"WHERE hex(actor_id)='{node2_actor}' AND table_name='target' AND active=1",
            )
            == 1,
            label="node2 ready publication at node1",
        )
        print("  node2 历史完整且 ready 声明已传播到 node1")

        print("阶段3:已有 node0+node2 两个其它 ready holder，node1 摘除必须成功")
        stop(procs[1])
        write_config(1, ["flight"], schema_dir)
        procs[1] = start_one(nodes[1])
        wait_until(
            lambda: procs[1].poll() is None and self_interest(nodes[1], "target") == 0,
            label="safe target removal",
        )
        node1_actor = H.sh(
            [
                "sqlite3",
                nodes[1]["db"],
                "SELECT hex(site_id) FROM crsql_site_id WHERE ordinal=0",
            ]
        ).stdout.strip()
        wait_until(
            lambda: scalar(
                nodes[0],
                "SELECT count(*) FROM node_interest "
                f"WHERE hex(actor_id)='{node1_actor}' AND table_name='target'",
            )
            == 0,
            label="target removal publication at writer",
        )
        time.sleep(4)  # 等广播 selector 的 3 秒 interest 缓存刷新

        H.sh(
            [
                H.BIN,
                "-c",
                nodes[0]["cfg"],
                "exec",
                "--param",
                f"{prefix}new",
                "--param",
                "after-handoff",
                "INSERT INTO target (id,data) VALUES (?,?)",
            ]
        )
        wait_until(
            lambda: H.count_rows_local(nodes[2], "target", prefix) == ROWS + 1,
            label="post-handoff replication",
        )
        time.sleep(5)
        node1_local = H.count_rows_local(nodes[1], "target", prefix)
        node1_api = H.count_rows(nodes[1], "target", prefix)
        print(f"  node1 local={node1_local}, API routed result={node1_api}")
        if node1_local != ROWS or node1_api != ROWS + 1:
            raise RuntimeError("post-removal storage/routing boundary is incorrect")

        print("\n✅ PASS:摘除门禁、回填后发布、handoff 后查询路由均符合预期。")
    finally:
        for proc in procs.values():
            stop(proc)


if __name__ == "__main__":
    main()
