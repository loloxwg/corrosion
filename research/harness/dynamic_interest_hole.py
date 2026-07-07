#!/usr/bin/env python3
"""失败用例:动态 interest 变更 → 永久数据洞(证 RL 跑中改 placement 的正确性 bug)。

假说(代码推理 + Codex 复核确认):
  节点 R 先不关心 target(gossip.interest=[flight])→ 对账把 target 版本当空版本发
  Changeset::Empty → process_empty_version 标 KnownDbVersion::Cleared → 关 gap。
  之后 R "重新关心" target → generate_sync 看不到那些版本的 gap(已 Cleared)→ 永不重取
  → R 本地永久缺 target 的历史版本,却以 holder 身份对外服务不完整数据 = 静默不一致。

对照:
  NodeA  写入方 + wildcard(持有全部,数据的源,证明集群里 target 历史是有的)。
  NodeR  先 interest=[flight],后"重新关心"target(改 node_interest + 重启带新 gossip.interest)。
  NodeS  从头就 interest=[flight,target](对照组,应拿到全部 target)。

判定:若 NodeR 重新关心 + 充分 settle 后,直读 sqlite 仍缺 target 历史行(而 NodeS 全有)
      → ✅ 复现永久数据洞(bug 坐实)。若 NodeR 补齐 → 假说被推翻(有救援路径)。

用法: python3 research/harness/dynamic_interest_hole.py
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
BG, BA, BP = 7700, 8700, 9700
ROWS = 15


def write_schema():
    schema_dir = os.path.join(WORK, "schema")
    os.makedirs(schema_dir, exist_ok=True)
    with open(os.path.join(schema_dir, "mission.sql"), "w") as f:
        for t in ("flight", "target"):
            f.write(f"CREATE TABLE {t} (id BLOB NOT NULL PRIMARY KEY, "
                    f"data TEXT NOT NULL DEFAULT '');\n")
        f.write("CREATE TABLE node_interest (actor_id BLOB NOT NULL, "
                "table_name TEXT NOT NULL, active INTEGER NOT NULL DEFAULT 1, "
                "PRIMARY KEY (actor_id, table_name));\n")
    return schema_dir


def cfg_path(i):
    return os.path.join(WORK, f"node{i}.toml")


def write_config(i, interest_list, schema_dir):
    """写单节点配置。i=0 为种子。interest_list=该节点 gossip.interest。"""
    boot = "" if i == 0 else f'"[::1]:{BG}"'
    interest = ", ".join(f'"{t}"' for t in interest_list)
    with open(cfg_path(i), "w") as f:
        f.write(f"""[db]
path = "{WORK}/node{i}.db"
schema_paths = ["{schema_dir}"]
[gossip]
addr = "[::]:{BG + i}"
external_addr = "[::1]:{BG + i}"
bootstrap = [{boot}]
plaintext = true
broadcast_strategy = "scored_reduce"
interest = [{interest}]
[api]
addr = "127.0.0.1:{BA + i}"
[admin]
path = "{WORK}/node{i}-admin.sock"
[telemetry]
prometheus.addr = "127.0.0.1:{BP + i}"
""")
    return {"i": i, "cfg": cfg_path(i), "api": BA + i, "prom": BP + i,
            "db": os.path.join(WORK, f"node{i}.db"),
            "log": os.path.join(WORK, f"node{i}.log")}


def start_one(nd):
    with open(nd["log"], "a") as log:
        return subprocess.Popen([H.BIN, "-c", nd["cfg"], "agent"],
                                stdout=log, stderr=subprocess.STDOUT)


def local_count(nd, table, prefix):
    return H.count_rows_local(nd, table, prefix)


def main():
    subprocess.run(["pkill", "-9", "-f", "target/debug/corrosion"], capture_output=True)
    time.sleep(2)
    if os.path.exists(WORK):
        shutil.rmtree(WORK)
    os.makedirs(WORK)
    schema_dir = write_schema()

    # node0=写入方+wildcard(源);node1=NodeR(先只关心 flight);node2=NodeS 对照(关心 flight+target)
    specs = {0: ["*"], 1: ["flight"], 2: ["flight", "target"]}
    nodes = {i: write_config(i, specs[i], schema_dir) for i in specs}
    procs = {i: start_one(nodes[i]) for i in nodes}

    def stop_all():
        for p in procs.values():
            p.send_signal(signal.SIGTERM)
        for p in procs.values():
            try:
                p.wait(timeout=5)
            except Exception:
                p.kill()

    try:
        # 等全部 ACTIVE
        t0 = time.time()
        while time.time() - t0 < 30:
            act = sum("considered ACTIVE" in open(nd["log"]).read()
                      for nd in nodes.values() if os.path.exists(nd["log"]))
            if act == len(nodes):
                break
            time.sleep(0.5)
        time.sleep(10)  # interest 传播 + selector 缓存

        prefix = f"h{int(time.time())}_"
        print(f"\n阶段1:NodeA(node0)写 {ROWS} 行 target(此时 NodeR/node1 不关心 target)")
        for j in range(ROWS):
            H.sh([H.BIN, "-c", nodes[0]["cfg"], "exec", "--param", f"{prefix}{j}",
                  "--param", f"tgt-{j}", "INSERT INTO target (id,data) VALUES (?,?)"])
        time.sleep(12)  # 广播 + 对账充分 settle(NodeR 会把 target 版本当空版本 Cleared)

        r1 = local_count(nodes[1], "target", prefix)
        s1 = local_count(nodes[2], "target", prefix)
        print(f"  直读 sqlite:NodeR(不关心)target={r1}  NodeS(对照,关心)target={s1}")
        print(f"  预期:NodeR=0(被 Empty→Cleared),NodeS={ROWS}(对照拿到)")

        print("\n阶段2:NodeR 重新关心 target(改 node_interest + 重启带新 gossip.interest)")
        # ① 动态改 node_interest(推送/查询侧看的)
        H.sh([H.BIN, "-c", nodes[1]["cfg"], "exec",
              "INSERT OR IGNORE INTO node_interest (actor_id, table_name) "
              "VALUES (crsql_site_id(), 'target')"])
        # ② 重启 NodeR,gossip.interest 改为 [flight,target](对账侧看的,静态配置)
        procs[1].send_signal(signal.SIGTERM)
        try:
            procs[1].wait(timeout=5)
        except Exception:
            procs[1].kill()
        write_config(1, ["flight", "target"], schema_dir)  # 同 db 路径,bookie/Cleared 持久化
        procs[1] = start_one(nodes[1])
        t0 = time.time()
        while time.time() - t0 < 30:
            if os.path.exists(nodes[1]["log"]) and \
               "considered ACTIVE" in open(nodes[1]["log"]).read():
                break
            time.sleep(0.5)
        print("  NodeR 已带 interest=[flight,target] 重启,等对账充分回填 ...")
        time.sleep(20)  # 给对账每一次机会去 backfill

        r2 = local_count(nodes[1], "target", prefix)
        s2 = local_count(nodes[2], "target", prefix)
        # 经查询路由能否查到(它现在是声明的 holder)
        rq = H.count_rows(nodes[1], "target", prefix)
        print(f"\n判定:")
        print(f"  NodeR 重新关心后 直读本地 target = {r2} / {ROWS}")
        print(f"  NodeS 对照            直读本地 target = {s2} / {ROWS}(证明数据在集群有源)")
        print(f"  NodeR 经 API 查询(它现在是 holder)target = {rq}")
        if r2 < ROWS and s2 == ROWS:
            print(f"\n  ❌✅ 复现永久数据洞:NodeR 重新关心 target 后,对账未回填历史"
                  f"({r2}<{ROWS}),而集群里数据是有的(NodeS={s2})。")
            print(f"     → Changeset::Empty→Cleared 关 gap,重新关心不重开 → 静默丢数据。bug 坐实。")
            if rq == ROWS:
                print(f"     ⚠ 更危险:API 查询返回 {rq}(靠查询路由从别的 holder 拿),"
                      f"掩盖了本地缺失 {ROWS - r2} 行 → 若路由到 NodeR 自己则返回不完整。")
        elif r2 == ROWS:
            print(f"\n  ✅ 假说被推翻:NodeR 补齐了 target({r2}={ROWS})→ 存在救援/回填路径,不是永久洞。")
        else:
            print(f"\n  ⚠ 结果不明确(NodeR={r2}, NodeS={s2}),需查 settle/传播时序。")
    finally:
        stop_all()


if __name__ == "__main__":
    main()
