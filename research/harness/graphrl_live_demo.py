#!/usr/bin/env python3
"""演示:内嵌 GNN(4.3.3 深度图强化学习)在真 corrosion 里【活着做决策】。

每个无人机节点(corrosion agent)启动时加载训练好的 GNN 权重,周期跑推理算适配度评分
score(表, 平台),驱动 selector 的推送目标选择——这是评审要看的"活的智能体":
模型真的在节点里跑、真的在决策推送目标(不是离线报告,不是手设规则)。

安全边界:活模型只在 interest 圈定的合法集内选推送目标(瞬态层),不改 placement/不碰
durability(推错自愈)。评分尺度为名义映射,真校准待半实物。

用法:先 python3 research/rl/export_weights.py 导出权重,再 python3 此脚本。
"""
import os
import shutil
import signal
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, HERE)
import run as H

WEIGHTS = os.path.join(REPO, "research", "rl", "gnn_weights.json")
WORK = H.WORK
BG, BA, BP = 7600, 8600, 9600
ROLE_TABLES = {"recon": ["flight"], "strike": ["battlefield", "target"], "jam": ["target"]}
ROLES = ["recon", "strike", "jam"]


def write_cfg(i, schema_dir):
    role = ROLES[i % 3]
    interest = ", ".join(f'"{t}"' for t in ROLE_TABLES[role])
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
interest = [{interest}]
[gossip.graphrl]
weights_path = "{WEIGHTS}"
critical_tables = ["target"]
[[gossip.graphrl.tables]]
name = "flight"
write_vol = 20.0
query_vol = 0.3
[[gossip.graphrl.tables]]
name = "battlefield"
write_vol = 6.0
query_vol = 2.5
[[gossip.graphrl.tables]]
name = "target"
write_vol = 3.0
query_vol = 5.0
[[gossip.graphrl.roles]]
name = "recon"
tables = ["flight"]
[[gossip.graphrl.roles]]
name = "strike"
tables = ["battlefield", "target"]
[[gossip.graphrl.roles]]
name = "jam"
tables = ["target"]
[api]
addr = "127.0.0.1:{BA + i}"
[admin]
path = "{WORK}/node{i}-admin.sock"
[telemetry]
prometheus.addr = "127.0.0.1:{BP + i}"
""")
    return {"i": i, "role": role, "cfg": cfg, "api": BA + i,
            "log": os.path.join(WORK, f"node{i}.log")}


def main():
    if not os.path.exists(WEIGHTS):
        sys.exit(f"缺权重 {WEIGHTS},先 python3 research/rl/export_weights.py")
    subprocess.run(["pkill", "-9", "-f", "target/debug/corrosion"], capture_output=True)
    time.sleep(2)
    if os.path.exists(WORK):
        shutil.rmtree(WORK)
    os.makedirs(WORK)
    schema_dir = os.path.join(WORK, "schema")
    os.makedirs(schema_dir)
    with open(os.path.join(schema_dir, "m.sql"), "w") as f:
        for t in ("flight", "battlefield", "target"):
            f.write(f"CREATE TABLE {t} (id BLOB NOT NULL PRIMARY KEY, data TEXT NOT NULL DEFAULT '');\n")
        f.write("CREATE TABLE node_interest (actor_id BLOB NOT NULL, table_name TEXT NOT NULL, "
                "active INTEGER NOT NULL DEFAULT 1, PRIMARY KEY (actor_id, table_name));\n")

    n = 6
    nodes = [write_cfg(i, schema_dir) for i in range(n)]
    print(f"== 内嵌 GNN 活模型演示:{n} 节点(侦查/打击/干扰),broadcast_strategy=rl ==")
    print(f"   权重:{WEIGHTS}")
    print(f"   角色:{[(nd['i'], nd['role']) for nd in nodes]}\n")
    procs = []
    for nd in nodes:
        with open(nd["log"], "w") as log:
            procs.append(subprocess.Popen([H.BIN, "-c", nd["cfg"], "agent"],
                                          stdout=log, stderr=subprocess.STDOUT))
    try:
        # 等 ACTIVE + 至少一次 InterestRefresh tick(3s)让 GNN 推理跑起来
        t0 = time.time()
        while time.time() - t0 < 30:
            act = sum("considered ACTIVE" in open(nd["log"]).read()
                      for nd in nodes if os.path.exists(nd["log"]))
            if act == n:
                break
            time.sleep(0.5)
        print("节点 ACTIVE,等 GNN 推理(interest 传播 + 3s tick)...")
        time.sleep(12)
        # 写点数据触发广播(selector 用 GNN 分选目标)
        prefix = f"g{int(time.time())}_"
        for t in ("flight", "battlefield", "target"):
            for j in range(5):
                H.sh([H.BIN, "-c", nodes[0]["cfg"], "exec", "--param", f"{prefix}{j}",
                      "--param", f"{t}-{j}", f"INSERT INTO {t} (id,data) VALUES (?,?)"])
        time.sleep(5)

        # 从日志提取活模型证据
        print("\n== 活模型证据(节点日志)==")
        loaded = infer = 0
        decisions = []
        for nd in nodes:
            txt = open(nd["log"]).read()
            if "已加载内嵌 GNN 权重" in txt:
                loaded += 1
            if "初次推理,评分表" in txt:
                infer += 1
            for line in txt.splitlines():
                if "graphrl 决策:" in line:
                    decisions.append((nd["i"], nd["role"], line.split("graphrl 决策:")[1].strip()))
        print(f"  加载 GNN 权重的节点:{loaded}/{n}")
        print(f"  跑了推理的节点:    {infer}/{n}")
        print(f"\n  模型的推送目标决策(每节点视角,GNN 现算):")
        for i, role, d in decisions[:12]:
            print(f"    node{i}({role}): {d}")
        if loaded == n and infer == n and decisions:
            print(f"\n  ✅ 内嵌 GNN 在 {n} 个节点里【活着】:加载权重→跑推理→算适配度→决策推送目标。")
            print(f"     (安全:只在 interest 合法集内选目标,不改 placement;评分尺度名义,真校准待半实物。)")
        else:
            print(f"\n  ⚠ 未完全就绪(loaded={loaded} infer={infer} decisions={len(decisions)}),看日志。")
    finally:
        for p in procs:
            p.send_signal(signal.SIGTERM)
        for p in procs:
            try:
                p.wait(timeout=5)
            except Exception:
                p.kill()


if __name__ == "__main__":
    main()
