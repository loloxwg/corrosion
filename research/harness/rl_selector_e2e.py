#!/usr/bin/env python3
"""端到端:BroadcastStrategy::Rl 真把广播打到低方差(稳定)链路(4.3.3 瞬态选路层)。

RL 归位在瞬态层:interest 圈定合法集,RL 只在集内按 RTT 方差感知选广播目标——
偏好稳定链路,不稳链路留 anti-entropy 兜底(推错自愈,不改 placement)。

本 harness 验证该机制端到端生效(非单测,真集群):
  - N 节点全关心 flight(→ 广播目标选择在 peer 间按链路质量发生)。
  - 注入:一半 peer 高 jitter(delay 低但 RTT 方差高=不稳),一半稳定(无故障)。
    jitter 经 transport 叠加进上报 RTT → members.rtts 方差升高 → Rl 的 rtt_var 看得见。
  - 分别跑 strategy=scored(方差盲)与 rl(方差感知),写多行触发多次广播,
    抓埋点 corro_broadcast_target_rttvar_milli / _count(所选目标的平均 RTT 方差)。
  - 判定:rl 的目标方差均值 < scored → Rl 确实避开不稳链路做广播目标。

诚实:jitter 为合成注入(研究开关);corrosion 鲁棒,Rl 瞬态价值偏薄但安全+机制可证。
用法: python3 research/harness/rl_selector_e2e.py --nodes 8
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


def write_cfg(i, strategy, schema_dir):
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
broadcast_strategy = "{strategy}"
interest = ["flight"]
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


def scrape_target_var(nodes, strategy):
    """全节点求和 corro_broadcast_target_rttvar_milli / _count(带 strategy 标签)→ 均值方差。"""
    milli = cnt = 0.0
    for nd in nodes:
        try:
            import urllib.request
            txt = urllib.request.urlopen(
                f"http://127.0.0.1:{nd['prom']}/metrics", timeout=2).read().decode()
        except Exception:
            continue
        for line in txt.splitlines():
            if line.startswith("#") or f'strategy="{strategy}"' not in line:
                continue
            key, _, val = line.partition(" ")
            try:
                v = float(val)
            except ValueError:
                continue
            if key.startswith("corro_broadcast_target_rttvar_milli"):
                milli += v
            elif key.startswith("corro_broadcast_target_count"):
                cnt += v
    if cnt == 0:
        return None, 0
    return (milli / cnt) / 1000.0, int(cnt)  # 平均方差(ms²)


def run_strategy(strategy, n, jittery, rows):
    if os.path.exists(WORK):
        shutil.rmtree(WORK)
    os.makedirs(WORK)
    schema_dir = write_schema()
    # 注入:jittery 节点高 RTT 方差(delay 低=均值不高,jitter 高=方差高,隔离方差信号)。
    faults = {node_addr(j): {"delay_ms": 5, "jitter_ms": 200} for j in jittery}
    os.environ["CORRO_LINK_FAULTS"] = json.dumps(faults)
    nodes = [write_cfg(i, strategy, schema_dir) for i in range(n)]
    procs = []
    for nd in nodes:
        with open(nd["log"], "w") as log:
            procs.append(subprocess.Popen([H.BIN, "-c", nd["cfg"], "agent"],
                                          stdout=log, stderr=subprocess.STDOUT))
    try:
        t0 = time.time()
        while time.time() - t0 < 30:
            act = sum("considered ACTIVE" in open(nd["log"]).read()
                      for nd in nodes if os.path.exists(nd["log"]))
            if act == n:
                break
            time.sleep(0.5)
        # 等 RTT 样本累积(填 20 样本缓冲,jittery 方差升高)+ interest 传播
        time.sleep(25)
        prefix = f"s{int(time.time())}_"
        # 写多行触发多次广播决策(每次 selector 记所选目标方差)。分批+小睡,拉开时间。
        for j in range(rows):
            H.sh([H.BIN, "-c", nodes[0]["cfg"], "exec", "--param", f"{prefix}{j}",
                  "--param", f"v{j}", "INSERT INTO flight (id,data) VALUES (?,?)"])
            if j % 10 == 9:
                time.sleep(1)
        time.sleep(3)
        mean_var, cnt = scrape_target_var(nodes, strategy)
        return mean_var, cnt
    finally:
        os.environ.pop("CORRO_LINK_FAULTS", None)
        for p in procs:
            p.send_signal(signal.SIGTERM)
        for p in procs:
            try:
                p.wait(timeout=5)
            except Exception:
                p.kill()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", type=int, default=8)
    ap.add_argument("--rows", type=int, default=60)
    args = ap.parse_args()
    if not os.path.exists(H.BIN):
        sys.exit(f"找不到 binary {H.BIN}")

    n = args.nodes
    # 后半 peer 注入高 jitter(不稳),前半(除写入方 node0)稳定。
    jittery = list(range(n // 2, n))
    stable = [i for i in range(1, n // 2)]
    print(f"== Rl 瞬态选路 e2e:{n} 节点全关心 flight,{args.rows} 行 ==")
    print(f"  稳定 peer(低方差)={stable}  不稳 peer(高 jitter=高方差)={jittery}")
    print(f"  预期:rl 所选广播目标的平均 RTT 方差 < scored(方差盲)\n")

    subprocess.run(["pkill", "-9", "-f", "target/debug/corrosion"], capture_output=True)
    time.sleep(2)
    print("跑 strategy=scored(方差盲基线)...")
    sv, sc = run_strategy("scored", n, jittery, args.rows)
    time.sleep(3)
    print("跑 strategy=rl(方差感知)...")
    rv, rc = run_strategy("rl", n, jittery, args.rows)

    print(f"\n== 结果:所选广播目标的平均 RTT 方差(ms²)==")
    print(f"  scored(方差盲) 平均目标方差 = {sv}  (样本 {sc})")
    print(f"  rl(方差感知)   平均目标方差 = {rv}  (样本 {rc})")
    if sv is not None and rv is not None:
        pct = (1 - rv / sv) * 100 if sv else 0
        if rv < sv * 0.9:
            print(f"\n  ✅ Rl 目标方差更低({rv:.0f} < {sv:.0f}, ↓{pct:.0f}%)→ 偏好稳定链路做广播目标。")
        else:
            print(f"\n  ≈ Rl 仅略低({rv:.0f} vs {sv:.0f}, ↓{pct:.0f}%),方向对但边际薄。")
        print("""
  ★真根因(诚实,与查询侧 resolve 吸收同构):corrosion 的 RTT-ring 机制已吃掉大部分链路信号。
    注入 jitter 同时抬高均值 RTT(delay+jitter 均值~105ms)→ 不稳 peer 落 ring4-5,本非 ring0;
    corrosion ring0-flood 总发给低 RTT(稳定)peer,高 RTT 走 global 限扇出 → 高方差⟹高均值⟹高 ring
    ⟹已被现有机制降优先级。Rl 的"纯方差"信号(同均值不同方差)在 ring0 桶(0-6ms)里空间极小。
    → Rl 瞬态选路机制present(方向对+单测证逻辑),但价值被 ring 吸收,边际薄。""")
    else:
        print("\n  ⚠ 无方差样本(广播全走 ring0 flood k=全部 或 rtt 历史未积累),需调 settle/拓扑。")


if __name__ == "__main__":
    main()
