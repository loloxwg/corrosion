#!/usr/bin/env python3
"""运行期 DDL 多节点集群 e2e(对应 runtime-ddl-plan.md Task 8)。

验证「控制面 POST /v1/schema 建表 → corro_ddl_log CRR 行复制 → 各接收节点按 seq
顺序 apply 建表钩子 → 建表前抢发的数据版本被拒后经 anti-entropy 补收」这条闭环,
在真实多节点集群 + 链路故障 + 后入网 三种条件下都收敛。

复制模式(本测试全程):broadcast_strategy = "random" + 无 interest 声明 + schema 里
无 node_interest 表 = **全量复制**(既不按 interest 裁推送轴,也不裁对账轴,见
selector.rs Random 分支与 util.rs::sync_interest_is_filtered)。这样 DDL 日志行与
业务数据都流向全网,把「谁收到」这个变量消掉,只考察运行期 DDL 本身的传播/应用。
corro_ddl_log 的广播是 interest-exempt 的,但仍受 CORRO_LINK_FAULTS 的链路故障影响,
故场景 4 的收敛必须靠中继 rebroadcast 或 anti-entropy sync。

场景(plan 六点):
  1. 起 6 节点:node0 配 api.allow_runtime_schema=true(控制面),其余 false。
  2. node0 POST /v1/schema 建 ont_inst__demo,立刻写 20 行数据。
  3. 轮询全部节点:表存在(直读 sqlite_master,绝不走会路由的 /v1/queries)+ 数据收齐
     + ddl_log_applied_seq 进度追平。
  4. 链路故障:CORRO_LINK_FAULTS 切断 node0→node3 直连广播(drop_p=1.0),另起一套
     集群重跑 2-3,node3 靠多跳 rebroadcast / anti-entropy 收敛(超时放宽),且全网
     广播丢弃计数 > 0 证明链路真在切。
  5. 后入网:健康集群里 DDL+数据已存在后,再起第 7 个节点,断言自动补齐建表历史+数据。
  6. 越权:对 node1(flag=false)POST /v1/schema → 403,且该节点未旁路建出表。

用法: python3 research/harness/ddl_runtime_test.py

踩坑备忘:改配置文件先读进变量再写,严禁 open(f,"w").write(open(f).read()) 单行套娃
(先截断再读=清空)。本测试直接用字符串模板生成配置,不做就地改写,从根上避开。
"""
import json
import os
import shutil
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import run as H  # 复用 BIN / sh / wait_active / scrape

ROOT = "/tmp/corro-ddl-harness"
TABLE = "ont_inst__demo"
DDL_PROGRESS_KEY = "ddl_log_applied_seq_v1"
DROPPED = "corro.research.link.broadcast.dropped"
ROWS = 20

# 两套集群用不同端口段,避免 A 拆除后 B 起来时端口/成员残留互扰。
A_BG, A_BA, A_BP = 7600, 8600, 9600  # 健康集群(场景 1/2/3/5/6),node6=后入网
B_BG, B_BA, B_BP = 7620, 8620, 9620  # 故障集群(场景 4)


# ---------- 配置生成(字符串模板,不就地改写) ----------

def write_cfg(work, i, bg, ba, bp, schema_dir, allow_runtime, boot_port):
    """生成单节点配置。allow_runtime_schema 放 [api] 段(见 config.rs::ApiConfig)。
    中立复制:broadcast_strategy=random + 不声明 interest = 全量复制。"""
    boot = "" if boot_port is None else f'"[::1]:{boot_port}"'
    cfg = os.path.join(work, f"node{i}.toml")
    with open(cfg, "w") as f:
        f.write(f"""[db]
path = "{work}/node{i}.db"
schema_paths = ["{schema_dir}"]
[gossip]
addr = "[::]:{bg + i}"
external_addr = "[::1]:{bg + i}"
bootstrap = [{boot}]
plaintext = true
broadcast_strategy = "random"
[api]
addr = "127.0.0.1:{ba + i}"
allow_runtime_schema = {"true" if allow_runtime else "false"}
[admin]
path = "{work}/node{i}-admin.sock"
[telemetry]
prometheus.addr = "127.0.0.1:{bp + i}"
""")
    return {"i": i, "cfg": cfg, "api": ba + i, "prom": bp + i, "gossip": bg + i,
            "db": os.path.join(work, f"node{i}.db"),
            "log": os.path.join(work, f"node{i}.log"),
            "allow": allow_runtime}


def make_cluster_dir(name):
    work = os.path.join(ROOT, name)
    if os.path.exists(work):
        shutil.rmtree(work)
    schema_dir = os.path.join(work, "schema")
    # 空 schema 目录:无 .sql → 启动时无初始表(ont_inst__demo 完全靠运行期 DDL 建),
    # execute_schema_from_paths 对空 statements 直接 Ok(见 util.rs)。
    os.makedirs(schema_dir, exist_ok=True)
    return work, schema_dir


def start_procs(nodes, faults_of=None):
    """faults_of: i -> CORRO_LINK_FAULTS dict 或 None。"""
    procs = []
    for nd in nodes:
        env = dict(os.environ)
        env.pop("CORRO_LINK_FAULTS", None)
        if faults_of is not None:
            fault = faults_of(nd["i"])
            if fault:
                env["CORRO_LINK_FAULTS"] = json.dumps(fault)
        with open(nd["log"], "w") as log:
            procs.append(subprocess.Popen([H.BIN, "-c", nd["cfg"], "agent"],
                                          stdout=log, stderr=subprocess.STDOUT,
                                          env=env))
    return procs


def stop_procs(procs):
    for p in procs:
        p.send_signal(signal.SIGTERM)
    for p in procs:
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            p.kill()


# ---------- 本地状态直读(绕查询路由) ----------

def _sqlite1(db, q):
    r = H.sh(["sqlite3", db, q])
    return r.stdout.strip()


def table_exists(nd, table):
    """直读本地 sqlite_master 判表是否已建(绝不走 /v1/queries——那会路由到持有者、
    掩盖本地缺失)。"""
    return _sqlite1(nd["db"],
                    "SELECT count(*) FROM sqlite_master "
                    f"WHERE type='table' AND name='{table}'") == "1"


def count_local(nd, table, prefix):
    """本地表内 prefix 行数;表还没建时返回 -1。"""
    if not table_exists(nd, table):
        return -1
    out = _sqlite1(nd["db"], f"SELECT count(*) FROM {table} WHERE pk LIKE '{prefix}%'")
    try:
        return int(out)
    except ValueError:
        return -1


def ddl_progress(nd):
    """本节点已应用到的 ddl_log seq(__corro_state 进度行,无则 0)。"""
    out = _sqlite1(nd["db"],
                   "SELECT CAST(value AS INTEGER) FROM __corro_state "
                   f"WHERE key='{DDL_PROGRESS_KEY}'")
    try:
        return int(out)
    except ValueError:
        return 0


# ---------- 控制面 API ----------

def post_schema(api_port, statements, timeout=10):
    """POST /v1/schema。返回 (status_code, body)。403/4xx 经 HTTPError 拿到状态码。"""
    url = f"http://127.0.0.1:{api_port}/v1/schema"
    req = urllib.request.Request(
        url, data=json.dumps(statements).encode(), method="POST",
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            raw = json.loads(raw)
        except ValueError:
            pass
        return e.code, raw


def insert_rows(nd, prefix, rows):
    """经 CLI exec 在持有该表的节点上写 rows 行(node0 POST 后本地已有表)。"""
    for j in range(rows):
        H.sh([H.BIN, "-c", nd["cfg"], "exec",
              "--param", f"{prefix}{j}", "--param", f"demo-{j}",
              f"INSERT INTO {TABLE} (pk, name) VALUES (?, ?)"])


def poll_converge(nodes, prefix, rows, seq, timeout):
    """轮询直到所有 nodes 收敛或超时。返回 (是否收敛, 每节点快照)。"""
    deadline = time.time() + timeout
    while True:
        snap = {nd["i"]: (table_exists(nd, TABLE),
                          count_local(nd, TABLE, prefix),
                          ddl_progress(nd)) for nd in nodes}
        if all(has and cnt == rows and prog >= seq
               for has, cnt, prog in snap.values()):
            return True, snap
        if time.time() >= deadline:
            return False, snap
        time.sleep(0.3)


def print_snap(nodes, snap, prefix):
    print(f"    {'节点':<6}{'flag':<7}{'表存在':<8}{'行数':<7}{'seq'}")
    for nd in nodes:
        has, cnt, prog = snap[nd["i"]]
        flag = "ctrl" if nd["allow"] else "-"
        print(f"    {nd['i']:<6}{flag:<7}{str(has):<8}{cnt:<7}{prog}")


# ---------- 场景 ----------

def scenario_healthy_and_late_join():
    """场景 1/2/3(健康集群建表+写数+全收敛)+ 场景 6(越权 403)+ 场景 5(后入网)。
    都跑在同一套健康的 6 节点集群上(场景 5 再起第 7 个)。返回 (ok, 明细 dict)。"""
    work, schema_dir = make_cluster_dir("healthy")
    nodes = [write_cfg(work, i, A_BG, A_BA, A_BP, schema_dir,
                       allow_runtime=(i == 0),
                       boot_port=None if i == 0 else A_BG)
             for i in range(6)]
    procs = start_procs(nodes)
    results = {}
    try:
        print("[健康集群] 6 节点(node0=控制面 flag=true,其余 false),等 ACTIVE ...")
        if not H.wait_active(nodes, timeout=60):
            print("  ⚠ 集群未全部 ACTIVE")
            return False, results
        time.sleep(3)  # 成员网格稳定

        # --- 场景 6:越权(先做,趁表还没被复制过来,能干净断言未旁路建表) ---
        print("\n【场景6】对 node1(flag=false)POST /v1/schema → 期望 403")
        code, body = post_schema(nodes[1]["api"],
                                 [f"CREATE TABLE {TABLE} (pk TEXT NOT NULL PRIMARY KEY)"])
        s6_403 = (code == 403)
        # 越权 POST 不得在**任何**节点建出表:该请求在 node1 的 flag 门禁处即被拒(未触碰
        # schema),也不得经复制把表带到全网。此刻 ont_inst__demo 尚未被任何合法控制面创建,
        # 故全 6 节点都应无此表——安全相关场景做全集群断言,而非只看 node1。
        s6_notable = all(not table_exists(nd, TABLE) for nd in nodes)
        results["s6"] = s6_403 and s6_notable
        print(f"    状态码={code}(期望403:{'✓' if s6_403 else '✗'}), "
              f"全 6 节点未旁路建表:{'✓' if s6_notable else '✗'} "
              f"→ {'✅ PASS' if results['s6'] else '❌ FAIL'}")

        # --- 场景 1/2/3:控制面建表 + 立刻写 20 行 + 全网收敛 ---
        prefix = f"h{int(time.time())}_"
        print(f"\n【场景1/2/3】node0 POST /v1/schema 建 {TABLE},立刻写 {ROWS} 行,前缀={prefix}")
        code, body = post_schema(nodes[0]["api"],
                                 [f"CREATE TABLE {TABLE} "
                                  "(pk TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL DEFAULT '')"])
        s1_post = (code == 200 and isinstance(body, dict) and body.get("applied"))
        print(f"    POST 状态码={code} applied={body.get('applied') if isinstance(body, dict) else body}"
              f" → {'✓' if s1_post else '✗'}")
        if not s1_post:
            results["s123"] = False
            return False, results
        # 立刻写(建表前抢发,peer 可能还没建表 → 数据版本被拒 → 后续 anti-entropy 补收)。
        insert_rows(nodes[0], prefix, ROWS)

        ok, snap = poll_converge(nodes, prefix, ROWS, seq=1, timeout=90)
        results["s123"] = ok
        print(f"    全 6 节点收敛(表+{ROWS}行+seq≥1):{'✅ PASS' if ok else '❌ FAIL(超时)'}")
        print_snap(nodes, snap, prefix)
        if not ok:
            return False, results

        # --- 场景 5:后入网第 7 个节点,自动补齐建表历史 + 数据 ---
        print("\n【场景5】DDL+数据已存在后,起第 7 个节点(node6),断言自动补齐")
        late = write_cfg(work, 6, A_BG, A_BA, A_BP, schema_dir,
                         allow_runtime=False, boot_port=A_BG)
        late_procs = start_procs([late])
        procs.extend(late_procs)
        if not H.wait_active(nodes + [late], timeout=60):
            print("  ⚠ 后入网节点未 ACTIVE")
            results["s5"] = False
            return False, results
        ok5, snap5 = poll_converge([late], prefix, ROWS, seq=1, timeout=90)
        results["s5"] = ok5
        print(f"    node6 补齐(表+{ROWS}行+seq≥1):{'✅ PASS' if ok5 else '❌ FAIL(超时)'}")
        print_snap([late], snap5, prefix)
        return all(results.values()), results
    finally:
        stop_procs(procs)


def scenario_link_fault():
    """场景 4:切断 node0→node3 直连广播(drop_p=1.0),重跑建表+写数,node3 靠多跳/
    对账收敛;全网广播丢弃计数 > 0 证明链路真在切。另起一套集群(端口段 B)。"""
    work, schema_dir = make_cluster_dir("fault")
    nodes = [write_cfg(work, i, B_BG, B_BA, B_BP, schema_dir,
                       allow_runtime=(i == 0),
                       boot_port=None if i == 0 else B_BG)
             for i in range(6)]
    node3_addr = f"[::1]:{B_BG + 3}"
    faults = {node3_addr: {"drop_p": 1.0}}  # 装在 node0 上 = 切 node0→node3 的广播发送
    procs = start_procs(nodes, faults_of=lambda i: faults if i == 0 else None)
    try:
        print("\n[故障集群] 6 节点,CORRO_LINK_FAULTS 切断 node0→node3 直连广播,等 ACTIVE ...")
        if not H.wait_active(nodes, timeout=60):
            print("  ⚠ 集群未全部 ACTIVE")
            return False
        time.sleep(3)

        prefix = f"f{int(time.time())}_"
        print(f"【场景4】node0 POST 建 {TABLE} + 写 {ROWS} 行(前缀={prefix}),node3 直连被切")
        code, body = post_schema(nodes[0]["api"],
                                 [f"CREATE TABLE {TABLE} "
                                  "(pk TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL DEFAULT '')"])
        if not (code == 200 and isinstance(body, dict) and body.get("applied")):
            print(f"    ✗ POST 失败:状态码={code} body={body}")
            return False
        insert_rows(nodes[0], prefix, ROWS)

        # 故障下收敛更慢(靠中继 rebroadcast + anti-entropy),超时放宽。
        ok, snap = poll_converge(nodes, prefix, ROWS, seq=1, timeout=150)
        dropped = int(sum(H.scrape(nd, [DROPPED])[DROPPED] for nd in nodes))
        node3 = snap[3]
        print(f"    全 6 节点收敛:{'✅' if ok else '❌(超时)'}  "
              f"node3=(表{node3[0]},行{node3[1]},seq{node3[2]})  "
              f"全网广播丢弃计数={dropped}")
        print_snap(nodes, snap, prefix)
        link_cut = dropped > 0
        print(f"    链路确被切(丢弃计数>0):{'✓' if link_cut else '✗'}")
        result = ok and link_cut
        print(f"  【场景4】→ {'✅ PASS' if result else '❌ FAIL'}")
        return result
    finally:
        stop_procs(procs)


def main():
    if not os.path.exists(H.BIN):
        sys.exit(f"找不到 binary {H.BIN},先 cargo build -p corrosion")
    if os.path.exists(ROOT):
        shutil.rmtree(ROOT)
    os.makedirs(ROOT)

    t0 = time.time()
    print("== 运行期 DDL 集群 e2e(复制模式:random 全量复制,无 interest 裁剪)==\n")

    ok_a, res_a = scenario_healthy_and_late_join()
    ok_b = scenario_link_fault()

    wall = time.time() - t0
    print("\n== 汇总 ==")
    verdicts = [
        ("场景1/2/3 控制面建表+写数+全网收敛", res_a.get("s123", False)),
        ("场景4 链路切断 node0→node3 仍收敛", ok_b),
        ("场景5 后入网自动补齐历史+数据", res_a.get("s5", False)),
        ("场景6 越权 POST → 403 且未旁路建表", res_a.get("s6", False)),
    ]
    for label, v in verdicts:
        print(f"  {'✅ PASS' if v else '❌ FAIL'}  {label}")
    all_ok = all(v for _, v in verdicts)
    print(f"\n总耗时 {wall:.1f}s — {'✅ 全部 PASS' if all_ok else '❌ 存在 FAIL'}")
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
