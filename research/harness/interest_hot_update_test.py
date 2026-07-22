#!/usr/bin/env python3
"""运行期 interest 热更新集群 e2e(对应 runtime-interest-plan.md Task 5)。

状态:5 场景全 PASS(连跑稳定)。落地过程中此 harness 暴露并驱动修复了两个产品缺陷:
 · bug #1(commit a1644813 `fix(sync): stamp last_sync_ts on zero-data sync rounds`):
   激活静默门 `initial_sync_is_complete` 的 `members_synced` 在 ≥~4-6 节点集群永不收敛
   (吃饱节点不把「已同步」标记打到它无需拉取的 peer)→ 启动/后入网激活卡死,并经单飞锁
   外溢阻塞运行期激活。修复:握手完成即视为完成一轮 sync,对所有握手成功的 peer 置位。
 · bug #2(commit `fix(sync): close reopened gaps for still-filtered tables`):运行期扩
   interest 时 reopen 重开**全部**过滤版本区间(过滤账本只存 actor+version 不存表),对
   「本节点仍不关心的其它表」也造出 needed gap;发送端重发 Changeset::Empty 关 gap,却被
   handle_changes 的 in-memory `seen` 短路缓存(首次过滤时已把这些版本记入)在到达
   process_multiple_changes 前丢弃 → gap 永不关闭 → 被采纳表激活卡死。修复:`seen` 缓存
   短路须让位于 bookie 权威 needed 集——版本仍被 needed(重开 gap)时不短路。
 两缺陷 Rust 双节点 e2e 均未覆盖(2 节点 members_synced 平凡可达;单张外部过滤表无残留
 gap);多节点 + 多表过滤(侦察/打击/干扰任务拓扑常态)才暴露,故此集群 harness 是必要覆盖。

验证 `POST /v1/interest {tables, epoch}` 在真实多节点 scored_reduce 集群上完整复用
启动期安置协议:epoch fencing → 热换 config → 重开被过滤历史 → reconcile(active 门控 +
摘除 min-replica 门禁)→ 异步回填激活。对照已落地的 Rust e2e
`runtime_interest_expansion_backfills`(commit 695c6908)的因果链,把它抬到 6 节点集群。

复制模式(全程):broadcast_strategy = "scored_reduce" + 每节点静态窄 interest(按角色分表)。
scored_reduce 发送端把「不关心该表的 peer」从快推里剔除;anti-entropy 同步端按 REQUESTER
声明的 interest 过滤 → 被过滤版本以 Changeset::Empty 到达 → 记入 __corro_filtered_version_ranges。
这层过滤账本正是运行期扩 interest 时要重开回填的对象。

拓扑(6 节点,i=0..5;表 = t_recon/t_strike/t_jam/t_solo):
  node0 recon  interest=[t_recon]          flag=OFF  ← 场景5(403)
  node1 strike interest=[t_strike]         flag=ON
  node2 jam    interest=[t_jam]            flag=ON  ← 场景2(扩 interest 收 t_recon),**最后启动**
  node3 recon  interest=[t_recon]          flag=ON   (t_recon 的作者/holder)
  node4 strike interest=[t_strike]         flag=ON  ← 场景4(epoch 回退)
  node5 jam    interest=[t_jam, t_solo]    flag=ON  ← 场景3(摘除唯一副本 t_solo)
  t_solo 只有 node5 声明 → 它是唯一 active holder(min_replicas 默认 1,摘除必被拒)。

flag 口径:除 node0 外全开 allow_runtime_interest。node0 关着,专供场景5 断 403;其余节点
都要接受合法 POST。

场景(plan 五点):
  1. 6 节点 scored_reduce + 静态窄 interest,写数落定 → 直读 sqlite 断言部分复制生效。
     **载荷断言**(可靠):各 holder 收到自己关心表的全部行(跨节点复制正向);后入网的 node2
     本地无 t_recon 且对 t_recon 作者(node3)的过滤账本 > 0(负向 + 场景2 前置)。
     **软报告**(不 gate):其它 live 节点对非关心表的泄漏行数——broadcast 收端不过滤,
     scored_reduce 只裁发送端,coverage/rebroadcast 可能泄漏(既有研究:一次泄漏会外扩),
     故 live 节点的「零泄漏」是尽力而为,不作判据。node2 的负向证据靠「后入网 + 只经 sync
     交付」这条可靠机制(与 Rust 测试写在 B 入网前同源),而非依赖发送端裁剪不泄漏。
  2. 运行期对 node2 扩 interest 收 t_recon(epoch=1)→ 断言:reopened_ranges>0、
     pending_activation、t_recon 历史回填到位、node_interest(node2,t_recon) 最终 active=1、
     对 t_recon 作者(node3)的过滤账本清空**且保持**(node2 现在关心 node3 的全部作者版本,
     不会被再过滤),后续新写实时到达。
  3. 对 node5 摘除 t_solo(它是唯一 active holder)→ 409(InterestRemovalUnsafe),行未删。
  4. 对 node4 先成功 apply epoch=3,再 POST 更低 epoch=2 → 409(StaleInterestEpoch)。
  5. 对 node0(flag=false)POST → 403。

用法: python3 research/harness/interest_hot_update_test.py

踩坑规约(同 ddl_runtime_test.py):
  - 配置用字符串模板生成,不就地改写(严禁 open(f,"w").write(open(f).read()) 先截断再读的套娃)。
  - 所有本地状态断言直读 sqlite(绝不走 /v1/queries——scored_reduce 下它会路由到持有者,
    掩盖本地裁剪)。
  - 回填/激活需 backfill 完成 + 静默(连续两拍 quiescent);debug 构建 sync backoff 以秒计,
    轮询超时给足(120s)。
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
import run as H  # 复用 BIN / sh / wait_active

ROOT = "/tmp/corro-interest-harness"
ROWS = 8
BG, BA, BP = 7660, 8660, 9660  # gossip / api / prometheus 端口段(避开 run.py / ddl 段)

# 角色 → 表(每角色恰好一张表,便于把「唯一 holder」做干净)
ROLE_TABLE = {"recon": "t_recon", "strike": "t_strike", "jam": "t_jam"}
ALL_TABLES = ["t_recon", "t_strike", "t_jam", "t_solo"]

# 每节点:角色 / interest / flag(node0 关 flag 供场景5)。
NODE_SPEC = {
    0: {"role": "recon",  "interest": ["t_recon"],           "flag": False},
    1: {"role": "strike", "interest": ["t_strike"],          "flag": True},
    2: {"role": "jam",    "interest": ["t_jam"],             "flag": True},
    3: {"role": "recon",  "interest": ["t_recon"],           "flag": True},
    4: {"role": "strike", "interest": ["t_strike"],          "flag": True},
    5: {"role": "jam",    "interest": ["t_jam", "t_solo"],   "flag": True},
}


# ---------- 配置生成(字符串模板,不就地改写) ----------

def write_schema(work):
    schema_dir = os.path.join(work, "schema")
    os.makedirs(schema_dir, exist_ok=True)
    with open(os.path.join(schema_dir, "mission.sql"), "w") as f:
        for t in ALL_TABLES:
            f.write(f"CREATE TABLE {t} (id TEXT NOT NULL PRIMARY KEY, "
                    f"data TEXT NOT NULL DEFAULT '');\n")
        # node_interest:用户 schema 表(CRR),各节点自声明 interest;active 列由 reconcile /
        # 激活任务维护。启动时 corrosion 从 gossip.interest 自写本行(run_root.rs)。
        f.write("CREATE TABLE node_interest (actor_id BLOB NOT NULL, "
                "table_name TEXT NOT NULL, active INTEGER NOT NULL DEFAULT 1, "
                "PRIMARY KEY (actor_id, table_name));\n")
    return schema_dir


def write_cfg(work, i, schema_dir):
    spec = NODE_SPEC[i]
    boot = "" if i == 0 else f'"[::1]:{BG}"'
    interest = ", ".join(f'"{t}"' for t in spec["interest"])
    cfg = os.path.join(work, f"node{i}.toml")
    with open(cfg, "w") as f:
        f.write(f"""[db]
path = "{work}/node{i}.db"
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
allow_runtime_interest = {"true" if spec["flag"] else "false"}
[admin]
path = "{work}/node{i}-admin.sock"
[telemetry]
prometheus.addr = "127.0.0.1:{BP + i}"
""")
    return {"i": i, "cfg": cfg, "api": BA + i, "prom": BP + i, "gossip": BG + i,
            "role": spec["role"], "interest": spec["interest"], "flag": spec["flag"],
            "db": os.path.join(work, f"node{i}.db"),
            "log": os.path.join(work, f"node{i}.log")}


def start_procs(nodes):
    procs = []
    for nd in nodes:
        with open(nd["log"], "w") as log:
            procs.append(subprocess.Popen([H.BIN, "-c", nd["cfg"], "agent"],
                                          stdout=log, stderr=subprocess.STDOUT))
    return procs


def stop_procs(procs):
    for p in procs:
        p.send_signal(signal.SIGTERM)
    for p in procs:
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            p.kill()


# ---------- 本地状态直读(绕查询路由;plain sqlite3,不加载 crsql 扩展) ----------

def _sqlite1(db, q):
    return H.sh(["sqlite3", db, q]).stdout.strip()


def local_site_hex(db):
    """本节点自身 actor_id(= crsql 本地 site_id,ordinal=0 行)的 hex。crsql_site_id 是普通
    btree 表,plain sqlite3 可读(见 bookie.rs::SELECT site_id FROM crsql_site_id WHERE ordinal=0)。"""
    return _sqlite1(db, "SELECT lower(hex(site_id)) FROM crsql_site_id WHERE ordinal=0")


def count_local(db, table, prefix):
    """本地表内 prefix 行数;表不存在或异常返回 -1。"""
    out = _sqlite1(db, f"SELECT count(*) FROM {table} WHERE id LIKE '{prefix}%'")
    try:
        return int(out)
    except ValueError:
        return -1


def filtered_ranges_total(db):
    """本节点 __corro_filtered_version_ranges 总行数。"""
    try:
        return int(_sqlite1(db, "SELECT count(*) FROM __corro_filtered_version_ranges"))
    except ValueError:
        return -1


def filtered_ranges_for(db, author_hex):
    """本节点过滤账本中,作者 = author_hex 的行数。"""
    q = ("SELECT count(*) FROM __corro_filtered_version_ranges "
         f"WHERE actor_id = x'{author_hex}'")
    try:
        return int(_sqlite1(db, q))
    except ValueError:
        return -1


def ni_active(db, table, site_hex=None):
    """node_interest 中(本节点 site_hex 若给)某表的 active 值;缺行返回 -2,异常 -1。
    site_hex=None 时不按 actor 过滤(仅当该表全局只有一个 holder 才唯一,如 t_solo)。"""
    where = f"table_name = '{table}'"
    if site_hex is not None:
        where += f" AND actor_id = x'{site_hex}'"
    out = _sqlite1(db, f"SELECT active FROM node_interest WHERE {where}")
    if out == "":
        return -2
    try:
        return int(out.splitlines()[0])
    except ValueError:
        return -1


# ---------- 控制面 API ----------

def post_interest(api_port, tables, epoch, timeout=15):
    """POST /v1/interest。返回 (status_code, body_dict_or_raw)。4xx 经 HTTPError 拿状态码。"""
    url = f"http://127.0.0.1:{api_port}/v1/interest"
    req = urllib.request.Request(
        url, data=json.dumps({"tables": tables, "epoch": epoch}).encode(),
        method="POST", headers={"Content-Type": "application/json"})
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


def insert_rows(nd, table, prefix, rows):
    """在持有该表的节点上经 CLI exec 写 rows 行(写入者即版本作者)。"""
    for j in range(rows):
        H.sh([H.BIN, "-c", nd["cfg"], "exec",
              "--param", f"{prefix}{j}", "--param", f"{table}-{j}",
              f"INSERT INTO {table} (id, data) VALUES (?, ?)"])


def poll(fn, timeout, interval=0.4):
    """轮询 fn() 为真或超时。返回 (是否成真, 最后一次值)。"""
    deadline = time.time() + timeout
    while True:
        val = fn()
        if val:
            return True, val
        if time.time() >= deadline:
            return False, val
        time.sleep(interval)


# ---------- 主流程 ----------

def run():
    work = ROOT
    if os.path.exists(work):
        shutil.rmtree(work)
    os.makedirs(work)
    schema_dir = write_schema(work)
    nodes = {i: write_cfg(work, i, schema_dir) for i in range(6)}

    # 启动顺序:先起 0/1/3/4/5(不含 node2),写数落定,再最后起 node2。
    # 这样 t_recon 等在 node2 入网前已写完(ring0 广播是 ephemeral,不重放给后入网者),
    # node2 只能经 anti-entropy sync 拿数据 → 关心的 t_jam 全量交付,不关心的 t_recon 被
    # 发送端过滤成空并记入过滤账本。这是「负向过滤真实发生」的可靠机制(照搬 Rust 测试
    # 「A 在 B 入网前写 tests2」),而不是指望 scored_reduce 发送端裁剪对 live 节点零泄漏。
    early = [nodes[i] for i in (0, 1, 3, 4, 5)]
    procs = start_procs(early)
    results = {}
    try:
        print("[集群] 先起 5 节点(node0/1/3/4/5,node2 后入网),等 ACTIVE ...")
        if not H.wait_active(early, timeout=90):
            print("  ⚠ 早期 5 节点未全部 ACTIVE"); return results
        time.sleep(3)  # 成员网格 + node_interest 复制稳定

        # --- 写数(各表由其 holder 写,写入者合法持有;node2 尚未入网)---
        prefix = f"x{int(time.time())}_"
        print(f"\n[写数] 各表 {ROWS} 行,前缀={prefix}(t_recon←node3, t_strike←node1, "
              f"t_jam/t_solo←node5)")
        insert_rows(nodes[3], "t_recon", prefix, ROWS)
        insert_rows(nodes[1], "t_strike", prefix, ROWS)
        insert_rows(nodes[5], "t_jam", prefix, ROWS)
        insert_rows(nodes[5], "t_solo", prefix, ROWS)

        # --- 场景1:部分复制正向 + node2 后入网的可靠负向证据 ---
        print("\n【场景1】部分复制:各 holder 收齐关心表 + node2 后入网只经 sync 交付")
        # 正向:跨节点复制到达关心表的另一个 holder(node0 收 t_recon,node4 收 t_strike)。
        ok_recv, _ = poll(lambda: count_local(nodes[0]["db"], "t_recon", prefix) == ROWS
                          and count_local(nodes[4]["db"], "t_strike", prefix) == ROWS,
                          timeout=90)
        print(f"    node0 收 t_recon={count_local(nodes[0]['db'], 't_recon', prefix)}, "
              f"node4 收 t_strike={count_local(nodes[4]['db'], 't_strike', prefix)} "
              f"(各期望 {ROWS}) → {'✓' if ok_recv else '✗'}")

        # 软报告:live 节点对非关心表的泄漏(broadcast 收端不过滤,只作观测不 gate)。
        leak0_strike = count_local(nodes[0]["db"], "t_strike", prefix)  # node0=recon,不关心 t_strike
        leak1_recon = count_local(nodes[1]["db"], "t_recon", prefix)    # node1=strike,不关心 t_recon
        print(f"    [软报告] live 节点泄漏观测:node0.t_strike={leak0_strike}, "
              f"node1.t_recon={leak1_recon}(0=无泄漏;非0=broadcast 收端泄漏,不判据)")

        # node2 后入网。
        print("    起 node2(后入网)...")
        procs += start_procs([nodes[2]])
        if not H.wait_active(list(nodes.values()), timeout=90):
            print("  ⚠ 全 6 节点未 ACTIVE"); return results
        node3_hex = local_site_hex(nodes[3]["db"])  # t_recon 作者 actor_id
        # node2 负向证据(可靠):经 sync 拿到 t_jam 全量;本地无 t_recon;对 node3(t_recon
        # 作者)过滤账本 > 0 —— 这既是「过滤真发生」也是场景2 reopen 的前置。
        ok_n2, _ = poll(lambda: count_local(nodes[2]["db"], "t_jam", prefix) == ROWS
                        and count_local(nodes[2]["db"], "t_recon", prefix) == 0
                        and filtered_ranges_for(nodes[2]["db"], node3_hex) > 0,
                        timeout=120)
        n2_jam = count_local(nodes[2]["db"], "t_jam", prefix)
        n2_recon = count_local(nodes[2]["db"], "t_recon", prefix)
        n2_filt3 = filtered_ranges_for(nodes[2]["db"], node3_hex)
        print(f"    node2: t_jam={n2_jam}(期望{ROWS}), t_recon本地={n2_recon}(期望0), "
              f"对node3过滤账本={n2_filt3}(期望>0) → {'✓' if ok_n2 else '✗'}")
        results["s1"] = ok_recv and ok_n2
        print(f"  【场景1】→ {'✅ PASS' if results['s1'] else '❌ FAIL'}")
        if not results["s1"]:
            return results

        # --- 场景5:node0(flag=false)POST → 403(在任何变更前做,状态干净) ---
        print("\n【场景5】对 node0(allow_runtime_interest=false)POST → 期望 403")
        code, body = post_interest(nodes[0]["api"], ["t_recon", "t_jam"], epoch=1)
        results["s5"] = (code == 403)
        print(f"    状态码={code}(期望403) → {'✅ PASS' if results['s5'] else '❌ FAIL'}  body={body}")

        # --- 场景3:node5 摘除唯一副本 t_solo → 409,行未删 ---
        print("\n【场景3】对 node5 摘除 t_solo(唯一 active holder)→ 期望 409 且行未删")
        node5_hex = local_site_hex(nodes[5]["db"])
        # 前置:node5 的 t_solo 必须已 active=1(否则摘除不过 min-replica 门禁的 active 分支)。
        ok_solo_active, _ = poll(
            lambda: ni_active(nodes[5]["db"], "t_solo", node5_hex) == 1
            and ni_active(nodes[5]["db"], "t_jam", node5_hex) == 1, timeout=120)
        print(f"    前置 node5 node_interest active: t_solo="
              f"{ni_active(nodes[5]['db'], 't_solo', node5_hex)}, "
              f"t_jam={ni_active(nodes[5]['db'], 't_jam', node5_hex)} "
              f"(期望 1/1) → {'✓' if ok_solo_active else '✗'}")
        # 摘除:desired 去掉 t_solo(保留 t_jam);t_solo 无其它 active holder → 409。
        code, body = post_interest(nodes[5]["api"], ["t_jam"], epoch=1)
        s3_409 = (code == 409)
        # 行未删:node5 自己的 t_solo node_interest 行仍在且 active=1(txn 整体回滚)。
        s3_kept = (ni_active(nodes[5]["db"], "t_solo", node5_hex) == 1)
        results["s3"] = ok_solo_active and s3_409 and s3_kept
        print(f"    状态码={code}(期望409), t_solo 行 active 仍="
              f"{ni_active(nodes[5]['db'], 't_solo', node5_hex)}(期望1) "
              f"→ {'✅ PASS' if results['s3'] else '❌ FAIL'}  body={body}")

        # --- 场景4:node4 先 apply epoch=3,再 POST 更低 epoch=2 → 409 ---
        print("\n【场景4】node4 先成功 apply epoch=3,再 POST epoch=2 → 期望 409(stale)")
        code_hi, body_hi = post_interest(nodes[4]["api"], ["t_strike", "t_recon"], epoch=3)
        s4_hi = (code_hi == 200 and isinstance(body_hi, dict) and body_hi.get("accepted"))
        code_lo, body_lo = post_interest(nodes[4]["api"], ["t_strike", "t_jam"], epoch=2)
        s4_lo = (code_lo == 409)
        results["s4"] = s4_hi and s4_lo
        print(f"    epoch=3 → {code_hi}(期望200 accepted={body_hi.get('accepted') if isinstance(body_hi, dict) else body_hi}); "
              f"epoch=2 → {code_lo}(期望409) → {'✅ PASS' if results['s4'] else '❌ FAIL'}")
        if isinstance(body_lo, dict):
            print(f"    回退 body={body_lo}")

        # --- 场景2:node2 运行期扩 interest 收 t_recon → 回填 + 激活 + 实时 ---
        print("\n【场景2】对 node2 扩 interest 收 t_recon(epoch=1)→ reopen + 回填 + 激活 + 实时")
        node2_hex = local_site_hex(nodes[2]["db"])
        # 前置:等 node2 的启动期 interest(t_jam)先激活到位再扩容。运行期扩 interest 面向的
        # 是「已就绪」节点——真实控制器只对已收敛的节点改 placement。若在启动激活尚未收尾
        # (它走 initial_sync 判据、独占单飞锁 interest_activation_lock)时就抢跑扩容,扩容激活
        # 任务会阻塞在锁上,两者经锁相互耦合。node2 后入网后启动激活约 10s 内完成(见
        # last_sync_ts 修复:吃饱节点也能收敛 members_synced),这里给足 120s。
        ok_settle, _ = poll(
            lambda: ni_active(nodes[2]["db"], "t_jam", node2_hex) == 1, timeout=120)
        print(f"    前置 node2 启动 interest 已激活 active(t_jam)="
              f"{ni_active(nodes[2]['db'], 't_jam', node2_hex)}(期望1) "
              f"→ {'✓' if ok_settle else '✗'}")
        filt_before = filtered_ranges_for(nodes[2]["db"], node3_hex)
        code, body = post_interest(nodes[2]["api"], ["t_jam", "t_recon"], epoch=1)
        s2_resp = (code == 200 and isinstance(body, dict) and body.get("accepted")
                   and body.get("reopened_ranges", 0) > 0 and body.get("pending_activation"))
        print(f"    POST → {code}, accepted={body.get('accepted') if isinstance(body, dict) else body}, "
              f"reopened_ranges={body.get('reopened_ranges') if isinstance(body, dict) else '?'}"
              f"(前置过滤账本={filt_before}), "
              f"pending={body.get('pending_activation') if isinstance(body, dict) else '?'} "
              f"→ {'✓' if s2_resp else '✗'}")
        # 回填 + 激活(载荷断言):t_recon 历史全量到 node2;node_interest(node2,t_recon) active=1;
        # node_interest(node2,t_jam) 仍 active=1;对 node3(t_recon 作者)的过滤账本清空**且保持**
        # (node2 现在关心 node3 的全部作者版本,sync 不再过滤它 → 账本不会回填)。
        ok_bf, _ = poll(
            lambda: count_local(nodes[2]["db"], "t_recon", prefix) == ROWS
            and ni_active(nodes[2]["db"], "t_recon", node2_hex) == 1
            and ni_active(nodes[2]["db"], "t_jam", node2_hex) == 1
            and filtered_ranges_for(nodes[2]["db"], node3_hex) == 0, timeout=120)
        print(f"    回填/激活: t_recon={count_local(nodes[2]['db'], 't_recon', prefix)}(期望{ROWS}), "
              f"active(t_recon)={ni_active(nodes[2]['db'], 't_recon', node2_hex)}, "
              f"active(t_jam)={ni_active(nodes[2]['db'], 't_jam', node2_hex)}(期望1/1), "
              f"node3过滤账本={filtered_ranges_for(nodes[2]['db'], node3_hex)}(期望0) "
              f"→ {'✓' if ok_bf else '✗'}")
        # 实时:激活后 node3 新写 t_recon 必须实时到 node2。
        live_prefix = f"live{int(time.time())}_"
        insert_rows(nodes[3], "t_recon", live_prefix, 3)
        ok_live, _ = poll(
            lambda: count_local(nodes[2]["db"], "t_recon", live_prefix) == 3, timeout=90)
        print(f"    实时: node3 新写 3 行 → node2 收 "
              f"{count_local(nodes[2]['db'], 't_recon', live_prefix)}(期望3) "
              f"→ {'✓' if ok_live else '✗'}")
        results["s2"] = ok_settle and s2_resp and ok_bf and ok_live
        print(f"  【场景2】→ {'✅ PASS' if results['s2'] else '❌ FAIL'}")

        return results
    finally:
        stop_procs(procs)


def main():
    if not os.path.exists(H.BIN):
        sys.exit(f"找不到 binary {H.BIN},先 cargo build -p corrosion")
    t0 = time.time()
    print("== 运行期 interest 热更新集群 e2e(6 节点 scored_reduce + 静态窄 interest)==\n")
    results = run()
    wall = time.time() - t0

    print("\n== 汇总 ==")
    verdicts = [
        ("场景1 部分复制(正向收齐 + node2 后入网负向过滤)", results.get("s1", False)),
        ("场景2 运行期扩 interest → reopen+回填+激活+实时", results.get("s2", False)),
        ("场景3 摘除唯一副本 t_solo → 409 且行未删",        results.get("s3", False)),
        ("场景4 epoch 回退 → 409(stale)",                 results.get("s4", False)),
        ("场景5 flag 关节点 POST → 403",                    results.get("s5", False)),
    ]
    for label, v in verdicts:
        print(f"  {'✅ PASS' if v else '❌ FAIL'}  {label}")
    all_ok = len(results) == 5 and all(v for _, v in verdicts)
    print(f"\n总耗时 {wall:.1f}s — {'✅ 全部 PASS' if all_ok else '❌ 存在 FAIL'}")
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()


# ============================================================================
# 缺陷落地记录(2026-07-22;此 harness 暴露并驱动修复,均已合入本分支)
# ----------------------------------------------------------------------------
# bug #1 (commit a1644813 `fix(sync): stamp last_sync_ts on zero-data sync rounds`)
#   激活静默门 initial_sync_is_complete 的 members_synced(所有成员 last_sync_ts.is_some())
#   在 ≥~4-6 节点集群永不收敛:last_sync_ts 原先只在发起方 outbound sync 且该 peer 有数据可拉
#   (进 readers)时置位,吃饱/种子节点对无需拉取的 peer 永不置位,且完全追平的节点在 stamp
#   前 early-return。插桩证实 bookie_quiescent 恒真、members_synced 恒假;2/3 节点可达,6 节点
#   180s 仍 0 激活。修复:握手完成=完成一轮 sync,对所有握手成功 peer 置位(拆分/early-return
#   之前)。防御:activate_pending_interest_when_synced 每 30s warn 卡在哪个子条件、等了多久。
#
# bug #2 (commit `fix(sync): close reopened gaps for still-filtered tables`)
#   运行期扩 interest 时 reopen 重开全部 __corro_filtered_version_ranges(账本只存 actor+version,
#   不存表),对本节点仍不关心的其它表也造 needed gap。发送端 serve_sync 会重发 Changeset::Empty
#   关 gap,但 handle_changes 的 in-memory `seen` 短路缓存(首次过滤时已记入这些版本)在 empty
#   到达 process_multiple_changes 前将其丢弃(no-seqs 且版本全在 seen)→ gap 永不关闭、
#   filtered 账本不重记 → reopened_ranges_are_complete 永等不到 → 采纳表激活卡死。
#   根因定位法:GAP_PROBE 插桩沿 serve→client-recv→handle_changes→PMC 逐跳追踪,发现 empty
#   在 handle_changes seen 短路处消失(未到 PMC)。修复:seen 短路让位于 bookie 权威 needed——
#   版本仍被 needed(重开 gap)时不短路,让 empty 流到 PMC 关 gap + 重记 filtered 账本。
#   回归:crates/corro-agent/src/agent/tests.rs::
#     runtime_interest_expansion_closes_residual_foreign_gaps(2 节点 + 残留外部过滤表 tests3)。
#
# 两缺陷 Rust 双节点 e2e 均未覆盖(2 节点 members_synced 平凡可达;单张外部过滤表无残留 gap),
# 多节点 + 多表过滤才暴露 —— 此集群 harness 即该覆盖面的常驻件。
# ============================================================================
