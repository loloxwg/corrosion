#!/usr/bin/env python3
"""Phase 2 验证：对账按 interest 过滤 + gap 关闭(B3)。

起一个 scored_reduce 集群(各节点自声明 interest)，写满 3 类数据，等 anti-entropy
多轮后检查：
  ① 功能：非写入节点上「非关心表」本地行数 = 0（sync 没回填）、「关心表」收齐。
  ② B3 硬不变量：__corro_bookkeeping_gaps 行数 + corro.sync.client.needed.v2 不随时间增长
     （证明无关版本的 gap 被 Changeset::Empty 显式关闭，没有死锁/无限重请求）。

用法: python3 research/harness/verify_phase2.py --nodes 9 --rows 20
"""

import argparse
import os
import shutil
import signal
import subprocess
import sys
import time

import run as H  # 复用 harness


def query_int(nd, sql):
    r = H.sh([H.BIN, "-c", nd["cfg"], "query", sql])
    try:
        return int(r.stdout.strip().split("|")[0])
    except (ValueError, IndexError):
        return -1


def gap_count(nd):
    # 直读本地 DB。经公开 query API 查询内部表可能被拒绝/路由，旧脚本会吞错返回 -1，
    # 进而把“观测失败”误当成 gap 没增长。
    r = H.sh(["sqlite3", nd["db"],
              "SELECT COALESCE(SUM(end - start + 1), 0) "
              "FROM __corro_bookkeeping_gaps"])
    try:
        return int(r.stdout.strip())
    except ValueError:
        return -1


def needed_v2(nd):
    return H.scrape(nd, ["corro.sync.client.needed.v2"])["corro.sync.client.needed.v2"]


def wait_quiescent(nodes, timeout, interval=5):
    """等待 gap 与 needed gauge 连续两次全为 0，区分瞬态对账与无限重试。"""
    deadline = time.time() + timeout
    first = None
    previous_clear = False
    latest = None
    while time.time() < deadline:
        gaps = {nd["i"]: gap_count(nd) for nd in nodes}
        need = {nd["i"]: needed_v2(nd) for nd in nodes}
        latest = (gaps, need)
        if first is None:
            first = latest
        clear = (all(v == 0 for v in gaps.values())
                 and all(v == 0 for v in need.values()))
        if clear and previous_clear:
            return first, latest, True
        previous_clear = clear
        time.sleep(interval)
    return first, latest, False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", type=int, default=9)
    ap.add_argument("--rows", type=int, default=20)
    ap.add_argument("--settle", type=int, default=20, help="写后等 anti-entropy 的秒数")
    ap.add_argument("--startup-timeout", type=int, default=180)
    ap.add_argument("--quiesce-timeout", type=int, default=120,
                    help="等待 gap/needed 连续两次归零的最长秒数")
    args = ap.parse_args()

    if not os.path.exists(H.BIN):
        sys.exit(f"找不到 binary {H.BIN}，先 cargo build -p corrosion")

    if os.path.exists(H.WORK):
        shutil.rmtree(H.WORK)
    os.makedirs(H.WORK)

    nodes = H.write_configs(args.nodes, 7400, 8400, 9400, "scored_reduce")
    procs = H.start(nodes)
    try:
        if not H.wait_active(nodes, timeout=args.startup_timeout):
            sys.exit("节点未全部 ACTIVE")
        H.write_interest(nodes)
        time.sleep(18)  # 等 interest 充分传播 + 推送端缓存刷新（排除时序竞态）

        prefix = f"v{int(time.time())}_"
        for t in H.TABLES:
            for j in range(args.rows):
                H.sh([H.BIN, "-c", nodes[0]["cfg"], "exec",
                      "--param", f"{prefix}{j}", "--param", f"{t}-{j}",
                      f"INSERT INTO {t} (id,data) VALUES (?,?)"])

        print(f"写入完成，等 anti-entropy {args.settle}s ...\n")
        time.sleep(args.settle)

        # B3：needed.v2 是 gauge，不是累计 counter。大集群中 0→1 可能只是采到一次
        # 正常对账，不能直接判“无限重试”。要求 gap 与 needed 连续两次全为 0。
        first, latest, quiescent = wait_quiescent(nodes, args.quiesce_timeout)
        gaps1, need1 = first
        gaps2, need2 = latest

        ok = True
        print(f"{'节点':<6}{'角色':<8}{'关心表(应收齐)':<22}{'非关心表(应=0)':<24}"
              f"{'gap t1→t2':<12}{'needed t1→t2':<14}")
        for nd in nodes:
            i, role = nd["i"], nd["role"]
            interested = H.ROLE_TABLES[role]
            others = [t for t in H.TABLES if t not in interested]
            # ★直读本地 db(count_rows_local),绕过查询路由(4.4.2)——否则非关心节点经路由
            #  从持有者拿回计数,会把「本地已裁剪」误判成「本地有数据」(假 FAIL)。
            int_counts = {t: H.count_rows_local(nd, t, prefix) for t in interested}
            oth_counts = {t: H.count_rows_local(nd, t, prefix) for t in others}

            # node0 是写入者，本地有全部数据，跳过"非关心缺失"判定
            is_writer = (i == 0)
            int_ok = all(c == args.rows for c in int_counts.values())
            # 非关心表须在「已应用业务表」与「buffered 暂存层」**都=0**(后者防收了未 apply 的泄漏)。
            oth_buf = {t: H.count_buffered_local(nd, t) for t in others}
            oth_ok = is_writer or (all(c == 0 for c in oth_counts.values())
                                   and all(c == 0 for c in oth_buf.values()))
            gap_clear = gaps2[i] == 0
            need_clear = need2[i] == 0
            if not (int_ok and oth_ok and gap_clear and need_clear):
                ok = False

            flag = "" if (int_ok and oth_ok and gap_clear and need_clear) else "  ✗"
            writer_tag = "(写)" if is_writer else ""
            print(f"{i:<6}{role:<8}"
                  f"{str(int_counts):<22}{str(oth_counts):<24}"
                  f"{gaps1[i]}→{gaps2[i]:<9}{need1[i]:.0f}→{need2[i]:.0f}{writer_tag}{flag}")

        # 诊断：对账 interest 过滤计数器(全集群求和)——确认 handle_need 过滤是否触发
        fk = ["corro.sync.interest.filtered.versions", "corro.sync.interest.kept.versions"]
        filt = sum(H.scrape(nd, fk)[fk[0]] for nd in nodes)
        kept = sum(H.scrape(nd, fk)[fk[1]] for nd in nodes)
        print(f"\n诊断：对账过滤版本数={int(filt)}  保留版本数={int(kept)}"
              f"  (filtered>0 说明 handle_need interest 过滤已触发)")

        print("\n判定：")
        data_ok = all(
            all(H.count_rows_local(nd, t, prefix) == args.rows
                for t in H.ROLE_TABLES[nd["role"]])
            and (nd["i"] == 0 or all(
                H.count_rows_local(nd, t, prefix) == 0
                and H.count_buffered_local(nd, t) == 0
                for t in H.TABLES if t not in H.ROLE_TABLES[nd["role"]]))
            for nd in nodes)
        print("  ① 非关心表本地缺失(sync 已过滤)：", "通过" if data_ok else "见上 ✗ 行")
        print("  ② B3 gap/needed 连续两次归零(无死锁/无限重请求)：",
              "通过" if quiescent else "✗ 未在时限内静默")
        total_ok = data_ok and quiescent and ok
        print("\n总判定：", "✅ Phase 2 PASS" if total_ok else "❌ FAIL")
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
