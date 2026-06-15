# 1b 设计：任务感知 anti-entropy（对账按 interest 过滤）

> 前提见 `active-push-plan.md §8`（placement 是降量 4.4.3 与 1M QPS 4.4.2 的共同支点）。
> 本文定数据流、记账状态机、协议改动与正确性边界。**设计文档，不含实现代码。**

## 1. 目标与边界

**目标**：让 anti-entropy（对账）只在节点间传输「该节点任务关心的表」的数据，断掉
1a 暴露的全量兜底——这是让全局传输总量**稳定**下降 30% 的关键轴（1a 证明只动推送轴会被兜底抵消）。

**边界（本期不做）**：查询路由（4.4.2）只定接口、不实现；动态 interest 变更与历史回填留作开放问题；
GNN/DRL（4.3.3）无关。

**正确性立场的转变**：从「每节点对全库最终一致」→「**每节点对其 interest 子集最终一致**」。
不关心的表在本地就是缺失，靠查询路由兜（§8）。CRDT 合并语义本身不动。

## 2. 现状地基（调研结论，带 文件:行号）

| 部件 | 位置 | 事实 |
|---|---|---|
| 账本 `BookedVersions` | `crates/corro-types/src/bookie.rs:27` | `partials`(版本→已收 seq)、`needed`(缺的版本区间 `RangeInclusiveSet`)、`max` |
| 对账清单生成 `generate_sync` | `crates/corro-types/src/sync.rs:281` | 产出 `heads`/`need`/`partial_need`，**全是版本号,表盲** |
| 缺口持久化 | `crates/corro-types/src/agent.rs` | `__corro_bookkeeping_gaps`(版本区间)/`__corro_seq_bookkeeping`(seq 区间)，**无表名列** |
| 完整性判定 `PartialVersion::is_complete` | `crates/corro-types/src/agent.rs:806` | `0..last_seq` 的 seq 无缝隙 = 完整 |
| 变更行 `Change` | `crates/corro-types/src/change.rs:20` | 每行带 `table`/`seq`/`db_version`；`crsql_changes`、`__corro_buffered_changes` 都有 `"table"` 列 |
| 应用落点（完整） `process_complete_version` | `crates/corro-agent/src/agent/util.rs:1303` | 手里有 `changes: Vec<Change>`，已在算 `changes_per_table` |
| 应用落点（缓冲） `process_incomplete_version` | `crates/corro-agent/src/agent/util.rs:1229` | 同上，可取每行 `change.table` |

**一句话**：表名在「变更行」这一层处处都有；但驱动对账的「账本/need」这一层完全表盲。

## 3. 核心难点：记账死锁（必须先解决的设计约束）

朴素方案「本地建版本→表映射表、本地过滤」**不成立**，因为：

1. `need` 的全部意义是「**我还没收到的版本**」。对没拉过的版本，本地无从知道它碰了哪些表
   → 无法判断「该不该拉」。**鸡生蛋。**
2. 若某版本**只改了我不关心的表**，我永远不拉 → gap 永远填不上 → `needed` 永远挂着它
   → **无限 re-request 死锁**，对账永不收敛。

**结论**：版本→表的判定信息必须来自**持有该版本的发送方**，且必须有机制让请求方
**关掉「与我无关」的 gap**。

## 4. 设计：发送方过滤 + 兴趣声明 + 满足确认

### 4.1 数据流

```
请求方 R（侦察节点，interest={flight}）          发送方 S（持有 actor A 的版本）
  │                                                │
  │ 1. 对账握手：发 SyncStateV1 + interest={flight} │
  │───────────────────────────────────────────────▶│
  │                                                │ 2. 对每个 R 缺的版本 V：
  │                                                │    查 S 本地「版本→表」映射
  │                                                │    a) V 含 flight 行 → 只发这些行
  │                                                │    b) V 不含 flight  → 不发数据,
  │                                                │       但回「V 对你已满足」标记
  │ 3. 收到 flight 行 → 应用                        │◀──── 数据 + 满足标记 ───────────
  │ 4. 收到满足标记(含 0 行的 V) → 关闭该 gap        │
  │    needed 移除 V，不再 re-request               │
  ▼                                                ▼
  R 对 flight 最终一致；战场环境的版本 gap 被「无关满足」关闭，不再拉
```

### 4.2 版本→表映射：挂在发送方

- **落点**：`process_complete_version`(`util.rs:1303`) / `process_incomplete_version`(`util.rs:1229`)
  ——应用变更时 `change.table` 现成，顺手写入。
- **存储**：新增持久化表（草案）
  ```sql
  CREATE TABLE __corro_version_tables (
      site_id BLOB NOT NULL,        -- 版本所属 actor
      db_version INTEGER NOT NULL,
      table_name TEXT NOT NULL,
      PRIMARY KEY (site_id, db_version, table_name)
  ) WITHOUT ROWID;
  ```
  （只需「版本含哪些表」的集合即可决定过滤；是否要 seq 区间见 §8 开放问题。）
- **为什么在发送方够用**：每个节点对「自己应用过的版本」都写这张表；对账时 S 用它过滤 R 要的版本。
  R 不需要预先知道——R 只声明 interest，由 S 告诉 R 结果。

### 4.3 协议改动（集成点，非最终签名）

1. **握手带 interest**：请求方对账时附带「我关心的表集合」。候选挂点：`SyncStateV1`
   (`sync.rs:80`) 加 `interest: Option<HashSet<TableName>>`（`None`=全量,向后兼容旧节点）。
2. **发送方过滤 + 满足信号**：`compute_available_needs`(`sync.rs:127`) / 发送变更的路径
   按 interest 过滤行；并产出「这些版本对该 interest 已满足」的回执（含 0 行的版本）。
3. **请求方关 gap**：收到满足回执 → 从 `needed` 移除对应版本区间（即使没收到任何行）。

### 4.4 记账状态机（gap 的三种归宿）

当前 gap 只有两态：缺失 / 已收齐。1b 加第三态：

```
版本 V 在 needed 中：
  ├─ 收到 V 的(我关心表的)全部 seq        → 收齐，移出 needed（既有逻辑）
  ├─ 收到「V 对我 interest 无关」满足回执   → 无关满足，移出 needed（★新增）
  └─ 未决                                  → 留在 needed，下轮再问
```

**完整性判定改造**：`is_complete` → 区分「对全量完整」与「对 interest 完整」。
按 interest 过滤后，一个版本「我关心的表的 seq 都齐了」即视为对我完整，
哪怕该版本还有别的表的 seq 我没收到。

## 5. 正确性论证

- **CRDT 不变**：只改「拉哪些版本的哪些行」，不改 merge/LWW/因果。已拉的数据合并语义完全一致。
- **最终一致性范围**：重定义为「每节点对其 interest 子集最终一致」。这是**有意的**模型变更，
  不是 bug——降量的代价（§8）。
- **不死锁**：满足回执让无关 gap 能关闭，`needed` 不再无限挂。
- **向后兼容**：`interest=None` 时行为 == 现状（全量）。旧节点不发 interest → 被当作全量,
  不破坏混合集群。
- **查得到**：不关心的表本地缺失 → 必须配查询路由（4.4.2）。本期只留接口：
  「本地无此表数据时，路由到 `interest_routing` 中持有该表的节点」。

## 6. 最小可行切片与分步

1. **版本→表映射（只读地基，可独立提交、可验证）**：在 `process_*_version` 写
   `__corro_version_tables`；加单测验证映射正确。**不碰协议**，先把地基铺对。
2. **发送方过滤（单向）**：`interest` 进握手 + 发送方按表过滤行 + 满足回执。
3. **请求方关 gap**：第三态记账 + interest 完整性判定。
4. **harness 验收**：mission 拓扑下 `sweep.py` 测「全局总量随规模稳定 ↓≥30%」、无 re-request 死锁、
   收敛只对 interest 表判定。
5. **查询路由占位**：本地缺表 → 路由桩（返回「应路由到 X」或简单代理），打通 4.4.2 接口。

## 7. 验收标准（对接 4.4.3 第三方测评口径）

- `sweep.py` 多规模 × 重复取均值：`scored_reduce`+1b 相对 `random` 全局总传输 **稳定 ↓≥30%**，
  且**不随规模反噬**（修掉 1a 的 30 节点转负）。
- 无对账死锁：`corro.sync.client.needed.v2` 不持续增长。
- interest 表对账后本地可见；非 interest 表本地缺失但查询路由可达。

## 8. 风险与开放问题

- **seq 区间是否要进映射表**：若只过滤「版本含不含某表」够用，则不需要；若要「同一版本里只发某表的行」
  的精细 seq 记账，映射表要带 seq 区间。先按「版本级含/不含」做，按需细化。
- **interest 动态变更**：节点任务变了（新关心一张表）→ 需回填历史版本。本期不做，记为开放。
- **历史版本回填**：新加入节点对其 interest 的历史数据如何高效补齐（不退化回全量）。
- **协议版本与混合集群**：`interest=None` 兼容老节点；需测混合集群不破坏收敛。
- **measurement**：继续用 `sweep.py` 均值+误差带，杜绝单次结论（1a 的教训）。
