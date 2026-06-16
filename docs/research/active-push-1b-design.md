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

### 4.0 关键修订（Codex 复核后）：版本级过滤 + 复用 Changeset::Empty

原设计「行级过滤 + 新增满足回执」有两个坑，复核后改为：

1. **不能行级过滤**：`handle_need` 的完整性判定是「版本内 `0..last_seq` 的 seq 全到齐」
   (`agent.rs:806` / `sync.rs:303`)。若只发该版本里关心表的行，请求方收到的 seq 是子集 →
   永远 partial → 死锁。**改为版本级（全有或全无）**：
   - 版本 V 碰了**任一**关心表 → **整版发**（含其非关心行，但一个事务是一个原子，
     mission 模型里写入多为单表事务，所以版本级≈行级、降量不打折）。
   - 版本 V **完全不碰**关心表 → 发 `Changeset::Empty`。
2. **不新增满足回执**：`handle_need` **现成**就会把空版本发成 `Changeset::Empty`
   (`peer/mod.rs:732-741`)，请求方 `process_empty_version`(`util.rs:~1016/1217`) 会
   **推进 db_version、关闭 gap**——这条路径已在生产中跑（compaction/清版本）。
   1b 只需让「对该请求方 interest 无关的版本」也走这条已测路径。**不碰 bookie 完整性判定。**
   ⚠ 用 `Changeset::Empty`，**不要**用 `Changeset::EmptySet`（客户端会忽略，`peer/mod.rs:1359`）。

### 4.1 数据流（修订）

```
请求方 R（侦察节点，interest={flight}）          发送方 S（持有 actor A 的版本）
  │ 1. 对账握手：SyncStateV1 + interest={flight}    │
  │───────────────────────────────────────────────▶│
  │                                                │ 2. 对每个 R 缺的版本 V：
  │                                                │    查 V 是否碰任一 interest 表
  │                                                │    a) 碰 → 整版发(既有 send_change_chunks)
  │                                                │    b) 不碰 → 计入 empties
  │ 3. 收到关心版本 → 应用(seq 完整,正常判定)        │◀── 数据 / Changeset::Empty ──
  │ 4. 收到 Changeset::Empty → process_empty_version│
  │    推进 db_version、关 gap(既有逻辑，不改 bookie) │
  ▼                                                ▼
  R 对 flight 最终一致；无关版本被 Empty 关 gap，不再 re-request、gap 表不增长
```

### 4.2 「版本是否碰 interest 表」怎么判定（发送方）

不需要新建 `__corro_version_tables` 映射表——`crsql_changes` 行本就带 `"table"`。
`handle_need`(`peer/mod.rs:378`) 的逐版本处理里，对每个版本判定：

```sql
SELECT EXISTS(
  SELECT 1 FROM crsql_changes
   WHERE site_id = :actor AND db_version = :v AND "table" IN (:interest...)
)
```

- 命中 → 走既有 `send_change_chunks`（整版，不改）。
- 不命中 → 加入 `empties`（既有 `Changeset::Empty` 路径）。
- `interest=None`（旧节点/关闭态）→ 跳过判定，全量发（== 现状）。

（持久映射表留作优化项：若每版本查一次 EXISTS 成热点，再建 `__corro_version_tables` 缓存，
见 §8。先用现成 `crsql_changes` 跑通。）

### 4.3 协议改动（集成点）

1. **握手带 interest**：`SyncStateV1`(`sync.rs:80`) 末尾加
   `#[speedy(default_on_eof)] pub interest: Option<HashSet<TableName>>`（`None`=全量，
   先例 `last_cleared_ts`；覆盖新旧节点混合集群集成测试）。请求方用 `gossip.interest` 填充。
2. **interest 串到发送方**：`serve_sync`(~1492) 读到请求方 `SyncStateV1.interest` →
   传入 `process_sync`(~811) → `handle_need`(378)。
3. **`node_interest` 表恒 include**：interest 集合里**永远加上 `node_interest`**（及其它系统复制表），
   否则节点间 interest 自身传不开，Phase 1 失效。

### 4.4 记账：复用既有，不加第三态

原计划的「第三态/interest-complete 判定」**取消**。因为版本级 + `Changeset::Empty` 后，
无关版本对请求方就是「空版本」，命中既有的「empty → 推进 db_version → 关 gap」语义，
`is_complete` / bookie `needed` 逻辑**完全不改**。这是本次复核最大的降险点。

## 5. 正确性论证

- **CRDT 不变**：只改「拉哪些版本」（版本级全有或全无），不改 merge/LWW/因果，不改 bookie 完整性判定。
- **最终一致性范围**：重定义为「每节点对其 interest 子集最终一致」。这是**有意的**模型变更，
  不是 bug——降量的代价（§8）。
- **不死锁 / gap 不增长**：无关版本走既有 `Changeset::Empty` → `process_empty_version` 关 gap，
  `__corro_bookkeeping_gaps` 不会无限增长（Codex 复核指出：reaper/compaction **不清** gap 表，
  `reaper.rs:155`，故必须靠 Empty 显式关 gap，否则流量不降反升）。这是 1b 的**硬不变量**。
- **向后兼容**：`interest=None` 时行为 == 现状（全量）。旧节点不发 interest → 被当作全量,
  不破坏混合集群。
- **查得到**：不关心的表本地缺失 → 必须配查询路由（4.4.2）。本期只留接口：
  「本地无此表数据时，路由到 `interest_routing` 中持有该表的节点」。

## 6. 最小可行切片与分步（修订）

1. **握手带 interest**：`SyncStateV1` 加 `interest` 字段(`default_on_eof`) + 请求方用
   `gossip.interest` 填充 + 串到 `serve_sync`/`process_sync`/`handle_need`。先不过滤，
   只把字段打通，跑混合集群兼容(新旧节点)不破坏收敛。
2. **发送方版本级过滤**：`handle_need` 对每个需求版本判 `EXISTS(... table IN interest ...)`，
   不命中 → 计入 `empties`(既有 `Changeset::Empty` 路径)；`interest=None` → 全量。
   `node_interest` 恒 include。
3. **验证 gap 关闭（B3 硬门槛）**：单测/集成测——非 interest 版本发 Empty 后请求方
   `__corro_bookkeeping_gaps` 行数收敛、不增长，`corro.sync.client.needed.v2` 不持续上涨。
4. **harness 验收**：mission 拓扑 `sweep.py` 多规模×重复——全局总量稳定 ↓≥30% 且不反噬；
   收敛只对 interest 表判定；非 interest 表本地缺失。
5. **查询路由占位**：本地缺表 → 路由桩，打通 4.4.2 接口。

## 7. 验收标准（对接 4.4.3 第三方测评口径）

- **降量**：`sweep.py` 多规模 × 重复取均值，`scored_reduce`+1b 相对 `random` 全局总传输
  **稳定 ↓≥30%** 且**不随规模反噬**（修掉 1a 的 30 节点转负）。
- **★gap 不增长（B3 硬不变量）**：`__corro_bookkeeping_gaps` 行数收敛、`corro.sync.client.needed.v2`
  不持续增长——证明无关版本的 gap 被 `Changeset::Empty` 显式关闭，没有「降量假象、实则 gap 爆炸」。
- **兼容**：新旧节点混合集群收敛正常(`interest=None` == 全量)。
- interest 表对账后本地可见；非 interest 表本地缺失但查询路由可达。

## 8. 风险与开放问题

- **每版本 EXISTS 查询的开销**：版本级判定每版本查一次 `crsql_changes`。若成热点，再建
  `__corro_version_tables(site_id, db_version, table_name)` 缓存(在 `process_*_version` 落点写)。
  先用现成 `crsql_changes` 跑通，按需优化。
- **行级精度的取舍**：版本级 = 整版全有或全无。多表混合事务会整版发(轻微过送)，但保证 seq 完整、
  不碰 bookie。行级精准(只发某表的行)需共享版本→表→seq 映射 + 改完整性判定，复杂度高，留作后续。
- **interest 动态变更**：节点任务变了（新关心一张表）→ 需回填历史版本。本期不做，记为开放。
- **历史版本回填**：新加入节点对其 interest 的历史数据如何高效补齐（不退化回全量）。
- **协议版本与混合集群**：`interest=None` 兼容老节点；需测混合集群不破坏收敛。
- **measurement**：继续用 `sweep.py` 均值+误差带，杜绝单次结论（1a 的教训）。
