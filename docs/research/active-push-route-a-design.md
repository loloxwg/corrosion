# 路线 A：单进程内 interest 裁剪 —— 增量实施设计

> 适用前提：**一个无人机只跑一个进程**(用户硬约束)。此约束下圈子方案(多进程)出局，
> 自研全栈(C)风险过高 → 走 A：在 corrosion 单进程/单 gossip 网内做"按收件人 interest 裁剪"。
> 结论经 Codex 两轮独立复核(2026-06-17)。背景见 `active-push-1b-design.md §7.5`、`active-push-cluster-design.md`。

## 1. 为什么是 A（排除 B/C 的依据）

- **B 圈子出局**：一进程=一 `cluster_id`(`agent.rs`)。杀伤链数据需求重叠(目标=打击+干扰都要)，
  单圈子表达不了；跨圈子双写会破坏 CRDT(隔离宇宙不合流)，查询网关按需拉扛不住 1M QPS。
- **C 自研出局(暂)**：等于重写成员/同步/查询全栈，1M QPS + 收敛可靠性风险更大。
- **A**：保留 corrosion 现成的 gossip 多跳(4.2.3)/CRDT/查询层(4.4.2)，只在发送路径补"按 interest 裁剪 payload"。
  覆盖合同四条要求，且可增量、最危险的部分能放最后。

## 2. 核心问题与正解（一句话）

corrosion 现在对一次广播只编码**一份 payload**，发给"关心任意涉及表的 union 集合"，收方整批 apply
→ 非关心数据搭便车泄漏(实测：单写不漏、混写漏)。**正解 = 源端按收件人 interest 行级裁剪 payload**
(发给谁就只装谁要的行)。这是唯一真减少发出字节的位置(接收端再丢不省传输)。

## 3. 增量三步（Codex 拆法，先低风险后碰记账）

### 第一步（先做，低风险，不碰记账 bookie）
- **禁止多表 batch / 按单表版本分组发**：一次广播的 payload 只含同一张表(或同一 interest 组)的行。
- **`PendingBroadcast` 改存结构化 changeset**(不再只存 `Bytes`)：选完目标后，按收件人 interest 组
  分别序列化出不同 payload。
- 量字节流量基线，验证降幅。**不动 bookie/版本语义**(单表版本天然不混表，绕开完整性裂缝)。
- 触点：`broadcast.rs:681`(`FullV2` 构造) / `broadcast/mod.rs:639`(提前序列化点) / `PendingBroadcast`(~`mod.rs:1193`)。

### 第二步（sync 表级过滤 + scoped 记账原型）
- `handle_need`(`peer/mod.rs:460/483`)从"版本级过滤"推进到"表级 SQL 过滤"(`AND "table" IN(...)`)，
  **先只对单表版本启用**，混表版本回退整版。
- scoped bookie 原型：记录"某 actor/version 对哪些表/seq 已满足"。

### 第三步（完整 scoped 记账 + RL 接入）
- 完整行级裁剪 + 动态 interest 回填，靠 scoped bookie。
- **GNN+DRL 接到 `BroadcastStrategy::Rl` 占位**(已留)，替换手调 `score()`，跑 100 节点对照。

## 4. 最危险的点（必须知道）

**记账(bookie)的版本完整性语义**：`PartialVersion::is_complete()` 要求 `0..last_seq` 无 gap
(`agent.rs:~815`)；`process_complete_version` 对"完整"版本插入所有行并标 Current(`util.rs:~1332`)。
若行级子集仍冒充"完整版本" → **节点以为版本收全、实则只对部分表收全 = 静默数据错误**(不是 panic)。
→ 前两步用"单表版本"绕开它，第三步才正面做 scoped 完整性判定。

## 5. RL/GNN 插在哪（4.3.3）

- **位置**：策略层(`BroadcastStrategy::Rl`，调用点 `broadcast/mod.rs:744/872`、sync peer 选择 `handlers.rs:1152`)。
- **边界**：合法范围**先由 interest 规则/模版裁定**(发给谁、内容裁到收件人关心的表)，**RL 只在合法范围内优化**。
- **动作空间**：{发给哪些 peer、每 peer 的表 scope、fanout k、rebroadcast 优先级/TTL、sync peer 顺序}。
- **奖励**：−全局发送字节 −重复广播 −错投 −时延 +关心节点按时可见 +链路预算满足。
- **正确性由 interest 模版 + bookie 规则硬约束，RL 不得碰** → 学坏也不破坏一致性。

## 6. 复用既有成果（非白做）

- Phase 1：selector + `interest`/`node_interest`(每节点自声明 interest，推送端聚合) = 第一步的"谁要什么"输入。
- Phase 2：sync 版本级过滤 + `Changeset::Empty` 关 gap(B3 已验证不死锁) = 第二步的地基。
- harness：`run.py`/`sweep.py`(均值+误差带)/`cluster_bench.py`(对照框架) = 量降幅 + 将来 RL 训练/评估环境。

## 6.5 第一步实测 + Codex 复核(已实现，commit "routeA-step1")

**实测**：`verify_phase2.py`(9 节点单表写) 通过——非关心表本地全 0(部分复制达成)、B3 不死锁。
即单进程内、不用圈子，非关心数据真的没送过来。

**Codex 复核指出的边界(如实记录，多为已知限制/权衡，非阻塞)**：
1. **多表事务残留**：分组 scope = 单表 Some(T) / 其余 None。**None(多表/0表)仍会合批**，
   且单条多表 `Changeset` 不拆 → 仍按 union 关心者发。所以"每个 PendingBroadcast 单表"**只对单表广播成立**；
   多表事务的多张表仍会一起送达(= §4.0 的版本级边界，行级留 step3)。harness 单表写不触发，故 verify 过。
2. **吞吐权衡**：表交替到达时 flush-on-scope-change 退化为近乎每条一个 PendingBroadcast，批处理收益降低，
   rebroadcast 路径尤甚。高写入 QPS 下需评估；同表连续写仍合批。
3. **活性靠 sync 兜底**：interest 已配但某表暂无解析关心者 + COVERAGE_QUOTA=0 → 零目标发送，
   依赖 anti-entropy 把数据补给关心节点。verify 中 interest 表均收齐 = sync 确实兜上；
   但属"运行期保证"而非"代码内证明"，大规模/分区下需专门验活性。

## 6.6 压实第一步：实测结论(2026-06-17)

**单规模降幅**(`sweep.py` random 全量 vs scored_reduce 部分复制，均值)：
6→77% / 12→77% / 18→68% / 24→68% / 30→61% / 42→40%[31~49] —— **随规模下滑且 42 节点噪声大**。

**根因 = harness 竞态(非真规模弱点)**：harness 在节点**启动后**才 `exec` 写 interest，
node_interest 传播 + 推送端缓存刷新需要时间；settle 太短时早期写入趁 interest 未就绪走全发泄漏，
压低降幅、放大方差。**真实部署 interest 是启动时静态配置，数据流动前已全知，无此竞态。**
验证：42 节点把 settle 由 14s 加到 **45s**(模拟"启动即配好")→ 降幅 **40%→58%[58~58]**，方差收敛成一个点。

**结论**：
- step1 在规模上**站得住**：42 节点稳定 58%，远超 30%；6→42 的真实趋势是 77%→58%(gossip 放大随规模温和上升)，
  非崩塌。100 节点需继续观察，但有充足余量。
- harness 度量必须给足 settle(已加 `SETTLE` 环境变量覆盖 + 随规模自适应)；真实部署用启动配置则无关。
- **多表事务残留**(`multitable_test.py` 实测确认)：单表写在非关心节点缺失(隔离✓)；
  单条事务同写多表时，非关心表搭便车(=版本级边界，符合文档，不更糟)，行级精度留 step3。

## 7. 风险与开放

- 第一步把"提前序列化"改成"选目标后按组序列化"，序列化次数 ×(interest 组数)，需看吞吐影响。
- rebroadcast 必须保持裁剪 scope，不能在中继重新混表。
- scoped bookie(第三步)是真正的协议级改动，与上游 rebase 成本高 —— 这是 A 的主要长期代价。
- 第一步即可出"降幅基线"，据此判断是否值得继续往二、三步走。
