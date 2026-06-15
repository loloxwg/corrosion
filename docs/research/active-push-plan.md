# 任务感知主动推送：改造方案与实验设计

## 0. 对接项目考核指标（北极星）

| 项目要求 | 已建组件 | 差距 |
|---|---|---|
| 4.4.3 **全局数据传输总量降 30%**（智能 vs 广播） | `Random`=广播基线 / `Scored`=智能推送；harness 可测 broadcast 量 | scored 当前是「重定向」非「减量」→ 须做**减量变体** |
| 4.3.3 GNN+DRL 学**适配度评分函数** | selector `score()`(链路+相关度,手调线性)；`Rl` 分支占位 | 须把 score 换成 GNN+DRL 学习的函数 |
| 4.2.3 任务→数据需求模版 + 多跳路由(gossip) | `interest_routing`(表→关心addr)雏形；SWIM+anti-entropy 多跳/容断 | 须建 侦察/打击/干扰 → 飞行状态/战场环境/目标 的真模版 |
| 4.4.2 1M QPS / 100 节点 / 500Kbps | corrosion SQL 查询层 | 读吞吐扩展，另立工作线（与推送不同轴） |

⚠ **关键张力**：4.4.3 要降量，但 corrosion 全量复制 + anti-entropy 会把数据同步到所有节点，
抵消降量。要真降 30%，需让推送+对账都「按任务需求分发」，配合查询路由（4.4.2 查得到≠每节点都存）。

**最近一步（直指 4.4.3）**：实现 scored 的减量变体（任务无关平台不走快速推送）+ 用 harness
跑「广播 vs 智能」的全局传输总量对照，逼近 30%。

## 1. 一句话

把 Corrosion 广播传播时的**随机选 peer** 换成**按价值打分选 Top-K**，再进阶到图强化学习决策，
对比随机基线证明送达率/延迟/带宽的改善。

## 2. 精确改造点

`crates/corro-agent/src/broadcast/mod.rs`（约 700–730 行，`run_transmissions` 内）：

```rust
let broadcast_to = {
    agent.members().read().states.iter()
        .filter_map(|(member_id, state)| {
            // 排除自己 / 异 cluster / ring0(本地广播时) / 已发过的
            if *member_id == actor_id || state.cluster_id != agent.cluster_id()
                || (pending.is_local && ring0.contains(&state.addr))
                || pending.sent_to.contains(&state.addr) { None }
            else { Some(state.addr) }
        })
        .choose_multiple(&mut rng, choose_count)   // ★★ 随机选 K 个 —— 改这里
};
```

候选已由 `filter_map` 筛好；`choose_multiple(rng, K)` 是纯随机。**研究即替换这一步**。

## 3. 现有可复用信号

- **链路质量（RTT）**：`Transport::new(&config.gossip, tx_rtt)` 已采集 RTT；可经 agent 暴露给打分。
- **节点角色/分层**：已有 `ring0`（优先核心成员）二层概念（`members.ring0()`）——把它细化成连续分值。
- **成员状态**：`members().read().states` 含 addr、cluster_id、活跃度等。
- **订阅/关注**：Corrosion 有 subscription（`subs`）机制，可推导"哪个 peer 关心哪些表/数据" → 数据相关度。
- **指标**：已有 `corro.gossip.*` / `corro.broadcast.spawn` 等 metric，实验直接采。

## 4. 实施分阶段

### 阶段 0：插桩抽象（不改行为）
把选 peer 抽成一个可替换函数 + 配置开关，便于 A/B：
```rust
trait BroadcastSelector {
    fn select(&self, candidates: &[MemberRef], payload: &Payload, k: usize) -> Vec<SocketAddr>;
}
// RandomSelector（=现状基线）  /  ScoredSelector  /  RlSelector
```
配置 `gossip.broadcast_strategy = "random" | "scored" | "rl"`，运行时切换跑对照。

### 阶段 1：打分式（Scored）
```
score(peer) = w1·数据相关度(订阅/分片重叠)
            + w2·链路质量(RTT 反向归一)
            + w3·节点角色(ring0/任务角色)
            + w4·(1 − 负载)
→ 选 score 最高的 K 个（可留少量随机名额防"饿死"边缘节点）
```
权重先手调，记录每项贡献，做消融。

### 阶段 2：任务感知（Mission-aware）
引入任务/本体语义：mutation 带 topic/tag，peer 带 interest profile，
数据相关度 = similarity(mutation.topic, peer.interest)。对应文档里的 interest-based dissemination。

### 阶段 3：图强化学习（GraphRL）
网络建图 G=(V,E)，状态=链路/负载/邻居/任务，动作=发给谁/发多少，
奖励=送达率↑、延迟↓、带宽↓。学习传播策略，替换打分函数。

## 5. 对照实验设计

| 维度 | 设置 |
|---|---|
| 基线 | Corrosion 原版（random + ring0） |
| 对照组 | scored / mission-aware / RL |
| 规模 | 多节点集群（用 `corro-devcluster` 起，或容器/虚拟网络）；逐步加到 50–100 节点 |
| 负载 | 注入 SQL 写（不同 topic / 热点分布），可模拟丢包/分区/动态拓扑 |
| 指标 | ① 收敛延迟（写入到全集群可见）② 送达率 ③ 广播总字节/冗余度 ④ 每节点负载 |
| 采集 | 复用 `corro.gossip.*` / `corro.broadcast.*` metric + 自加埋点 |

预期结论：相同收敛目标下，主动推送把无关传播砍掉 → 带宽↓且热点数据延迟↓。

## 6. 风险与注意

- **公平性/活性**：纯 Top-K 可能饿死低分边缘节点 → 保留随机探索名额 + 配合 anti-entropy 兜底（Corrosion 的 sync 仍保证最终收敛）。
- **正确性不变**：只改"发给谁"，不改 CRDT 合并与对账 → 最终一致性不受影响，主动推送只优化"多快、多省"。
- **信号获取成本**：RTT/订阅已有；负载/任务语义需新增采集，注意别让打分本身成为开销瓶颈。
- **实验可复现**：固定随机种子、记录拓扑与负载脚本。

## 7. 下一步

1. 起本地多节点 Corrosion 集群，跑通 SQL 写读 + 同步，确立基线行为与指标基准。
2. 做阶段 0 抽象 + 配置开关（`RandomSelector` 等价现状，确保零回归）。
3. 实现 `ScoredSelector`，先用 RTT + ring0 两个信号验证管线打通。
4. 接订阅推导数据相关度 → mission-aware。
5. 搭实验脚本采四项指标，跑对照。
