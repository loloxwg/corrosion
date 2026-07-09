# 交付索引 —— 任务驱动智能数据主动推送

研究载体:[superfly/corrosion](https://github.com/superfly/corrosion) 研究 fork,分支 `research/active-push`。
本文件把**四项考核指标**映射到**证据(脚本/报告/图)与复现命令**。诚实边界见各项末与技术报告。

> 总览先读:[`active-push-technical-report.md`](active-push-technical-report.md)(★ 综述问题/失败教训/最终方案/实测/诚实边界)。
> 合同原文:[`active-push-plan.md`](active-push-plan.md) §0.1。

## 复现前置

```bash
cargo build -p corrosion            # 构建 agent 二进制(target/debug/corrosion)
# harness 依赖:python3、sqlite3、ab(ApacheBench)、matplotlib(仅 sweep 出图)
```

---

## 4.4.3 全局数据传输降 30%  —— ✅ localhost 54~77%(42 节点 54%,外推 100 节点≈44%,均>30%)

| 证据 | 命令 | 验什么 |
|---|---|---|
| 降量多规模均值 | `python3 research/harness/sweep.py --sizes 6 12 18 24 --rows 20 --repeats 3` | 总传输降幅(广播式 vs 智能),多规模×重复取均值+误差带 |
| 部分复制 + 无死锁 | `SKIP_WRITE_INTEREST=1 python3 research/harness/verify_phase2.py --nodes 9` | 关心表本地收齐、非关心表**业务表+buffered 双层都=0**(直读 db 绕路由)、gap/needed 不增长 |
| 泄漏定位(推送 vs 对账) | `python3 research/harness/debug_leak.py 6` | 非关心节点 battlefield 本地=0,确认两轴都按 interest 过滤 |
| 多表事务残留边界 | `python3 research/harness/multitable_test.py` | 单事务多表=版本级边界(行级精度未做,诚实) |

**机制**:interest 模型(`node_interest` 复制表)+ 推送按表分组 + 对账 `handle_need` 版本级过滤(四个 serving 分支统一)。
**诚实**:测量须**直读本地 db(`count_rows_local`)绕开查询路由**,否则路由会把本地裁剪掩盖成"到处都有"(假 FAIL)。

## 4.4.2 任意节点查任意数据 + 1M QPS  —— 查询路由 ✅ correctness;1M=单节点微基准外推(未半实物验证)

| 证据 | 命令 | 验什么 |
|---|---|---|
| 查询路由 correctness | `python3 research/harness/query_routing_test.py` | 本地有则本地答、没有则路由到持有者拿回正确结果 |
| QPS 基准 + 外推 | `python3 research/harness/qps_bench.py --nodes 1` | 单节点本地点查 ≥25K QPS、100 节点外推 2.5M、500Kbps 路由上限 442K |

**论证**(技术报告 §3.1):1M 必须靠本地命中(部分复制让高频查询本地化);500Kbps 跨节点路由 ≈89 QPS/link,
路由聚合上限 <1M → 本地命中是数学必然。**诚实**:无 100 节点半实物;单机数是下界;聚合 ×100 是外推。

## 4.2.3 数据需求模版 + gossip 多跳 + 智能推送模型  —— ✅

| 证据 | 命令/位置 | 验什么 |
|---|---|---|
| 数据需求模版(节点自描述) | `node_interest` 复制表;corrosion 启动从 `gossip.interest` **自写**(`run_root.rs::write_own_interest`) | 每节点自声明关心的表,经 crsqlite 复制到全集群 |
| interest wildcard | `python3 research/harness/wildcard_test.py` | `interest=["*"]`=全量节点;与精确表名两语义;不破坏共存节点的部分复制 |
| gossip 多跳 | 保留 corrosion 原生 SWIM + gossip(容链路通断) | interest/部分复制/查询路由叠加其上 |
| 智能推送模型 | 见 4.3.3 | GNN+DRL 适配度评分 |

## 4.3.3 GNN+DRL 适配度评分函数  —— ✅ 核心验证

| 证据 | 命令/位置 | 验什么 |
|---|---|---|
| 训练 + 评估 | `cd research/rl && pip install -r requirements.txt && python3 evaluate.py` | 监督 GNN 省 65%(达 greedy);RL 加方差风险省 rule 69%/greedy 4% |
| 证据图 | `research/rl/fig_score_heatmap.png` / `fig_variance_aware.png` / `fig_ablation.png` | 评分函数 / RL 避高方差链路 / 归因消融 |
| **端到端 sim-to-real** | `python3 research/harness/rl_e2e_latency.py` | 真 RL 模型选低方差 holder(var 0.11 vs greedy 1.69)→ 真 corrosion 路由查询 P50 **16ms vs 207ms** |
| **集群 placement 真跑** | `python3 research/harness/rl_placement_e2e.py --nodes 9 --repeats 3` | RL placement 驱动真 selector 推送:达标 ↓94.8% vs 广播;含 random_cut 同副本预算基线 + resolve 合成对照 |

**RL 优势分两部分**:① 基数(高写少存→字节)= **已由 4.4.3 降量验证**;② 方差规避(查询密集放稳定 holder→延迟)= `rl_e2e_latency.py` 验证。
**诚实**:字节量上显示不出②(corrosion anti-entropy/多路径太鲁棒,实测丢包反更省字节),故改测**查询路由延迟**;链路注入用应用层(`transport.rs` `CORRO_LINK_FAULTS`,无 sudo),QUIC 重传级高保真留 dummynet 附录。

**★集群级归因(2026-07-06,`rl_placement_e2e.py` + Codex 对抗复核,诚实负结果)**:把 RL placement 接进真集群(RL 算 placement→每节点 interest→selector 推送,RL 不进热路径),逐层证死 RL 的"智能"在**当前 corrosion 集群**里显不出来:
- **字节**:RL vs 同副本预算随机砍(random_cut)推送字节 **↓-0.1%**(RL 甚至略差)→ 降量为 **cardinality 主导(副本数)**,非 RL 读写权衡(当前单表写入/无丢包限速条件下)。
- **延迟**:`resolve_table_holder`(`api/public/mod.rs:420`,SQL 无 ORDER BY)按顺序选**首个** holder,**不认方差**。合成对照证死:target 放 {快+慢} 两 holder,对调快慢角色后 `{both}` 恒命中同一节点(node1)的延迟 → RL 挑的低方差非 critical holder 路由永远选不到;target 的 critical holder 硬约束必含且可能高方差。
- **附带发现**:`api/public/mod.rs:809` **空 interest = 隐式全量节点**(本地答不路由 + `interest_for_sync`=None 全量对账),无法表达"只查不存"的 route-only 节点;稀疏 placement 让需要者空手即触发。harness 用哨兵表(`__route_only__` 恒空)workaround 证明并绕过(路由 P50 1ms→179ms 恢复)。
- **结论**:RL 达标合同(placement 驱动真推送 ↓94.8%),但集群级智能被两架构点吸收(推送只认副本数、路由不认方差)。合同 4.3.3 落点为**攻关报告级评分函数**,不要求 RL 进生产推送路径,故此负结果不影响达标。

**★resolve 方差感知改造 + RL 价值双向证伪(2026-07-06,生产改动)**:把 `resolve_table_holder`(`api/public/mod.rs`)从"选 SQL 顺序首个 holder"改为"选 **RTT(ring)最优** holder"(`min_by_key(ring)`,未知 RTT 排最后)——这是独立于 RL 的**正确生产改进**(异构网络路由到最近 holder,查询延迟更低)。配套研究开关:`transport.rs` 把 `CORRO_LINK_FAULTS` 注入延迟叠加进上报 RTT,使 ring 看得见合成故障(默认空 map 无影响)。
- **改造生效证据**(合成对调探针,`rl_placement_e2e.py::probe_resolve_behavior`):target 放 {快 20ms + 慢 220ms} 两 holder,**对调快慢角色后 `{both}` 恒命中「快」holder(组A/组B 都 23ms)**——改前恒命中固定 node1(与快慢无关),改后恒命中快的(与节点顺序无关),同一实验结论翻转 → resolve 确实按 RTT 选最优。
- **★RL 价值被吸收(Codex #2d 预言证实)**:改后 RL vs random_cut 全集群查询延迟仍**平手(都 1ms)**。根因:两策略**共享 target 的 critical holder(硬约束必含)**,resolve 选其中 RTT 最优的那个 → 选到同一个好 holder,RL 精挑的非 critical holder 无用武之地。**RL 价值两头堵:改前被路由挡(选不到 RL 的好 holder)、改后被路由吸收(路由自己找到好 holder)。** RL 干净显价值只剩"唯一 holder 无路由选择"的受控场景(`rl_e2e_latency.py` 13×)。
- **回归**:`query_routing_test.py` 正确性 PASS;`cargo test -p corro-agent --lib` 43 passed(`test_lagging_subscribers` 已知 flaky,隔离通过)。resolve 改进值得留(生产查询延迟收益),RL 集群解锁作诚实负结果记录。

**★RL 归位:瞬态选路层(BroadcastStrategy::Rl)+ 动态 placement 正确性 bug(2026-07-06)**:厘清 RL 的**安全落点**——不是动态改 placement(谁长期持有),而是在 interest 圈定的合法集内优化"这条广播发给谁/扇出/路径"(瞬态层,推错自愈不丢数据)。
- **动态 placement 的正确性 bug(实测坐实,`dynamic_interest_hole.py`)**:节点先不关心某表→对账把该表版本发 `Changeset::Empty`→标 `KnownDbVersion::Cleared` 关 gap;之后动态"重新关心"→`generate_sync` 不再请求(Cleared 与"真有数据"不区分,bookie.rs:211)→**永久数据洞**。实测:NodeR 重新关心 target 后本地仍 0/15,对照 NodeS 15/15(数据在集群有源)。更危险:NodeR 以 holder 身份对查询本地答 0(假 holder)。Codex 复核确认真 bug + 推送(node_interest 动态)/对账(gossip.interest 静态)读**两套 interest 状态**。**故 RL 不可动态改 placement**;静态 interest(所有考核用法)安全。
- **RL 归位实现(selector.rs)**:`BroadcastStrategy::Rl` 从占位回退→真实现:合法集先由 `interest_pool`(同 scored_reduce 的正确性/降量边界)圈定,**RL 仅在集内**用方差感知打分(`rl_score`=基础分−方差惩罚,`VARIANCE_WEIGHT=0.8`,偏好低 RTT 方差=稳定链路做广播目标,不稳链路留 anti-entropy 兜底)。方差信号来自 `members.rtts` 最近 20 样本(`Rtt::variance()`)。**安全:只在合法集内重排,不改 placement/不碰 durability,推错自愈。** 权重可离线 RL 学习/蒸馏,现手设为 RL 学到的方向(sim-to-real 已证方差感知降查询延迟)。
- **回归**:selector 单测 11 passed(新增 rl_prefers_low_variance / rl_stays_within_interest_set / rl_falls_back_to_full_without_interest);`cargo test -p corro-agent --lib` 46 passed(flaky 隔离通过)。
- **端到端实测**(`rl_selector_e2e.py`,8 节点全关心 flight,注入 jitter 产 RTT 方差):埋点 `corro.broadcast.target.rttvar_milli`(所选目标平均方差,按策略)。scored 961 vs **rl 914 ms²(↓5%)**,方向对但边际薄。**★真根因(与查询侧 resolve 吸收同构)**:corrosion 的 RTT-ring 机制已吃掉大部分链路信号——注入 jitter 同时抬高均值 RTT→不稳 peer 落 ring4-5 非 ring0,ring0-flood 总发低 RTT peer→高方差⟹高均值⟹高 ring⟹已被现有机制降优先级;Rl 的"纯方差"信号(同均值不同方差)在 ring0 桶空间极小。机制present(方向对+单测证逻辑),价值被 ring 吸收。
- **对合同**:RL 决策"这次推给哪平台/走哪路径"=瞬态选路(安全落点),"谁长期持有"=interest 规则(静态,durability 层)。RL 是攻关报告级策略层验证,不碰正确性。诚实:Rl 瞬态价值薄(被 ring 吸收),但安全+机制可证。

**★内嵌 GNN 活模型(2026-07-09):训练好的深度图强化学习模型在真 corrosion 里活着决策推送目标。** 为评审"看得见的智能",把 GNN 从离线 Python 产物做成 agent 里的**活组件**:
- **权重导出→Rust 手写前向→数值对齐**:`research/rl/export_weights.py` 导出 GNN 权重+参考态势为 JSON;`crates/corro-agent/src/broadcast/graphrl.rs` 手写前向(matmul+relu+二部图消息传递,无 torch/ONNX);单测 `parity_matches_python` 保证 Rust 前向 == PyTorch(误差<1e-3)。
- **活模型接线**:agent 启动加载权重(`GossipConfig.graphrl` 配置:weights_path + 数据属性 + 角色映射 + critical);InterestRefresh tick(3s)周期跑推理,用当前态势(interest=needer图 + members实时链路,归一化到训练尺度)算 `score(表,平台)` 适配度评分表;selector 的 `BroadcastStrategy::Rl` 用 GNN 分给推送目标打分(`GNN_AFFINITY_WEIGHT=3.0` 压过链路项)。
- **端到端演示**(`research/harness/graphrl_live_demo.py`,6 节点侦查/打击/干扰):实测 **6/6 节点加载 GNN + 6/6 跑推理 + 现算决策推送目标**(日志 `graphrl 决策: target 最优推送目标 → node (适配度 1.00)`),随态势 3s 刷新。**每个无人机节点里跑着 GNN,实时决策数据推送目标 = 评审要的活智能体。**
- **安全**:活模型只在 interest 合法集内选推送目标(瞬态层),不改 placement/不碰 durability,推错自愈。空态势/加载失败优雅退化(Rl 回退启发式)。
- **诚实边界**:① 评分尺度为名义映射(RTT→训练尺度),真校准待半实物真流量;② 模型输出仍塌缩到 critical(target 1.0/其余≈0,与 §4.4 一致);③ 价值薄(三处吸收),但**机制活、可演、安全、合同对口**(4.3.3"智能决策推送目标平台/路径")。回归:selector+graphrl 单测 14 passed;lib 47 passed(flaky 隔离通过)。

---

## 文档与工具

- **设计文档**:[1b-design](active-push-1b-design.md)(对账过滤)、[route-a-design](active-push-route-a-design.md)(单进程裁剪)、
  [query-routing-design](active-push-query-routing-design.md)(4.4.2)、[rl-design](active-push-rl-design.md)(4.3.3)、
  [cluster-design](active-push-cluster-design.md)(圈子方案,因"一进程"约束未采用,存档)。
- **harness 基础**:`research/harness/run.py`(mission 拓扑 + 配置生成 + 度量)。
- **教学原型**:`sandbox/gossipdb/`(200 行 HLC/CRDT/SWIM/对账,吃透 corrosion 用,非生产)。

## 未尽事项(诚实)

① 100 节点半实物真聚合(本期单节点实测 + ×100 外推);② 高并发缓存/连接池调优;
③ RL 链路优势的注入延迟端到端验证;④ 单事务多表的行级精度(scoped bookie,高风险);⑤ interest 动态变更/历史回填。
