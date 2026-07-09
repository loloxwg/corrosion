# corrosion 主动推送 —— 端到端数据流时序 + AI(RL)挂载位置

研究分支 `research/active-push` 的完整数据流图解。对接四项考核:
降量(4.4.3)/ QPS(4.4.2)/ 查询路由(4.4.2)/ 智能推送 placement(4.3.3)。

---

## 0. 背景:三个常驻循环(一直在转)

```
┌─ SWIM/gossip(foca)─────── 每节点互探活,谁在线/谁挂了
│     └─▶ 测 RTT ──▶ members.add_rtt ──▶ 算 ring(0=最近…5=最远)
│
├─ InterestRefresh ──────── 每 3 秒重读 node_interest 表
│     └─▶ 重建 interest_routing:{表 → 关心它的节点}  ← 推送/路由都查它
│
└─ Metrics ──────────────── 每 10 秒抓指标
```

代码:`broadcast/mod.rs:527`(interest_interval=3s)、`members.rs:171`(add_rtt)、
`members.rs:184`(recalculate_rings,RING_BUCKETS=[0..6,6..15,15..50,50..100,100..200,200..300]ms)。

---

## 1-3. 写入 → 广播 → 对账(数据怎么传开)

```
 Client      NodeA(写入方)        NodeB(关心该表)      NodeC(不关心)
   │              │                     │                   │
   │─写 target──▶│                     │                   │
   │              │ ① SQLite 落盘        │                   │
   │              │   cr-sqlite 产 CRDT change              │
   │              │                     │                   │
   │              │═══ 广播轴(eager push,秒级)═══         │
   │              │ ② selector 选目标:                     │
   │              │   查 interest_routing["target"]         │
   │              │   scored_reduce→只发关心者              │
   │              │──③ 推 change ──────▶│                   │
   │              │                     │ ④ 应用+可能 rebroadcast
   │              │      ✗ 不推给 NodeC(不关心→省流量)     │
   │              │                     │                   │
   │              │═══ 对账轴(anti-entropy,周期,补漏)═══ │
   │              │◀─⑤ SyncStart(带 interest)──────────────│
   │              │   generate_sync:按版本区间拉缺的       │
   │              │   handle_need:interest 过滤,非关心表不发│
   │              │──⑥ 补 NodeB 漏收的版本─▶│               │
   │              │      ✗ NodeC 不关心 target→对账也不给它 │
```

关键张力:**广播省下的量,对账不能又补回来** → 两轴都按 interest 过滤,才真降传输(4.4.3)。
代码:`broadcast/selector.rs`(选目标)、`api/peer/mod.rs`(handle_need interest 过滤)。

---

## 4. 查询:本地命中 vs 路由

```
 —— 情形A:查自己关心/持有的表(本地命中,快)——
 Client      NodeB(持有 target)
   │              │
   │─查 target──▶│ SQLite 本地执行(≈125ns)
   │◀── 结果 ─────│  ← 1M QPS 靠这个(部分复制让高频查询本地化)


 —— 情形B:查自己没存的表(路由,4.4.2)——
 Client   NodeC(没存target)      Router              最优holder      次优holder
   │          │              (resolve_table_holder)  (RTT好)         (RTT差)
   │─查target▶│                     │                   │               │
   │          │ my_interest 无 target                   │               │
   │          │──① 谁持有 target?──▶│                   │               │
   │          │                     │ ② 查 node_interest 得所有holder   │
   │          │                     │   ★min_by_key(ring)选RTT最优 ←改这│
   │          │                     │   (改前:选SQL第一个,不看RTT)    │
   │          │◀─③ 最优holder地址───│                   │               │
   │          │──④ open_bi 转发查询 QueryForward ───────▶│               │
   │          │                     │           ⑤ bi.rs 本地执行         │
   │          │◀──────────── ⑥ 流式回传 QueryEvent ──────│               │
   │◀─结果────│                                          ✗ 次优不选(RTT差)
```

代码:`api/public/mod.rs:420`(resolve_table_holder,改后 min_by_key(ring))、
`api/public/mod.rs:809`(空 interest=隐式全量→不路由)、`agent/bi.rs`(持有者侧执行)。

---

## 5. AI(RL 模型)挂在哪:决定"谁存哪张表"

AI 不在数据流的"管道"里,它在**上游决定那张路由表**(被广播/对账/查询三处都读的 interest_routing)。

```
   ┌──────────────────────────────────────────────┐
   │   🧠 RL/AI 模型(态势数据智能推送,4.3.3)      │
   │                                                │
   │   输入:任务角色(侦察/打击/干扰)             │
   │        + 链路质量/方差 + 读写量 + 谁需要        │
   │   输出:placement = 每张表该放哪些节点          │
   │   (GNN 二部图 + DRL 微调学"适配度评分函数")   │
   └───────────────────┬────────────────────────────┘
                        │ 周期写入(路径B:在 InterestRefresh 3s tick 里算)
                        ▼
              ┌─────────────────────────┐
              │  node_interest 表        │  ← AI 的输出落在这
              │  interest_routing        │     {表 → 关心/持有它的节点}
              └───────┬──────────┬───────┘
                      │          │          │
        ┌─────────────┘   ┌──────┘   └───────────┐
        ▼                 ▼                       ▼
  ① 广播 selector    ② 对账过滤           ③ 查询 resolve
   "推给谁"           "同步给谁"          "本地没有→查谁拿"
   (阶段2)            (阶段3)             (阶段4-B)
```

**一句话:AI 决定"数据摆哪"(placement),这三处执行都读它。AI 不碰热路径,只每 3 秒决定一次摆放。**

### AI 想优化啥
- **省流量(4.4.3)**:高写低查的数据(遥测)少放几个节点 → 广播/对账都省。
- **保延迟(4.3.3)**:高查的数据(目标)放在低方差好链路节点 → 查询路由过去快。
- **守约束**:critical 节点必须本地有(硬 SLA),不能为省而砍。

### 诚实现状(2026-07-06 逐层测明白)
- AI **能跑、能达标**(placement 驱动真推送 ↓94.8% vs 广播)。
- **省流量**部分靠"少放副本"就够(random 砍一样省)→ AI 字节智能 ≈ 0。
- **保延迟**部分在集群里**被查询路由吸收**:resolve 改成选 RTT 最优 holder 后,
  critical(必存)里通常已有好 holder,AI 精挑的非 critical 副本用不上。
- AI 干净显价值只在"单 holder 无路由选择"的受控场景(rl_e2e_latency.py 13×)。
- **副产品**:为解锁 AI 而做的 resolve 改进(选 RTT 最优 holder)本身是独立于 AI 的
  真生产改进(异构网络查询延迟更低)。
- 合同 4.3.3 落点=**攻关报告级评分函数**,不要求 AI 进生产 → 满足。

详见 `DELIVERY.md` §4.3.3、技术报告 §4.3。

---

## 6. 为什么 AI 的"聪明"在集群里显不出来(两幕时序)

RL 的智能 = 把数据放在快链路节点。但查询路由怎么选 holder,决定了这份聪明有没有用。

### 改前:RL 被"无视"

```
 消费者C          路由R              Fast holder      Slow holder
(查target)     (resolve)         (RL放·快链路)   (critical·慢链路)
   │                                    ▲
   │            RL 把副本放在快节点 F ───┘  ← RL 的"智能"
   │
   │──① 查 target ──▶│
   │                 │ ② 选"列表第一个"holder(SQL顺序,不看快慢)
   │                 │──③ 恰好选中 Slow ─────────────────▶│
   │                 │                                     │ ④ 本地查(慢)
   │◀──────────────── ⑤ 慢响应 ⏱️207ms ───────────────────│
   │
   ✗ RL 放的 Fast 没被选中 → 聪明被无视
```

### 改后:RL 被"吸收"

```
 消费者C          路由R(改后)        critical holder    RL额外副本
(查target)     (按RTT选最优)      (规则必存·快链路)   (RL精挑·也快)
   │                                    ▲                  ▲
   │       规则:critical必存,RL/随机都含它┘                  │RL额外放的
   │
   │──① 查 target ──▶│
   │                 │ ② 比较所有holder的RTT(ring),选最快
   │                 │──③ 选中critical里最快的──▶│
   │                 │                            │ ④ 本地查(快)
   │◀──────────────── ⑤ 快响应 ⏱️1ms ────────────│
   │
   🔁 critical里已有好holder → RL额外副本用不上 → 被吸收
```

### 两头堵

```
              路由怎么选 holder?
        ┌────────────┴────────────┐
   选"第一个"(改前)          选"最快"(改后)
        │                          │
   不看RL放哪                  路由自己找到快的
        │                          │
   RL的Fast没被选            但critical里就有快的
        │                          │
   ❌ RL被【无视】            ❌ RL被【吸收】
        └────────── 都轮不到 RL 加分 ──────────┘

  副产品 ✅:改前→改后那步(选第一个→选最快)本身让所有查询更快,
           独立于 RL = 白赚的真改进。
```

**证据**:合成对调探针(`rl_placement_e2e.py::probe_resolve_behavior`)——target 放
{快20ms+慢220ms},对调快慢角色后 `{both}` 恒命中「快」holder(组A/组B 都 23ms);
改前恒命中固定 node1(与快慢无关)。结论翻转 → resolve 确实按 RTT 选最优。

---

## 7. GNN 与 RL 的关系:运行时跑 GNN,RL 是离线训练方法

评审常问"你这 RL 到底在哪跑"——分清楚:**GNN 是模型(结构),RL 是训练方法(怎么学会)。**

```
GNN = 图神经网络 = 模型架构(算 score(数据,平台) 的网络形状)
RL  = 强化学习   = 训练方法(靠奖励优化权重,不需标准答案)
"GNN+DRL" = 一个 GNN 架构,权重用【监督预训练 + RL 微调】学出来
```

### 时序:训练(离线一次) vs 运行(agent 里活着)

```
—— 训练阶段(离线,Python,一次性)——
  ① GNN 架构(二部图消息传递)
  ② 监督预训练:用 greedy 标签教(BCE loss,模仿启发式)   ← 不是 RL,是监督
  ③ RL 微调:REINFORCE 用成本当奖励优化                  ← 这才是 RL/DRL
        └─▶ 定权重 → export_weights.py 导出 gnn_weights.json

—— 运行阶段(每个 corrosion 节点,活着)——
  节点启动 ─加载权重─▶ graphrl.rs
     每 3s(InterestRefresh tick):
        构态势(interest needer图 + members 实时链路)
           └─▶ GNN 前向推理(matmul+relu+消息传递)   ← 只跑 GNN,不学习
                 └─▶ score(表,平台) → selector 选推送目标
```

### 关键区分(答辩要点)

| | GNN | RL |
|---|---|---|
| 是什么 | 模型架构 | 训练方法 |
| 何时 | **运行时(agent 里活着跑推理)** | **离线训练时(一次,定权重)** |
| 在 corrosion 里 | ✅ 每节点跑前向 | ❌ 不跑(权重已固化) |
| 需要 | 训练好的权重 | 奖励信号(成本模型) |

**所以:部署/演示里活着跑的是 GNN 前向推理;RL 是它"当初怎么学会"的方法,运行时不发生。**
真要运行时也学(在线 RL,拿真实流量当奖励在线更新)= 需真实 reward = 半实物,列后续。

**诚实**:RL 微调优化的成本模型是合成的(没飞起来无真 reward)→ 学出的评分未校准
(GNN 输出塌缩到 critical,见技术报告 §4.4);真校准待半实物真流量。
