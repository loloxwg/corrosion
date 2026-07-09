# 适配度评分表设计 —— RL 模型驱动推送目标(4.3.3)

把训练好的 GNN+DRL **适配度评分函数** `score(数据, 平台)` 真正用起来:不做动态 placement
(会踩数据洞 bug,见技术报告 §4.4/§6),而是**驱动 selector 的推送目标选择**(瞬态层,安全)。
这才是合同 4.3.3 措辞"智能决策推送的目标平台/路径"的正确落点。

## 0. 一句话

**离线**用训练好的 GNN 算一张 `(表 → 平台 → 适配度分)` 静态表 → 下发 → **selector** 在
interest 合法集内用「适配度分 + 实时链路」选推送目标。**不碰 interest(谁持有),不跑周期任务,
不在 Rust 里跑模型。**

## 1. 核心边界(三个不混)

| 概念 | 是什么 | 谁定 | 变不变 |
|---|---|---|---|
| **interest(行存在)** | 谁持有/存该表 = durability = 合法集 | 配置(任务模版) | **静态,跑中绝不变**(变=数据洞 bug) |
| **适配度分(score)** | 该表推给该平台的优先级 = 瞬态偏好 | 离线 GNN 算 | 静态(任务/拓扑变才重算,事件驱动) |
| **链路质量(ring/rtt_var)** | 该平台此刻链路好坏 | members 实时 | 动态,**selector 现场加,不进表** |

**适配度分只在"已经关心(持有)的节点"里重排推送顺序,不改谁持有 → 不踩数据洞。**
重算适配度分也安全(不动 interest);只有 interest 本身变(任务重规划)才需安全协议,那是独立立项。

## 2. 存储:给 node_interest 加一列 score(不新建表)

`node_interest` 已是 `(actor_id, table_name, active)` 的 CRR 复制表。加一列:

```sql
CREATE TABLE node_interest (
    actor_id   BLOB NOT NULL,
    table_name TEXT NOT NULL,
    active     INTEGER NOT NULL DEFAULT 1,
    score      REAL NOT NULL DEFAULT 1.0,   -- 新增:推送适配度[0,1],1.0=默认(等价现状)
    PRIMARY KEY (actor_id, table_name)
);
```

- **行存在** = 该节点关心/持有该表 = 合法集(不变,durability)。
- **score** = 推送优先级(离线 GNN 算,静态)。默认 1.0 → 不配 score 时行为等价现状。
- 复用现成机制:各节点启动**自写**自己的 `(表, score)`(`run_root.rs::write_own_interest` 扩展),
  经 crsqlite 复制到全集群 → 发送方读得到所有 peer 的 score 来排序。**无新表、无新复制路径。**

## 3. 离线计算(Python,训练好的 GNN)

```
1. 构任务态势 Situation(角色/needer/critical/write_vol/query_vol)——**链路特征置中性/均值**
   (link_cost 设常数、link_var=0),使输出只反映【任务适配度】,不含链路(链路 selector 现场加)。
2. 训练好的 GNN → score(table, platform) 原始分。
3. 每张表内归一化到 [0,1](softmax 或 min-max)。
4. 生成每节点的 (table, score) → 写进该节点 config 的 interest 段。
```

产物:每节点一份 `interest = [{table, score}]`(或 config 里 `[[gossip.interest_score]]`),
部署时随配置下发。**算一次。**

**为什么中性化链路**:GNN 训练时把 link_cost/link_var 当输入特征,输出里混了链路。要做到
"表静态、算一次",必须把链路从表里剥掉(置中性),否则链路一变就要重算表 → 退回周期任务。
链路的动态部分由 selector 用实时 `ring/rtt_var` 承担(职责单一)。

## 4. selector 使用(rl_score 从"手设方差"→"表 + 实时链路")

现状 `rl_score = 基础分 − 方差惩罚`(方差手设常数)。改为:

```rust
fn rl_score(c: &Candidate, affinity: f64, rng) -> f64 {
    // affinity: 该 (表, 候选节点) 的离线适配度分[0,1](从 node_interest.score 读)
    // 链路项 + 方差惩罚:members 实时信号(动态,现场加)
    let link  = 1.0 / (1.0 + c.ring.unwrap_or(UNKNOWN_RING) as f64);
    let var_p = c.rtt_var.map(|v| VARIANCE_WEIGHT * (v/(1.0+v))).unwrap_or(0.0);
    let jitter = rng.gen::<f64>() * JITTER_WEIGHT;
    AFFINITY_WEIGHT * affinity + link - var_p + jitter
}
```

- **affinity(表,节点)**:离线 GNN 学到的任务适配度(critical/SLA + 使用强度),从表查。
- **link − var_p**:此刻链路好坏,members 实时。
- 在 interest 合法集内按此打分取 Top-K 推送目标。**这就是"训练的适配度模型真正决策推送目标"。**

Candidate 需带上该表的 affinity:构造时从 `interest_routing`/`node_interest` 带出 score
(现 interest_routing 是 `{表→[addr]}`,扩为 `{表→[(addr, score)]}`)。

## 5. 重算(事件驱动,非周期)

| 事件 | 重算适配度表? | 碰 interest? | 安全 |
|---|---|---|---|
| 链路质量变 | ❌(链路不进表) | 否 | selector 实时承担 |
| 节点上下线 | 新节点自写自己的 (表,score) | 否(自声明,全新 bootstrap) | ✅ |
| 任务重规划/数据模版变 | ✅ 重算 | ✅ interest 变 | ❌ 需安全协议(独立立项) |

静态任务(合同考核)下:**部署算一次,跑中不重算。**

## 6. 诚实边界

- **价值大概率仍薄**(技术报告 §4.4 三处吸收:字节被 cardinality、延迟被 resolve、选路被 ring 吸收)。
  但这是**合同要的适配度评分模型、放在对的层(推送目标)、由真训练的评分驱动**——比现手设方差蒸馏强,
  作为攻关成果更站得住。
- **模型不进 Rust**:只把模型**输出**(适配度分)当数据下发,Rust selector 查表,不跑推理、不轮询。
- **链路剥离是关键**:表只表达任务适配度(静态),链路交 selector 实时——这一步让"算一次"成立、
  消除周期任务。若照搬现 GNN 混链路的输出,则退回"跟链路变而重算"。
- **不碰正确性**:score 只影响 interest 合法集内的推送顺序,推错自愈(anti-entropy 兜底),
  不改 placement/不碰 durability。

## 7. 落地步骤(建议)

1. `node_interest` 加 `score` 列(schema + `write_own_interest` 自写 score,默认 1.0 向后兼容)。
2. `interest_routing` 缓存扩为携带 score;`Candidate` 构造时带出 (表, 该节点 score)。
3. `rl_score` 改用 affinity + 实时链路(§4)。
4. 离线脚本(`research/rl/`):训练 GNN → 中性链路跑 → 归一化 → 生成每节点 interest_score config。
5. e2e:对照 `Rl`(affinity 驱动)vs `scored_reduce`,验证 critical/高使用平台被优先推送。
