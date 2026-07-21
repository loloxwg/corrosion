# 从 Interest-aware 主动复制到任务驱动语义复制

> 状态：未来演进方向，不代表当前全部实现。
>
> 当前基线仍是按表 interest 的主动部分复制；本文件明确区分现状、近期可落地能力和中长期研究能力。

## 1. 当前已经实现：Interest-aware 主动部分复制

当前系统应准确描述为：**以表级 interest 为 placement 边界、以主动广播降低到达时延、以
anti-entropy 保证最终补洞的部分复制框架**。

### 1.1 七阶段数据流

```text
SQLite 事务提交
  → ① crsql_changes 记录事务版本
  → ② 生成 Changeset::FullV2 并提取涉及表
  → ③ node_interest(active=1) 圈定合法 holder
  → ④ Broadcast Selector 选择本次优先目标
  → ⑤ QUIC 主动推送 Changeset
  → ⑥ 接收节点 epidemic rebroadcast 多跳传播
  → ⑦ Anti-Entropy 按 interest 修复遗漏版本
```

其中，严格按 interest 缩小快速推送集合的是 `scored_reduce` 和 `rl`；`random` 是随机广播基线，
`scored` 主要做排序而不保证排除无关节点。控制表 `node_interest` 恒不被 interest 过滤。

### 1.2 控制面与数据面边界

- **placement 控制面**：`interest` 决定谁长期保存哪些表，`active` 表示 holder 是否完成历史回填；
  `handoff`、`interest_epoch`、`interest_min_replicas` 保护 placement 变更。
- **主动推送数据面**：selector 只在合法 holder 集合内决定本次先发给谁。RL/GNN 当前使用适配度、
  interest relevance、RTT ring、RTT 方差和探索抖动，不自行改变 placement。
- **修复面**：主动推送允许遗漏；anti-entropy 按版本补洞。事务只要触及任意感兴趣表，就发送完整
  db version，保持现有事务原子边界。
- **查询面**：本地不是 ready holder 时可把查询路由到 active holder；查询路由不改变 Changeset
  的复制规则。

### 1.3 当前尚未实现

- 任务或本体自动生成 `gossip.interest`。
- 进程运行期间通过 API/Consul watch 热更新 interest。
- RL/GNN 自动改变长期 placement。
- 按 `mission_id`、区域、目标类别、优先级或任意语义谓词过滤行。
- Corrosion 内置的无人机任务执行器、执行权仲裁或 exactly-once 外部动作。

因此当前可以称为 **Interest-aware Replication**，但不能把后续章节描述成已经完成的
**Task-driven Semantic Replication**。

## 2. 演进目标：任务语义编译为确定性复制计划

目标不是让大模型在每条 Changeset 的热路径中判断“该发给谁”，而是让任务语义在控制面生成一个
可审计、可复现、可回滚的 `InterestPlan`，再复用现有 Corrosion 复制执行层：

```text
Mission / Task
  → Task Graph + Ontology/规则
  → InterestPlan
  → placement controller 串行执行
  → node_interest(active/readiness)
  → 现有主动推送 + anti-entropy + 查询路由
```

建议的计划对象：

```yaml
plan_id: reconnaissance-sector-a-v3
mission_id: M-2026-0715-01
node: UAV-07
tables: [target, flight, sensor]
critical_tables: [target, mission]
min_replicas: 2
interest_epoch: 12
source_revision: ontology-42
```

本体、规则或模型负责**生成建议**；控制器负责校验覆盖约束并产生确定性计划；Corrosion 只负责
安全执行计划。在线大模型不进入逐事务复制路径。

## 3. 近期阶段 A：任务到表级 InterestPlan

这是最小且最推荐的第一步，底层复制协议无需增加语义过滤。

### 3.1 输入与输出

```text
侦察任务 → target、flight、sensor
打击任务 → target、battlefield、weapon_status
后勤任务 → supply、vehicle、maintenance
```

编译结果仍然是现有配置：

```toml
[gossip]
interest = ["target", "flight", "sensor"]
critical_tables = ["target"]
interest_min_replicas = 2
interest_epoch = 12
```

### 3.2 执行纪律

placement controller 必须通过 Consul、etcd 或等价共识服务持有单一 lease，串行执行：

```text
计算新计划
  → 给新 holder 增加 interest 并递增 epoch
  → 等待历史回填和 active=1
  → 检查新 placement 达到 min_replicas
  → 摘除旧 holder 并递增其 epoch
  → 等待 node_interest 传播和 selector 缓存刷新
```

本地 `interest_epoch` 只能拒绝旧配置重放，不能阻止两个控制器基于同一旧视图并发摘除，因此外部
lease 是自动化 placement 的前置条件。

### 3.3 阶段 A 验收标准

- 同一任务和本体版本必须生成字节级稳定的 InterestPlan。
- 每个计划必须记录来源版本、plan hash、目标节点、epoch、审批者和回滚计划。
- 扩容节点在 `active=0` 时不得承接查询或成为主动推送合法目标。
- 摘除前必须满足在线 ready 副本数；控制器崩溃后能够从已记录阶段继续执行。
- 现有动态 handoff、旧 epoch fencing、100-agent 收敛和传输量回归不得退化。

## 4. 中期阶段 B：稳定分区键驱动的细粒度复制

当一张表同时混有多个任务、区域或安全域的数据时，表级 interest 会复制无关行。但下一步不应直接
支持任意语义谓词，而应先选择少量稳定分区键：

- `mission_id`
- `region_id`
- `tenant_id` 或安全域
- 必要时再增加稳定的 `entity_class`

优先约束业务事务尽量只写一个复制作用域，例如同一事务内保持相同 `mission_id`。如果一个 db version
混入多个作用域，只要任一作用域命中就仍发送完整版本；这是保留事务边界时必须接受的过量复制。

阶段 B 可选两种实现，按风险从低到高排序：

1. **物理分区或派生表**：把稳定作用域映射为独立复制表，继续复用现有表级 interest。实现简单，
   但会增加表和 schema 管理成本。
2. **版本作用域索引**：为 `(actor_id, db_version)` 保存确定性 scope 摘要，主动推送和 anti-entropy
   使用同一匹配函数。该索引必须与业务事务原子产生，并作为控制数据可靠传播，否则会出现推送与补洞
   口径不一致。

阶段 B 暂不承诺行级裁剪。若要在一个事务版本内只发送部分行，就必须重新设计 Changeset、bookie、
gap 闭合和 CR-SQLite 合并语义，风险显著高于版本级过滤。

## 5. 中长期阶段 C：本体和任务图驱动的语义计划

节点只声明任务和角色，本体推理得到所需概念，再编译成受限、确定性的机器计划：

```text
区域防空跟踪
  → 敌方空中目标、雷达航迹、身份识别、威胁等级、邻近友机
  → 表集合 + 稳定 scope 谓词 + critical 等级 + TTL
  → InterestPlan
```

这一阶段的语义系统仍是**控制面编译器**，不是数据面解释器。允许大模型处理非结构化任务描述，但其
输出必须经过 ontology/schema 校验、覆盖约束修复和人工或策略审批，最终下发确定性计划。

只有在阶段 B 的版本作用域索引、补洞一致性和泄漏验证成熟后，才考虑引入受限语义谓词。任意 SQL、
自然语言谓词或逐行大模型分类不进入复制热路径。

## 6. 无人机任务分发：持久任务通道与实时控制通道分离

任务驱动复制不等于用数据库替代全部指控链路。建议保留双通道：

| 通道 | 适合内容 | 语义 |
|---|---|---|
| Corrosion 主动复制 | 任务计划、航路点、目标清单、规则约束、执行状态和结果 | 持久、可补洞、可恢复、最终一致 |
| 点对点实时控制 | 急停、返航、临时转向、武器释放授权等安全关键指令 | 低时延、显式确认、独立安全链路 |

任务写入数据库并复制到 UAV 本地后，可以使用 Corrosion subscription/update 流通知本地任务执行器，
避免轮询。但复制幂等不等于执行副作用幂等，执行器至少需要：

- 全局唯一 `task_id` 和重复通知去重。
- 明确的 `pending → accepted → running → completed/failed/cancelled` 状态机。
- 执行权、lease 或唯一 owner，防止多个节点并发执行同一排他任务。
- 崩溃恢复点和可重放输入；外部动作采用幂等键或补偿动作。
- 对安全关键指令使用独立确认协议，不依赖最终一致数据库状态代替实时授权。

## 7. 正确性与失败边界

未来扩展必须保持以下不变量：

- 主动推送负责低时延，anti-entropy 负责最终补洞；二者必须使用同一 InterestPlan 和匹配语义。
- 新 holder 未完成历史回填前不得发布 ready；摘除旧 holder 前必须存在足够的在线 ready 副本。
- 网络分区期间允许 placement 视图短暂陈旧，但不得并发删除最后的有效副本。
- 计划重试必须幂等；旧 plan/epoch 不得覆盖新 placement。
- 模型故障、推理超时或 ontology 不可用时维持最后一个已验证计划，不在数据面猜测新 placement。
- 语义不命中导致的 `Changeset::Empty` 必须可追踪，后续扩大计划时能够重新打开被过滤版本范围。
- “最终可补齐”依赖仍有完整历史源、节点最终重连且同步持续运行，不表述为无条件 exactly-once 送达。

## 8. 验证路线与推进门禁

| 阶段 | 必须验证的核心问题 | 通过后才能进入下一阶段 |
|---|---|---|
| A：任务→表 | 计划确定性、串行 handoff、回填 readiness、旧 epoch fencing、回滚 | 任务变化能安全改变表级 placement |
| B：稳定 scope | push/sync 匹配同口径、混合事务行为、历史扩容、无数据泄漏 | 版本级细粒度复制在故障下仍能收敛 |
| C：本体计划 | 推理可解释、schema 校验、覆盖约束、错误计划隔离 | 语义系统不会直接破坏 durability |
| UAV 执行闭环 | 重复投递、断链恢复、双执行、取消竞态、安全指令确认 | 外部副作用可控且可审计 |

每阶段都应复用 100-agent 传输/收敛测试，并增加网络分区、控制器崩溃、旧计划重放、混合事务、历史源
丢失和任务重复执行故障注入。核心指标至少包括总传输量、收敛时延、未满足 gap、ready 副本数、
计划执行阶段、订阅积压及任务重复执行数。

## 9. 路线结论

- **现在**：Interest-aware、表级、版本级过滤的主动部分复制。
- **下一步**：实现任务/本体规则到表级 InterestPlan 的编译和单控制器安全编排。
- **随后**：在业务数据具备稳定 `mission_id/region_id` 后，研究版本级作用域索引。
- **远期**：把任务图与本体编译为受限语义计划，而不是让大模型进入复制热路径。
- **始终不变**：Corrosion 负责可靠传播与补洞；任务语义负责生成可审计的复制意图；实时安全控制保留
  独立点对点通道。

