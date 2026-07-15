# 动态 interest 历史回填协议

## Context

部分复制节点在不关心表 `T` 时，会从同步端收到覆盖相关 db version 的
`Changeset::Empty`，并把这些版本记为 `KnownDbVersion::Cleared`。旧实现随后把它们视为
“已完整拥有”，所以节点扩大 interest 后不会再次请求历史数据。回归场景中，NodeR 重新关心
`target` 后本地仍为 0/15，而持续关心的 NodeS 为 15/15。

硬不变量：

- 节点对外声明持有新表之前，必须能重新请求此前因过滤而跳过的历史版本。
- 新 holder 完成历史回填前必须保持 `node_interest.active=0`，查询和推送都不得把它当作 ready。
- 摘除 active holder 前，必须保留配置要求数量的其它在线 ready holder。
- 重启和重复同步必须幂等，不能重放全部已应用历史。
- 回填 bookkeeping 与 SQLite 持久状态必须原子提交；失败时节点不得继续启动并冒充完整 holder。

## Decision

新增内部表 `__corro_filtered_version_ranges`。节点在部分同步模式下收到完整
`Changeset::Empty` 时，将 actor 与版本范围和 `Cleared` 状态放在同一事务中持久化。

节点同时在 `__corro_state.sync_interest_v1` 保存上一次启动采用的有效同步 interest。下一次
启动发现 interest 扩大时：

1. 读取并合并此前记录的 filtered version ranges。
2. 在持有 bookie writer lock 的同一 SQLite 事务中，把这些范围重新加入
   `__corro_bookkeeping_gaps`。
3. 删除已消费的 filtered ranges，保存新的 interest 快照并提交事务。
4. 事务成功后才发布新的内存 bookie 状态，随后启动 sync loop。

`node_interest.active` 现在是 readiness 发布位：新增 interest 先写 `active=0`，后台只观察本次
重新打开的版本区间；这些 gap 和 partial 全部消失并连续确认两次后，才在广播事务中切为
`active=1`。本地查询同样检查该位，回填期间会路由到其它 ready holder。

摘除 interest 时，节点先等待相关候选 holder 进入 SWIM 在线成员视图，再在同一事务中统计
其它 `active=1` holder。精确表可由同表或 wildcard holder 覆盖；摘除 wildcard 只能由其它
wildcard 覆盖。数量低于 `gossip.interest_min_replicas`（默认 1）则启动失败并保留旧声明。

选择“记录被过滤范围”，而不是在每次扩张时重扫 `1..head`，避免重新请求和重放已经正常应用的
全部历史。版本仍由 CR-SQLite 幂等合并；无数据的真实空版本被重新请求一次也不会改变业务状态。

## Failure handling

- 迁移、状态解析、gap 写入或事务提交失败：agent 启动失败，不以不完整 holder 身份服务。
- 节点在事务提交前崩溃：旧 interest 快照和 filtered ranges 保留，下次启动会重试。
- 节点在事务提交后、sync 完成前崩溃：reopened gaps 已持久化，下次启动继续请求。
- 集群没有仍持有完整历史的 wildcard/源节点：gap 会保持未满足，节点不会静默把它当成完整历史。
- 在线 ready 副本不足：摘除事务回滚，旧 holder 声明保持 active，agent fail-closed。
- readiness/删除声明采用 CRDT 最终一致传播；控制器必须串行执行 placement 变更，摘除后还需等待
  声明传播和 selector 缓存刷新，不能把它当作跨节点线性一致事务。

## Validation

- 单测：interest 仅在集合扩大或切换为 wildcard 时判定扩张。
- 单测：reopened gaps 与已有 gaps 合并，且不推进 actor head。
- 端到端：`python3 research/harness/dynamic_interest_hole.py`；修复前 NodeR 为 0/15，修复后
  NodeR 本地、NodeS 对照和 NodeR API 均为 15/15，且回填完成后 readiness 才变为 1。
- handoff：`python3 research/harness/dynamic_interest_handoff.py`；第一次摘除因 1<2 fail-closed，
  node2 回填并 ready 后第二次摘除成功；后续 node1 本地保持 12 行，API 路由返回 13 行。
- 回归：`cargo test -p corro-types --lib`、`cargo test -p corro-agent --lib`。

## Operational plan

- 部署前先保留至少一个 wildcard/完整历史节点作为回填源。
- 先在一个非关键 holder 上扩大 interest，观察日志
  `reopened filtered versions after interest expansion`、bookkeeping gaps 和同步 lag。
- 观察 `node_interest.active=1` 后再开始摘除旧 holder；每次 placement 只操作一个节点。
- 摘除成功后，等待删除声明传播到写入方以及 3 秒 selector 缓存刷新，再判断新 placement 生效。
- 回滚可停用新二进制并恢复旧配置；新增内部表不会修改业务表。不要在回填未完成时删除最后一个
  完整历史 holder。

## Remaining boundary

当前协议支持由控制器串行执行的“扩容回填 → ready 发布 → 最小副本门禁 → 摘除”。它不提供跨节点
共识或 compare-and-swap：两个 holder 并发基于旧视图摘除仍可能同时通过本地检查。因此生产控制器
必须串行化 placement 变更；若要支持无协调并发摘除，还需引入带 epoch 的集中控制面或共识事务。
