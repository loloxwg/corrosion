# 动态 interest 历史回填协议

## Context

部分复制节点在不关心表 `T` 时，会从同步端收到覆盖相关 db version 的
`Changeset::Empty`，并把这些版本记为 `KnownDbVersion::Cleared`。旧实现随后把它们视为
“已完整拥有”，所以节点扩大 interest 后不会再次请求历史数据。回归场景中，NodeR 重新关心
`target` 后本地仍为 0/15，而持续关心的 NodeS 为 15/15。

硬不变量：

- 节点对外声明持有新表之前，必须能重新请求此前因过滤而跳过的历史版本。
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

选择“记录被过滤范围”，而不是在每次扩张时重扫 `1..head`，避免重新请求和重放已经正常应用的
全部历史。版本仍由 CR-SQLite 幂等合并；无数据的真实空版本被重新请求一次也不会改变业务状态。

## Failure handling

- 迁移、状态解析、gap 写入或事务提交失败：agent 启动失败，不以不完整 holder 身份服务。
- 节点在事务提交前崩溃：旧 interest 快照和 filtered ranges 保留，下次启动会重试。
- 节点在事务提交后、sync 完成前崩溃：reopened gaps 已持久化，下次启动继续请求。
- 集群没有仍持有完整历史的 wildcard/源节点：gap 会保持未满足，节点不会静默把它当成完整历史。

## Validation

- 单测：interest 仅在集合扩大或切换为 wildcard 时判定扩张。
- 单测：reopened gaps 与已有 gaps 合并，且不推进 actor head。
- 端到端：`python3 research/harness/dynamic_interest_hole.py`；修复前 NodeR 为 0/15，修复后
  NodeR 本地、NodeS 对照和 NodeR API 均为 15/15。
- 回归：`cargo test -p corro-types --lib`、`cargo test -p corro-agent --lib`。

## Operational plan

- 部署前先保留至少一个 wildcard/完整历史节点作为回填源。
- 先在一个非关键 holder 上扩大 interest，观察日志
  `reopened filtered versions after interest expansion`、bookkeeping gaps 和同步 lag。
- 回填完成且本地行数/校验一致后，再允许查询路由把该节点视为新表 holder。
- 回滚可停用新二进制并恢复旧配置；新增内部表不会修改业务表。不要在回填未完成时删除最后一个
  完整历史 holder。

## Remaining boundary

本协议修复的是“interest 扩大后回填历史”。它尚未实现动态摘除时的副本 handoff，也没有把推送侧
运行时 `node_interest` 与对账侧启动配置统一成一个在线变更事务。因此完整的动态 placement 仍需
独立完成摘除前 `min_replicas`/handoff 和路由发布门禁。
