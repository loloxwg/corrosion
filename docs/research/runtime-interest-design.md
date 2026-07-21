# 运行期 interest 热更新设计

> 2026-07-22。动因:任务/本体驱动的 placement 变更(InterestPlan 执行通道)不能靠重启;
> 三个动态 interest 修复(f6b8698 回填 / f664c00 handoff 门控 / 514047e epoch fencing)
> 已建立安全协议,本设计把它们从「仅启动时」升级为「运行期可触发」。
> 前置勘察结论见 §1;与 runtime-ddl-design.md 同款 API 形态。

## 1. 现状勘察(设计依据)

- **安全协议已存在但只在启动跑一次**:`reconcile_own_interest`(唯一调用点
  run_root.rs 启动编排)+ `activate_pending_interest_when_synced`(一次性后台任务)
  + `reopen_filtered_versions_after_interest_expansion`(启动时)+
  `validate_configured_interest_epoch`(启动前置)。
- **口径分裂(未修,本设计必须一并修)**:sync 侧 `interest_for_sync` /
  `sync_interest_is_filtered` 只读 `config.gossip.interest`(静态);推送路由
  `load_interest_routing`(3s 刷新)与查询路由读 `node_interest` 表。运行期只写表
  不改 config → 对账按旧口径继续把新关心表的版本当空版本 Clear,数据洞。
- **杠杆**:`AgentInner.config` 是 `ArcSwap<Config>`,`Agent::set_config` 已存在
  (生产无调用者);sync 侧所有读点都是每次调用即时 `agent.config()` → 热换 config
  即时全侧生效,无需把 sync 改造成读表。
- `node_interest` schema:`(actor_id BLOB, table_name TEXT, active INTEGER, PK(actor_id,table_name))`,
  用户 schema 提供,epoch 存 `__corro_state`。

## 2. 需求与决策(用户授权自主定案,此处记录依据)

| 维度 | 决定 | 依据 |
|---|---|---|
| 入口 | **`POST /v1/interest`**,body=`{"tables": [...], "epoch": N}` | 与 /v1/schema 同款形态;Consul watch 后续再加(API 是它的底座) |
| 语义 | **全量期望集**(非增量 delta) | 与 `gossip.interest` 配置语义一致,`reconcile_own_interest` 原样复用 |
| 作用域 | **只改本节点自己的 interest**(actor=crsql_site_id) | node_interest 本就是自声明模型;他节点由其自己的 API 改 |
| 门禁 | `api.allow_runtime_schema` 同款:新配置项 **`api.allow_runtime_interest`**(默认 false) | 与 DDL 一致的最小暴露面 |
| epoch | **必填且须 > 已 applied**(0 拒绝) | 运行期变更全部走 fencing,不留兼容后门;复用 `check_interest_epoch` |
| 口径统一 | 热更新时 **`set_config` 热换 `gossip.interest`+`interest_epoch`** | sync 侧即时生效,分裂闭合;推送侧 3s 刷新表,窗口内新表多推无害(拒收+对账兜底语义已存在) |
| 重启持久化 | **不做**;调用方(未来 InterestPlan 控制器 / 运维)负责同步配置文件 | epoch fencing 使漂移**响亮失败**(旧 config 重启 → StaleInterestEpoch 拒绝启动),不会静默回退——这是特性不是缺陷 |

## 3. 执行序(镜像启动编排,运行期化)

`POST /v1/interest {tables, epoch}` handler,ConcurrencyLimit=1 + 内部单飞锁(防
API 与启动激活任务并发):

```text
① 校验:flag 开 / tables 语法(表名或 "*") / epoch > 0
② epoch fencing:check_interest_epoch(epoch, applied, placement_changed) —— 事务外预检
③ 热换 config:new_config = current.clone + gossip.interest = tables + interest_epoch = epoch
   agent.set_config(new_config)      ← 此刻起 sync 口径 = 新 interest(停止把新表版本 Clear)
④ 重开历史:reopen_filtered_versions_after_interest_expansion(agent)
   (effective_sync_interest 现在读到新 config;扩大则重开 __corro_filtered_version_ranges)
⑤ reconcile_own_interest(agent, live_actors):
   - 新增表 → INSERT active=0(未 ready,推送/查询路由不会指向本节点)
   - 摘除表 → count_ready_holders ≥ interest_min_replicas 才 DELETE,否则整体拒绝
   - 事务内二次 epoch 校验 + 落 applied epoch
⑥ 有 pending → spawn activate_pending_interest_when_synced(回填完成判定 → active=1)
⑦ 响应:{accepted, pending_activation: bool, reopened_ranges: n, error?}
```

失败回滚语义:②-⑤ 任一步失败 → **config 回滚到旧值**(set_config 换回),响应 4xx/5xx;
⑤ 的事务性保证 node_interest/epoch 不半写。④ 在 ⑤ 前与启动顺序一致
(先重开 gap 再写新行,激活判据 `reopened_ranges` 才有值可等)。

摘除方向:config 热换后 sync 开始过滤被摘表(后续版本记入 filtered ranges,若未来
重新关心可回填);本地已有数据**不删除**(placement 摘除 ≠ 数据删除),active 行删除
后查询路由自然改道其它 holder。

## 4. 组件改动

- `config.rs`:`ApiConfig.allow_runtime_interest: bool`(serde default false)+ builder setter。
- `run_root.rs`:`reconcile_own_interest` / `reopen_filtered_versions_after_interest_expansion` /
  `activate_pending_interest_when_synced` 从启动私有流程提为可复用(参数化 desired
  interest+epoch,而非只读 config;启动路径改为传 config 值调用,行为不变)。
  运行期激活任务与启动激活任务互斥(单飞:`tokio::sync::Mutex` 或 Agent 上的标志)。
- `api/public/mod.rs`:`api_v1_interest` handler(照 `api_v1_schema` 骨架)。
- `util.rs`:路由注册 `/v1/interest`(并发限 1)。
- 测试:单测(epoch 拒绝/flag 403/removal 门禁拒绝)+ 集成(运行期扩 interest →
  被过滤历史回填 → active=1 → 查询路由指向本节点;运行期摘除 → 副本不足拒绝);
  harness:`dynamic_interest_hot_update.py`(先静态窄 interest 集群写数 → 运行期扩
  → 断言历史回填+新数据到达+active 置位;摘除场景)。

## 5. 诚实边界

- 重启不持久化:调用方须同步配置文件;不同步则重启时 epoch fencing **拒绝启动**
  (响亮失败,防静默回退,运维需知)。
- 口径统一只覆盖热更新路径:直接手写 node_interest 表(绕过 API)仍会分裂——
  文档声明表的自声明行由 corrosion 管理,外部写入不受支持。
- 推送侧 3s 缓存窗口:热更后最长 3s 推送路由仍旧口径;方向性无害
  (多推=浪费一点带宽,少推=对账兜底)。
- `activate` 判据沿用现有(回填区间清空/初始 sync 静默 2 连击),未加超时上限:
  持有历史的节点全部离线时 pending 可能长期不激活——与启动语义一致,监控靠
  active=0 行可见性。
