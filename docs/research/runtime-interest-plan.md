# 运行期 interest 热更新实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `POST /v1/interest {tables, epoch}` 运行期变更本节点 interest,完整复用启动期安全协议(epoch fencing → 热换 config 统一口径 → 重开被过滤历史 → reconcile(active 门控+摘除门禁)→ 异步回填激活)。

**Architecture:** 核心洞察:启动函数(`validate/check_interest_epoch`、`reopen_filtered_versions_after_interest_expansion`、`reconcile_own_interest`、`activate_pending_interest_when_synced`)全部内部读 `agent.config()`;`config` 是 ArcSwap → handler **先 `set_config` 热换新 interest+epoch,再原样调用这些函数**,零参数化改造。失败则 config 回滚。设计见 `runtime-interest-design.md`。

**Tech Stack:** Rust(axum),Python harness。分支 `research/active-push`。惯例同 runtime-ddl-plan(每 Task commit;flaky 口径同)。

---

### Task 1: 配置项 `api.allow_runtime_interest`

**Files:** `crates/corro-types/src/config.rs`

- [ ] ApiConfig 加 `#[serde(default)] pub allow_runtime_interest: bool,`(紧跟 allow_runtime_schema);ConfigBuilder 加字段+setter `api_allow_runtime_interest(bool)`+build 接线(照 allow_runtime_schema 三处依样)。
- [ ] `cargo check --workspace` 过(补构造点)。
- [ ] Commit `feat(config): add api.allow_runtime_interest flag`

---

### Task 2: 启动函数复用化(可见性 + 单飞)

**Files:** `crates/corro-agent/src/agent/run_root.rs`,`crates/corro-types/src/agent.rs`

- [ ] 把 handler 需要调用的函数提为 `pub(crate)`:`reconcile_own_interest`、`reopen_filtered_versions_after_interest_expansion`、`activate_pending_interest_when_synced`、`check_interest_epoch`(及其依赖的辅助如 `load_removal_candidate_actors`/live_actors 获取——检查 handler 侧如何拿 live_actors:启动时来自成员载入;运行期从 `agent.members()` 现读,若已有等价 helper 复用之,没有则提一个 `pub(crate) fn live_actor_ids(agent) -> Vec<ActorId>`)。
- [ ] **激活任务单飞**:`AgentInner` 加 `interest_activation_lock: Arc<tokio::sync::Mutex<()>>`(corro-types/src/agent.rs,照现有字段风格;或 run_root 模块级 static —— 选 Agent 字段,多 agent 测试才不串扰)。`activate_pending_interest_when_synced` 任务体开头 `let _g = lock.try_lock()`,已持有则 log + return(旧任务会扫到新 pending 行?不会——旧任务只置本次启动的 active=0 行…检查实现:UPDATE 是全量 `WHERE actor_id=me AND active=0`,所以旧任务完成时会把新 pending 一并激活——**过早激活风险**!因此单飞语义须是:新任务等旧任务结束(`lock().await`)后重新判定,不是 try_lock 放弃。实现:任务体 `let _g = lock.lock().await;` 串行化即可,激活判据在锁内重算)。
- [ ] 启动调用点行为不变(编译+现有测试过:`cargo test -p corro-agent --lib` 全绿)。
- [ ] Commit `refactor(sync): expose interest reconcile machinery for runtime use`

---

### Task 3: `POST /v1/interest` handler

**Files:** `crates/corro-agent/src/api/public/mod.rs`(handler),`crates/corro-agent/src/agent/util.rs`(路由,照 /v1/schema 块,并发限 1)

- [ ] 请求/响应类型:

```rust
#[derive(Debug, Deserialize)]
pub struct InterestUpdate { pub tables: Vec<String>, pub epoch: u64 }

#[derive(Debug, Serialize)]
pub struct InterestResponse {
    pub accepted: bool,
    pub pending_activation: bool,
    pub reopened_ranges: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub time: f64,
}
```

- [ ] handler 骨架(照 api_v1_schema 的 reply 闭包风格):

```rust
pub async fn api_v1_interest(
    Extension(agent): Extension<Agent>,
    axum::extract::Json(req): axum::extract::Json<InterestUpdate>,
) -> (StatusCode, axum::Json<InterestResponse>) {
    // ① flag 403;tables 语法校验(非空字符串;"*" 允许;重复去重)400;epoch==0 → 400
    // ② 预检 fencing:读 __corro_state applied epoch,check_interest_epoch(req.epoch, applied,
    //    placement_changed=当前 node_interest 行集 != desired)→ 失败 409(冲突语义)
    // ③ 热换 config:let old = agent.config();
    //    let mut new = (*old).clone(); new.gossip.interest = tables; new.gossip.interest_epoch = epoch;
    //    agent.set_config(new);
    // ④ reopen_filtered_versions_after_interest_expansion(&agent).await
    //    → Err: agent.set_config 回滚 old,500
    // ⑤ reconcile_own_interest(&agent, live_actors).await
    //    → Err(InterestRemovalUnsafe) → 回滚 config,409(带 holder 计数信息)
    //    → Err(Stale/ReusedInterestEpoch) → 回滚,409;其它 Err → 回滚,500
    // ⑥ pending → spawn_counted(activate_pending_interest_when_synced(...))(单飞锁内部保证)
    // ⑦ 200 {accepted:true, pending_activation, reopened_ranges}
}
```

  注意:③-⑤ 期间另一请求被 ConcurrencyLimit(1) 挡住,但**启动流程不会并发**(handler 只在服务起来后可达,启动编排已完成)。回滚窗口内 sync 短暂用了新口径又回旧——方向性无害(多收=多存点,少收=对账兜底),注释说明。

- [ ] 集成测试 `runtime_interest_api`(tests.rs,单 agent):flag off→403;epoch 0→400;合法扩 interest→200 + node_interest 出现 active=0 行(或已激活)+ config 口径已换(`agent.config().gossip.interest` 断言);同 epoch 重放(placement 不变)→200 幂等 no-op 或 409(与 check_interest_epoch 语义一致,以实现为准断言);epoch 回退→409;摘除到副本不足→409 且行未删。
- [ ] Commit `feat(api): POST /v1/interest for runtime placement updates`

---

### Task 4: 运行期扩缩 e2e(Rust 双节点)

**Files:** `crates/corro-agent/src/agent/tests.rs`

- [ ] `runtime_interest_expansion_backfills`(照 runtime_ddl_propagates_to_peer 骨架):A、B 互联,B 静态 interest=["t1"](scored_reduce 策略,t2 版本会被过滤记录);A 写 t2 数据若干版本;等 B 把 t2 版本记入 `__corro_filtered_version_ranges`;B `POST /v1/interest {tables:["t1","t2"], epoch:1}` → 轮询断言:①`__corro_filtered_version_ranges` 清空(gap 重开);②t2 历史行到达 B 本地;③node_interest B 行 t2 最终 active=1;④B 后续实时收到 A 的 t2 新写入。
- [ ] 3 连跑稳定;全量 lib 回归。
- [ ] Commit `test(sync): runtime interest expansion end-to-end`

---

### Task 5: harness 场景(Python)

**Files:** `research/harness/interest_hot_update_test.py`

- [ ] 照 ddl_runtime_test.py 结构,场景:①6 节点 scored_reduce+静态窄 interest,写数落定;②对 node2 运行期扩 interest(API)→ 历史回填+active 置位+新数据到达(直读 sqlite 断言);③对 node3 摘除唯一副本表 → 409 拒绝;④epoch 回退 → 409;⑤flag 关节点 → 403。踩坑规约同 ddl 脚本(直读 db/禁配置套娃/超时给足)。
- [ ] 跑通全 PASS;Commit `test(research): interest hot-update cluster harness`

---

### Task 6: 文档收口

**Files:** `docs/research/DELIVERY.md`、`task-driven-semantic-replication-roadmap.md` §1.3、`active-push-technical-report.md`(§7.2)

- [ ] roadmap「运行期热更新 interest」移入已实现;报告新增 §7.2(机制+口径分裂闭合+诚实边界:重启持久化靠调用方/epoch fencing 响亮失败/绕 API 手写表不支持/3s 推送缓存窗口);DELIVERY 索引+证据行。
- [ ] Commit `docs(research): record runtime interest hot-update capability`

---

## Self-Review 结论(已跑)

- 设计 §3 七步 ↔ Task 3 handler ①-⑦ 一一对应;§4 组件 ↔ Task 1-3;§5 边界 ↔ Task 6。
- 已知留白(有意):Task 2 单飞锁的过早激活分析已写进任务文本(旧任务全量 UPDATE active=0 行 → 必须串行化不是放弃);handler 拿 live_actors 的具体来源(members 现读)由 Task 2 现场定,已标注。
- 类型一致:`InterestUpdate`/`InterestResponse`/`api_v1_interest`/`interest_activation_lock` 各 Task 名称一致。
