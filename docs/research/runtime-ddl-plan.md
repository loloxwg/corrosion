# 运行期 DDL 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 单一控制面经 `POST /v1/schema` 发起加表/加列 DDL,经 CRR 表 `corro_ddl_log` 分发全集群,各节点按 seq 顺序应用;未知表数据变更按版本粒度拒收、对账兜底。

**Architecture:** DDL 语句作为一行数据写入 CRR 控制表,复用 corrosion 复制面(广播+anti-entropy)分发;数据面提交后钩子按 seq 顺序调既有 `execute_schema` 本地应用,进度存 `__corro_state`。`corro_ddl_log` 进控制表豁免清单(selector 不按 interest 过滤、sync 恒 include)。设计见 `runtime-ddl-design.md`。

**Tech Stack:** Rust(axum / rusqlite / cr-sqlite),Python harness(research/harness)。分支 `research/active-push`。

**惯例:** 每个 Task 完成即 commit;`cargo test -p corro-agent --lib` 里 `test_lagging_subscribers` 是已知 flaky(与本工作无关,单跑通过即可)。

---

### Task 1: 配置项 `api.allow_runtime_schema`

**Files:**
- Modify: `crates/corro-types/src/config.rs:173-186`(ApiConfig)

- [ ] **Step 1: 加字段**(照 `PgConfig.readonly` 惯例,config.rs:193)

```rust
// ApiConfig 内追加:
    #[serde(default)]
    pub allow_runtime_schema: bool,
```

- [ ] **Step 2: 修复编译**(全仓 ApiConfig 字面量构造点补字段;`cargo check -p corro-types -p corro-agent -p corrosion` 找到所有报错点,统一补 `allow_runtime_schema: false`,测试构造亦同)

- [ ] **Step 3: 验证** `cargo check --workspace` 通过

- [ ] **Step 4: Commit** `feat(config): add api.allow_runtime_schema flag`

---

### Task 2: `corro_ddl_log` 表常量与启动注入

**Files:**
- Modify: `crates/corro-types/src/schema.rs`(文件顶部常量区)
- Modify: `crates/corro-agent/src/agent/run_root.rs:128` 附近

- [ ] **Step 1: 定义常量**(corro-types/src/schema.rs,`init_schema` 之前)

```rust
/// 运行期 DDL 分发日志表。控制面单写、seq 单调;进控制表豁免清单
/// (selector 不按 interest 过滤、sync 恒 include,见 selector.rs / peer/mod.rs)。
pub const DDL_LOG_TABLE: &str = "corro_ddl_log";

pub const DDL_LOG_SCHEMA: &str = "CREATE TABLE corro_ddl_log (
  seq INTEGER NOT NULL PRIMARY KEY,
  sql TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL DEFAULT ''
);";
```

- [ ] **Step 2: 启动注入**(run_root.rs,`execute_schema_from_paths(&agent).await` 之前)

```rust
// corro_ddl_log 是 corrosion 自有 CRR 表,不依赖用户 schema 文件;
// 走 execute_schema 以复用 apply_schema 的 crsql_as_crr + __corro_schema 注册。
execute_schema(&agent, vec![corro_types::schema::DDL_LOG_SCHEMA.to_owned()]).await?;
```

(`execute_schema` 已在 util.rs:1471,幂等:apply_schema diff 对已存在同定义表为 no-op。)

- [ ] **Step 3: 手动验证**:起单节点(或跑任一 tokio 集成测试),sqlite3 查
  `SELECT name FROM sqlite_master WHERE name LIKE 'corro_ddl_log%'` 应见主表 + `corro_ddl_log__crsql_clock`(CRR 化成功)。

- [ ] **Step 4: Commit** `feat(schema): bootstrap corro_ddl_log CRR table at startup`

---

### Task 3: 控制表豁免(selector + sync)

**Files:**
- Modify: `crates/corro-agent/src/broadcast/selector.rs:161-174` 与其测试模块
- Modify: `crates/corro-agent/src/api/peer/mod.rs:398-404`(interest_set)

- [ ] **Step 1: 写失败测试**(selector.rs tests,照 selector.rs:411 `control_table_broadcast_bypasses_interest_filter` 依样)

```rust
#[test]
fn ddl_log_broadcast_bypasses_interest_filter() {
    // 与 control_table_broadcast_bypasses_interest_filter 同构:
    // tables = vec!["corro_ddl_log".to_string()],interest_routing 只含无关 peer,
    // 断言 scored_reduce/rl 返回全候选(未被 interest 过滤)。
    // 具体构造 Candidate 的 helper 抄该测试即可。
}
```

- [ ] **Step 2: 跑测试确认失败** `cargo test -p corro-agent --lib selector -- ddl_log` → FAIL

- [ ] **Step 3: 实现**(selector.rs:161 单常量改数组)

```rust
// 控制面表:元数据须比数据有更强传播保证,不受 interest 过滤。
// 注意与 corro-types::schema::DDL_LOG_TABLE / peer/mod.rs interest_set 保持同步。
const CONTROL_TABLES: [&str; 2] = ["node_interest", "corro_ddl_log"];
```

`interest_pool` 判断改为:

```rust
    if tables.iter().any(|t| CONTROL_TABLES.contains(&t.as_str())) {
        return None;
    }
```

(全文件 grep `CONTROL_TABLE` 其余引用点同步改。)

- [ ] **Step 4: sync 豁免**(peer/mod.rs:398 `interest_set`)

```rust
fn interest_set(interest: &Option<Vec<String>>) -> Option<std::collections::HashSet<String>> {
    interest.as_ref().map(|tables| {
        let mut set: std::collections::HashSet<String> = tables.iter().cloned().collect();
        set.insert("node_interest".to_string());
        set.insert("corro_ddl_log".to_string());
        set
    })
}
```

- [ ] **Step 5: 跑测试** `cargo test -p corro-agent --lib selector` 全过

- [ ] **Step 6: Commit** `feat(sync): exempt corro_ddl_log from interest filtering`

---

### Task 4: `POST /v1/schema` handler

**Files:**
- Modify: `crates/corro-agent/src/api/public/mod.rs`(新 handler,放 `api_v1_transactions` 附近)
- Modify: `crates/corro-agent/src/agent/util.rs:195`(路由注册 + use 引入)

- [ ] **Step 1: handler 实现**(api/public/mod.rs)

```rust
#[derive(Debug, Serialize, Deserialize)]
pub struct SchemaResponse {
    pub applied: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub time: f64,
}

#[tracing::instrument(skip_all)]
pub async fn api_v1_schema(
    Extension(agent): Extension<Agent>,
    axum::extract::Json(statements): axum::extract::Json<Vec<String>>,
) -> (StatusCode, axum::Json<SchemaResponse>) {
    let start = Instant::now();
    let reply = |code, err: Option<String>| {
        (code, axum::Json(SchemaResponse {
            applied: err.is_none(),
            error: err,
            time: start.elapsed().as_secs_f64(),
        }))
    };

    if !agent.config().api.allow_runtime_schema {
        return reply(StatusCode::FORBIDDEN,
            Some("runtime schema updates are disabled on this node (api.allow_runtime_schema)".into()));
    }
    if statements.is_empty() {
        return reply(StatusCode::BAD_REQUEST, Some("at least one statement is required".into()));
    }

    // 先本地应用:apply_schema 自带增量校验,破坏性变更(删表/删列/改列)
    // 会返回 SchemaError("won't drop/remove/change ... without the destructive flag")→ 400。
    // 本地成功后才写日志表,保证 corro_ddl_log 里只有可应用的 DDL。
    if let Err(e) = crate::agent::util::execute_schema(&agent, statements.clone()).await {
        return reply(StatusCode::BAD_REQUEST, Some(e.to_string()));
    }

    // 一次 POST = 一行日志(语句合并),seq = MAX+1(单一控制面写入,无冲突)。
    let joined = statements.join(";\n");
    let res = make_broadcastable_changes(&agent, None, move |tx| {
        tx.prepare_cached(
            "INSERT INTO corro_ddl_log (seq, sql, created_at) \
             VALUES ((SELECT COALESCE(MAX(seq), 0) + 1 FROM corro_ddl_log), ?, ?)",
        )
        .map_err(|source| ChangeError::Rusqlite { source, actor_id: None, version: None })?
        .execute(rusqlite::params![joined, time::OffsetDateTime::now_utc().to_string()])
        .map_err(|source| ChangeError::Rusqlite { source, actor_id: None, version: None })?;
        Ok(())
    })
    .await;

    match res {
        // 本地已 apply 但日志写失败:返回 500,调用方重试;重复 apply 是 no-op,安全。
        Err(e) => reply(StatusCode::INTERNAL_SERVER_ERROR, Some(e.to_string())),
        Ok(_) => reply(StatusCode::OK, None),
    }
}
```

(import 按编译器提示补;`ChangeError::Rusqlite` 字段名以现有定义为准,handler 内其他 `make_broadcastable_changes` 调用可参照。)

- [ ] **Step 2: 注册路由**(util.rs,`/v1/transactions` 块后照抄一块)

```rust
        .route(
            "/v1/schema",
            post(api_v1_schema).route_layer(
                tower::ServiceBuilder::new()
                    .layer(HandleErrorLayer::new(|_error: BoxError| async {
                        Ok::<_, Infallible>((
                            StatusCode::SERVICE_UNAVAILABLE,
                            "max concurrency limit reached".to_string(),
                        ))
                    }))
                    .layer(LoadShedLayer::new())
                    // DDL 低频且串行语义,并发限 1
                    .layer(ConcurrencyLimitLayer::new(1)),
            ),
        )
```

并在 util.rs 顶部 use 里加 `api_v1_schema`。

- [ ] **Step 3: 集成测试**(crates/corro-agent/src/agent/tests.rs,照该文件既有 `#[tokio::test]` 起 agent 的模式,如 tests.rs:79 起的用例)

```rust
#[tokio::test(flavor = "multi_thread")]
async fn runtime_schema_api() -> eyre::Result<()> {
    // 1. 起单 agent(照现有 launch helper),config 里 api.allow_runtime_schema = true
    // 2. POST /v1/schema ["CREATE TABLE ont_inst__uav (pk TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL DEFAULT '')"]
    //    → 200;查 sqlite_master 有 ont_inst__uav + __crsql_clock 表;
    //    corro_ddl_log 有 seq=1 该行。
    // 3. 再 POST 同语句 → 200(幂等 no-op),corro_ddl_log seq=2。
    // 4. POST ["CREATE TABLE ont_inst__uav (pk TEXT NOT NULL PRIMARY KEY)"](删列)
    //    → 400,corro_ddl_log 无新行。
    // 5. flag=false 的 agent → POST 返回 403。
    Ok(())
}
```

- [ ] **Step 4: 跑测试** `cargo test -p corro-agent --lib runtime_schema_api` → PASS

- [ ] **Step 5: Commit** `feat(api): POST /v1/schema for runtime additive DDL`

---

### Task 5: 未知表变更按版本拒收(乱序防线)

**Files:**
- Modify: `crates/corro-agent/src/agent/util.rs`(process_multiple_changes 事务循环,~1049 之前)
- Test: `crates/corro-agent/src/agent/tests.rs`

现状(已核实):未知表变更在 `INSERT INTO crsql_changes` 报错后,若事务未回滚走 util.rs:1069 `continue`(整版本内已部分写入的风险取决于报错时点),若回滚则毒化整批。目标:**进事务前按版本粒度前置过滤**,行为确定化。

- [ ] **Step 1: 写失败测试**(tests.rs)

```rust
#[tokio::test(flavor = "multi_thread")]
async fn unknown_table_change_rejected_without_poisoning() -> eyre::Result<()> {
    // 1. 起 agent(标准测试 schema)。
    // 2. 构造两个 ChangeV1:一个触及测试 schema 已有表(合法),
    //    一个 table = "not_yet_created"(未知表),同批喂 process_multiple_changes。
    // 3. 断言:返回 Ok;合法 change 已应用(查目标表行存在);
    //    未知表 change 的版本未被标记(bookie contains_version == false)→ 留给 sync 重试。
    // 4. 用 execute_schema 建出 not_yet_created 表后,重放该 change → 应用成功。
    Ok(())
}
```

- [ ] **Step 2: 确认失败**(现状下第 3 步断言大概率不稳定/失败)

- [ ] **Step 3: 实现前置过滤**(util.rs,`process_single_version` 调用之前)

```rust
            // 乱序防线:版本触及本地 schema 未知的表(DDL 尚未到达)→ 整版本跳过,
            // 不标已处理、不 mark cleared,留在 needed 由 anti-entropy 在 DDL 应用后补齐。
            // 版本粒度(而非行粒度):部分应用会把版本标为已处理,丢失未知表部分。
            let has_unknown_table = {
                let schema = agent.schema().read();
                change
                    .changes()
                    .iter()
                    .any(|c| !schema.tables.contains_key(c.table.as_str()))
            };
            if has_unknown_table {
                counter!("corro.changes.unknown_table.skipped").increment(1);
                continue;
            }
```

(`Changeset::Empty` 无 changes,天然不受影响;`changes()` 访问器与 `c.table` 字段名以 corro-types/broadcast.rs 现有定义为准。)

- [ ] **Step 4: 跑测试** → PASS;顺跑 `cargo test -p corro-agent --lib agent::tests` 无回归

- [ ] **Step 5: Commit** `fix(sync): reject versions touching unknown tables without poisoning batch`

---

### Task 6: DDL 应用钩子(顺序应用 + 空洞停车)

**Files:**
- Modify: `crates/corro-agent/src/agent/util.rs`(新函数 `apply_pending_ddl` + 提交后钩子)
- Modify: `crates/corro-agent/src/agent/run_root.rs`(启动扫尾)
- Test: `crates/corro-agent/src/agent/tests.rs`

- [ ] **Step 1: 实现 `apply_pending_ddl`**(util.rs,`execute_schema` 附近)

```rust
pub const DDL_APPLIED_SEQ_KEY: &str = "ddl_log_applied_seq_v1";

/// 按 seq 顺序应用 corro_ddl_log 中未应用的 DDL。
/// 遇 seq 空洞即停(等 anti-entropy 补行后由下次触发续跑)。
/// 幂等:apply_schema 是 diff 语义;并发重入安全(重放=no-op,进度只前进)。
/// 进度推进与 DDL 应用非同事务:崩溃窗口=已应用未推进 → 重放 no-op 后推进,可接受。
pub async fn apply_pending_ddl(agent: Agent) {
    let (applied, rows) = {
        let conn = match agent.pool().read().await {
            Ok(conn) => conn,
            Err(e) => { warn!("apply_pending_ddl: no read conn: {e}"); return; }
        };
        let applied: i64 = conn
            .prepare_cached("SELECT CAST(value AS INTEGER) FROM __corro_state WHERE key = ?")
            .and_then(|mut s| s.query_row([DDL_APPLIED_SEQ_KEY], |r| r.get(0)))
            .unwrap_or(0);
        let rows: Vec<(i64, String)> = match conn
            .prepare_cached("SELECT seq, sql FROM corro_ddl_log WHERE seq > ? ORDER BY seq ASC")
            .and_then(|mut s| {
                s.query_map([applied], |r| Ok((r.get(0)?, r.get(1)?)))?
                    .collect::<rusqlite::Result<_>>()
            }) {
            Ok(rows) => rows,
            Err(e) => { warn!("apply_pending_ddl: read ddl log: {e}"); return; }
        };
        (applied, rows)
    };

    let mut expected = applied + 1;
    for (seq, sql) in rows {
        if seq != expected {
            info!("apply_pending_ddl: gap before seq {seq} (expected {expected}), waiting for sync");
            return; // 空洞停车,不跳号
        }
        if let Err(e) = execute_schema(&agent, vec![sql]).await {
            // 单条失败:不推进、不跳号,下次触发重试(日志表里只有控制面验证过的 DDL,
            // 失败通常是暂时性资源问题)。
            error!("apply_pending_ddl: failed to apply ddl seq {seq}: {e}");
            return;
        }
        if let Ok(conn) = agent.pool().write_low().await {
            if let Err(e) = conn.execute(
                "INSERT OR REPLACE INTO __corro_state (key, value) VALUES (?, ?)",
                rusqlite::params![DDL_APPLIED_SEQ_KEY, seq],
            ) {
                error!("apply_pending_ddl: failed to record progress at seq {seq}: {e}");
                return;
            }
        }
        info!("apply_pending_ddl: applied ddl seq {seq}");
        expected += 1;
    }
}
```

(pool 的读写方法名以 `SplitPool` 现有 API 为准:读 `read()`,写参照 run_root/util 既有低优先级写用法。)

- [ ] **Step 2: 提交后钩子**(util.rs:1211 提交成功后的 `tokio::spawn`/`match_changes` 处,同一位置追加)

```rust
        // DDL 日志表来了新行 → 触发顺序应用(独立 task,不阻塞 apply 路径)
        if changesets.iter().any(|cs| {
            cs.changes().iter().any(|c| c.table.as_str() == corro_types::schema::DDL_LOG_TABLE)
        }) {
            spawn_counted(apply_pending_ddl(agent.clone()));
        }
```

- [ ] **Step 3: 启动扫尾**(run_root.rs,`execute_schema_from_paths` 之后)

```rust
    // 停机期间经 sync 落库的 DDL 行没有触发钩子,启动补一次
    apply_pending_ddl(agent.clone()).await;
```

- [ ] **Step 4: 集成测试**(tests.rs)

```rust
#[tokio::test(flavor = "multi_thread")]
async fn ddl_log_applies_in_order_and_parks_on_gap() -> eyre::Result<()> {
    // 1. 起 agent。直接往 corro_ddl_log 写 seq=1(CREATE TABLE t1...)与 seq=3(CREATE TABLE t3...)
    //    (经 make_broadcastable_changes 或普通写连接均可,模拟远端到达)。
    // 2. 调 apply_pending_ddl:断言 t1 已建、t3 未建(空洞停车),
    //    __corro_state ddl_log_applied_seq_v1 == 1。
    // 3. 补 seq=2(CREATE TABLE t2...),再调:t2、t3 均建成,进度 == 3。
    // 4. 再调一次(无新行):no-op 不报错(幂等)。
    Ok(())
}
```

- [ ] **Step 5: 跑测试** → PASS

- [ ] **Step 6: Commit** `feat(sync): ordered DDL application from corro_ddl_log with gap parking`

---

### Task 7: 双节点端到端(Rust 集成)

**Files:**
- Test: `crates/corro-agent/src/agent/tests.rs`

- [ ] **Step 1: 写测试**(照 tests.rs 既有双 agent 互联用例的起法,如 sync/broadcast 类测试)

```rust
#[tokio::test(flavor = "multi_thread")]
async fn runtime_ddl_propagates_to_peer() -> eyre::Result<()> {
    // 1. 起两个互联 agent:A(allow_runtime_schema=true)、B(false)。
    // 2. A: POST /v1/schema 建 ont_inst__uav → 200。
    // 3. A: 立刻 POST /v1/transactions 插一行 uav 数据(竞态窗口:B 可能还没建表)。
    // 4. 轮询 B(带超时,照既有测试的 poll 风格):
    //    a. sqlite_master 出现 ont_inst__uav(DDL 经 corro_ddl_log 复制+钩子应用);
    //    b. 该行数据到达(先到被拒收的话由 sync 补齐)。
    // 5. 断言 B 的 __corro_state 进度 == A 的 corro_ddl_log MAX(seq)。
    Ok(())
}
```

- [ ] **Step 2: 跑测试** `cargo test -p corro-agent --lib runtime_ddl_propagates` → PASS(注意乱序场景靠 sync 兜底,超时给足,参照既有测试的等待时长)

- [ ] **Step 3: 全量回归** `cargo test -p corro-types -p corro-agent` (flaky 单跑规则见头部)

- [ ] **Step 4: Commit** `test(sync): runtime DDL end-to-end propagation`

---

### Task 8: harness 多节点 e2e(Python)

**Files:**
- Create: `research/harness/ddl_runtime_test.py`(复用 `research/harness/run.py` 的配置生成/起集群/API helper,照 `wildcard_test.py` 的结构)

- [ ] **Step 1: 写脚本**,场景:

```python
# 1. 起 6 节点;node0 配 api.allow_runtime_schema = true(控制面),其余 false。
# 2. node0 POST /v1/schema 建 ont_inst__demo,立刻写 20 行数据。
# 3. 轮询全部节点:表存在(直读 sqlite,用 count_rows_local 风格绕查询路由——
#    教训:验证本地状态绝不能走会路由的 /v1/queries)+ 数据收齐。
# 4. 乱序注入:CORRO_LINK_FAULTS 切断 node0→node3 直连(drop_p=1.0),重复 2-3,
#    node3 靠多跳/对账收敛(超时放宽)。
# 5. 后入网:起第 7 个节点,断言自动补齐建表历史+数据。
# 6. 越权:对 node1(flag=false)POST /v1/schema → 403。
# 踩坑备忘(此前三次踩中):改配置文件严禁 open(f,"w").write(open(f).read()) 单行套娃
# ——先读进变量再写。
```

- [ ] **Step 2: 跑通** `python research/harness/ddl_runtime_test.py` 全场景 PASS

- [ ] **Step 3: Commit** `test(research): runtime DDL cluster e2e harness`

---

### Task 9: ontology-web 对接(隔壁仓库,分支 feat/corrosion-instance-backend)

**Files:**
- Modify: `backend/services/corrosion_client.py`
- Modify: `backend/services/corrosion_schema.py`(ensure_type_table)
- Modify: `config/corrosion.toml`(sidecar 配置加 `[api] allow_runtime_schema = true`)

- [ ] **Step 1: client 加方法**(corrosion_client.py,照 `_post_json` 既有用法)

```python
def schema(statements: list[str]) -> None:
    """运行期 DDL:经控制面 sidecar 的 POST /v1/schema 全集群分发。"""
    body = _post_json("/v1/schema", statements)
    payload = json.loads(body)
    if not payload.get("applied"):
        raise InstanceStorageError(409, payload.get("error") or "schema update failed")
```

- [ ] **Step 2: ensure_type_table 改造**(corrosion_schema.py):删「写 schema 文件 + corrosion_client.reload()」,改:

```python
def ensure_type_table(object_type_key: str) -> None:
    table_name = table_name_for(object_type_key)
    corrosion_client.schema([type_table_ddl(table_name)])
    logger.info("corrosion 表已确保(集群分发): %s", table_name)
```

(schema 文件写入按设计可留作审计;若保留,只写文件、不再调 reload。)

- [ ] **Step 3: 验证**:跑 `backend/scripts/verify_corrosion_e2e.py` + 新增「建 ObjectType → 多节点可查」用例;pytest 失败集合与基线 diff 一致(基线≈39 failed,main 自带)。

- [ ] **Step 4: Commit**(ontology-web 仓库)`feat(corrosion): distribute type table DDL via /v1/schema`

---

### Task 10: 文档收口

**Files:**
- Modify: `docs/research/DELIVERY.md`(设计文档索引行 + 能力清单)
- Modify: `docs/research/task-driven-semantic-replication-roadmap.md`(§1.3「尚未实现」中「运行期建表」移入已实现,注明机制)
- Modify: `docs/research/active-push-technical-report.md`(§7 或新小节:运行期 DDL 一段,含乱序防线与豁免设计)

- [ ] **Step 1: 三处更新**(各一段,按实测结果写,含诚实边界:删表不在内/最终一致语义)
- [ ] **Step 2: Commit** `docs(research): record runtime DDL capability`

---

## Self-Review 结论(已跑)

- **Spec 覆盖**:设计 §3.1→Task 4;§3.2→Task 2+3;§3.3→Task 6;§3.4→Task 5;§3.5→Task 9;§5 测试→Task 4/5/6/7/8;§4 错误表→分散在 Task 4(400/403/500)、Task 6(不跳号/重试)、Task 7/8(重启/新节点)。无缺口。
- **类型一致**:`DDL_LOG_TABLE`/`DDL_LOG_SCHEMA`/`DDL_APPLIED_SEQ_KEY`/`apply_pending_ddl`/`api_v1_schema` 各 Task 间名称一致;`CONTROL_TABLES` 双写点(selector 常量 + peer interest_set)已在注释中互指。
- **已知留白(有意)**:`ChangeError::Rusqlite` 字段、`SplitPool` 写方法名、tests.rs 起 agent 的 helper 名——以现场代码为准,均已标注参照物;执行者按编译器/既有用例对齐,不属于占位符。
