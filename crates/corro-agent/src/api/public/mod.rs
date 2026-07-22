use std::{
    collections::BTreeSet,
    net::SocketAddr,
    ops::Deref,
    time::{Duration, Instant},
};

use crate::api::utils::CountedBody;
use crate::transport::Transport;
use antithesis_sdk::assert_sometimes;
use axum::{
    extract::{ConnectInfo, Query},
    response::IntoResponse,
    Extension,
};
use bytes::{BufMut, BytesMut};
use compact_str::ToCompactString;
use corro_types::{
    actor::ActorId,
    agent::{Agent, ChangeError},
    api::{
        ColumnName, ExecResponse, ExecResult, HealthQuery, HealthResponse, QueryEvent, Statement,
        TableStatRequest, TableStatResponse,
    },
    base::CrsqlDbVersion,
    broadcast::{BiPayload, BiPayloadV1, Timestamp},
    change::{insert_local_changes, InsertChangesInfo, SqliteValue},
    config::Config,
    persistent_gauge,
    schema::parse_sql,
    sqlite::SqlitePoolError,
};
use futures::SinkExt;
use hyper::StatusCode;
use metrics::{counter, histogram};
use rusqlite::{params_from_iter, OptionalExtension, ToSql, Transaction};
use serde::{Deserialize, Serialize};
use spawn::spawn_counted;
use speedy::Writable;
use sqlite_pool::{Committable, InterruptibleTransaction, SqliteConn};

use tokio::{
    sync::{
        mpsc::{self, channel},
        oneshot,
    },
    task::block_in_place,
    time::timeout,
};
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tracing::{debug, error, trace, warn};

use corro_types::broadcast::broadcast_changes;
use tripwire::Tripwire;

use crate::agent::run_root::{
    activate_pending_interest_when_synced, check_interest_epoch, live_actor_ids,
    reconcile_own_interest, reopen_filtered_versions_after_interest_expansion,
    INTEREST_EPOCH_STATE_KEY,
};

pub mod pubsub;

pub mod update;

#[derive(Clone, Copy, Debug, Default, Deserialize)]
pub struct TimeoutParams {
    #[serde(default)]
    pub timeout: Option<u64>,
}

pub async fn make_broadcastable_changes<F, T>(
    agent: &Agent,
    timeout: Option<u64>,
    f: F,
) -> Result<(T, Option<CrsqlDbVersion>, Duration), ChangeError>
where
    F: FnOnce(&InterruptibleTransaction<Transaction>) -> Result<T, ChangeError>,
{
    let actor_id = agent.actor_id();
    trace!("getting conn...");
    let mut conn = agent.pool().write_priority().await?;
    trace!("got conn");

    let start = Instant::now();
    let ts = Timestamp::from(agent.clock().new_timestamp());

    block_in_place(move || {
        trace!("acquiring bookie write lock...");
        let bookie_write = agent.bookie().write_lock_blocking();
        let mut book_writer = bookie_write.write_tx(agent.booked());

        let tx = conn
            .immediate_transaction()
            .map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: Some(actor_id),
                version: None,
            })?;

        let timeout = timeout.map(Duration::from_secs);
        let tx = InterruptibleTransaction::new(tx, timeout, "query_endpoint");

        let _ = tx
            .prepare_cached("SELECT crsql_set_ts(?)")
            .map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: Some(actor_id),
                version: None,
            })?
            .query_row([&ts], |row| row.get::<_, String>(0))
            .map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: Some(actor_id),
                version: None,
            })?;

        // Execute whatever might mutate state data
        let ret = f(&tx)?;

        let insert_info = insert_local_changes(agent, &tx, &mut book_writer)?;
        tx.commit().map_err(|source| {
            let ce = ChangeError::Rusqlite {
                source,
                actor_id: Some(actor_id),
                version: insert_info.as_ref().map(|info| info.db_version),
            };
            if let Some(issue) = ce.fatal_db_issue() {
                error!("fatal DB issue detected: {issue}");
                agent.mark_unhealthy(issue);
            }
            ce
        })?;

        let elapsed = start.elapsed();
        histogram!("corro.agent.changes.processing.time.seconds", "source" => "local")
            .record(start.elapsed());

        match insert_info {
            None => Ok((ret, None, elapsed)),
            Some(InsertChangesInfo {
                db_version,
                last_seq,
                ts,
            }) => {
                trace!("committed tx, db_version: {db_version}, last_seq: {last_seq:?}");

                book_writer.commit();

                let agent = agent.clone();

                spawn_counted(
                    async move { broadcast_changes(agent, db_version, last_seq, ts).await },
                );

                Ok::<_, ChangeError>((ret, Some(db_version), elapsed))
            }
        }
    })
}

#[tracing::instrument(skip_all, err)]
fn execute_statement<T>(
    tx: &InterruptibleTransaction<T>,
    stmt: &Statement,
) -> rusqlite::Result<usize>
where
    T: Deref<Target = rusqlite::Connection> + Committable,
{
    let mut prepped = tx.prepare(stmt.query())?;

    match stmt {
        Statement::Simple(_)
        | Statement::Verbose {
            params: None,
            named_params: None,
            ..
        } => prepped.execute([]),
        Statement::WithParams(_, params)
        | Statement::Verbose {
            params: Some(params),
            ..
        } => prepped.execute(params_from_iter(params)),
        Statement::WithNamedParams(_, params)
        | Statement::Verbose {
            named_params: Some(params),
            ..
        } => prepped.execute(
            params
                .iter()
                .map(|(k, v)| (k.as_str(), v as &dyn ToSql))
                .collect::<Vec<(&str, &dyn ToSql)>>()
                .as_slice(),
        ),
    }
}

#[tracing::instrument(skip_all)]
pub async fn api_v1_transactions(
    // axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
    Extension(agent): Extension<Agent>,
    axum::extract::Query(params): axum::extract::Query<TimeoutParams>,
    axum::extract::Json(statements): axum::extract::Json<Vec<Statement>>,
) -> (StatusCode, axum::Json<ExecResponse>) {
    let actor_id = agent.actor_id().to_string();
    if statements.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            axum::Json(ExecResponse {
                results: vec![ExecResult::Error {
                    error: "at least 1 statement is required".into(),
                }],
                time: 0.0,
                version: None,
                actor_id: Some(actor_id),
            }),
        );
    }

    counter!("corro.api.connection.count", "protocol" => "http").increment(1);
    assert_sometimes!(true, "Corrosion receives transactions through HTTP API");
    let res = make_broadcastable_changes(&agent, params.timeout, move |tx| {
        let mut total_rows_affected = 0;

        let results = statements
            .iter()
            .map(|stmt| {
                let start = Instant::now();
                let res = execute_statement(tx, stmt).map_err(|e| ChangeError::Rusqlite {
                    source: e,
                    actor_id: None,
                    version: None,
                });

                match res {
                    Ok(rows_affected) => {
                        total_rows_affected += rows_affected;
                        Ok(ExecResult::Execute {
                            rows_affected,
                            time: start.elapsed().as_secs_f64(),
                        })
                    }
                    Err(e) => Err(e),
                }
            })
            .collect::<Result<Vec<ExecResult>, ChangeError>>();

        results
    })
    .await;

    let (results, version, elapsed) = match res {
        Ok(res) => res,
        Err(e) => {
            error!("could not execute statement(s): {e}");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(ExecResponse {
                    results: vec![ExecResult::Error {
                        error: e.to_string(),
                    }],
                    time: 0.0,
                    version: None,
                    actor_id: Some(actor_id),
                }),
            );
        }
    };

    (
        StatusCode::OK,
        axum::Json(ExecResponse {
            results,
            time: elapsed.as_secs_f64(),
            version: version.map(Into::into),
            actor_id: Some(actor_id),
        }),
    )
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SchemaResponse {
    /// True whenever `execute_schema` (the local DDL apply) succeeded,
    /// regardless of whether the `corro_ddl_log` write/broadcast afterwards
    /// also succeeded. On the 403/400 paths (disabled, empty body, reserved
    /// table, or a rejected/invalid DDL statement) nothing was applied, so
    /// this is `false`. On the 500 path (log write failed after a successful
    /// apply) the schema change IS live locally, so this is `true` even
    /// though `error` is set.
    pub applied: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub time: f64,
}

/// `POST /v1/schema`: apply runtime (additive-only) DDL and, on success,
/// record it as a single row in the `corro_ddl_log` CRR table so the change
/// broadcasts to the rest of the cluster; receivers replay it in seq order via
/// `apply_pending_ddl`, and this endpoint spawns the same sweep after the log
/// write to keep the control plane's own progress current.
///
/// 门禁顺序:runtime-schema 开关 -> 非空校验 -> corro_ddl_log 保留表守卫(与
/// execute_schema_from_paths 的守卫镜像,见 agent/util.rs)-> 本地 apply(增量
/// 校验,破坏性变更 400)-> 写 corro_ddl_log 一行(seq = MAX+1,单一控制面写入
/// 无并发冲突)。本地 apply 成功但日志写失败时返回 500,重复 POST 是 no-op,
/// 调用方可安全重试。
///
/// 崩溃窗口:本地 apply(execute_schema 的事务)与写日志
/// (make_broadcastable_changes 的事务)是两个独立事务,中间没有原子性保证。
/// 若进程在两者之间崩溃:schema 已在本地生效,但 corro_ddl_log 未写入、也
/// 未广播给其他节点;此时调用方连响应都收不到(连接直接断开),因此必须把
/// "无响应"当作未知结果处理——原样重试同一 POST 即可:execute_schema 对已
/// 应用过的 DDL 是幂等 no-op(不会二次报错),重试请求会顺着流程走到写日志
/// 那一步,把这次真正补上。调用方不应假设"没收到响应"等于"没有生效"。
#[tracing::instrument(skip_all)]
pub async fn api_v1_schema(
    Extension(agent): Extension<Agent>,
    axum::extract::Json(statements): axum::extract::Json<Vec<String>>,
) -> (StatusCode, axum::Json<SchemaResponse>) {
    let start = Instant::now();
    let reply = |code: StatusCode, applied: bool, err: Option<String>| {
        (
            code,
            axum::Json(SchemaResponse {
                applied,
                error: err,
                time: start.elapsed().as_secs_f64(),
            }),
        )
    };

    if !agent.config().api.allow_runtime_schema {
        return reply(
            StatusCode::FORBIDDEN,
            false,
            Some(
                "runtime schema updates are disabled on this node (api.allow_runtime_schema)"
                    .into(),
            ),
        );
    }
    if statements.is_empty() {
        return reply(
            StatusCode::BAD_REQUEST,
            false,
            Some("at least one statement is required".into()),
        );
    }

    // corro_ddl_log 是 corrosion 自有控制表,不能被当作普通用户表重新定义/改写。
    // 镜像 execute_schema_from_paths(agent/util.rs)里的同一守卫。
    match parse_sql(&statements.join(";")) {
        Ok(parsed) => {
            if parsed
                .tables
                .contains_key(corro_types::schema::DDL_LOG_TABLE)
            {
                return reply(
                    StatusCode::BAD_REQUEST,
                    false,
                    Some(format!(
                        "table '{}' is reserved by corrosion (runtime DDL log) and cannot be defined via POST /v1/schema",
                        corro_types::schema::DDL_LOG_TABLE
                    )),
                );
            }
        }
        Err(e) => {
            return reply(StatusCode::BAD_REQUEST, false, Some(e.to_string()));
        }
    }

    // 先本地应用:execute_schema 自带增量校验,破坏性变更(删表/删列/改列)
    // 会返回 SchemaError("won't drop/remove/change ... without the destructive flag")→ 400。
    // 本地成功后才写日志表,保证 corro_ddl_log 里只有可应用的 DDL。
    if let Err(e) = crate::agent::util::execute_schema(&agent, statements.clone()).await {
        return reply(StatusCode::BAD_REQUEST, false, Some(e.to_string()));
    }

    // 从这里开始 schema 已经在本地生效(applied=true 与日志写入结果无关)——
    // 见上方崩溃窗口说明与 SchemaResponse::applied 的文档注释。
    //
    // 一次 POST = 一行日志(语句合并),seq = MAX+1(单一控制面写入,无冲突)。
    // seq 计的是"被接受的 POST 次数",不是去重后的 DDL 次数:同一条语句重复
    // POST(幂等 no-op apply)仍会各自追加一行重复记录。因此任何回放这份日志
    // 的消费方(Task 6 的远端 apply 钩子)必须逐行走 execute_schema(增量 diff、
    // 天然幂等),绝不能把 sql 列当成可以直接裸执行的语句——重复行直接
    // execute 可能在没有增量校验的情况下出错或产生非预期效果。
    let joined = statements.join(";\n");
    let res = make_broadcastable_changes(&agent, None, move |tx| {
        tx.prepare_cached(
            "INSERT INTO corro_ddl_log (seq, sql, created_at) \
             VALUES ((SELECT COALESCE(MAX(seq), 0) + 1 FROM corro_ddl_log), ?, ?)",
        )
        .map_err(|source| ChangeError::Rusqlite {
            source,
            actor_id: None,
            version: None,
        })?
        .execute(rusqlite::params![joined, ddl_log_created_at()])
        .map_err(|source| ChangeError::Rusqlite {
            source,
            actor_id: None,
            version: None,
        })?;
        Ok(())
    })
    .await;

    match res {
        // 本地已 apply 但日志写失败:返回 500,调用方重试;重试时 execute_schema
        // 对已生效的 DDL 是 no-op,只会把这次日志行补上,安全。
        Err(e) => reply(StatusCode::INTERNAL_SERVER_ERROR, true, Some(e.to_string())),
        Ok(_) => {
            // 控制面的 corro_ddl_log 写入直接落库(make_broadcastable_changes),不走
            // process_multiple_changes,故不会触发提交钩子。这里显式跑一次 apply_pending_ddl
            // 推进本节点的 ddl_log_applied_seq_v1:自己的行经 execute_schema 是 no-op,进度
            // 单调前进,避免每次启动扫尾把整段 DDL 历史当 no-op 重放(O(n)/boot)。同时自愈
            // 此前任何滞后进度。
            spawn_counted(crate::agent::util::apply_pending_ddl(agent.clone()));
            reply(StatusCode::OK, true, None)
        }
    }
}

/// RFC3339 timestamp for `corro_ddl_log.created_at`. Falls back to the
/// `Display` format (only reachable if formatting somehow fails, which
/// `OffsetDateTime::now_utc()` never triggers in practice) rather than
/// panicking on a control-plane write.
fn ddl_log_created_at() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| time::OffsetDateTime::now_utc().to_string())
}

#[derive(Debug, Deserialize)]
pub struct InterestUpdate {
    /// Full desired set of tables this node should replicate (not a delta).
    /// `["*"]` means full replication; must be explicit — an empty list is rejected.
    pub tables: Vec<String>,
    /// Monotonic placement fencing token. Must be > 0 and strictly increasing
    /// versus the persisted `interest_epoch_v1`.
    pub epoch: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InterestResponse {
    /// True only when the placement change was accepted and committed
    /// (`node_interest` reconciled + epoch persisted). Every 4xx/5xx path is
    /// `false`, and on those paths the hot-swapped config has been rolled back.
    pub accepted: bool,
    /// True when at least one newly-added table was inserted as `active = 0`
    /// (pending backfill); a background task will flip it to `active = 1` once
    /// the reopened history is synced. False for pure removals / no-ops.
    pub pending_activation: bool,
    /// Number of per-actor filtered-version ranges reopened by the interest
    /// expansion (0 when nothing was previously filtered out).
    pub reopened_ranges: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub time: f64,
}

/// `POST /v1/interest`: change THIS node's replication interest at runtime,
/// mirroring the boot-time placement protocol (`run_root.rs`) step-for-step but
/// triggered live. The seven steps below map 1:1 onto the boot orchestration:
/// epoch fencing precheck → hot-swap `gossip.interest`/`interest_epoch` config →
/// reopen previously-filtered history → reconcile `node_interest` (active gating
/// + removal min-replica guard, second epoch check inside the txn) → spawn the
/// async activation task.
///
/// Config hot-swap is the whole lever: every sync/reconcile read point calls
/// `agent.config()` fresh, so `set_config` makes the new scope effective
/// immediately without parameterizing those functions.
///
/// Rollback: on ANY failure in the reopen/reconcile steps the config is swapped
/// back to the pre-request value before replying. That leaves a brief window
/// where sync used the new scope and then reverted — directionally harmless:
/// an over-accept just stores a bit more, an over-filter is repaired by
/// anti-entropy, and the recorded filtered ranges make even that recoverable.
///
/// `live_actor_ids` reflects SWIM's current membership view; a holder that just
/// died but hasn't yet been marked Down briefly still counts as live. That is
/// inherent SWIM latency and identical to the boot path's removal guard.
#[tracing::instrument(skip_all)]
pub async fn api_v1_interest(
    Extension(agent): Extension<Agent>,
    Extension(tripwire): Extension<Tripwire>,
    axum::extract::Json(req): axum::extract::Json<InterestUpdate>,
) -> (StatusCode, axum::Json<InterestResponse>) {
    let start = Instant::now();
    let reply = |code: StatusCode,
                 accepted: bool,
                 pending_activation: bool,
                 reopened_ranges: usize,
                 error: Option<String>| {
        (
            code,
            axum::Json(InterestResponse {
                accepted,
                pending_activation,
                reopened_ranges,
                error,
                time: start.elapsed().as_secs_f64(),
            }),
        )
    };

    // ① gates: flag, table syntax, epoch.
    if !agent.config().api.allow_runtime_interest {
        return reply(
            StatusCode::FORBIDDEN,
            false,
            false,
            0,
            Some(
                "runtime interest updates are disabled on this node (api.allow_runtime_interest)"
                    .into(),
            ),
        );
    }
    if req.tables.is_empty() {
        // `interest` semantics: empty = "care about everything". We refuse to
        // infer that here — the caller must say so explicitly with ["*"], the
        // same constraint boot-time epoch validation enforces.
        return reply(
            StatusCode::BAD_REQUEST,
            false,
            false,
            0,
            Some(
                "tables must be a non-empty list; use [\"*\"] for full replication".into(),
            ),
        );
    }
    if req.tables.iter().any(|table| table.trim().is_empty()) {
        return reply(
            StatusCode::BAD_REQUEST,
            false,
            false,
            0,
            Some("table names must be non-empty strings".into()),
        );
    }
    if req.epoch == 0 {
        return reply(
            StatusCode::BAD_REQUEST,
            false,
            false,
            0,
            Some("epoch must be greater than 0".into()),
        );
    }
    // Dedupe; `desired` is the canonical set used for both the precheck and the
    // hot-swapped config (order-insensitive, matching reconcile's set semantics).
    let desired: BTreeSet<String> = req.tables.iter().map(|t| t.trim().to_string()).collect();
    let tables: Vec<String> = desired.iter().cloned().collect();

    // ② epoch fencing precheck (outside any write txn): read the applied epoch
    // and whether the placement actually changes, then reuse check_interest_epoch.
    // This is the loud, cheap gate before we mutate config; reconcile re-checks
    // the same predicate inside its txn as a belt-and-suspenders guard.
    let precheck = match agent.pool().read().await {
        Ok(conn) => block_in_place(|| -> Result<(Option<u64>, bool), rusqlite::Error> {
            let current = match conn.prepare_cached(
                "SELECT table_name FROM node_interest WHERE actor_id = crsql_site_id()",
            ) {
                Ok(mut stmt) => stmt
                    .query_map([], |row| row.get::<_, String>(0))?
                    .collect::<Result<BTreeSet<_>, _>>()?,
                // node_interest may not be defined (fail-soft, same as boot readers).
                Err(e) if e.to_string().contains("no such table: node_interest") => {
                    BTreeSet::new()
                }
                Err(e) => return Err(e),
            };
            let applied = conn
                .prepare_cached("SELECT value FROM __corro_state WHERE key = ?")?
                .query_row([INTEREST_EPOCH_STATE_KEY], |row| row.get::<_, String>(0))
                .optional()?
                .and_then(|value| value.parse::<u64>().ok());
            Ok((applied, current != desired))
        }),
        Err(e) => {
            return reply(
                StatusCode::INTERNAL_SERVER_ERROR,
                false,
                false,
                0,
                Some(format!("could not acquire read connection: {e}")),
            );
        }
    };
    let (applied_epoch, placement_changed) = match precheck {
        Ok(v) => v,
        Err(e) => {
            return reply(
                StatusCode::INTERNAL_SERVER_ERROR,
                false,
                false,
                0,
                Some(format!("interest precheck failed: {e}")),
            );
        }
    };
    if let Err(e) = check_interest_epoch(req.epoch, applied_epoch, placement_changed) {
        return reply(StatusCode::CONFLICT, false, false, 0, Some(e.to_string()));
    }

    // ③ hot-swap config: from here sync/reconcile read the new interest scope.
    // Keep the pre-request Config for rollback on any later failure.
    let old_config: Config = {
        let guard = agent.config();
        (**guard).clone()
    };
    let mut new_config = old_config.clone();
    new_config.gossip.interest = tables.clone();
    new_config.gossip.interest_epoch = req.epoch;
    agent.set_config(new_config);

    // ④ reopen history that was previously Cleared while the table was out of
    // scope. On failure roll the config back and 500.
    let reopened_ranges = match reopen_filtered_versions_after_interest_expansion(&agent).await {
        Ok((_reopened_versions, ranges)) => ranges,
        Err(e) => {
            agent.set_config(old_config);
            return reply(
                StatusCode::INTERNAL_SERVER_ERROR,
                false,
                false,
                0,
                Some(format!("could not reopen filtered versions: {e}")),
            );
        }
    };
    let reopened_ranges_count: usize = reopened_ranges
        .values()
        .map(|ranges| ranges.iter().count())
        .sum();

    // ⑤ reconcile self-declared node_interest: additions land active=0, removals
    // are gated by the min-replica guard, and the epoch is re-checked + persisted
    // inside the txn. Any failure rolls the config back.
    let has_pending = match reconcile_own_interest(&agent, live_actor_ids(&agent)).await {
        Ok(pending) => pending,
        Err(e) => {
            agent.set_config(old_config);
            let msg = e.to_string();
            // reconcile_own_interest flattens ChangeError into eyre; map the
            // conflict-class failures (removal guard / epoch fencing) to 409 and
            // everything else to 500. Substrings match the ChangeError Display.
            let code = if msg.contains("unsafe interest removal")
                || msg.contains("stale interest epoch")
                || msg.contains("was reused for a different placement")
            {
                StatusCode::CONFLICT
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            };
            return reply(code, false, false, 0, Some(msg));
        }
    };

    // ⑥ if anything is pending backfill, spawn the single-flight activation task
    // (its internal lock serializes against the boot activation task). Mirror the
    // boot call site's parameter shape (run_root.rs).
    if has_pending {
        spawn_counted(activate_pending_interest_when_synced(
            agent.clone(),
            agent.bookie().clone(),
            reopened_ranges,
            tripwire.clone(),
        ));
    }

    // ⑦ success.
    reply(
        StatusCode::OK,
        true,
        has_pending,
        reopened_ranges_count,
        None,
    )
}

#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    #[error("pool connection acquisition error")]
    Pool(#[from] SqlitePoolError),
    #[error("sqlite error: {0}")]
    Rusqlite(#[from] rusqlite::Error),
}

// Prototype query routing only extracts straightforward SELECT table references
// such as `SELECT ... FROM table ...`. JOINs, subqueries, CTEs, and quoted edge
// cases need a real sqlite3_parser pass before this is production-complete.
fn referenced_tables(sql: &str) -> Vec<String> {
    const CLAUSE_END: &[&str] = &[
        "where",
        "group",
        "order",
        "limit",
        "having",
        "union",
        "intersect",
        "except",
    ];

    let tokens = sql_tokens(sql);
    let mut tables = BTreeSet::new();
    let mut idx = 0;

    while idx < tokens.len() {
        if !tokens[idx].eq_ignore_ascii_case("from") {
            idx += 1;
            continue;
        }

        idx += 1;
        while idx < tokens.len() {
            let token = tokens[idx].as_str();
            let lower = token.to_ascii_lowercase();
            if CLAUSE_END.contains(&lower.as_str()) || lower == "join" || lower == "on" {
                break;
            }
            if token == "," {
                idx += 1;
                continue;
            }
            if token == "(" {
                break;
            }
            if let Some(table) = normalize_table_token(token) {
                tables.insert(table);
            }

            idx += 1;
            while idx < tokens.len() && tokens[idx] != "," {
                let lower = tokens[idx].to_ascii_lowercase();
                if CLAUSE_END.contains(&lower.as_str()) || lower == "join" || lower == "on" {
                    break;
                }
                idx += 1;
            }
        }
    }

    tables.into_iter().collect()
}

fn sql_tokens(sql: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut chars = sql.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '\'' => {
                for next in chars.by_ref() {
                    if next == '\'' {
                        break;
                    }
                }
            }
            '"' | '`' => {
                if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
                let quote = ch;
                let mut quoted = String::new();
                for next in chars.by_ref() {
                    if next == quote {
                        break;
                    }
                    quoted.push(next);
                }
                if !quoted.is_empty() {
                    tokens.push(quoted);
                }
            }
            '[' => {
                if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
                let mut bracketed = String::new();
                for next in chars.by_ref() {
                    if next == ']' {
                        break;
                    }
                    bracketed.push(next);
                }
                if !bracketed.is_empty() {
                    tokens.push(bracketed);
                }
            }
            ',' | '(' | ')' => {
                if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
                tokens.push(ch.to_string());
            }
            ch if ch.is_whitespace() || ch == ';' => {
                if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
            }
            _ => current.push(ch),
        }
    }

    if !current.is_empty() {
        tokens.push(current);
    }

    tokens
}

fn normalize_table_token(token: &str) -> Option<String> {
    let table = token
        .trim_matches(|ch| matches!(ch, '"' | '\'' | '`' | '[' | ']'))
        .rsplit('.')
        .next()
        .unwrap_or(token)
        .trim();

    if table.is_empty() {
        None
    } else {
        Some(table.to_ascii_lowercase())
    }
}

async fn resolve_table_holder(agent: &Agent, table: &str) -> Option<SocketAddr> {
    let conn = match agent.pool().read().await {
        Ok(conn) => conn,
        Err(e) => {
            warn!("query routing: could not acquire connection for node_interest lookup: {e}");
            return None;
        }
    };

    block_in_place(|| {
        let actor_ids: Vec<ActorId> = {
            // wildcard "*" 节点存全部 → 可做任意表的持有者(回退/兜底路由目标)。
            let mut stmt = match conn.prepare_cached(
                "SELECT actor_id FROM node_interest \
                 WHERE (table_name = ? OR table_name = '*') AND active = 1",
            ) {
                Ok(stmt) => stmt,
                Err(e) => {
                    warn!("query routing: could not prepare node_interest lookup: {e}");
                    return None;
                }
            };

            let actor_ids = match stmt.query_map([table], |row| row.get::<_, ActorId>(0)) {
                Ok(rows) => rows.filter_map(Result::ok).collect(),
                Err(e) => {
                    warn!("query routing: could not query node_interest for {table}: {e}");
                    return None;
                }
            };
            actor_ids
        };

        let self_actor_id = agent.actor_id();
        let cluster_id = agent.cluster_id();
        let members = agent.members().read();
        // 选 RTT(ring)最优的 holder,而非首个(此前 find_map 取 SQL 顺序第一个,不认链路质量,
        // 使 RL 的低方差 placement 路由永远选不到)。ring=RTT 分桶(0=最好);未知 RTT(None)按
        // 最差处理排最后,给它被探测的机会但不优先。这样 placement 把数据放在低延迟可达的 holder
        // 才有意义(RL 集群价值的解锁点)。
        actor_ids
            .into_iter()
            .filter_map(|actor_id| {
                if actor_id == self_actor_id {
                    return None;
                }
                members.states.get(&actor_id).and_then(|state| {
                    if state.cluster_id == cluster_id && state.addr != agent.gossip_addr() {
                        Some((state.ring.unwrap_or(u8::MAX), state.addr))
                    } else {
                        None
                    }
                })
            })
            .min_by_key(|(ring, _)| *ring)
            .map(|(_, addr)| addr)
    })
}

/// `Some` means the replicated readiness table was available; `None` preserves the
/// legacy configuration-only behavior for deployments without `node_interest`.
async fn local_table_ready(agent: &Agent, table: &str) -> Option<bool> {
    let conn = match agent.pool().read().await {
        Ok(conn) => conn,
        Err(e) => {
            warn!("query routing: could not acquire connection for local readiness: {e}");
            return None;
        }
    };

    block_in_place(|| {
        conn.prepare_cached(
            "SELECT EXISTS(SELECT 1 FROM node_interest \
             WHERE actor_id = crsql_site_id() AND active = 1 \
             AND (table_name = ? OR table_name = '*'))",
        )
        .and_then(|mut stmt| stmt.query_row([table], |row| row.get(0)))
        .map_err(|e| warn!("query routing: could not read local node_interest readiness: {e}"))
        .ok()
    })
}

async fn send_query_error(data_tx: &mpsc::Sender<QueryEvent>, message: impl ToString) {
    let _ = data_tx
        .send(QueryEvent::Error(message.to_string().to_compact_string()))
        .await;
}

async fn relay_query_forward(
    agent: Agent,
    transport: Transport,
    holder_addr: SocketAddr,
    stmt: Statement,
    data_tx: mpsc::Sender<QueryEvent>,
    timeout_secs: Option<u64>,
) {
    if let Err(e) = relay_query_forward_inner(
        agent,
        transport,
        holder_addr,
        stmt,
        data_tx.clone(),
        timeout_secs,
    )
    .await
    {
        send_query_error(&data_tx, e).await;
    }
}

async fn relay_query_forward_inner(
    agent: Agent,
    transport: Transport,
    holder_addr: SocketAddr,
    stmt: Statement,
    data_tx: mpsc::Sender<QueryEvent>,
    timeout_secs: Option<u64>,
) -> Result<(), String> {
    let (send, recv) = transport
        .open_bi(holder_addr)
        .await
        .map_err(|e| format!("query routing: could not open bi stream to {holder_addr}: {e}"))?;

    let statement_json = serde_json::to_vec(&stmt)
        .map_err(|e| format!("query routing: could not encode statement: {e}"))?;
    let payload = BiPayload::V1 {
        data: BiPayloadV1::QueryForward { statement_json },
        cluster_id: agent.cluster_id(),
    };

    let mut encode_buf = BytesMut::new();
    payload
        .write_to_stream((&mut encode_buf).writer())
        .map_err(|e| format!("query routing: could not encode BiPayload: {e}"))?;

    let mut write = FramedWrite::new(
        send,
        LengthDelimitedCodec::builder()
            .max_frame_length(100 * 1_024 * 1_024)
            .new_codec(),
    );
    write
        .send(encode_buf.freeze())
        .await
        .map_err(|e| format!("query routing: could not write QueryForward: {e}"))?;
    write
        .flush()
        .await
        .map_err(|e| format!("query routing: could not flush QueryForward: {e}"))?;
    let mut send = write.into_inner();
    let _ = send.finish();

    let mut read = FramedRead::new(
        recv,
        LengthDelimitedCodec::builder()
            .max_frame_length(100 * 1_024 * 1_024)
            .new_codec(),
    );
    let read_timeout = Duration::from_secs(timeout_secs.unwrap_or(60).max(1));

    loop {
        let frame = timeout(read_timeout, StreamExt::next(&mut read))
            .await
            .map_err(|_| format!("query routing: timed out waiting for {holder_addr}"))?;

        let Some(frame) = frame else {
            break;
        };
        let frame =
            frame.map_err(|e| format!("query routing: could not read from {holder_addr}: {e}"))?;
        let qe = serde_json::from_slice::<QueryEvent>(&frame)
            .map_err(|e| format!("query routing: bad QueryEvent from {holder_addr}: {e}"))?;

        if data_tx.send(qe).await.is_err() {
            break;
        }
    }

    Ok(())
}

// 查询路由(4.4.2)复用：持有者侧 bi.rs 收到 QueryForward 后调它本地执行 + 产 QueryEvent。
pub(crate) async fn build_query_rows_response(
    agent: &Agent,
    client_addr: SocketAddr,
    data_tx: mpsc::Sender<QueryEvent>,
    stmt: Statement,
    timeout: Option<u64>,
) -> Result<(), (StatusCode, ExecResult)> {
    let (res_tx, res_rx) = oneshot::channel();

    let pool = agent.pool().clone();

    tokio::spawn(async move {
        let conn = match pool.read().await {
            Ok(conn) => conn,
            Err(e) => {
                _ = res_tx.send(Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    ExecResult::Error {
                        error: e.to_string(),
                    },
                )));
                return;
            }
        };

        // default timeout of 1 minute if no timeout is provided
        let timeout_secs = timeout.unwrap_or(60);
        let timeout: Option<Duration> =
            (timeout_secs > 0).then(|| Duration::from_secs(timeout_secs));

        let conn = InterruptibleTransaction::new(conn.conn(), timeout, "query");
        trace!(%client_addr, "Preparing statement {}", stmt.query());

        // prepare_cached:复用每连接的 prepared statement 缓存,省掉重复 SQL 解析(高 QPS 热路径)。
        // 同 SQL(尤其参数化查询)命中缓存;变化 SQL 走 rusqlite LRU(默认16)淘汰,有界不爆。
        let prepped_res = block_in_place(|| conn.prepare_cached(stmt.query()));

        let mut prepped = match prepped_res {
            Ok(prepped) => prepped,
            Err(e) => {
                _ = res_tx.send(Err((
                    StatusCode::BAD_REQUEST,
                    ExecResult::Error {
                        error: e.to_string(),
                    },
                )));
                return;
            }
        };

        if !prepped.readonly() {
            _ = res_tx.send(Err((
                StatusCode::BAD_REQUEST,
                ExecResult::Error {
                    error: "statement is not readonly".into(),
                },
            )));
            return;
        }

        block_in_place(|| {
            let col_count = prepped.column_count();
            trace!("inside block in place, col count: {col_count}");

            if let Err(e) = data_tx.blocking_send(QueryEvent::Columns(
                prepped
                    .columns()
                    .into_iter()
                    .map(|col| ColumnName(col.name().to_compact_string()))
                    .collect(),
            )) {
                error!("could not send back columns: {e}");
                return;
            }

            let start = Instant::now();

            trace!(%client_addr, "Executing statement {}", stmt.query());
            let elapsed = start.elapsed();

            let query = match &stmt {
                Statement::Simple(_)
                | Statement::Verbose {
                    params: None,
                    named_params: None,
                    ..
                } => prepped.query(()),
                Statement::WithParams(_, params)
                | Statement::Verbose {
                    params: Some(params),
                    ..
                } => prepped.query(params_from_iter(params)),
                Statement::WithNamedParams(_, params)
                | Statement::Verbose {
                    named_params: Some(params),
                    ..
                } => prepped.query(
                    params
                        .iter()
                        .map(|(k, v)| (k.as_str(), v as &dyn ToSql))
                        .collect::<Vec<(&str, &dyn ToSql)>>()
                        .as_slice(),
                ),
            };

            let mut rows = match query {
                Ok(rows) => rows,
                Err(e) => {
                    _ = res_tx.send(Err((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        ExecResult::Error {
                            error: e.to_string(),
                        },
                    )));
                    return;
                }
            };

            trace!(%client_addr, elapsed = %elapsed.as_secs(), "Statement finished executing {}", stmt.query());

            if elapsed > Duration::from_secs(10) {
                warn!(%client_addr, elapsed = %elapsed.as_secs(), "Slow read statement {}!", stmt.query());
            }

            if let Err(_e) = res_tx.send(Ok(())) {
                error!("could not send back response through oneshot channel, aborting");
                return;
            }

            let mut rowid = 1;

            trace!("about to loop through rows!");

            loop {
                match rows.next() {
                    Ok(Some(row)) => {
                        trace!("got a row: {row:?}");
                        match (0..col_count)
                            .map(|i| row.get::<_, SqliteValue>(i))
                            .collect::<rusqlite::Result<Vec<_>>>()
                        {
                            Ok(cells) => {
                                if let Err(e) =
                                    data_tx.blocking_send(QueryEvent::Row(rowid.into(), cells))
                                {
                                    error!("could not send back row: {e}");
                                    return;
                                }
                                rowid += 1;
                            }
                            Err(e) => {
                                _ = data_tx.blocking_send(QueryEvent::Error(e.to_compact_string()));
                                return;
                            }
                        }
                    }
                    Ok(None) => {
                        // done!
                        break;
                    }
                    Err(e) => {
                        _ = data_tx.blocking_send(QueryEvent::Error(e.to_compact_string()));
                        return;
                    }
                }
            }

            _ = data_tx.blocking_send(QueryEvent::EndOfQuery {
                time: elapsed.as_secs_f64(),
                change_id: None,
            });
        });
    });

    match res_rx.await {
        Ok(res) => res,
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            ExecResult::Error {
                error: e.to_string(),
            },
        )),
    }
}

pub async fn api_v1_queries(
    Extension(agent): Extension<Agent>,
    Extension(transport): Extension<Transport>,
    ConnectInfo(client_addr): ConnectInfo<SocketAddr>,
    axum::extract::Query(params): axum::extract::Query<TimeoutParams>,
    axum::extract::Json(stmt): axum::extract::Json<Statement>,
) -> impl IntoResponse {
    let (mut tx, body) = CountedBody::channel(
        persistent_gauge!("corro.api.active.streams", "source" => "queries", "protocol" => "http"),
    );

    counter!("corro.api.queries.count").increment(1);
    // TODO: timeout on data send instead of infinitely waiting for channel space.
    let (data_tx, mut data_rx) = channel(512);

    let start = Instant::now();
    tokio::spawn(async move {
        let mut buf = BytesMut::new();

        while let Some(row_res) = data_rx.recv().await {
            {
                let mut writer = (&mut buf).writer();
                if let Err(e) = serde_json::to_writer(&mut writer, &row_res) {
                    _ = tx
                        .send_data(
                            serde_json::to_vec(&serde_json::json!(QueryEvent::Error(
                                e.to_compact_string()
                            )))
                            .expect("could not serialize error json")
                            .into(),
                        )
                        .await;
                    return;
                }
            }

            buf.extend_from_slice(b"\n");

            if let Err(e) = tx.send_data(buf.split().freeze()).await {
                error!("could not send data through body's channel: {e}");
                return;
            }
        }
        debug!("query body channel done");
    });

    trace!("building query rows response...");
    assert_sometimes!(true, "Corrosion accepts queries");

    let route_table = {
        let my_interest = agent.config().gossip.interest.clone();
        // 空 interest 保留 corrosion 原生全量复制语义。非空配置还必须通过 active=1
        // readiness 门禁；新增 interest 回填期间即使配置已包含，也不会在本地查询。
        if my_interest.is_empty() {
            None
        } else {
            let my_interest = my_interest.into_iter().collect::<BTreeSet<_>>();
            let wildcard = my_interest.contains("*");
            let mut route = None;
            for table in referenced_tables(stmt.query()) {
                let configured_local = wildcard || my_interest.contains(&table);
                let ready = local_table_ready(&agent, &table)
                    .await
                    .unwrap_or(configured_local);
                if !ready {
                    route = Some(table);
                    break;
                }
            }
            route
        }
    };

    let query_res = if let Some(table) = route_table {
        match resolve_table_holder(&agent, &table).await {
            Some(holder_addr) => {
                tokio::spawn(relay_query_forward(
                    agent.clone(),
                    transport,
                    holder_addr,
                    stmt,
                    data_tx,
                    params.timeout,
                ));
                Ok(())
            }
            None => {
                send_query_error(&data_tx, format!("no holder found for table {table}")).await;
                Ok(())
            }
        }
    } else {
        build_query_rows_response(&agent, client_addr, data_tx, stmt, params.timeout).await
    };

    match query_res {
        Ok(_) => {
            histogram!("corro.api.queries.processing.time.seconds", "result" => "success")
                .record(start.elapsed());
            hyper::Response::builder()
                .status(StatusCode::OK)
                .body(axum::body::Body::new(body))
                .expect("could not build query response body")
        }
        Err((status, res)) => {
            histogram!("corro.api.queries.processing.time.seconds", "result" => "error")
                .record(start.elapsed());
            hyper::Response::builder()
                .status(status)
                .body(
                    serde_json::to_vec(&res)
                        .expect("could not serialize query error response")
                        .into(),
                )
                .expect("could not build query response body")
        }
    }
}

pub async fn api_v1_health(
    Extension(agent): Extension<Agent>,
    Query(query): Query<HealthQuery>,
) -> (StatusCode, axum::Json<HealthResponse>) {
    match check_health(&agent).await {
        Ok((gaps, members)) => {
            let status = query.failure_status.unwrap_or(503);
            let error_status =
                StatusCode::from_u16(status).unwrap_or(StatusCode::SERVICE_UNAVAILABLE);
            let p99_lag = match agent.metrics_tracker().quantile_lag(0.99) {
                Some(lag) => lag,
                None => {
                    error!("no p99 lag information available");
                    return (
                        error_status,
                        axum::Json(HealthResponse::Error(
                            "no p99 lag information available".into(),
                        )),
                    );
                }
            };

            let queue_size = agent.metrics_tracker().queue_size();
            let status = if query.gaps.is_some_and(|max| gaps > max)
                || query.max_queue.is_some_and(|max| queue_size > max)
                // we use queue size and p99 lag as a stronger metric for an unhealthy node
                // since a different node that is slow to send out changes can cause worse commit lag
                // even though the node is perfectly fine.
                || (query.p99_lag.is_some_and(|max| p99_lag > max)
                    && query.queue_size.is_none_or(|max| queue_size > max))
            {
                error_status
            } else {
                StatusCode::OK
            };
            (
                status,
                axum::Json(HealthResponse::Response {
                    gaps,
                    members,
                    p99_lag,
                    queue_size,
                }),
            )
        }
        Err(e) => {
            error!("could not check health: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(HealthResponse::Error(e.to_string())),
            )
        }
    }
}

async fn check_health(agent: &Agent) -> eyre::Result<(i64, i64)> {
    let read_conn = match agent.pool().read().await {
        Ok(conn) => conn,
        Err(e) => {
            error!("could not acquire read connection for health check: {e}");
            return Err(eyre::eyre!("unable to grab write conn"));
        }
    };

    let gaps = read_conn
        .prepare_cached("SELECT COALESCE(SUM(end - start + 1), 0) FROM __corro_bookkeeping_gaps")?
        .query_row([], |row| row.get::<_, i64>(0))?;

    let members = read_conn.prepare_cached(r#"
            SELECT COALESCE(COUNT(*), 0) FROM __corro_members WHERE json_extract(foca_state, "$.state") = "Alive""#)?
        .query_row([], |row| row.get::<_, i64>(0))?;

    Ok((gaps, members))
}
/// Query the table status of the current node
///
/// Currently this endpoint only supports querying the row count for a
/// selection of provided tables.  Table names are checked for
/// existence before querying
pub async fn api_v1_table_stats(
    Extension(agent): Extension<Agent>,
    axum::extract::Json(ts_req): axum::extract::Json<TableStatRequest>,
) -> (StatusCode, axum::Json<TableStatResponse>) {
    async fn count_table_lengths(
        agent: &Agent,
        ts_req: TableStatRequest,
    ) -> eyre::Result<(i64, Vec<String>)> {
        debug!("Querying row count for {} tables", ts_req.tables.len());
        let conn = agent.pool().read().await?;

        block_in_place(move || -> eyre::Result<(i64, Vec<String>)> {
            let valid_tables: BTreeSet<String> = conn
                .prepare_cached("select name from sqlite_schema where type = 'table'")?
                .query_map([], |row| row.get(0))?
                .filter_map(|name| name.ok())
                .collect();

            let mut invalid_tables = vec![];
            let mut total_count = 0;
            for table in ts_req.tables.into_iter() {
                if !valid_tables.contains(&table) {
                    error!("Table name {} doesn't exist!", &table);
                    invalid_tables.push(table);
                    continue;
                }

                let count: i64 = conn
                    .prepare_cached(&format!("SELECT COUNT(*) FROM {}", &table))?
                    .query_row((), |row| row.get(0))?;

                total_count += count;
            }
            Ok((total_count, invalid_tables))
        })
    }

    match count_table_lengths(&agent, ts_req).await {
        Ok((count, invalid_tables)) => (
            StatusCode::OK,
            axum::Json(TableStatResponse {
                total_row_count: count,
                invalid_tables,
            }),
        ),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(TableStatResponse {
                total_row_count: 0,
                // Since we don't know what error occurred or if any
                // tables were valid, we just return an empty list
                invalid_tables: vec![],
            }),
        ),
    }
}

#[cfg(test)]
mod tests {
    use corro_types::{
        api::RowId,
        base::CrsqlDbVersion,
        broadcast::{BroadcastInput, BroadcastV1, ChangeV1, Changeset},
        config::Config,
        schema::SqliteType,
    };
    use futures::StreamExt;
    use tokio::sync::mpsc::error::TryRecvError;
    use tokio_util::codec::{Decoder, LinesCodec};
    use tripwire::Tripwire;

    use super::*;

    use crate::{agent::setup, agent::util::execute_schema};

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_api_db_execute() -> eyre::Result<()> {
        _ = tracing_subscriber::fmt::try_init();

        let (tripwire, _tripwire_worker, _tripwire_tx) = Tripwire::new_simple();

        let dir = tempfile::tempdir()?;

        let (agent, mut agent_options) = setup(
            Config::builder()
                .db_path(dir.path().join("corrosion.db").display().to_string())
                .gossip_addr("127.0.0.1:0".parse()?)
                .api_addr("127.0.0.1:0".parse()?)
                .build()?,
            tripwire,
        )
        .await?;

        let rx_bcast = &mut agent_options.rx_bcast;

        execute_schema(&agent, vec![corro_tests::TEST_SCHEMA.to_owned()]).await?;

        let (status_code, body) = api_v1_transactions(
            Extension(agent.clone()),
            axum::extract::Query(TimeoutParams { timeout: None }),
            axum::Json(vec![Statement::WithParams(
                "insert into tests (id, text) values (?,?)".into(),
                vec!["service-id".into(), "service-name".into()],
            )]),
        )
        .await;

        println!("{body:?}");

        assert_eq!(status_code, StatusCode::OK);

        assert!(body.0.results.len() == 1);

        let msg = rx_bcast
            .recv()
            .await
            .expect("not msg received on bcast channel");

        assert!(matches!(
            msg,
            BroadcastInput::AddBroadcast(BroadcastV1::Change(ChangeV1 {
                changeset: Changeset::FullV2 {
                    version: CrsqlDbVersion(1),
                    ..
                },
                ..
            }))
        ));

        assert_eq!(agent.booked().read().last(), Some(CrsqlDbVersion(1)));

        println!("second req...");

        let (status_code, body) = api_v1_transactions(
            Extension(agent.clone()),
            axum::extract::Query(TimeoutParams { timeout: None }),
            axum::Json(vec![Statement::WithParams(
                "update tests SET text = ? where id = ?".into(),
                vec!["service-name".into(), "service-id".into()],
            )]),
        )
        .await;

        println!("{body:?}");

        assert_eq!(status_code, StatusCode::OK);

        assert!(body.0.results.len() == 1);

        // no actual changes!
        assert!(matches!(rx_bcast.try_recv(), Err(TryRecvError::Empty)));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_api_db_query() -> eyre::Result<()> {
        _ = tracing_subscriber::fmt::try_init();

        let (tripwire, _tripwire_worker, _tripwire_tx) = Tripwire::new_simple();

        let dir = tempfile::tempdir()?;

        let (agent, _agent_options) = setup(
            Config::builder()
                .db_path(dir.path().join("corrosion.db").display().to_string())
                .gossip_addr("127.0.0.1:0".parse()?)
                .api_addr("127.0.0.1:0".parse()?)
                .build()?,
            tripwire,
        )
        .await?;

        execute_schema(&agent, vec![corro_tests::TEST_SCHEMA.to_owned()]).await?;

        let (status_code, body) = api_v1_transactions(
            Extension(agent.clone()),
            axum::extract::Query(TimeoutParams { timeout: None }),
            axum::Json(vec![
                Statement::WithParams(
                    "insert into tests (id, text) values (?,?)".into(),
                    vec!["service-id".into(), "service-name".into()],
                ),
                Statement::WithParams(
                    "insert into tests (id, text) values (?,?)".into(),
                    vec!["service-id-2".into(), "service-name-2".into()],
                ),
            ]),
        )
        .await;

        // println!("{body:?}");

        assert_eq!(status_code, StatusCode::OK);

        assert!(body.0.results.len() == 2);

        println!("transaction body: {body:?}");

        let (rtt_tx, _rtt_rx) = tokio::sync::mpsc::channel(1);
        let gossip_config = agent.config().gossip.clone();
        let transport = Transport::new(&gossip_config, rtt_tx).await?;

        let res = api_v1_queries(
            Extension(agent.clone()),
            Extension(transport),
            ConnectInfo("127.0.0.1:1234".parse().unwrap()),
            axum::extract::Query(TimeoutParams { timeout: None }),
            axum::Json(Statement::Simple("select * from tests".into())),
        )
        .await
        .into_response();

        assert_eq!(res.status(), StatusCode::OK);

        let mut body = res.into_body().into_data_stream();

        let mut lines = LinesCodec::new();

        let mut buf = BytesMut::new();

        buf.extend_from_slice(&body.next().await.unwrap()?);

        let s = lines.decode(&mut buf).unwrap().unwrap();

        let cols: QueryEvent = serde_json::from_str(&s).unwrap();

        assert_eq!(cols, QueryEvent::Columns(vec!["id".into(), "text".into()]));

        buf.extend_from_slice(&body.next().await.unwrap()?);

        let s = lines.decode(&mut buf).unwrap().unwrap();

        let row: QueryEvent = serde_json::from_str(&s).unwrap();

        assert_eq!(
            row,
            QueryEvent::Row(RowId(1), vec!["service-id".into(), "service-name".into()])
        );

        buf.extend_from_slice(&body.next().await.unwrap()?);

        let s = lines.decode(&mut buf).unwrap().unwrap();

        let row: QueryEvent = serde_json::from_str(&s).unwrap();

        assert_eq!(
            row,
            QueryEvent::Row(
                RowId(2),
                vec!["service-id-2".into(), "service-name-2".into()]
            )
        );

        buf.extend_from_slice(&body.next().await.unwrap()?);

        let s = lines.decode(&mut buf).unwrap().unwrap();

        let query_evt: QueryEvent = serde_json::from_str(&s).unwrap();

        assert!(matches!(query_evt, QueryEvent::EndOfQuery { .. }));

        assert!(body.next().await.is_none());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_api_db_schema() -> eyre::Result<()> {
        _ = tracing_subscriber::fmt::try_init();
        let (tripwire, _tripwire_worker, _tripwire_tx) = Tripwire::new_simple();

        let dir = tempfile::tempdir()?;

        let (agent, _agent_options) = setup(
            Config::builder()
                .db_path(dir.path().join("corrosion.db").display().to_string())
                .gossip_addr("127.0.0.1:0".parse()?)
                .api_addr("127.0.0.1:0".parse()?)
                .build()?,
            tripwire,
        )
        .await?;

        execute_schema(
            &agent,
            vec![
                "CREATE TABLE tests2 (id BIGINT NOT NULL PRIMARY KEY, foo TEXT);".into(),
                "CREATE TABLE tests (id BIGINT NOT NULL PRIMARY KEY, foo TEXT);".into(),
            ],
        )
        .await?;

        // scope the schema reader in here
        {
            let schema = agent.schema().read();
            let tests = schema
                .tables
                .get("tests")
                .expect("no tests table in schema");

            let id_col = tests.columns.get("id").unwrap();
            assert_eq!(id_col.name, "id");
            assert_eq!(id_col.sql_type(), (SqliteType::Integer, Some("BIGINT")));
            assert!(!id_col.nullable);
            assert!(id_col.primary_key);

            let foo_col = tests.columns.get("foo").unwrap();
            assert_eq!(foo_col.name, "foo");
            assert_eq!(foo_col.sql_type(), (SqliteType::Text, Some("TEXT")));
            assert!(foo_col.nullable);
            assert!(!foo_col.primary_key);
        }

        execute_schema(
            &agent,
            vec![
                "CREATE TABLE tests2 (id BIGINT NOT NULL PRIMARY KEY, foo TEXT);".into(),
                "CREATE TABLE tests (id BIGINT NOT NULL PRIMARY KEY, foo TEXT);".into(),
            ],
        )
        .await?;

        {
            let schema = agent.schema().read();
            let tests = schema
                .tables
                .get("tests")
                .expect("no tests table in schema");

            let id_col = tests.columns.get("id").unwrap();
            assert_eq!(id_col.name, "id");
            assert_eq!(id_col.sql_type(), (SqliteType::Integer, Some("BIGINT")));
            assert!(!id_col.nullable);
            assert!(id_col.primary_key);

            let foo_col = tests.columns.get("foo").unwrap();
            assert_eq!(foo_col.name, "foo");
            assert_eq!(foo_col.sql_type(), (SqliteType::Text, Some("TEXT")));
            assert!(foo_col.nullable);
            assert!(!foo_col.primary_key);

            let tests = schema
                .tables
                .get("tests2")
                .expect("no tests2 table in schema");

            let id_col = tests.columns.get("id").unwrap();
            assert_eq!(id_col.name, "id");
            assert_eq!(id_col.sql_type(), (SqliteType::Integer, Some("BIGINT")));
            assert!(!id_col.nullable);
            assert!(id_col.primary_key);

            let foo_col = tests.columns.get("foo").unwrap();
            assert_eq!(foo_col.name, "foo");
            assert_eq!(foo_col.sql_type(), (SqliteType::Text, Some("TEXT")));
            assert!(foo_col.nullable);
            assert!(!foo_col.primary_key);
        }

        // w/ existing table!

        let create_stmt = "CREATE TABLE tests3 (id BIGINT NOT NULL PRIMARY KEY, foo TEXT, updated_at INTEGER NOT NULL DEFAULT 0);";

        {
            // adding the table and an index
            let conn = agent.pool().write_priority().await?;
            conn.execute_batch(create_stmt)?;
            conn.execute_batch("CREATE INDEX tests3_updated_at ON tests3 (updated_at);")?;
            assert_eq!(
                conn.execute(
                    "INSERT INTO tests3 VALUES (123, 'some foo text', 123456789);",
                    ()
                )?,
                1
            );
            assert_eq!(
                conn.execute(
                    "INSERT INTO tests3 VALUES (1234, 'some foo text 2', 1234567890);",
                    ()
                )?,
                1
            );
        }

        execute_schema(&agent, vec![create_stmt.to_owned()]).await?;

        {
            let schema = agent.schema().read();

            // check that the tests table is still there!
            let tests = schema
                .tables
                .get("tests")
                .expect("no tests table in schema");

            let id_col = tests.columns.get("id").unwrap();
            assert_eq!(id_col.name, "id");
            assert_eq!(id_col.sql_type(), (SqliteType::Integer, Some("BIGINT")));
            assert!(!id_col.nullable);
            assert!(id_col.primary_key);

            let foo_col = tests.columns.get("foo").unwrap();
            assert_eq!(foo_col.name, "foo");
            assert_eq!(foo_col.sql_type(), (SqliteType::Text, Some("TEXT")));
            assert!(foo_col.nullable);
            assert!(!foo_col.primary_key);

            let tests = schema
                .tables
                .get("tests3")
                .expect("no tests3 table in schema");

            let id_col = tests.columns.get("id").unwrap();
            assert_eq!(id_col.name, "id");
            assert_eq!(id_col.sql_type(), (SqliteType::Integer, Some("BIGINT")));
            assert!(!id_col.nullable);
            assert!(id_col.primary_key);

            let foo_col = tests.columns.get("foo").unwrap();
            assert_eq!(foo_col.name, "foo");
            assert_eq!(foo_col.sql_type(), (SqliteType::Text, Some("TEXT")));
            assert!(foo_col.nullable);
            assert!(!foo_col.primary_key);

            let updated_at_col = tests.columns.get("updated_at").unwrap();
            assert_eq!(updated_at_col.name, "updated_at");
            assert_eq!(
                updated_at_col.sql_type(),
                (SqliteType::Integer, Some("INTEGER"))
            );
            assert!(!updated_at_col.nullable);
            assert!(!updated_at_col.primary_key);

            let updated_at_idx = tests.indexes.get("tests3_updated_at").unwrap();
            assert_eq!(updated_at_idx.name, "tests3_updated_at");
            assert_eq!(updated_at_idx.tbl_name, "tests3");
            assert_eq!(updated_at_idx.columns.len(), 1);
            assert!(updated_at_idx.where_clause.is_none());
        }

        let conn = agent.pool().read().await?;
        let count: usize =
            conn.query_row("SELECT COUNT(*) FROM tests3__crsql_clock;", (), |row| {
                row.get(0)
            })?;
        // should've created a specific qty of clock table rows, just a sanity check!
        assert_eq!(count, 4);

        Ok(())
    }
}
