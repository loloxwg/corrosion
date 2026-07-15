//! Start the root agent tasks

use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, Instant},
};

use crate::agent::util::execute_schema_from_paths;
use crate::{
    agent::{
        handlers::{self, spawn_handle_db_maintenance},
        metrics,
        reaper::spawn_reaper,
        setup, util, AgentOptions,
    },
    broadcast::runtime_loop,
    transport::Transport,
};

use crate::api::public::make_broadcastable_changes;
use corro_types::{
    actor::ActorId,
    agent::{Agent, Bookie, ChangeError},
    base::{CrsqlDbVersion, CrsqlSeq},
    bookie::{BookieDbParams, ComputedChanges},
    channel::bounded,
    config::{BroadcastStrategy, Config, PerfConfig},
};

use futures::FutureExt;
use rangemap::RangeInclusiveSet;
use rusqlite::{params, OptionalExtension};
use spawn::spawn_counted;
use tokio::task::{block_in_place, JoinHandle};
use tracing::{error, info, warn};
use tripwire::Tripwire;

/// Start a new agent with an existing configuration
///
/// First initialise `AgentOptions` state via `setup()`, then spawn a
/// new task that runs the main agent state machine
pub async fn start_with_config(
    conf: Config,
    tripwire: Tripwire,
) -> eyre::Result<(Agent, Bookie, Transport, Vec<JoinHandle<()>>)> {
    let (agent, opts) = setup(conf.clone(), tripwire.clone()).await?;
    let transport = opts.transport.clone();

    let (bookie, handles) = run(agent.clone(), opts, conf.perf).await?;

    Ok((agent, bookie, transport, handles))
}

async fn run(
    agent: Agent,
    opts: AgentOptions,
    pconf: PerfConfig,
) -> eyre::Result<(Bookie, Vec<JoinHandle<()>>)> {
    let AgentOptions {
        gossip_server_endpoint,
        transport,
        api_listeners,
        mut tripwire,
        rx_bcast,
        rx_apply,
        rx_clear_buf,
        rx_changes,
        rx_foca,
        subs_manager,
        subs_bcast_cache,
        updates_bcast_cache,
        rtt_rx,
    } = opts;

    // Get our gossip address and make sure it's valid
    let gossip_addr = gossip_server_endpoint.local_addr()?;

    //// Start PG server to accept query requests from PG clients
    // TODO: pull this out into a separate function?
    if let Some(pg_confs) = agent.config().api.pg.clone() {
        info!("Starting PostgreSQL wire-compatible server");
        for pg_conf in pg_confs {
            let pg_server = corro_pg::start(agent.clone(), pg_conf, tripwire.clone()).await?;
            info!(
                "Started PostgreSQL wire-compatible server, listening at {}",
                pg_server.local_addr
            );
        }
    }

    let (to_send_tx, to_send_rx) = bounded(pconf.to_send_channel_len, "to_send");
    let (notifications_tx, notifications_rx) =
        bounded(pconf.notifications_channel_len, "notifications");

    let member_states = util::load_member_states(&agent).await;

    //// Start the main SWIM runtime loop
    runtime_loop(
        // here the agent already has the current cluster_id, we don't need to pass one
        agent.actor(None, agent.config().gossip.member_id),
        agent.clone(),
        transport.clone(),
        rx_foca,
        rx_bcast,
        to_send_tx,
        notifications_tx,
        tripwire.clone(),
        member_states.clone(),
    );

    //// Update member connection RTTs
    handlers::spawn_rtt_handler(&agent, rtt_rx, tripwire.clone());

    handlers::spawn_swim_announcer(&agent, gossip_addr, tripwire.clone());

    // Load existing cluster members into the SWIM runtime
    util::initialise_foca(&agent, member_states).await;

    // placement 摘除门禁依赖实时成员视图，因此先消费 SWIM 通知，再协调 node_interest。
    // 此时数据同步和 HTTP API 尚未启动，不会在门禁完成前对外提供服务。
    spawn_counted(handlers::handle_notifications(
        agent.clone(),
        notifications_rx,
        tripwire.clone(),
    ));

    // Load schema from paths
    if let Err(e) = execute_schema_from_paths(&agent).await {
        error!("could not execute schema: {e}");
    }

    // interest 扩大时，必须先重新打开此前因过滤而 Cleared 的版本，再启动 sync loop。
    // 失败时停止启动，避免节点继续使用已关闭的历史缺口。
    let (reopened, reopened_ranges) =
        reopen_filtered_versions_after_interest_expansion(&agent).await?;
    if reopened > 0 {
        info!(
            reopened_versions = reopened,
            "reopened filtered versions after interest expansion"
        );
    }

    // 新 interest 先以 active=0 发布；只有历史缺口回填完成后才切换为 active=1。
    // 摘除则在同一事务内检查其它在线 ready holder 的最小副本数，不满足即停止启动。
    let ready_holder_actors = load_removal_candidate_actors(&agent).await;
    let live_actors = wait_for_restored_live_members(&agent, &ready_holder_actors).await;
    let has_pending_interest = reconcile_own_interest(&agent, live_actors).await?;

    let mut handles = vec![];
    // Setup client http API
    let mut http_handles = util::setup_http_api_handler(
        &agent,
        transport.clone(),
        &mut tripwire,
        subs_bcast_cache,
        updates_bcast_cache,
        &subs_manager,
        api_listeners,
    )
    .await?;
    handles.append(&mut http_handles);

    spawn_counted(util::clear_buffered_meta_loop(
        agent.clone(),
        rx_clear_buf,
        tripwire.clone(),
    ));

    spawn_counted(metrics::metrics_loop(
        agent.clone(),
        transport.clone(),
        tripwire.clone(),
    ));

    spawn_counted(corro_types::sqlite::query_metrics_loop(tripwire.clone()));

    spawn_counted(handlers::handle_gossip_to_send(
        transport.clone(),
        to_send_rx,
        tripwire.clone(),
    ));
    spawn_handle_db_maintenance(&agent);

    let bookie = agent.bookie().clone();

    // Bookie was fully loaded by setup(). Walk it to schedule apply for any
    // fully-buffered (gap-free) partials that were never applied before shutdown.
    let start = Instant::now();
    {
        let guard = bookie.owned_guard();
        for (&actor_id, booked) in bookie.iter(&guard) {
            let bookedr = booked.read();
            for (version, partial) in bookedr.partials.iter() {
                let gaps_count = partial.seqs.gaps(&(CrsqlSeq(0)..=partial.last_seq)).count();
                if gaps_count == 0 {
                    info!(%actor_id, %version, "found fully buffered, unapplied, changes! scheduling apply");
                    let tx_apply = agent.tx_apply().clone();
                    let version = *version;
                    tokio::spawn(async move {
                        if let Err(e) = tx_apply.send((actor_id, version)).await {
                            error!("could not schedule buffered changes application: {e}");
                        }
                    });
                }
            }
        }
    }
    info!("Checked bookie partials in {:?}", start.elapsed());

    spawn_counted(
        util::sync_loop(
            agent.clone(),
            bookie.clone(),
            transport.clone(),
            tripwire.clone(),
        )
        .inspect(|_| info!("corrosion agent sync loop is done")),
    );

    spawn_counted(
        util::apply_fully_buffered_changes_loop(
            agent.clone(),
            bookie.clone(),
            rx_apply,
            tripwire.clone(),
        )
        .inspect(|_| info!("corrosion buffered changes loop is done")),
    );

    if let Err(e) = spawn_reaper(&agent, tripwire.clone()) {
        error!("could not spawn reaper: {e}");
    }

    info!("Starting peer API on udp/{gossip_addr} (QUIC)");

    //// Start an incoming (corrosion) connection handler.  This
    //// future tree spawns additional message type sub-handlers
    handlers::spawn_gossipserver_handler(&agent, &bookie, &tripwire, gossip_server_endpoint);

    let changes_handle = spawn_counted(
        handlers::handle_changes(agent.clone(), bookie.clone(), rx_changes, tripwire.clone())
            .inspect(|_| info!("corrosion handle changes loop is done")),
    );
    handles.push(changes_handle);

    if has_pending_interest {
        spawn_counted(activate_pending_interest_when_synced(
            agent.clone(),
            bookie.clone(),
            reopened_ranges,
            tripwire.clone(),
        ));
    }

    Ok((bookie, handles))
}

async fn wait_for_restored_live_members(
    agent: &Agent,
    expected: &BTreeSet<ActorId>,
) -> BTreeSet<ActorId> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let live = agent
            .members()
            .read()
            .states
            .keys()
            .copied()
            .collect::<BTreeSet<_>>();
        if expected.is_subset(&live) || tokio::time::Instant::now() >= deadline {
            return live;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn load_removal_candidate_actors(agent: &Agent) -> BTreeSet<ActorId> {
    let desired = agent
        .config()
        .gossip
        .interest
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let Ok(conn) = agent.pool().read().await else {
        return BTreeSet::new();
    };
    block_in_place(|| {
        let removed = conn
            .prepare_cached(
                "SELECT table_name FROM node_interest \
                 WHERE actor_id = crsql_site_id() AND active = 1",
            )
            .and_then(|mut stmt| {
                stmt.query_map([], |row| row.get::<_, String>(0))?
                    .filter(|row| row.as_ref().is_ok_and(|table| !desired.contains(table)))
                    .collect::<Result<Vec<_>, _>>()
            })
            .unwrap_or_default();
        if removed.is_empty() {
            return BTreeSet::new();
        }

        conn.prepare_cached(
            "SELECT actor_id, table_name FROM node_interest \
             WHERE active = 1 AND actor_id != crsql_site_id()",
        )
        .and_then(|mut stmt| {
            stmt.query_map([], |row| {
                Ok((row.get::<_, ActorId>(0)?, row.get::<_, String>(1)?))
            })?
            .filter(|row| {
                row.as_ref().is_ok_and(|(_, holder_table)| {
                    removed.iter().any(|removed_table| {
                        if removed_table == "*" {
                            holder_table == "*"
                        } else {
                            holder_table == removed_table || holder_table == "*"
                        }
                    })
                })
            })
            .map(|row| row.map(|(actor_id, _)| actor_id))
            .collect::<Result<BTreeSet<_>, _>>()
        })
        .unwrap_or_default()
    })
}

async fn reconcile_own_interest(
    agent: &Agent,
    live_actors: BTreeSet<ActorId>,
) -> eyre::Result<bool> {
    // 新增项先写 active=0，等待历史回填完成后再发布为 ready；删除 active=1 项前，必须有
    // `interest_min_replicas` 个其它在线 ready holder。控制器必须串行提交 placement 变更，
    // 因为复制表本身不提供跨节点的线性一致 compare-and-swap。
    let interest = agent.config().gossip.interest.clone();
    if interest.is_empty() {
        return Ok(false);
    }
    let desired = interest.iter().cloned().collect::<BTreeSet<_>>();
    let required = agent.config().gossip.interest_min_replicas.max(1);
    let desired_for_tx = desired.clone();
    let res = make_broadcastable_changes(agent, None, move |tx| {
        let current = tx
            .prepare_cached(
                "SELECT table_name, active FROM node_interest \
                 WHERE actor_id = crsql_site_id()",
            )
            .map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: None,
                version: None,
            })?
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, bool>(1)?))
            })
            .map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: None,
                version: None,
            })?
            .collect::<Result<BTreeMap<_, _>, _>>()
            .map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: None,
                version: None,
            })?;

        let ready_holders = tx
            .prepare_cached(
                "SELECT actor_id, table_name FROM node_interest \
                 WHERE active = 1 AND actor_id != crsql_site_id()",
            )
            .map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: None,
                version: None,
            })?
            .query_map([], |row| {
                Ok((row.get::<_, ActorId>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: None,
                version: None,
            })?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: None,
                version: None,
            })?;

        for (table, active) in current
            .iter()
            .filter(|(table, _)| !desired_for_tx.contains(*table))
        {
            if *active {
                let ready = count_ready_holders(table, &ready_holders, &live_actors);
                if ready < required {
                    return Err(ChangeError::InterestRemovalUnsafe {
                        table: table.clone(),
                        ready,
                        required,
                    });
                }
            }
            tx.execute(
                "DELETE FROM node_interest \
                 WHERE actor_id = crsql_site_id() AND table_name = ?",
                &[table as &dyn rusqlite::ToSql],
            )
            .map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: None,
                version: None,
            })?;
        }

        for table in desired_for_tx
            .iter()
            .filter(|table| !current.contains_key(*table))
        {
            tx.execute(
                "INSERT INTO node_interest (actor_id, table_name, active) \
                 VALUES (crsql_site_id(), ?, 0)",
                &[table as &dyn rusqlite::ToSql],
            )
            .map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: None,
                version: None,
            })?;
        }

        Ok(desired_for_tx
            .iter()
            .any(|table| !current.get(table).copied().unwrap_or(false)))
    })
    .await;
    match res {
        Ok((pending, _, _)) => {
            info!(?interest, pending, "reconciled self-declared node interest");
            Ok(pending)
        }
        Err(ChangeError::Rusqlite { ref source, .. })
            if source.to_string().contains("no such table: node_interest") =>
        {
            warn!("could not write node_interest because the schema does not define it");
            Ok(false)
        }
        Err(e) => Err(eyre::eyre!("could not safely reconcile node_interest: {e}")),
    }
}

fn count_ready_holders(
    removed_table: &str,
    ready_holders: &[(ActorId, String)],
    live_actors: &BTreeSet<ActorId>,
) -> usize {
    ready_holders
        .iter()
        .filter(|(actor_id, table)| {
            live_actors.contains(actor_id)
                && if removed_table == "*" {
                    table == "*"
                } else {
                    table == removed_table || table == "*"
                }
        })
        .map(|(actor_id, _)| actor_id)
        .collect::<BTreeSet<_>>()
        .len()
}

fn reopened_ranges_are_complete(
    bookie: &Bookie,
    reopened: &BTreeMap<ActorId, RangeInclusiveSet<CrsqlDbVersion>>,
) -> bool {
    reopened.iter().all(|(actor_id, ranges)| {
        let Some(booked) = bookie.get(actor_id) else {
            return false;
        };
        let booked = booked.read();
        let has_needed = booked.needed().iter().any(|needed| {
            ranges.iter().any(|reopened| {
                needed.start() <= reopened.end() && reopened.start() <= needed.end()
            })
        });
        let has_partial = booked
            .partials
            .keys()
            .any(|version| ranges.iter().any(|range| range.contains(version)));
        !has_needed && !has_partial
    })
}

fn initial_sync_is_complete(agent: &Agent, bookie: &Bookie) -> bool {
    let members_synced = agent
        .members()
        .read()
        .states
        .values()
        .all(|state| state.last_sync_ts.is_some());
    let guard = bookie.owned_guard();
    let bookie_quiescent = bookie.iter(&guard).all(|(_, booked)| {
        let booked = booked.read();
        booked.needed().is_empty() && booked.partials.is_empty()
    });
    members_synced && bookie_quiescent
}

async fn activate_pending_interest_when_synced(
    agent: Agent,
    bookie: Bookie,
    reopened_ranges: BTreeMap<ActorId, RangeInclusiveSet<CrsqlDbVersion>>,
    mut tripwire: Tripwire,
) {
    let mut consecutive_quiescent = 0;
    let started = Instant::now();
    loop {
        tokio::select! {
            _ = &mut tripwire => return,
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
        }

        let complete = if reopened_ranges.is_empty() {
            started.elapsed() >= Duration::from_secs(5) && initial_sync_is_complete(&agent, &bookie)
        } else {
            reopened_ranges_are_complete(&bookie, &reopened_ranges)
        };
        if complete {
            consecutive_quiescent += 1;
        } else {
            consecutive_quiescent = 0;
        }
        if consecutive_quiescent < 2 {
            continue;
        }

        match make_broadcastable_changes(&agent, None, |tx| {
            tx.execute(
                "UPDATE node_interest SET active = 1 \
                 WHERE actor_id = crsql_site_id() AND active = 0",
                &[],
            )
            .map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: None,
                version: None,
            })
        })
        .await
        {
            Ok((activated, _, _)) => {
                info!(activated, "activated ready node interest after backfill");
                return;
            }
            Err(e) => {
                warn!("could not activate pending node interest; will retry: {e}");
                consecutive_quiescent = 0;
            }
        }
    }
}

const SYNC_INTEREST_STATE_KEY: &str = "sync_interest_v1";

fn effective_sync_interest(agent: &Agent) -> Vec<String> {
    let cfg = agent.config();
    if matches!(cfg.gossip.broadcast_strategy, BroadcastStrategy::Random)
        || cfg.gossip.interest.is_empty()
        || cfg.gossip.interest.iter().any(|table| table == "*")
    {
        return vec!["*".to_string()];
    }

    cfg.gossip
        .interest
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn sync_interest_expanded(previous: &[String], current: &[String]) -> bool {
    let previous_full = previous.iter().any(|table| table == "*");
    let current_full = current.iter().any(|table| table == "*");
    if previous_full {
        return false;
    }
    if current_full {
        return true;
    }

    let previous = previous.iter().collect::<BTreeSet<_>>();
    current.iter().any(|table| !previous.contains(table))
}

async fn reopen_filtered_versions_after_interest_expansion(
    agent: &Agent,
) -> eyre::Result<(u64, BTreeMap<ActorId, RangeInclusiveSet<CrsqlDbVersion>>)> {
    let current = effective_sync_interest(agent);
    let current_json = serde_json::to_string(&current)?;
    let bookie = agent.bookie().clone();
    let mut conn = agent.pool().write_normal().await?;

    block_in_place(move || {
        let bookie_write = bookie.write_lock_blocking();
        let tx = conn.immediate_transaction()?;
        let previous_json: Option<String> = tx
            .prepare_cached("SELECT value FROM __corro_state WHERE key = ?")?
            .query_row([SYNC_INTEREST_STATE_KEY], |row| row.get(0))
            .optional()?;
        let previous = previous_json
            .as_deref()
            .map(serde_json::from_str::<Vec<String>>)
            .transpose()?;
        let expanded = previous
            .as_deref()
            .is_some_and(|previous| sync_interest_expanded(previous, &current));

        let mut reopened: BTreeMap<ActorId, RangeInclusiveSet<CrsqlDbVersion>> = BTreeMap::new();
        if expanded {
            let mut stmt = tx.prepare_cached(
                "SELECT actor_id, start, end FROM __corro_filtered_version_ranges",
            )?;
            let rows = stmt.query_map([], |row| {
                Ok((
                    row.get::<_, ActorId>(0)?,
                    row.get::<_, CrsqlDbVersion>(1)?,
                    row.get::<_, CrsqlDbVersion>(2)?,
                ))
            })?;
            for row in rows {
                let (actor_id, start, end) = row?;
                reopened.entry(actor_id).or_default().insert(start..=end);
            }
        }

        let reopened_count = reopened
            .values()
            .flat_map(|ranges| ranges.iter())
            .map(|range| u64::from(*range.end()) - u64::from(*range.start()) + 1)
            .sum();
        let reopened_for_readiness = reopened.clone();
        let mut booked_writes = Vec::with_capacity(reopened.len());
        let mut changes = Vec::with_capacity(reopened.len());
        for (actor_id, ranges) in reopened {
            let booked = bookie.ensure(actor_id);
            let mut booked_write = bookie_write.write_tx(&booked);
            let gaps = booked_write.compute_and_apply_reopened_gaps(ranges);
            changes.push(ComputedChanges::new(actor_id).with_gaps(gaps));
            booked_writes.push(booked_write);
        }
        BookieDbParams::from_changes(&changes).execute(&tx)?;

        if expanded {
            tx.execute("DELETE FROM __corro_filtered_version_ranges", [])?;
        }
        tx.execute(
            "INSERT OR REPLACE INTO __corro_state (key, value) VALUES (?, ?)",
            params![SYNC_INTEREST_STATE_KEY, current_json],
        )?;
        tx.commit()?;
        for booked_write in booked_writes {
            booked_write.commit();
        }

        Ok((reopened_count, reopened_for_readiness))
    })
}

#[cfg(test)]
mod interest_backfill_tests {
    use super::{count_ready_holders, sync_interest_expanded};
    use corro_types::actor::ActorId;
    use std::collections::BTreeSet;

    fn actor(byte: u8) -> ActorId {
        ActorId::from_bytes([byte; 16])
    }

    #[test]
    fn removal_counts_distinct_online_covering_holders() {
        let holders = vec![
            (actor(1), "target".into()),
            (actor(1), "*".into()),
            (actor(2), "*".into()),
            (actor(3), "target".into()),
            (actor(4), "flight".into()),
        ];
        let live = BTreeSet::from([actor(1), actor(2), actor(4)]);

        assert_eq!(count_ready_holders("target", &holders, &live), 2);
        assert_eq!(count_ready_holders("*", &holders, &live), 2);
        assert_eq!(count_ready_holders("flight", &holders, &live), 3);
    }

    #[test]
    fn detects_only_effective_interest_expansion() {
        assert!(sync_interest_expanded(
            &["flight".into()],
            &["flight".into(), "target".into()]
        ));
        assert!(sync_interest_expanded(&["flight".into()], &["*".into()]));
        assert!(sync_interest_expanded(
            &["flight".into()],
            &["target".into()]
        ));
        assert!(!sync_interest_expanded(
            &["flight".into(), "target".into()],
            &["flight".into()]
        ));
        assert!(!sync_interest_expanded(&["*".into()], &["flight".into()]));
        assert!(!sync_interest_expanded(
            &["flight".into()],
            &["flight".into()]
        ));
    }
}
