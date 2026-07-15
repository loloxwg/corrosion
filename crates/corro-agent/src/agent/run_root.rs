//! Start the root agent tasks

use std::{
    collections::{BTreeMap, BTreeSet},
    time::Instant,
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

    // Load schema from paths
    if let Err(e) = execute_schema_from_paths(&agent).await {
        error!("could not execute schema: {e}");
    }

    // 自写 interest:从 gossip.interest 写入复制表 node_interest(4.2.3 数据需求模版的节点自描述)。
    // 让 interest 配置化、不依赖外部写,且启动即可见(减轻传播竞态)。schema 须含 node_interest 表。
    write_own_interest(&agent).await;

    // interest 扩大时，必须先重新打开此前因过滤而 Cleared 的版本，再启动 sync loop。
    // 失败时停止启动，避免节点以 holder 身份对外提供不完整历史。
    let reopened = reopen_filtered_versions_after_interest_expansion(&agent).await?;
    if reopened > 0 {
        info!(
            reopened_versions = reopened,
            "reopened filtered versions after interest expansion"
        );
    }

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
    spawn_counted(handlers::handle_notifications(
        agent.clone(),
        notifications_rx,
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

    Ok((bookie, handles))
}

/// 启动时把本节点 `gossip.interest` 写入复制表 `node_interest`(走正常本地写路径→复制到全集群)。
/// 空 interest = 关心全部(全量节点),不写。`node_interest` 表须在 schema 中(否则只 warn,不致命)。
/// 对接 4.2.3「数据需求模版」的节点自描述;让 interest 配置化、不依赖外部写、启动即可见。
async fn write_own_interest(agent: &Agent) {
    let interest = agent.config().gossip.interest.clone();
    if interest.is_empty() {
        return;
    }
    let to_write = interest.clone();
    let res = make_broadcastable_changes(agent, None, move |tx| {
        // 启动即权威:先删本 actor 不在当前 interest 的旧行,再 upsert 当前。
        // 防 db 复用 + interest 变更后残留 —— 尤其旧 "*"(wildcard)行会把本节点误当
        // 任意表的全量持有者/收件人(查询路由 table='*'、推送 selector 并入所有表),破坏部分复制。
        let placeholders = std::iter::repeat("?")
            .take(to_write.len())
            .collect::<Vec<_>>()
            .join(",");
        let del_sql = format!(
            "DELETE FROM node_interest WHERE actor_id = crsql_site_id() \
             AND table_name NOT IN ({placeholders})"
        );
        tx.prepare_cached(&del_sql)
            .map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: None,
                version: None,
            })?
            .execute(rusqlite::params_from_iter(to_write.iter()))
            .map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: None,
                version: None,
            })?;
        let mut stmt = tx
            .prepare_cached(
                "INSERT OR IGNORE INTO node_interest (actor_id, table_name) \
                 VALUES (crsql_site_id(), ?)",
            )
            .map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: None,
                version: None,
            })?;
        for t in &to_write {
            stmt.execute([t]).map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: None,
                version: None,
            })?;
        }
        Ok(())
    })
    .await;
    match res {
        Ok(_) => info!("wrote self-declared interest to node_interest: {interest:?}"),
        Err(e) => {
            warn!("could not write node_interest (is the node_interest table in your schema?): {e}")
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

async fn reopen_filtered_versions_after_interest_expansion(agent: &Agent) -> eyre::Result<u64> {
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

        Ok(reopened_count)
    })
}

#[cfg(test)]
mod interest_backfill_tests {
    use super::sync_interest_expanded;

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
