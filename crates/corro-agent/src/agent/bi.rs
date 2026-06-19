use crate::api::peer::serve_sync;
use corro_types::{
    agent::{Agent, Bookie},
    api::Statement,
    broadcast::{BiPayload, BiPayloadV1},
};
use futures::SinkExt;
use metrics::counter;
use speedy::Readable;
use std::time::Duration;
use tokio::time::timeout;
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tracing::{debug, error, trace, warn};
use tripwire::Tripwire;

/// Spawn a task that listens for incoming bi-directional sync streams
/// on a given connection.
///
/// For every incoming stream, spawn another task to handle the
/// stream.  Valid incoming BiPayload messages are passed to
/// `crate::api::peer::serve_sync()`
pub fn spawn_bipayload_handler(
    agent: &Agent,
    bookie: &Bookie,
    tripwire: &Tripwire,
    conn: &quinn::Connection,
) {
    let conn = conn.clone();
    let agent = agent.clone();
    let bookie = bookie.clone();
    let mut tripwire = tripwire.clone();
    tokio::spawn(async move {
        loop {
            let (tx, rx) = tokio::select! {
                tx_rx_res = conn.accept_bi() => match tx_rx_res {
                    Ok(tx_rx) => tx_rx,
                    Err(e) => {
                        debug!("could not accept bidirectional stream from connection: {e}");
                        return;
                    }
                },
                _ = &mut tripwire => {
                    debug!("connection cancelled");
                    return;
                }
            };

            counter!("corro.peer.stream.accept.total", "type" => "bi").increment(1);

            trace!(
                "accepted a bidirectional stream from {}",
                conn.remote_address()
            );

            // TODO: implement concurrency limit for sync requests
            let remote = conn.remote_address();
            tokio::spawn({
                let agent = agent.clone();
                let bookie = bookie.clone();
                async move {
                    let mut framed = FramedRead::new(
                        rx,
                        LengthDelimitedCodec::builder()
                            .max_frame_length(100 * 1_024 * 1_024)
                            .new_codec(),
                    );

                    loop {
                        match timeout(Duration::from_secs(5), StreamExt::next(&mut framed)).await {
                            Err(_e) => {
                                warn!("timed out receiving bidirectional frame");
                                return;
                            }
                            Ok(None) => {
                                return;
                            }
                            Ok(Some(res)) => match res {
                                Ok(b) => {
                                    match BiPayload::read_from_buffer(&b) {
                                        Ok(payload) => {
                                            match payload {
                                                BiPayload::V1 { data, cluster_id } => match data {
                                                    BiPayloadV1::SyncStart {
                                                        actor_id,
                                                        trace_ctx,
                                                        interest,
                                                    } => {
                                                        trace!(
                                                            "framed read buffer len: {}",
                                                            framed.read_buffer().len()
                                                        );

                                                        // println!("got sync state: {state:?}");
                                                        if let Err(e) = serve_sync(
                                                            &agent, &bookie, actor_id, trace_ctx,
                                                            cluster_id, interest, framed, tx,
                                                        )
                                                        .await
                                                        {
                                                            warn!("could not complete receiving sync: {e}");
                                                        }
                                                        break;
                                                    }
                                                    BiPayloadV1::QueryForward {
                                                        statement_json,
                                                    } => {
                                                        // 持有者侧(4.4.2)：本地执行转发来的查询，结果行流式回传。
                                                        let stmt: Statement = match serde_json::from_slice(&statement_json) {
                                                            Ok(s) => s,
                                                            Err(e) => {
                                                                warn!("query forward: bad statement json: {e}");
                                                                break;
                                                            }
                                                        };
                                                        let (qe_tx, mut qe_rx) =
                                                            tokio::sync::mpsc::channel(512);
                                                        let agent2 = agent.clone();
                                                        tokio::spawn(async move {
                                                            if let Err(e) = crate::api::public::build_query_rows_response(
                                                                &agent2, remote, qe_tx, stmt, Some(30),
                                                            )
                                                            .await
                                                            {
                                                                warn!("query forward: local exec failed: {e:?}");
                                                            }
                                                        });
                                                        let mut w = FramedWrite::new(
                                                            tx,
                                                            LengthDelimitedCodec::builder()
                                                                .max_frame_length(100 * 1_024 * 1_024)
                                                                .new_codec(),
                                                        );
                                                        while let Some(qe) = qe_rx.recv().await {
                                                            match serde_json::to_vec(&qe) {
                                                                Ok(bytes) => {
                                                                    if let Err(e) =
                                                                        w.send(bytes.into()).await
                                                                    {
                                                                        warn!("query forward: send back failed: {e}");
                                                                        break;
                                                                    }
                                                                }
                                                                Err(e) => warn!(
                                                                    "query forward: serialize failed: {e}"
                                                                ),
                                                            }
                                                        }
                                                        let _ = w.into_inner().finish();
                                                        break;
                                                    }
                                                },
                                            }
                                        }

                                        Err(e) => {
                                            warn!("could not decode BiPayload: {e}");
                                        }
                                    }
                                }

                                Err(e) => {
                                    error!("could not read framed payload from bidirectional stream: {e}");
                                }
                            },
                        }
                    }
                }
            });
        }
    });
}
