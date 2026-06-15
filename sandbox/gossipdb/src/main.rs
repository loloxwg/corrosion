//! 组装启动：解析配置 → 建 Node → 启动 gossip 后台循环 → 跑 HTTP 服务。

use std::sync::Arc;

use clap::Parser;

use gossipdb::api;
use gossipdb::config::Config;
use gossipdb::gossip::{Node, Timings};
use gossipdb::store::MemStore;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = Config::parse();

    let store = Arc::new(MemStore::new());
    let node = Node::bind(cfg.id, cfg.bind, store, cfg.seeds.clone(), Timings::default()).await?;
    node.spawn();

    println!(
        "node {} gossip on {} | http on {} | seeds={:?}",
        node.id, node.addr, cfg.http, cfg.seeds
    );

    let app = api::router(node.clone());
    let listener = tokio::net::TcpListener::bind(cfg.http).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
