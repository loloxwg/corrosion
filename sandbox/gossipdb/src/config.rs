//! 命令行配置。

use clap::Parser;
use std::net::SocketAddr;

/// gossipdb 节点。
#[derive(Parser, Debug)]
#[command(name = "gossipdb")]
pub struct Config {
    /// 本节点 id（需 < 65519，唯一）。
    #[arg(long)]
    pub id: u16,

    /// gossip UDP 监听地址。
    #[arg(long, default_value = "127.0.0.1:7000")]
    pub bind: SocketAddr,

    /// HTTP API 监听地址。
    #[arg(long, default_value = "127.0.0.1:8000")]
    pub http: SocketAddr,

    /// 种子节点的 UDP 地址，逗号分隔。
    #[arg(long, value_delimiter = ',', default_value = "")]
    pub seeds: Vec<SocketAddr>,
}
