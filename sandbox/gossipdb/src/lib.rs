//! gossipdb：无中心、最终一致的 KV 学习原型。
//!
//! 见 docs/superpowers/specs/2026-06-15-gossipdb-design.md。

pub mod api;
pub mod config;
pub mod gossip;
pub mod hlc;
pub mod store;
pub mod wire;
