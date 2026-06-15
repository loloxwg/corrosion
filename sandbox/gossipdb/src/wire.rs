//! 节点间 UDP 报文格式。用 serde_json 编解码（原型优先可读，日后可换 bincode）。

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::store::Mutation;

pub type NodeId = u16;

/// gossip 报文。每条都带发送者 (from, addr)，接收方据此发现成员并刷新存活。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Msg {
    /// 推送路：捎带最近若干变更 + 已知 peer 列表（用于传递式发现）。
    Push {
        from: NodeId,
        addr: String,
        mutations: Vec<Mutation>,
        peers: Vec<(NodeId, String)>,
    },
    /// 对账路第一步：发送方把自己的 key->hlc 摘要发给对方。
    SyncReq {
        from: NodeId,
        addr: String,
        digest: HashMap<String, u64>,
    },
    /// 对账路第二步：对方回传「我有而你缺、或我比你新」的完整条目。
    SyncResp {
        from: NodeId,
        addr: String,
        mutations: Vec<Mutation>,
    },
    /// SWIM 直接探测。
    Ping {
        from: NodeId,
        addr: String,
        seq: u64,
    },
    /// SWIM 探测应答（seq 回显请求方的 seq）。
    Ack {
        from: NodeId,
        addr: String,
        seq: u64,
    },
    /// SWIM 间接探测：请接收方代为 ping `target`（target 是其 gossip 地址）。
    PingReq {
        from: NodeId,
        addr: String,
        seq: u64,
        target: String,
    },
}

impl Msg {
    /// 取出发送者身份，用于成员发现与故障检测刷新。
    pub fn sender(&self) -> (NodeId, &str) {
        match self {
            Msg::Push { from, addr, .. } => (*from, addr),
            Msg::SyncReq { from, addr, .. } => (*from, addr),
            Msg::SyncResp { from, addr, .. } => (*from, addr),
            Msg::Ping { from, addr, .. } => (*from, addr),
            Msg::Ack { from, addr, .. } => (*from, addr),
            Msg::PingReq { from, addr, .. } => (*from, addr),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("serialize msg")
    }

    pub fn decode(bytes: &[u8]) -> Option<Msg> {
        serde_json::from_slice(bytes).ok()
    }
}
