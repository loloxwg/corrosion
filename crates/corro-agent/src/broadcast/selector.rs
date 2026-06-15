//! 广播目标选择：主动推送研究的接缝。
//!
//! 把"选哪些 peer 广播 mutation"从 broadcast 主循环里抽出来，便于 A/B 对照：
//! - Random：现状基线，候选中纯随机选 K 个（与改造前行为等价）。
//! - Scored：阶段1，按价值打分选 Top-K。当前用链路质量（RTT 分桶 `ring`）+ 探索抖动。
//! - Rl：阶段3，图强化学习决策（暂回退 Scored）。
//!
//! 详见 docs/research/active-push-plan.md。

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;

use corro_types::config::BroadcastStrategy;
use rand::{rngs::StdRng, seq::IteratorRandom, Rng};
use tracing::trace;

/// 一个候选 peer 及其打分所需信号。
/// 信号随研究推进而扩充（数据相关度、负载……）；当前只带链路质量。
#[derive(Debug, Clone, Copy)]
pub struct Candidate {
    pub addr: SocketAddr,
    /// RTT 分桶：0=链路最好(最低延迟)，越大越差；None=尚无 RTT 样本。
    pub ring: Option<u8>,
}

// 打分权重（阶段1/2 手调；阶段3 可由 RL 学习）。
const UNKNOWN_RING: f64 = 4.0; // 未知链路按中等偏差处理，给它被探测的机会
const JITTER_WEIGHT: f64 = 0.25; // 探索抖动：避免总固定打同几个 peer
const RELEVANCE_WEIGHT: f64 = 2.0; // 数据相关度：关心该数据的 peer 强加分(压过链路项)

/// 从候选 peer 中选出最多 k 个作为本次广播目标。
///
/// `tables`：本次 mutation 涉及的表；`interest_routing`：表->关心它的 addr。
/// 二者用于 mission-aware 相关度打分；为空时相关度不起作用（退化为纯链路打分）。
pub fn select_broadcast_targets(
    strategy: BroadcastStrategy,
    candidates: &[Candidate],
    tables: &[String],
    interest_routing: &HashMap<String, Vec<SocketAddr>>,
    k: usize,
    rng: &mut StdRng,
) -> Vec<SocketAddr> {
    match strategy {
        BroadcastStrategy::Random => random_targets(candidates, k, rng),
        BroadcastStrategy::Scored => {
            scored_targets(candidates, tables, interest_routing, k, rng)
        }
        BroadcastStrategy::Rl => {
            // TODO(research): 阶段3 GraphRL。暂复用打分式。
            trace!("broadcast strategy Rl 尚未实现，本次回退打分式");
            scored_targets(candidates, tables, interest_routing, k, rng)
        }
    }
}

/// 基线：纯随机选 K（用 IteratorRandom，与改造前实现一致）。
fn random_targets(candidates: &[Candidate], k: usize, rng: &mut StdRng) -> Vec<SocketAddr> {
    candidates.iter().map(|c| c.addr).choose_multiple(rng, k)
}

/// 打分式：链路质量 + 数据相关度 + 探索抖动，取 Top-K。
fn scored_targets(
    candidates: &[Candidate],
    tables: &[String],
    interest_routing: &HashMap<String, Vec<SocketAddr>>,
    k: usize,
    rng: &mut StdRng,
) -> Vec<SocketAddr> {
    // 这条 mutation 的"关心者"集合 = 涉及表的兴趣 addr 之并。
    let interested: HashSet<SocketAddr> = tables
        .iter()
        .filter_map(|t| interest_routing.get(t))
        .flatten()
        .copied()
        .collect();

    let mut scored: Vec<(f64, SocketAddr)> = candidates
        .iter()
        .map(|c| (score(c, &interested, rng), c.addr))
        .collect();
    // 分高者优先。
    scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(Ordering::Equal));
    scored.into_iter().take(k).map(|(_, addr)| addr).collect()
}

/// 单个候选的价值分 = 数据相关度 + 链路质量 + 探索抖动。
/// 后续按 active-push-plan 可再叠加：节点角色、负载。
fn score(c: &Candidate, interested: &HashSet<SocketAddr>, rng: &mut StdRng) -> f64 {
    let ring = c.ring.map(|r| r as f64).unwrap_or(UNKNOWN_RING);
    // 链路分：ring0→1.0, ring1→0.5, ring2→0.33……越近越高。
    let link = 1.0 / (1.0 + ring);
    // 相关度：关心这条数据的 peer 强加分（mission-aware 核心）。
    let relevance = if interested.contains(&c.addr) {
        RELEVANCE_WEIGHT
    } else {
        0.0
    };
    let jitter = rng.gen::<f64>() * JITTER_WEIGHT;
    relevance + link + jitter
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;

    fn cand(port: u16, ring: Option<u8>) -> Candidate {
        Candidate {
            addr: format!("127.0.0.1:{port}").parse().unwrap(),
            ring,
        }
    }

    fn no_interest() -> HashMap<String, Vec<SocketAddr>> {
        HashMap::new()
    }

    #[test]
    fn scored_prefers_low_ring() {
        // 一个 ring0(好链路) + 多个 ring9(差链路)，无相关度信号时选 1 个应总是 ring0。
        let good = cand(9000, Some(0));
        let mut candidates = vec![good];
        for p in 9001..9010 {
            candidates.push(cand(p, Some(9)));
        }
        let mut rng = StdRng::seed_from_u64(42);
        for _ in 0..50 {
            let picked = scored_targets(&candidates, &[], &no_interest(), 1, &mut rng);
            assert_eq!(picked, vec![good.addr], "应选链路最好的 ring0");
        }
    }

    #[test]
    fn scored_prefers_interested_over_better_link() {
        // 关心数据的远端(ring9) 应压过 不关心的近端(ring0)——相关度权重压过链路。
        let near_uninterested = cand(9000, Some(0));
        let far_interested = cand(9001, Some(9));
        let candidates = vec![near_uninterested, far_interested];
        let mut routing = HashMap::new();
        routing.insert("todos".to_string(), vec![far_interested.addr]);
        let mut rng = StdRng::seed_from_u64(7);
        for _ in 0..50 {
            let picked = scored_targets(
                &candidates,
                &["todos".to_string()],
                &routing,
                1,
                &mut rng,
            );
            assert_eq!(picked, vec![far_interested.addr], "应优先推给关心该表的 peer");
        }
    }

    #[test]
    fn scored_returns_at_most_k() {
        let candidates: Vec<_> = (9000..9010).map(|p| cand(p, Some(1))).collect();
        let mut rng = StdRng::seed_from_u64(1);
        assert_eq!(scored_targets(&candidates, &[], &no_interest(), 3, &mut rng).len(), 3);
        assert_eq!(scored_targets(&candidates, &[], &no_interest(), 100, &mut rng).len(), 10);
    }

    #[test]
    fn random_returns_at_most_k() {
        let candidates: Vec<_> = (9000..9005).map(|p| cand(p, None)).collect();
        let mut rng = StdRng::seed_from_u64(1);
        assert_eq!(random_targets(&candidates, 2, &mut rng).len(), 2);
        assert_eq!(random_targets(&candidates, 99, &mut rng).len(), 5);
    }
}
