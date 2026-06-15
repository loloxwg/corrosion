//! 广播目标选择：主动推送研究的接缝。
//!
//! 把"选哪些 peer 广播 mutation"从 broadcast 主循环里抽出来，便于 A/B 对照：
//! - Random：现状基线，候选中纯随机选 K 个（与改造前行为等价）。
//! - Scored：阶段1，按价值打分选 Top-K。当前用链路质量（RTT 分桶 `ring`）+ 探索抖动。
//! - Rl：阶段3，图强化学习决策（暂回退 Scored）。
//!
//! 详见 docs/research/active-push-plan.md。

use std::cmp::Ordering;
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

// 打分权重（阶段1 手调；阶段3 可由 RL 学习）。
const UNKNOWN_RING: f64 = 4.0; // 未知链路按中等偏差处理，给它被探测的机会
const JITTER_WEIGHT: f64 = 0.25; // 探索抖动：避免总固定打同几个 peer

/// 从候选 peer 中选出最多 k 个作为本次广播目标。
pub fn select_broadcast_targets(
    strategy: BroadcastStrategy,
    candidates: &[Candidate],
    k: usize,
    rng: &mut StdRng,
) -> Vec<SocketAddr> {
    match strategy {
        BroadcastStrategy::Random => random_targets(candidates, k, rng),
        BroadcastStrategy::Scored => scored_targets(candidates, k, rng),
        BroadcastStrategy::Rl => {
            // TODO(research): 阶段3 GraphRL。暂复用打分式。
            trace!("broadcast strategy Rl 尚未实现，本次回退打分式");
            scored_targets(candidates, k, rng)
        }
    }
}

/// 基线：纯随机选 K（用 IteratorRandom，与改造前实现一致）。
fn random_targets(candidates: &[Candidate], k: usize, rng: &mut StdRng) -> Vec<SocketAddr> {
    candidates.iter().map(|c| c.addr).choose_multiple(rng, k)
}

/// 打分式：偏好链路好（低 ring）的 peer，叠加探索抖动后取 Top-K。
fn scored_targets(candidates: &[Candidate], k: usize, rng: &mut StdRng) -> Vec<SocketAddr> {
    let mut scored: Vec<(f64, SocketAddr)> = candidates
        .iter()
        .map(|c| (score(c, rng), c.addr))
        .collect();
    // 分高者优先。
    scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(Ordering::Equal));
    scored.into_iter().take(k).map(|(_, addr)| addr).collect()
}

/// 单个候选的价值分。当前 = 链路质量 + 探索抖动。
/// 后续按 active-push-plan 叠加：数据相关度、节点角色、负载。
fn score(c: &Candidate, rng: &mut StdRng) -> f64 {
    let ring = c.ring.map(|r| r as f64).unwrap_or(UNKNOWN_RING);
    // 链路分：ring0→1.0, ring1→0.5, ring2→0.33……越近越高。
    let link = 1.0 / (1.0 + ring);
    let jitter = rng.gen::<f64>() * JITTER_WEIGHT;
    link + jitter
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

    #[test]
    fn scored_prefers_low_ring() {
        // 一个 ring0(好链路) + 多个 ring9(差链路)，选 1 个应几乎总是 ring0。
        // link 差距(1.0 vs 0.1)远大于抖动上限(0.25)，故确定性偏向 ring0。
        let good = cand(9000, Some(0));
        let mut candidates = vec![good];
        for p in 9001..9010 {
            candidates.push(cand(p, Some(9)));
        }
        let mut rng = StdRng::seed_from_u64(42);
        for _ in 0..50 {
            let picked = scored_targets(&candidates, 1, &mut rng);
            assert_eq!(picked, vec![good.addr], "应选链路最好的 ring0");
        }
    }

    #[test]
    fn scored_returns_at_most_k() {
        let candidates: Vec<_> = (9000..9010).map(|p| cand(p, Some(1))).collect();
        let mut rng = StdRng::seed_from_u64(1);
        assert_eq!(scored_targets(&candidates, 3, &mut rng).len(), 3);
        assert_eq!(scored_targets(&candidates, 100, &mut rng).len(), 10);
    }

    #[test]
    fn random_returns_at_most_k() {
        let candidates: Vec<_> = (9000..9005).map(|p| cand(p, None)).collect();
        let mut rng = StdRng::seed_from_u64(1);
        assert_eq!(random_targets(&candidates, 2, &mut rng).len(), 2);
        assert_eq!(random_targets(&candidates, 99, &mut rng).len(), 5);
    }
}
