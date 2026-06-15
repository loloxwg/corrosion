//! 广播目标选择：主动推送研究的接缝。
//!
//! 把"选哪些 peer 广播 mutation"从 broadcast 主循环里抽出来，便于 A/B 对照：
//! - Random：现状基线，候选中纯随机选 K 个（与改造前行为等价）。
//! - Scored：阶段1，按价值打分选 Top-K（数据相关度/链路质量/角色/负载）。
//! - Rl：阶段3，图强化学习决策。
//!
//! 详见 docs/research/active-push-plan.md。

use std::net::SocketAddr;

use corro_types::config::BroadcastStrategy;
use rand::{rngs::StdRng, seq::IteratorRandom};
use tracing::trace;

/// 从候选 peer 中选出最多 k 个作为本次广播目标。
pub fn select_broadcast_targets(
    strategy: BroadcastStrategy,
    candidates: &[SocketAddr],
    k: usize,
    rng: &mut StdRng,
) -> Vec<SocketAddr> {
    match strategy {
        BroadcastStrategy::Random => random_targets(candidates, k, rng),
        BroadcastStrategy::Scored | BroadcastStrategy::Rl => {
            // TODO(research): 阶段1 打分 / 阶段3 GraphRL。
            // 暂回退到随机，保证开关可切换且不破坏收敛与行为。
            trace!(
                "broadcast strategy {:?} 尚未实现，本次回退随机选择",
                strategy
            );
            random_targets(candidates, k, rng)
        }
    }
}

fn random_targets(candidates: &[SocketAddr], k: usize, rng: &mut StdRng) -> Vec<SocketAddr> {
    // 用 IteratorRandom（与改造前的实现一致）保证行为等价。
    candidates.iter().copied().choose_multiple(rng, k)
}
