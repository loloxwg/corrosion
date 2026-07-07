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
use metrics::counter;
use rand::{rngs::StdRng, seq::IteratorRandom, Rng};

/// 一个候选 peer 及其打分所需信号。
/// 信号随研究推进而扩充（数据相关度、负载……）；当前带链路质量(ring)+ RTT 方差。
#[derive(Debug, Clone, Copy)]
pub struct Candidate {
    pub addr: SocketAddr,
    /// RTT 分桶：0=链路最好(最低延迟)，越大越差；None=尚无 RTT 样本。
    pub ring: Option<u8>,
    /// 最近 RTT 样本的方差(ms²)；None=样本不足。Rl 策略用它避高方差(不稳)链路。
    pub rtt_var: Option<f64>,
}

// 打分权重（阶段1/2 手调；阶段3 由 RL 学习/蒸馏)。
const UNKNOWN_RING: f64 = 4.0; // 未知链路按中等偏差处理，给它被探测的机会
const JITTER_WEIGHT: f64 = 0.25; // 探索抖动：避免总固定打同几个 peer
const RELEVANCE_WEIGHT: f64 = 2.0; // 数据相关度：关心该数据的 peer 强加分(压过链路项)
const COVERAGE_QUOTA: usize = 0; // 减量变体：除关心者外额外保留的覆盖名额(0=严格部分副本,容断靠 sync 兜底)
const VARIANCE_WEIGHT: f64 = 0.8; // Rl：链路方差惩罚权重(避不稳链路,RL 学到的核心信号)

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
        BroadcastStrategy::ScoredReduce => {
            scored_reduce_targets(candidates, tables, interest_routing, k, rng)
        }
        BroadcastStrategy::Rl => {
            rl_targets(candidates, tables, interest_routing, k, rng)
        }
    }
}

/// 基线：纯随机选 K（用 IteratorRandom，与改造前实现一致）。
fn random_targets(candidates: &[Candidate], k: usize, rng: &mut StdRng) -> Vec<SocketAddr> {
    candidates.iter().map(|c| c.addr).choose_multiple(rng, k)
}

/// 这条 mutation 的"关心者"集合 = 涉及表的兴趣 addr 之并。
/// wildcard:声明 `*`(关心全部)的 peer 以特殊键 `"*"` 存在 routing 里,任何广播都并入它。
fn interested_set(
    tables: &[String],
    interest_routing: &HashMap<String, Vec<SocketAddr>>,
) -> HashSet<SocketAddr> {
    let mut set: HashSet<SocketAddr> = tables
        .iter()
        .filter_map(|t| interest_routing.get(t))
        .flatten()
        .copied()
        .collect();
    if let Some(all) = interest_routing.get("*") {
        set.extend(all.iter().copied());
    }
    set
}

/// 减量变体（对接 4.4.3）：先把候选池缩到「关心者 + 少量覆盖配额」，再在缩小的池子里打分取 Top-K。
/// 任务无关平台被剔出快速推送 → 推送传输量随 |关心者|/|全量| 下降。
/// 无 interest 路由信号时无从判断该砍谁，退化为打分式（不减量，保正确性与活性）。
fn scored_reduce_targets(
    candidates: &[Candidate],
    tables: &[String],
    interest_routing: &HashMap<String, Vec<SocketAddr>>,
    k: usize,
    rng: &mut StdRng,
) -> Vec<SocketAddr> {
    match interest_pool(candidates, tables, interest_routing) {
        Some(pool) => scored_targets(&pool, tables, interest_routing, k, rng),
        None => scored_targets(candidates, tables, interest_routing, k, rng),
    }
}

/// Rl(阶段3，安全的瞬态层)：合法范围先由 interest 规则圈定(同 scored_reduce，保正确性/降量)，
/// **RL 只在合法集内**用方差感知打分优化"这条广播发给谁"——偏好低方差(稳定)链路做目标，
/// 高方差链路留给 anti-entropy 兜底。推错自愈(不丢数据/不改 placement/不碰 durability)。
/// 权重可由离线 RL 学习/蒸馏;当前 VARIANCE_WEIGHT 手设为 RL 学到的方向(避不稳链路)。
fn rl_targets(
    candidates: &[Candidate],
    tables: &[String],
    interest_routing: &HashMap<String, Vec<SocketAddr>>,
    k: usize,
    rng: &mut StdRng,
) -> Vec<SocketAddr> {
    let interested = interested_set(tables, interest_routing);
    match interest_pool(candidates, tables, interest_routing) {
        Some(pool) => rl_scored_targets(&pool, &interested, k, rng),
        None => rl_scored_targets(candidates, &interested, k, rng),
    }
}

/// interest 合法集：关心者 + COVERAGE_QUOTA 覆盖名额。
/// None = 无 interest 信号(关闭态) → 调用方退化为全打分(不减量，保活性)。
/// scored_reduce 与 rl 共用此正确性/降量边界，只在打分函数上分化。
fn interest_pool(
    candidates: &[Candidate],
    tables: &[String],
    interest_routing: &HashMap<String, Vec<SocketAddr>>,
) -> Option<Vec<Candidate>> {
    let interested = interested_set(tables, interest_routing);
    if interested.is_empty() {
        // ① 完全没配 interest(关闭态) → 退化为打分式全发(基线行为)。
        // ② interest 已启用但这些表暂无解析到的关心者(传播竞态/确无关心者)
        //    → 绝不全发(否则泄漏全网且永久留存)，只发覆盖配额，缺的由 sync 兜底。
        if interest_routing.is_empty() {
            return None;
        }
        counter!("corro.broadcast.interest.unresolved").increment(1);
    }
    let mut others: Vec<Candidate> = candidates
        .iter()
        .filter(|c| !interested.contains(&c.addr))
        .copied()
        .collect();
    others.sort_by_key(|c| c.ring.unwrap_or(u8::MAX));
    Some(
        candidates
            .iter()
            .filter(|c| interested.contains(&c.addr))
            .copied()
            .chain(others.into_iter().take(COVERAGE_QUOTA))
            .collect(),
    )
}

/// Rl 打分式：在给定(已由 interest 圈定的)合法集内，按方差感知价值取 Top-K。
fn rl_scored_targets(
    candidates: &[Candidate],
    interested: &HashSet<SocketAddr>,
    k: usize,
    rng: &mut StdRng,
) -> Vec<SocketAddr> {
    let mut scored: Vec<(f64, SocketAddr)> = candidates
        .iter()
        .map(|c| (rl_score(c, interested, rng), c.addr))
        .collect();
    scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(Ordering::Equal));
    scored.into_iter().take(k).map(|(_, addr)| addr).collect()
}

/// Rl 单候选价值 = 打分式基础分(相关度+链路均值+抖动) − 方差惩罚。
/// 方差归一化 v/(1+v)∈[0,1)，越不稳扣越多 → 同均值链路里偏好稳定的做广播目标。
/// 这是 RL 学到的核心信号(sim-to-real 已证方差感知降查询延迟)蒸馏进瞬态选路。
fn rl_score(c: &Candidate, interested: &HashSet<SocketAddr>, rng: &mut StdRng) -> f64 {
    let base = score(c, interested, rng);
    let var_penalty = c
        .rtt_var
        .map(|v| VARIANCE_WEIGHT * (v / (1.0 + v)))
        .unwrap_or(0.0);
    base - var_penalty
}

/// 打分式：链路质量 + 数据相关度 + 探索抖动，取 Top-K。
fn scored_targets(
    candidates: &[Candidate],
    tables: &[String],
    interest_routing: &HashMap<String, Vec<SocketAddr>>,
    k: usize,
    rng: &mut StdRng,
) -> Vec<SocketAddr> {
    let interested = interested_set(tables, interest_routing);

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
            rtt_var: None,
        }
    }

    fn cand_var(port: u16, ring: Option<u8>, rtt_var: f64) -> Candidate {
        Candidate {
            addr: format!("127.0.0.1:{port}").parse().unwrap(),
            ring,
            rtt_var: Some(rtt_var),
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
    fn reduce_drops_uninterested_peers() {
        // 1 个关心者 + 9 个无关 peer，k 很大。减量应只推给 关心者 + COVERAGE_QUOTA 个覆盖，
        // 而非全量 10 个 —— 这才是降量。
        let interested = cand(9000, Some(5));
        let mut candidates = vec![interested];
        for p in 9001..9010 {
            candidates.push(cand(p, Some(1)));
        }
        let mut routing = HashMap::new();
        routing.insert("flight".to_string(), vec![interested.addr]);
        let mut rng = StdRng::seed_from_u64(3);
        let picked = scored_reduce_targets(
            &candidates,
            &["flight".to_string()],
            &routing,
            100,
            &mut rng,
        );
        assert_eq!(picked.len(), 1 + COVERAGE_QUOTA, "应只剩关心者+覆盖配额");
        assert!(picked.contains(&interested.addr), "关心者必须在内");
    }

    #[test]
    fn reduce_falls_back_to_scored_without_interest() {
        // 无 interest 路由信号时不该乱砍：退化为打分式，k>=候选数则全选。
        let candidates: Vec<_> = (9000..9005).map(|p| cand(p, Some(1))).collect();
        let mut rng = StdRng::seed_from_u64(1);
        let picked = scored_reduce_targets(&candidates, &[], &no_interest(), 100, &mut rng);
        assert_eq!(picked.len(), 5, "无相关度信号应退化为不减量");
    }

    #[test]
    fn reduce_strict_excludes_uninterested() {
        // COVERAGE_QUOTA=0 严格部分副本：interest 已配置时，非关心者一律不发，
        // 哪怕链路更好(near ring0)——避免非关心数据泄漏，缺的由 sync 兜底。
        let interested = cand(9000, Some(9));
        let far = cand(9001, Some(9));
        let near = cand(9002, Some(0)); // 非关心者中链路最好，仍不该被选
        let candidates = vec![interested, far, near];
        let mut routing = HashMap::new();
        routing.insert("flight".to_string(), vec![interested.addr]);
        let mut rng = StdRng::seed_from_u64(11);
        let picked = scored_reduce_targets(
            &candidates,
            &["flight".to_string()],
            &routing,
            100,
            &mut rng,
        );
        assert_eq!(picked, vec![interested.addr], "应只发关心者，非关心者全排除");
    }

    #[test]
    fn wildcard_peer_receives_every_table() {
        // 声明 "*"(关心全部)的 peer,无论广播哪张表都应被选中——哪怕它没精确关心该表。
        let star = cand(9000, Some(9)); // 链路差,但 wildcard 关心全部,仍必入
        let other = cand(9001, Some(0)); // 链路好但不关心 battlefield,严格部分副本下应排除
        let candidates = vec![star, other];
        let mut routing = HashMap::new();
        routing.insert("*".to_string(), vec![star.addr]);
        let mut rng = StdRng::seed_from_u64(5);
        let picked = scored_reduce_targets(
            &candidates,
            &["battlefield".to_string()],
            &routing,
            100,
            &mut rng,
        );
        assert_eq!(picked, vec![star.addr], "wildcard peer 必收任意表,非关心者排除");
    }

    #[test]
    fn rl_prefers_low_variance_among_interested() {
        // 两个都关心该表、同 ring；Rl 选 1 个应偏好低方差(稳定)链路。
        let stable = cand_var(9000, Some(1), 1.0);
        let jittery = cand_var(9001, Some(1), 50.0);
        let candidates = vec![stable, jittery];
        let mut routing = HashMap::new();
        routing.insert("todos".to_string(), vec![stable.addr, jittery.addr]);
        let mut rng = StdRng::seed_from_u64(7);
        for _ in 0..50 {
            let picked = select_broadcast_targets(
                BroadcastStrategy::Rl,
                &candidates,
                &["todos".to_string()],
                &routing,
                1,
                &mut rng,
            );
            assert_eq!(picked, vec![stable.addr], "Rl 应偏好低方差链路做广播目标");
        }
    }

    #[test]
    fn rl_stays_within_interest_set() {
        // interest 已配置:非关心的近邻(ring0)不该被 Rl 选——瞬态优化不越正确性边界。
        let interested = cand_var(9000, Some(9), 1.0);
        let near_uninterested = cand_var(9001, Some(0), 1.0);
        let candidates = vec![interested, near_uninterested];
        let mut routing = HashMap::new();
        routing.insert("flight".to_string(), vec![interested.addr]);
        let mut rng = StdRng::seed_from_u64(3);
        let picked = select_broadcast_targets(
            BroadcastStrategy::Rl,
            &candidates,
            &["flight".to_string()],
            &routing,
            100,
            &mut rng,
        );
        assert_eq!(picked, vec![interested.addr], "Rl 只在 interest 合法集内选,非关心者排除");
    }

    #[test]
    fn rl_falls_back_to_full_without_interest() {
        // 无 interest 信号(关闭态)→ Rl 退化为不减量全打分,保活性(不误砍)。
        let candidates: Vec<_> = (9000..9005).map(|p| cand(p, Some(1))).collect();
        let mut rng = StdRng::seed_from_u64(1);
        let picked = select_broadcast_targets(
            BroadcastStrategy::Rl,
            &candidates,
            &[],
            &no_interest(),
            100,
            &mut rng,
        );
        assert_eq!(picked.len(), 5, "无 interest 信号 Rl 退化为不减量");
    }

    #[test]
    fn random_returns_at_most_k() {
        let candidates: Vec<_> = (9000..9005).map(|p| cand(p, None)).collect();
        let mut rng = StdRng::seed_from_u64(1);
        assert_eq!(random_targets(&candidates, 2, &mut rng).len(), 2);
        assert_eq!(random_targets(&candidates, 99, &mut rng).len(), 5);
    }
}
