//! 混合逻辑时钟（Hybrid Logical Clock）。
//!
//! 把 64 位整数拆成两段：高 48 位放毫秒物理时间，低 16 位放逻辑计数器。
//! 这样直接按 u64 比大小，就等价于「先比物理时间，再比逻辑计数器」。
//!
//! 作用：多节点写同一个 key 时，提供一个既贴近真实时间、又不会被时钟
//! 漂移搞乱因果顺序的版本号，配合 node_id 平局裁决，让全集群对「谁更新」
//! 得到一致结论。

use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

const LOGICAL_BITS: u32 = 16;
const LOGICAL_MASK: u64 = (1 << LOGICAL_BITS) - 1;

/// 取一个 HLC 的物理部分（毫秒）。
pub fn physical(hlc: u64) -> u64 {
    hlc >> LOGICAL_BITS
}

/// 取一个 HLC 的逻辑计数器部分。
pub fn logical(hlc: u64) -> u64 {
    hlc & LOGICAL_MASK
}

fn pack(physical: u64, logical: u64) -> u64 {
    (physical << LOGICAL_BITS) | (logical & LOGICAL_MASK)
}

/// 逻辑计数器 +1；若已到上限则进位到物理部分，避免回绕破坏单调性
/// （同一毫秒内写入超过 65536 次的极端情况）。
fn bump(physical: u64, logical: u64) -> u64 {
    if logical >= LOGICAL_MASK {
        pack(physical + 1, 0)
    } else {
        pack(physical, logical + 1)
    }
}

/// 当前墙上时间（毫秒）。可在测试中通过 `Clock::with_now` 替换。
fn wall_clock_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_millis() as u64
}

/// 一个节点持有的 HLC 状态。内部用 `Mutex` 保护，可被多任务共享。
pub struct Hlc {
    last: Mutex<u64>,
    // 注入式时钟，便于测试。返回当前墙上毫秒。
    now: fn() -> u64,
}

impl Hlc {
    pub fn new() -> Self {
        Hlc {
            last: Mutex::new(0),
            now: wall_clock_millis,
        }
    }

    /// 用自定义时钟构造（测试用）。
    pub fn with_now(now: fn() -> u64) -> Self {
        Hlc {
            last: Mutex::new(0),
            now,
        }
    }

    /// 本地发生一次写入，产生一个新的、严格大于上次的 HLC。
    pub fn tick(&self) -> u64 {
        let mut last = self.last.lock().unwrap();
        let wall = (self.now)();
        let last_phys = physical(*last);

        let new = if wall > last_phys {
            // 时间前进了：物理部分跟上，逻辑计数器归零。
            pack(wall, 0)
        } else {
            // 同一毫秒内连写（或时钟回拨）：物理部分保持，逻辑计数器 +1（带进位保护）。
            bump(last_phys, logical(*last))
        };
        *last = new;
        new
    }

    /// 收到远端 HLC 时更新本地时钟，保证本地随后产生的 HLC 大于已见过的任何值。
    pub fn observe(&self, remote: u64) {
        let mut last = self.last.lock().unwrap();
        let wall = (self.now)();
        let max_phys = physical(*last).max(physical(remote)).max(wall);

        let new = if max_phys == physical(*last) && max_phys == physical(remote) {
            // 本地和远端物理部分都等于 max：逻辑计数器取两者较大再 +1。
            bump(max_phys, logical(*last).max(logical(remote)))
        } else if max_phys == physical(*last) {
            bump(max_phys, logical(*last))
        } else if max_phys == physical(remote) {
            bump(max_phys, logical(remote))
        } else {
            // 墙上时间领先：新的一毫秒，逻辑计数器归零。
            pack(max_phys, 0)
        };
        *last = new;
    }
}

impl Default for Hlc {
    fn default() -> Self {
        Self::new()
    }
}

/// 比较两个版本 (hlc, node)。node 仅用于打破 hlc 平局，保证全集群一致裁决。
/// 返回 true 表示 a 比 b 更新。
pub fn newer(a: (u64, u16), b: (u64, u16)) -> bool {
    a > b
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    // 固定时钟：始终返回 1000ms，用于测试同一毫秒内连写。
    fn frozen_now() -> u64 {
        1000
    }

    #[test]
    fn tick_is_strictly_monotonic_within_same_millis() {
        let hlc = Hlc::with_now(frozen_now);
        let a = hlc.tick();
        let b = hlc.tick();
        let c = hlc.tick();
        assert!(b > a, "连续 tick 必须严格递增");
        assert!(c > b);
        // 物理部分相同，靠逻辑计数器区分。
        assert_eq!(physical(a), 1000);
        assert_eq!(logical(a), 0);
        assert_eq!(logical(b), 1);
        assert_eq!(logical(c), 2);
    }

    // 可前进的时钟。
    static CLOCK: AtomicU64 = AtomicU64::new(1000);
    fn advancing_now() -> u64 {
        CLOCK.load(Ordering::SeqCst)
    }

    #[test]
    fn tick_resets_logical_when_wall_advances() {
        CLOCK.store(2000, Ordering::SeqCst);
        let hlc = Hlc::with_now(advancing_now);
        let a = hlc.tick();
        assert_eq!(physical(a), 2000);
        assert_eq!(logical(a), 0);

        CLOCK.store(2001, Ordering::SeqCst);
        let b = hlc.tick();
        assert_eq!(physical(b), 2001);
        assert_eq!(logical(b), 0, "时间前进后逻辑计数器应归零");
        assert!(b > a);
    }

    #[test]
    fn observe_makes_local_exceed_remote() {
        let hlc = Hlc::with_now(frozen_now);
        // 远端来自未来（物理时间更大）。
        let remote = pack(5000, 7);
        hlc.observe(remote);
        let next = hlc.tick();
        assert!(next > remote, "观测到远端后，本地新 HLC 必须超过远端");
    }

    #[test]
    fn logical_saturation_carries_into_physical() {
        // 同一毫秒内写满计数器后，下一次 tick 应进位物理部分而非回绕。
        let hlc = Hlc::with_now(frozen_now);
        // 直接把内部状态推到逻辑上限。
        *hlc.last.lock().unwrap() = pack(1000, LOGICAL_MASK);
        let next = hlc.tick();
        assert!(next > pack(1000, LOGICAL_MASK), "进位后必须仍严格递增");
        assert_eq!(physical(next), 1001, "计数器满应进位到物理部分");
        assert_eq!(logical(next), 0);
    }

    #[test]
    fn newer_uses_node_to_break_ties() {
        let hlc_val = pack(1000, 3);
        assert!(newer((hlc_val, 9), (hlc_val, 2)), "hlc 相等时 node 大者胜");
        assert!(!newer((hlc_val, 2), (hlc_val, 9)));
        // hlc 不同则 node 不起作用。
        assert!(newer((pack(1000, 4), 0), (pack(1000, 3), 9)));
    }
}
