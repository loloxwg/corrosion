//! 存储层：把每个 key 的值统一建模成 CRDT，merge = CvRDT join。
//!
//! 三种类型（用 `crdts` 库）：
//!   - Register：LWW 寄存器，marker=(hlc,node)。覆盖型写入，并发取 marker 大者。
//!               删除 = 写一个 Deleted 值，靠 marker 与并发写竞争。
//!   - Counter ：PN-Counter，可加可减，并发增量自动求和，不丢更新。
//!   - Set     ：OR-Set，元素可加可删，并发自动求并。
//!
//! 所有类型的 merge 都满足交换/结合/幂等（CvRDT），所以无论 gossip 以什么顺序、
//! 重复多少次送达，所有副本最终收敛到同一状态。

use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Mutex;

#[cfg(test)]
use crdts::CmRDT;
use crdts::{CvRDT, lwwreg::LWWReg, orswot::Orswot, pncounter::PNCounter};
use serde::{Deserialize, Serialize};

/// LWW 寄存器的排序标记：(hlc, node)。hlc 大者胜，平局 node 大者胜。
pub type Marker = (u64, u16);

/// 寄存器的值：正常值或墓碑（删除）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RegVal {
    Val(Vec<u8>),
    Deleted,
}

/// 一个 key 的值，三选一。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Crdt {
    Register(LWWReg<RegVal, Marker>),
    Counter(PNCounter<u16>),
    Set(Orswot<String, u16>),
}

impl Crdt {
    /// 合并另一个同类型 CRDT 进自己，返回是否真的变了（用于判断要不要继续转发）。
    /// 类型不匹配时忽略远端（不应发生：key 类型由首次写入固定）。
    pub fn merge(&mut self, other: Crdt) -> bool {
        match (self, other) {
            (Crdt::Register(a), Crdt::Register(b)) => {
                let before = a.clone();
                a.merge(b);
                *a != before
            }
            (Crdt::Counter(a), Crdt::Counter(b)) => {
                let before = a.clone();
                a.merge(b);
                *a != before
            }
            (Crdt::Set(a), Crdt::Set(b)) => {
                let before = a.clone();
                a.merge(b);
                *a != before
            }
            _ => false,
        }
    }

    /// 类型名，供调试展示。
    pub fn type_name(&self) -> &'static str {
        match self {
            Crdt::Register(_) => "register",
            Crdt::Counter(_) => "counter",
            Crdt::Set(_) => "set",
        }
    }

    /// 内容哈希，供对账比对（哈希不同就把整份状态发过去 merge）。
    pub fn content_hash(&self) -> u64 {
        let bytes = serde_json::to_vec(self).expect("serialize crdt");
        let mut h = DefaultHasher::new();
        bytes.hash(&mut h);
        h.finish()
    }
}

/// 一次状态传播单元：某个 key 的完整 CRDT 状态。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Mutation {
    pub key: String,
    pub crdt: Crdt,
}

/// 存储抽象。
pub trait Store: Send + Sync {
    /// 读取一个 key 的 CRDT 状态。
    fn get(&self, key: &str) -> Option<Crdt>;

    /// 合并一条远端/本地状态。返回 true 表示状态发生了变化。
    fn apply(&self, mutation: &Mutation) -> bool;

    /// key -> 内容哈希，用于对账比对。
    fn digest(&self) -> HashMap<String, u64>;

    /// 全部条目。
    fn snapshot(&self) -> Vec<Mutation>;
}

/// 内存实现。
pub struct MemStore {
    map: Mutex<HashMap<String, Crdt>>,
}

impl MemStore {
    pub fn new() -> Self {
        MemStore {
            map: Mutex::new(HashMap::new()),
        }
    }
}

impl Default for MemStore {
    fn default() -> Self {
        Self::new()
    }
}

impl Store for MemStore {
    fn get(&self, key: &str) -> Option<Crdt> {
        self.map.lock().unwrap().get(key).cloned()
    }

    fn apply(&self, mutation: &Mutation) -> bool {
        let mut map = self.map.lock().unwrap();
        match map.get_mut(&mutation.key) {
            Some(existing) => existing.merge(mutation.crdt.clone()),
            None => {
                map.insert(mutation.key.clone(), mutation.crdt.clone());
                true
            }
        }
    }

    fn digest(&self) -> HashMap<String, u64> {
        self.map
            .lock()
            .unwrap()
            .iter()
            .map(|(k, c)| (k.clone(), c.content_hash()))
            .collect()
    }

    fn snapshot(&self) -> Vec<Mutation> {
        self.map
            .lock()
            .unwrap()
            .iter()
            .map(|(k, c)| Mutation {
                key: k.clone(),
                crdt: c.clone(),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn register(bytes: &str, marker: Marker) -> Crdt {
        Crdt::Register(LWWReg {
            val: RegVal::Val(bytes.as_bytes().to_vec()),
            marker,
        })
    }

    fn mutation(key: &str, crdt: Crdt) -> Mutation {
        Mutation {
            key: key.to_string(),
            crdt,
        }
    }

    fn reg_value(s: &MemStore, key: &str) -> Option<Vec<u8>> {
        match s.get(key)? {
            Crdt::Register(r) => match r.val {
                RegVal::Val(b) => Some(b),
                RegVal::Deleted => None,
            },
            _ => None,
        }
    }

    #[test]
    fn register_is_lww() {
        let s = MemStore::new();
        s.apply(&mutation("k", register("v1", (10, 1))));
        // 更小 marker 的写被忽略。
        assert!(!s.apply(&mutation("k", register("v0", (5, 1)))));
        assert_eq!(reg_value(&s, "k").unwrap(), b"v1");
        // 更大 marker 胜出。
        assert!(s.apply(&mutation("k", register("v2", (20, 1)))));
        assert_eq!(reg_value(&s, "k").unwrap(), b"v2");
    }

    #[test]
    fn register_tie_broken_by_node() {
        let s = MemStore::new();
        s.apply(&mutation("k", register("from1", (10, 1))));
        // 同 hlc，node 大者胜。
        assert!(s.apply(&mutation("k", register("from2", (10, 2)))));
        assert_eq!(reg_value(&s, "k").unwrap(), b"from2");
    }

    #[test]
    fn delete_via_register_tombstone() {
        let s = MemStore::new();
        s.apply(&mutation("k", register("v", (10, 1))));
        let tomb = Crdt::Register(LWWReg {
            val: RegVal::Deleted,
            marker: (20, 1),
        });
        s.apply(&mutation("k", tomb));
        assert!(reg_value(&s, "k").is_none(), "墓碑后读不到");
        // 更晚的写复活。
        s.apply(&mutation("k", register("back", (30, 1))));
        assert_eq!(reg_value(&s, "k").unwrap(), b"back");
    }

    #[test]
    fn counter_does_not_lose_concurrent_increments() {
        // 模拟分区：A、B 各自从空 counter +1，再互相合并。
        let mut a = PNCounter::<u16>::new();
        a.apply(a.inc(1));
        let mut b = PNCounter::<u16>::new();
        b.apply(b.inc(2));

        let sa = MemStore::new();
        sa.apply(&mutation("c", Crdt::Counter(a.clone())));
        // A 收到 B 的状态。
        sa.apply(&mutation("c", Crdt::Counter(b.clone())));

        if let Some(Crdt::Counter(c)) = sa.get("c") {
            assert_eq!(c.read().to_string(), "2", "并发 +1/+1 必须 = 2，不丢更新");
        } else {
            panic!("expected counter");
        }
    }

    #[test]
    fn set_merges_to_union() {
        let mut a = Orswot::<String, u16>::new();
        a.apply(a.add("radar".to_string(), a.read_ctx().derive_add_ctx(1)));
        let mut b = Orswot::<String, u16>::new();
        b.apply(b.add("air".to_string(), b.read_ctx().derive_add_ctx(2)));

        let s = MemStore::new();
        s.apply(&mutation("tags", Crdt::Set(a)));
        s.apply(&mutation("tags", Crdt::Set(b)));

        if let Some(Crdt::Set(set)) = s.get("tags") {
            let members = set.read().val;
            assert!(members.contains("radar"));
            assert!(members.contains("air"));
            assert_eq!(members.len(), 2, "并发 add 不同元素 = 并集");
        } else {
            panic!("expected set");
        }
    }

    #[test]
    fn merge_is_idempotent() {
        let s = MemStore::new();
        let m = mutation("k", register("v", (10, 1)));
        assert!(s.apply(&m));
        assert!(!s.apply(&m), "重复合并同一状态不应改变");
    }
}
