# gossipdb 设计文档

日期：2026-06-15
状态：已批准，进入实现

## 1. 目标

用 Rust 做一个**无中心、最终一致**的 KV 数据库学习原型：

- 客户端连**任意节点**读写
- 同一个业务 key 可在**任意节点写**（多写者）
- 写入靠 **gossip** 在集群内扩散
- 写冲突靠 **HLC + LWW**（混合逻辑时钟 + 最后写入胜出）裁决
- 集群最终**收敛**到同一份数据
- 节点宕机不影响其余节点读写；重启后能追平

非目标（本期不做，留扩展点）：强一致、事务、持久化、CRDT、分片、主动推送打分。

## 2. 选型

| 层 | 选择 | 理由 |
|---|---|---|
| gossip 传播 + 成员/故障 | **手写**：tokio UDP + 周期随机推送 + last-seen 超时判活 | 学习原型优先"快 + 概念摊开"，避免 foca 的集成样板 |
| 网络 | `tokio` + UDP | 异步收发 |
| 序列化 | `serde` + `bincode`/`serde_json` | gossip 报文编解码 |
| 存储 | 内存 `HashMap`，藏在 `Store` trait 后 | 日后可换 RocksDB |
| HTTP API | 轻量 HTTP（`axum`） | PUT/GET/members |

**选型说明（2026-06-15 调整）**：原计划用 `foca`（生产级 SWIM）。但 foca 集成需要实现 Codec/Runtime/Identity/BroadcastHandler/定时轮等样板，与"快速学习原型"目标冲突。改为手写 gossip 循环：约 300 行，把推送、对账、故障检测每个概念都显式摊开，零外部 gossip 库依赖。代价是故障检测用简单的 last-seen 超时（非完整 SWIM 的 suspect 机制），对原型足够。`foca` 作为日后升级项保留。

为什么不选 chitchat：chitchat 是 Scuttlebutt 模型（每节点只写自己那片 key，无冲突），与"任意节点写同一个 key"的多写语义不符。

### 删除（墓碑）

DELETE 不物理移除，而是写一条 `deleted=true` 的墓碑（带新 HLC），像普通写入一样
扩散并参与 LWW 竞争：删晚于写则删生效，写晚于删则数据复活。读取时墓碑视为不存在
（返回 404）。墓碑须出现在 digest/snapshot 中才能正确扩散删除。墓碑 GC（compaction）
本期不做，永久保留。

### 故障检测（手写版）

每个节点维护 `peers: HashMap<NodeId, PeerInfo{ addr, last_seen, state }>`。

```
收到任意来自 peer 的报文 → 更新该 peer 的 last_seen，state = alive
每轮 tick：
    now - last_seen > suspect_timeout  → state = suspect
    now - last_seen > dead_timeout     → state = dead（不再推送，但保留条目以便重连）
重新收到报文 → 立即恢复 alive
```

## 3. 架构分层

```
HTTP API        PUT /kv/{k}   GET /kv/{k}   GET /members
   │
Store (trait)   内存 HashMap<String, Entry>   ← 留 trait，日后换 RocksDB
   │
Merge           LWW：(hlc, node) 大者胜
   │
两条传播路 ───────────────────────────────────────────
  ① 推送   写入 → Mutation → foca BroadcastHandler 随 gossip 扩散（快，UDP 可能丢）
  ② 对账   定期随机挑 peer，交换 key→hlc 摘要，拉取缺失/更旧的条目（防丢 + 新节点追平）
   │
foca            SWIM：成员发现 + 故障检测
   │
tokio UDP       收发
```

## 4. 数据模型

```rust
struct Entry {
    value: Vec<u8>,
    hlc: u64,   // (毫秒物理时间 << 16) | 逻辑计数器
    node: u16,  // 写入节点 id，用于 HLC 平局裁决
}

struct Mutation {
    key: String,
    value: Vec<u8>,
    hlc: u64,
    node: u16,
}
```

### HLC 规则

```
本地写入：
    phys = max(本地 hlc 的物理部分, 当前墙上毫秒)
    若 phys == 本地物理部分 → 逻辑计数器 +1
    否则 → 逻辑计数器 = 0
    hlc = (phys << 16) | 逻辑计数器

收到远端 hlc：
    更新本地 hlc，使物理部分 = max(本地, 远端, 墙上时间)，逻辑计数器单调不减
```

### Merge（LWW）

```
merge(local: Option<Entry>, remote: Entry):
    None                                  → 用 remote
    Some(l) if (remote.hlc, remote.node) > (l.hlc, l.node) → 用 remote
    否则                                   → 留 local
```

merge 必须满足：可交换、可结合、幂等（无论收到顺序，最终结果一致）。

## 5. 两条传播路（设计核心）

- **只靠①推送**：UDP 丢包 + 新节点收不到历史写入 → 永久缺数据。
- **只靠②对账**：能补齐但慢（周期性）。
- **两条叠加**：推送负责快速收敛，对账兜底防漏并让新节点追平。对应 Dynamo 的 gossip + anti-entropy（对账先用 key→hlc 列表，不上 Merkle 树）。

### 对账协议（简化 anti-entropy）

```
每 T 秒，随机挑一个存活 peer：
  1. 发送本地摘要：{ key: hlc }
  2. peer 比对，回传"我比你新的条目" + "你有我没有的 key 请求"
  3. 双方各自 merge
```

## 6. 模块划分（单 crate）

```
src/
  hlc.rs          HLC 生成与比较
  store.rs        Store trait + 内存实现 + merge 逻辑
  wire.rs         gossip 报文类型 + 编解码（serde）
  gossip.rs       UDP 收发 + 周期推送 + 成员/故障检测（推送路）
  anti_entropy.rs 周期对账（对账路）
  api.rs          HTTP 接口
  config.rs       节点配置（id、监听地址、seed peers）
  main.rs         组装启动
```

## 7. 砍掉项与扩展点

| 砍掉 | 扩展点 |
|---|---|
| RocksDB 持久化 | `Store` 是 trait，换实现即可 |
| CRDT（counter/set） | `merge` 逻辑可按 field 分策略 |
| 主动推送打分（Mission-aware） | 留 `select_peers()` 钩子，先纯随机 |
| 分片 / Token Ring | 全节点全量复制 |

## 8. 验收标准（DoD）

起 3 个进程互相 join：

1. **扩散收敛**：节点 A `PUT k=1`，稍等后 B/C `GET k` 都返回 1。
2. **冲突裁决**：A、B 几乎同时写同一个 k，最终三节点读到的值一致（HLC/LWW）。
3. **故障容忍**：杀掉 C，A/B 继续读写正常；C 重启后通过对账追平。

## 8.1 安全限制（已知，原型有意未做）

gossip 走明文 UDP、**无认证**。这是学习原型的有意取舍（仅限 127.0.0.1）。
真实部署前必须补齐（自动安全审查于 2026-06-15 标记，HIGH×3 + MED×1）：

- **未认证写入 / SyncReq 反射 / peers 列表投毒 / 伪造地址踢节点**：根因都是明文 UDP 无认证。
  修复方向：每包加集群预共享密钥的 HMAC，或在 UDP 上套 Noise/mTLS；
  校验 mutation 的 `node` 与认证发送者一致；对 SyncReq 按源限流并限制响应体大小；
  `Push.peers` 仅作提示，须经握手探测确认后才入活跃表；区分「实测 peer」与「广播 peer」，
  不允许广播项驱逐实测项。
- 这些在战场/无人机等开放无线环境是**必须项**，不能因「内网」豁免。

## 8.2 CRDT 数据类型（2026-06-15 扩展，已实现）

把每个 key 的值统一建模成 CRDT（用 `crdts` 库），merge = CvRDT join，取代原先的标量 LWW 比较：

- **Register**：`LWWReg<RegVal, (hlc,node)>`。覆盖型，并发取 marker 大者（=原 LWW 行为）。
  删除 = 写 `RegVal::Deleted`，靠 marker 与并发写竞争（替代独立 deleted 字段）。
- **Counter**：`PNCounter<u16>`（actor=node id）。可加可减，并发增量求和，**不丢更新**。
- **Set**：`Orswot<String, u16>`。元素可加可删，并发求并集。

key 类型由首次写入固定；混类型则拒绝。覆盖型并发 v1 走 LWW（会丢一个），MV-Register 留 v2。

传播改为状态式：Mutation 携带整份 CRDT 状态，apply=merge（幂等）。对账 digest 从
`key→hlc` 改为 `key→内容哈希`，哈希不同就发整份状态过去 merge。

限制：状态式非 delta；内容哈希因 CRDT 内部 map 序列化顺序可能不稳定而偶发多余同步
（不影响收敛）；counter/set 不支持整 key 删除。

## 8.3 传输：UDP + TCP 混合（2026-06-15 调整）

按 SWIM 家族（memberlist/Serf）主流做法，分流传输：

- **UDP**：推送路（小、频繁、容丢）+ 故障检测（收到任意报文刷新 last_seen）
- **TCP**：对账路（SyncReq/SyncResp，可靠、可任意大小），帧格式 = 4 字节大端长度前缀 + JSON

动机：原先对账也走 UDP，多个 key 的整份 CRDT 状态塞一个 UDP 包会撞 ~64KB 上限并静默丢弃；
TCP 解决大小上限与丢包，且对账协议简化为一次 req→resp。TCP 帧上限 16MB 防超大长度前缀攻击。
TCP 来源是临时端口，故 peer 登记用「连接真实 IP + 报文端口」（缓解信任退化，非完整 auth）。
TCP 服务端用信号量限并发连接（MAX_SYNC_CONNS=32）+ 每连接 5s 超时（防连接洪泛/slowloris），
帧上限降至 4MB。read_frame 仍按长度预分配（受帧上限+并发上限约束）；增量读为后续硬化项。
TLS/mTLS 尚未加（§8.1）。

## 8.4 SWIM 风格故障检测（2026-06-15 扩展）

原故障检测靠"被动听到任意报文刷新 last_seen"，100 节点下每 peer 信号稀疏会误判。
改为 SWIM 核心三招（走 UDP）：

**新报文**
- `Ping{from, addr, seq}`：直接探测
- `Ack{from, addr, seq}`：应答
- `PingReq{from, addr, seq, target}`：请对方代为探测 target（间接探测）

**探测循环（每 probe_period 一轮，round-robin 覆盖所有非 dead peer）**
1. 取队首 peer M（队空则用当前非 dead peer 洗牌重填），发 `Ping(seq)`，等 ack_timeout。
2. 未收到 `Ack(seq)` → 找 K 个其他 peer 发 `PingReq(seq, target=M)`，等 indirect_timeout。
3. 仍未收到 → `mark_suspect(M)`；收到（直接或间接中继）→ `mark_alive(M)`。

**间接中继**：helper 收到 `PingReq{origin, seq, target}` → 用新 helper_seq ping target；
收到 target 的 ack → 向 origin 回 `Ack(origin seq)`。origin 只认自己的 seq，直接/间接都算成功。

**状态机**：alive →（探测全失败）→ suspect →（suspect_to_dead 超时未澄清）→ dead；
收到该 peer 任意报文或探测成功 → 立即回 alive（自证/重新可达）。

**Timings**：probe_period=1s, ack_timeout=300ms, indirect_timeout=400ms,
indirect K=3, suspect_to_dead=3s。

**限制（原型）**：未做 SWIM+ 的 incarnation 号与主动 suspect gossip（靠"探测成功即 alive"
作澄清）；成员仍随 Push 全量携带（增量传播留后续）；acked/relay 表偶有少量陈旧项残留。

## 9. 测试策略

- 单元：HLC 比较、LWW merge 的交换/结合/幂等性。
- 集成：两节点用内存传输（不依赖真 UDP）跑收敛测试。
- 手动：脚本起 3 进程，跑一遍 DoD 三条。
