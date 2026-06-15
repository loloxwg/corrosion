# gossipdb

无中心、最终一致的 KV 数据库学习原型（Rust）。

连任意节点读写，写入靠 gossip 在集群内扩散，同一个 key 可在任意节点写，
冲突用 HLC + LWW（混合逻辑时钟 + 最后写入胜出）裁决，集群最终收敛到同一份数据。
节点宕机不影响其余节点，重启后自动追平。

> 学习原型：内存存储、明文 UDP、无认证。**不要直接上生产**。
> 设计与取舍见 [`docs/superpowers/specs/2026-06-15-gossipdb-design.md`](docs/superpowers/specs/2026-06-15-gossipdb-design.md)。

## 构建

```bash
cargo build
cargo test     # 12 个单元测试
```

需要 Rust 1.95+（edition 2024）。

## 运行

每个节点要两个地址：`--bind` 是节点间 gossip 的 UDP 口，`--http` 是客户端用的口。
`--seeds` 是启动时先认识的节点，之后自动发现全集群。**连哪个节点都等价**（无中心）。

```bash
# 单机起 3 节点；生产里把 127.0.0.1 换成各机 IP
target/debug/gossipdb --id 1 --bind 127.0.0.1:7001 --http 127.0.0.1:8001 --seeds 127.0.0.1:7002,127.0.0.1:7003 &
target/debug/gossipdb --id 2 --bind 127.0.0.1:7002 --http 127.0.0.1:8002 --seeds 127.0.0.1:7001,127.0.0.1:7003 &
target/debug/gossipdb --id 3 --bind 127.0.0.1:7003 --http 127.0.0.1:8003 --seeds 127.0.0.1:7001,127.0.0.1:7002 &
```

| 参数 | 说明 | 默认 |
|---|---|---|
| `--id` | 节点 id，集群内唯一，需 < 65519 | 必填 |
| `--bind` | gossip UDP 监听地址 | `127.0.0.1:7000` |
| `--http` | HTTP API 监听地址 | `127.0.0.1:8000` |
| `--seeds` | 种子节点 UDP 地址，逗号分隔 | 空 |

## 数据类型（CRDT）

每个 key 的值是一种 CRDT，类型由**首次写入**决定（别在一个 key 上混用类型）：

| 类型 | 端点前缀 | 并发语义 |
|---|---|---|
| **Register**（LWW 寄存器） | `/kv` | 覆盖型，并发取 hlc 大者，**会丢一个**（v1 取舍） |
| **Counter**（PN-Counter） | `/counter` | 增量型，并发加减**自动求和，不丢更新** |
| **Set**（OR-Set） | `/set` | 集合，并发增删**自动求并集** |

合并都满足交换/结合/幂等（CvRDT），gossip 乱序/重复/丢包都能收敛。

## HTTP API

| 方法 | 路径 | 作用 |
|---|---|---|
| `PUT` | `/kv/{key}` | 写寄存器，body 即 value（任意字节） |
| `GET` | `/kv/{key}` | 读寄存器，200+body 或 404 |
| `DELETE` | `/kv/{key}` | 删除寄存器（写墓碑），204 |
| `POST` | `/counter/{key}` | 计数器增量，body 是整数（如 `5` 或 `-3`） |
| `GET` | `/counter/{key}` | 读计数器，`{"value": N}` |
| `POST` | `/set/{key}/add` | 集合加元素，body 是元素 |
| `POST` | `/set/{key}/remove` | 集合删元素 |
| `GET` | `/set/{key}` | 读集合，`{"members": [...]}` |
| `GET` | `/members` | 集群成员与存活状态 |
| `GET` | `/debug` | dump 本节点内部状态（含每个 key 的 CRDT 类型与值） |

```bash
# 写
curl -X PUT 127.0.0.1:8001/kv/user:1 -d '{"name":"张三"}'

# 读（连任意节点，写到 1 读 3）
curl 127.0.0.1:8003/kv/user:1            # -> {"name":"张三"}

# 看版本（响应头 x-hlc / x-node）
curl -i 127.0.0.1:8001/kv/user:1

# 删除（之后 GET 返回 404；删除会扩散到全集群）
curl -X DELETE 127.0.0.1:8001/kv/user:1

# 计数器（并发不丢更新）
curl -X POST 127.0.0.1:8001/counter/hits -d 1
curl -X POST 127.0.0.1:8002/counter/hits -d 5
curl 127.0.0.1:8003/counter/hits            # -> {"value":6}

# 集合（并发求并集）
curl -X POST 127.0.0.1:8001/set/tags/add -d radar
curl -X POST 127.0.0.1:8002/set/tags/add -d air
curl 127.0.0.1:8003/set/tags                # -> {"members":["air","radar"]}

# 运维
curl 127.0.0.1:8001/members
curl 127.0.0.1:8001/debug
```

从代码里用就是普通 HTTP 调用（Python 示例）：

```python
import requests
NODE = "http://127.0.0.1:8001"
requests.put(f"{NODE}/kv/score", data="100")
print(requests.get(f"{NODE}/kv/score").text)   # 100
requests.delete(f"{NODE}/kv/score")
```

## 必须知道的语义（不然会踩坑）

1. **最终一致，不是立即一致**：写完立刻去*另一个*节点读可能还没到（约 500ms 窗口）；
   写完读*同一个*节点一定读得到。
2. **Register（`/kv`）同 key 多处写 = 最后写的赢**（按 HLC 比，平局比 node_id），并发写丢一个。
   要不丢更新就用 **Counter（`/counter`）或 Set（`/set`）**——它们并发自动求和/求并。
3. **删除是墓碑**：删除不物理移除，而是写一条"已删除"标记，靠 HLC 与并发写竞争——
   删晚于写则删生效，写晚于删则数据复活。墓碑目前永久保留（无 GC，见下）。

## 一键演示

```bash
bash scripts/demo.sh        # register：扩散收敛、冲突一致、杀节点+重启追平
bash scripts/demo_crdt.sh   # CRDT：counter 并发不丢更新、set 求并集
```

## 架构

```
HTTP API        PUT/GET/DELETE /kv/{k}   GET /members  GET /debug
   │
Store (trait)   内存 HashMap<key, Crdt>             ← 留 trait，日后换 RocksDB
   │
Merge           CvRDT join：register=LWW / counter=求和 / set=并集
   │
两条传播路 ───────────────────────────────────
  ① 推送   写入 → recent 缓冲 → 每 500ms 随机推 3 个 peer（UDP，快，容丢）
  ② 对账   每 2s 随机挑 1 peer，TCP 交换 key→哈希 摘要补齐缺/旧条目（兜底 + 新节点追平）
   │
成员/故障       last-seen 超时：alive → suspect(3s) → dead(8s)，重连自动恢复
   │
传输            UDP=推送/故障检测（容丢小包）  TCP=对账/大消息（可靠任意大小）
```

模块：`hlc`（版本号）/ `store`（存储+merge）/ `wire`（报文）/ `gossip`（传播+故障）/
`api`（HTTP）/ `config`（参数）/ `main`（组装）。

## 已知限制

- **无认证**：明文 UDP，任何能发包的人都能读写/投毒，仅限可信网络。真实部署需加
  HMAC 预共享密钥或 Noise/mTLS（见 spec §8.1）。
- **仅内存**：重启丢数据（靠对账从其他活节点追回；全员重启则全丢）。
- **墓碑无 GC**：删除的 key 永久占位。
- **CRDT 状态式传播**：每次发整份 CRDT 状态，未做 delta 增量优化；对账靠内容哈希，
  CRDT 内部 map 序列化顺序可能不稳定，会偶发多余整份同步（不影响收敛）。
- **传输混合**：推送/故障检测走 UDP（容丢），对账/大消息走 TCP（可靠）；尚未加 TLS（见 spec §8.1）。
- **Counter/Set 不支持整 key 删除**（set 元素可删）；覆盖型并发 v1 走 LWW（会丢一个，MV-Register 留 v2）。
- **全量复制**：每节点存全量，无分片。
- **无事务、无强一致**。

## 后续扩展点（已留接口）

- `Store` 是 trait → 接 **RocksDB** 持久化
- CRDT 再扩展：**MV-Register**（覆盖型并发不丢、留 siblings）、字段级 merge（一行多字段各用各策略）
- `pick_peers()` 现纯随机 → 换**主动推送打分**（分片重叠度 / 链路质量 / 节点角色），
  即 Mission-aware gossip
```
