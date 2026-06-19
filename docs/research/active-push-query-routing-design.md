# 跨平台查询路由设计(对接 4.4.2)

> 背景：路线 A 实现部分复制后，节点只**本地存它 interest 的表**。4.4.2 要"任意节点查任意数据"
> (1M QPS / 100 节点 / 500Kbps)。本地没有的表 → 必须路由到持有它的节点。

## 1. 目标与约束

- **任意节点的 `/v1/queries` 能答任意表**：本地有 → 本地查(快)；本地无 → 转发到持有节点取回。
- **不破坏现有本地查询快路径**：interest 内的查询零额外开销(corrosion 本地 SQLite 读，这是 1M QPS 的底)。
- 单进程(不引多实例)；复用 corrosion 现成机制，侵入可控。

## 2. 关键约束与架构选择

**`api_addr` 不跨节点可见**：成员状态(`members.rs` MemberState)只 gossip `addr`(gossip 地址)，
`api_addr` 只在本地 Agent。所以**不能**让节点直接 HTTP 调对方的查询端口。

→ **路由复用 QUIC 双向流 + `BiPayload`**(`transport.open_bi` + `bi.rs` 分发，sync 已在用)：
按 **gossip 地址**(成员状态已知)寻址，over 现有加密 QUIC；新增一个查询转发消息类型。

## 3. 数据流

```
客户端 → 节点R(recon, interest={flight})  /v1/queries  "SELECT * FROM battlefield ..."
  │
  │ ① 解析查询涉及的表(复用 pubsub 的 sqlite3_parser → table_columns)
  │ ② 表 ∈ 我的 interest? 
  │      是 → 本地执行(build_query_rows_response，现状,零额外开销)
  │      否 → 路由:
  │ ③ 查持有者:node_interest[battlefield] → actor_id → members 解析 gossip 地址
  │ ④ transport.open_bi(holder_addr) 发 BiPayloadV1::QueryForward { stmt }
  ▼
节点H(strike, 有 battlefield)：bi.rs 收到 QueryForward
  │ ⑤ 本地执行该查询(复用 build_query_rows_response/同款执行)
  │ ⑥ 把结果行流式写回 bi-stream
  ▼
节点R：把收到的行**原样转发**给客户端(流式)
```

## 4. 改动点

| 位置 | 改动 |
|---|---|
| `corro-types/src/broadcast.rs` | `BiPayloadV1` 加 `QueryForward { stmt: Statement }`(speedy `default_on_eof`) |
| `corro-agent/src/agent/bi.rs` | 分发 `QueryForward` → 本地执行查询 + 把行流式写回 bi-stream |
| `corro-agent/src/api/public/mod.rs` | `api_v1_queries`/`build_query_rows_response` 前置：提取表→判 interest→本地 or 路由 |
| 持有者解析 helper | 表 → node_interest 持有者 actor_id → members gossip 地址(复用推送端 `load_interest_routing` 同款) |
| 表提取 helper | 复用 `pubsub.rs` 的解析得到查询涉及的表集合 |

## 5. 1M QPS 怎么够

- **大多数查询是本地的**：placement 跟着任务=访问模式走(侦察主查飞行状态、打击主查战场环境…），
  本地命中 = corrosion 原生 SQLite 读，极快。1M QPS 主要靠这条快路径。
- **路由的是少数跨任务查询**：多一跳 QUIC。
- 后续优化(本期不做)：路由结果缓存、多持有者负载均衡、本地存"热表索引"。

## 6. 边界与失败处理

- **查询跨多表且分属不同 interest**：本期先支持"单表 / 全在本地 / 全在同一持有者"；跨持有者的 JOIN 留后续。
- **无持有者 / 持有者不可达**：返回明确错误或换一个持有者重试(node_interest 可能多个持有者)。
- **正确性**：路由只读、不改数据；持有者本地执行 = 与直接在持有者上查等价。
- **interest=空(全量节点)**：本地有全部表 → 永远本地查,不触发路由(回归安全)。

## 6.5 Codex 复核后的关键调整(开工前必须解决,否则返工)

1. **wire type 不能裸接两套序列化**：`BiPayloadV1` 用 speedy，而 `Statement`/`QueryEvent` 只派生 serde。
   → 新变体 **`QueryForward { sql: Vec<u8>, params_json: Vec<u8> }`**(speedy)：把 SQL 文本 + 参数 JSON 编码成字节承载。
   结果回传同理(QueryEvent 行 → JSON 字节帧)。**先定死这个 wire type,其余才不返工。**
2. **每次查询独占一条新 bi-stream**：`bi.rs:80` 读首个 BiPayload 后把流交给 `serve_sync` 长流，
   之后无法再切协议。QueryForward 必须**与 sync 完全并列**(新开 bi-stream),不混进 SyncStart 那条。
3. **抽 QueryEvent sink**：把 `build_query_rows_response`(`public/mod.rs:274`)的执行核心抽出来
   (它产出 `QueryEvent::Columns/Row/EndOfQuery` 到一个 `mpsc::Sender<QueryEvent>`)，HTTP 与 QUIC **共享**，
   不重复实现 `readonly()` 检查(第 322 行)与参数处理。**只读保证要在持有者端强制**,不只入口检查。
4. **holder 解析 helper + node_interest**：`load_interest_routing`(`broadcast/mod.rs:74`)私有且在广播 loop 缓存；
   抽成可复用的 pub helper(表→持有者 actor_id→members `addr`)。node_interest 目前仅 harness 建/写
   (生产路径无)——原型阶段沿用 harness 的；查询应过滤 `active`。wildcard(interest=[]) 节点待办。
5. **并发与背压**：sync 有 `agent.limits().sync` 信号量,查询**需独立限流**(别和 sync 抢);
   QUIC 回传要有超时/背压;持有者不可达要换持有者重试。
6. **表提取**:`pubsub.rs` 的解析是私有且绑订阅;需新增 `pub fn extract_select_tables(sql, schema)`,
   并处理子查询/CTE(`children` 表名不会自动并入)——边界先文档化(本期支持单表/简单 SELECT)。

**1M QPS 的真风险**(Codex 强调):不是 `open_bi`,而是**远程查询比例 + 结果集大小 + 500Kbps 带宽**。
路由只能是少数路径;高 QPS 全靠本地命中(placement 跟着访问模式)。本期先做对(correctness),QPS 压测后续。

## 7. 分步

1. `QueryForward` 消息 + bi.rs 分发 + 持有者本地执行并流式回传(先不接 /v1/queries,用一个内部测试触发)。
2. `/v1/queries` 前置路由判定(表提取 + interest 判断 + 持有者解析 + 转发/中继)。
3. harness 验收：recon 节点查 battlefield → 路由到 strike → 取回正确结果；本地表查询仍走快路径。
4. (后续)QPS 压测 + 缓存/负载均衡。
