# 运行期 DDL(集群建表)设计

> 2026-07-21。动机:本体(ontology-web)每个 ObjectType 绑定一张 `ont_inst__<key>` 表,
> ObjectType 运行期可新增 → corrosion 需要运行期建表并分发全集群。
> DDL 低频(对象类型级,非数据级),但必须可靠到达每个现在与未来的节点。

## 1. 需求(已与用户确认)

| 维度 | 决定 |
|---|---|
| DDL 权威源 | **单一控制面**(ontology-web 的 sidecar 节点独家发起),集群内只分发+应用 |
| 能力范围 | **加表 + 加列**(纯增量;删表/删列/改列仍走带外 destructive 流程,不进本机制) |
| 乱序容忍 | **拒收 + 对账兜底**:未知表数据变更当次拒收不标已处理,DDL 到位后 anti-entropy 补齐 |
| 接入口 | **新 HTTP API `POST /v1/schema`**,本体侧 `ensure_type_table` 从「写文件+subprocess reload」改为一次 HTTP 调用 |

## 2. 方案选择

DDL 分发通道三候选:

- **A. CRR 控制表分发(选定)**:DDL 语句作为一行数据写入 CRR 表,复用 corrosion
  自己的复制面(广播 + anti-entropy)到全网。分发、重试、断线补齐、新节点入网补历史
  全部免费;与 `node_interest` 同 pattern(元数据经 CRR 表传播)。
- B. gossip 协议加 DDL 消息类型:动 wire 协议,且广播尽力而为,掉包要自建重试 =
  重造 anti-entropy。无 A 不具备的优点,否。
- C. 控制面逐节点 HTTP 推:corrosion 改动最小,但控制面须维护全员名单;半联通网络
  漏发节点永远建不了表 —— anti-entropy 能补数据,补不了 DDL 本身。致命,否。

关键前置事实(已核实):

- `execute_schema(agent, statements)`(`corro-agent/src/agent/util.rs:1471`)已接受任意
  SQL 语句列表,持 schema 写锁、diff、`constrain()` 校验齐备。admin `reload` 只是
  「读文件 → 喂它」的壳。运行期 DDL 的本地应用 = 直接调它。
- `init_schema`(`corro-types/src/schema.rs:205`)启动时从 **db 内 `__corro_schema` 表**
  恢复 schema(非文件)→ 运行期建的表重启后自动认得,零额外持久化。
- 本体侧 `corrosion_schema.py::type_table_ddl` 已生成完整 DDL 文本
  (CREATE TABLE 全列 `NOT NULL DEFAULT` 符合 CRR 约束 + CREATE INDEX),
  换 HTTP 调用即闭掉此前评审缺口①(多节点 schema 分发)。

## 3. 组件设计

### 3.1 HTTP API:`POST /v1/schema`

- Body:DDL 语句字符串数组(与 `/v1/transactions` 同形)。
- Handler 流程:解析 → **拒绝破坏性变更**(删表/删列/改列 → 400,带原因)→
  本地 `execute_schema` → 成功后把语句写入 `corro_ddl_log`(seq = 本地
  `MAX(seq)+1`,单写者下即全局单调;经 `make_broadcastable_changes` 走正常复制)
  → 200 返回应用摘要。
- 配置门:`api.allow_runtime_schema`(默认 **false**),仅控制面 sidecar 打开。
  单一控制面靠配置强制;其余节点收到请求返回 403。

### 3.2 DDL 日志表:`corro_ddl_log`(corrosion 自有 CRR 表)

```sql
CREATE TABLE corro_ddl_log (
  seq INTEGER NOT NULL PRIMARY KEY,   -- 控制面单写,单调递增
  sql TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL DEFAULT ''
);
```

- 启动时 corrosion 自动确保存在并 CRR 化(不依赖用户 schema 文件)。
- 单写者 → seq 无冲突;seq 即全局 DDL 顺序。
- **控制表豁免**(硬要求,复用多跳实验教训「元数据须比数据有更强传播保证」):
  - selector `interest_pool` 对触及本表的广播返回 None(不被 interest 过滤,
    与 `node_interest`/CONTROL_TABLE 同待遇);
  - sync 侧 `interest_set`(peer/mod.rs)恒 include(同时作用于 SyncStart 声明与
    serving 侧 `touches_interest` 过滤);
  - GNN/critical 等策略面无需感知(豁免在合法集层,先于策略)。

### 3.3 应用钩子(数据面)

- 位置:`process_multiple_changes` 提交后,检查本批 `changes_per_table` 是否触及
  `corro_ddl_log`;是则唤起 schema 同步任务(独立 task,不在 apply 事务内跑 DDL)。
- 任务逻辑:读 `seq > last_applied_seq` 的行,**按 seq 严格顺序**逐条
  `execute_schema`;每应用一条,`last_applied_seq` 推进(存 `__corro_state`,
  与该条 DDL 同一事务提交)。
- **遇 seq 空洞即停**,等 anti-entropy 补齐后由下次触及重新唤起 —— 不跳号,
  保证各节点按同序应用。
- 幂等:`apply_schema` 是 diff 语义,重放已建表 = no-op;崩溃恢复后从
  `last_applied_seq` 续跑安全。
- 启动时也跑一次(处理「停机期间经 sync 收到 DDL 行但没触发钩子」的情况)。

### 3.4 乱序防线(未知表数据先到)

- 现状待核实并按需改造:今天未知表变更进 apply 大概率整批事务报错回滚。
  目标行为:**逐条隔离** —— 应用前按当前 schema 过滤未知表的 change,
  被过滤的版本**不标已处理、不 mark cleared**,留在 needed;同批其他表不受毒化。
- DDL 应用后,缺失版本由 anti-entropy 正常补齐(复用现有修复面,零新机制)。
- 窗口分析:控制面「建表 → 写数据」有先后顺序,且 `corro_ddl_log` 豁免
  interest 过滤传播最快,竞态窗口秒级;DDL 低频场景代价可忽略。

### 3.5 本体侧改造(ontology-web,一行级)

- `ensure_type_table`:删「写 schema 文件 + subprocess `corrosion reload`」,
  改 `_post_json("/v1/schema", [ddl])`(已有 helper,与 `/v1/transactions` 同构)。
- schema 文件可留作审计产物(可选),不再是权威源。

## 4. 错误处理

| 场景 | 行为 |
|---|---|
| 破坏性 DDL 提交 | API 400,不落日志表 |
| 非控制面节点收到 POST /v1/schema | 403(`allow_runtime_schema=false`) |
| 本地 execute_schema 失败(控制面) | API 4xx/5xx 带 SchemaError,不写日志表(先本地成功再分发,保证日志表里只有可应用的 DDL) |
| 从节点应用某条 DDL 失败 | 记日志、`last_applied_seq` 不推进,下次触发重试;不跳号 |
| seq 空洞 | 停在空洞前,等 sync 补行 |
| 节点重启 | schema 从 `__corro_schema` 恢复;钩子启动扫尾补应用 |
| 新节点入网 | anti-entropy 补全 `corro_ddl_log` 历史 → 顺序应用 → 再补数据 |

## 5. 测试计划

- **单测**:破坏性 DDL 拒绝;`corro_ddl_log` 豁免(selector 返回 None / sync include);
  顺序应用 + 空洞停车;幂等重放;`last_applied_seq` 事务性。
- **e2e(harness)**:
  1. 控制面 POST 建表后立刻写数据 → 全网(含 DDL 竞态节点)收敛;
  2. 节点重启 → 表仍在、`last_applied_seq` 正确;
  3. 新节点入网 → 自动补齐建表历史 + 数据;
  4. 乱序注入(`CORRO_LINK_FAULTS` 堵控制面→某节点直连,逼数据经多跳先到)→
     不毒批 + DDL 到位后收敛;
  5. ontology-web 侧 `verify_corrosion_e2e.py` 增加「新建 ObjectType → 多节点可查」用例。

## 6. 诚实边界

- 删表/删列不在本机制内(走带外 destructive 流程);本体删 ObjectType 时表残留,
  由后续治理(低频 + 存储便宜,可接受)。
- DDL 全网生效是最终一致(秒级传播延迟),不提供「建表即全网可见」的强保证 ——
  已按「拒收+对账兜底」语义消化。
- 加列后旧行的新列取 DEFAULT;cr-sqlite `begin_alter/commit_alter` 路径由
  `apply_schema` 既有实现负责,本设计不新增列变更逻辑。
