# 任务驱动智能数据主动推送 —— 技术研究报告

> 研究载体：[superfly/corrosion](https://github.com/superfly/corrosion)(Rust + SWIM + cr-sqlite + QUIC
> 的 masterless 最终一致分布式 SQLite)的研究 fork,分支 `research/active-push`。
> 对接考核指标 4.2.3 / 4.3.3 / 4.4.2 / 4.4.3(原文见 `active-push-plan.md §0.1`)。
> 本报告综述问题、方法、踩过的坑、最终方案与实测结论,并诚实标注边界。

## 1. 问题与总体思路

军工"任务驱动智能数据主动推送":侦察/打击/干扰等异构无人节点,按**任务关联的数据需求**
把态势数据**精准推送给合理平台**,降低全局传输冗余(4.4.3 降 30%),同时支持**任意节点查任意数据**
(4.4.2 1M QPS),核心技术是**GNN+DRL 学适配度评分函数**(4.3.3)与**gossip 多跳路由**(4.2.3)。

**核心洞察(贯穿全程)**:全局传输 = 推送轴(broadcast) + 对账轴(anti-entropy)。
corrosion 默认**全量复制**(每节点存全部),要降量必须把它改成**部分复制**(节点只存任务需要的数据),
而部分复制又要求**查询路由**(本地没有的数据查得到)。这两件事是同一枚硬币:placement(谁存什么)
是 4.4.3 与 4.4.2 的共同支点。

## 2. 降量(4.4.3):从踩坑到部分复制

### 2.1 几次失败教训(诚实)
- **只改推送轴不够**:推送端按 interest 减少目标后,**对账轴会全量兜底补回来**,净降量不稳,
  规模越大越反噬(实测 30 节点一度净增 20%)。→ 必须**两轴都按 interest 过滤**。
- **单次 localhost 测量方差极大**(同 18 节点见过 +47% 和 −33%)→ 一律**重复取均值 + 误差带**。
- **"挑收件人"还不够**:corrosion 把多条不同表的改动**攒成一个 payload** 发给 union 关心者,
  收方整批 apply → 非关心数据**搭便车泄漏**。根因是 **payload 混表**,正解是**源端按表分组/裁剪**。
- **接收端过滤救不回流量**:字节读入即计数,丢弃只省存储 → 降量必须在**发送端**做。

### 2.2 最终方案(单进程,不改一致性核心)
- **interest 模型**:每节点自声明 interest(关心的表),经复制表 `node_interest` 传到全集群
  (= 4.2.3"数据需求模版"的落地)。
- **推送轴**:广播按表分组(消除混表搭便车)+ 只发关心者 + selector 接缝(留 RL 接入位)。
- **对账轴**:握手带 interest,`handle_need` **版本级过滤**——版本不碰关心表则发 `Changeset::Empty`
  (复用 crsqlite `crsql_set_db_version` 推进度水位、干净关缺口),**不碰 bookie 收齐判定**(安全)。
  过滤覆盖**全部 serving 分支**:Full/Partial × (已应用 `crsql_changes` / 缓冲 `__corro_buffered_changes`)。
  早期只过滤 Full+已应用分支,缓冲窗口/Partial 请求下中转节点会把非关心表原始行直发(并发/大规模/
  多源 serving 才触发的潜在泄漏);现四分支统一走 `touches_interest`,堵死该缺口。

### 2.3 实测结论
- 部分复制达成:各角色节点只本地有任务相关表,非关心表本地为 0,无对账死锁(`verify_phase2.py`)。
- **★测量纠偏(诚实、重要)**:部分复制后,非关心节点本地无该表,但 API 查询(`/v1/queries`)会经
  **查询路由(4.4.2)转发到持有者**返回计数——这会把「本地已裁剪」误读成「到处都有」(假泄漏/假 FAIL)。
  验证本地裁剪必须**直读本节点 sqlite db(绕路由)**:`count_rows_local`。修正测量后 9 节点 `verify_phase2.py`
  PASS(关心表收齐、非关心表本地 0、gap/needed 不增长)。降量机制一直正确,是测量工具被自身路由欺骗。
- **全局传输降幅(localhost harness,`sweep.py` 多规模×重复均值+误差带)**:
  小规模 68~77%(6~24 节点);大规模(SETTLE=45 长 settle 排除传播竞态 + repeats=3)
  **18→64.4% / 30→57.8% / 42→54.0%**,误差带很紧(±1~2%)。**全程远超 30%(42 节点仍 24 点余量)。**
- **★规模下滑的诚实修正**:早期把规模下滑归因为"harness 启动写 interest 的传播竞态"。
  但**长 settle 排除竞态后降幅仍随规模下降**(64→58→54%)→ 这是**真实规模效应**,非纯竞态:
  固定开销(对账握手字节、SWIM 成员、`node_interest` 表自身复制)随节点数增长、不随 interest 缩减,
  占比变大吃掉相对降幅。**好在下降在减速**(18→30 降 6.6 点,30→42 仅 3.8 点),且近似 log(n) 线性
  (斜率 ~-12 点/log单位)→ **log-线性外推 100 节点 ≈44%(仍超 30%约 14 点)**。
- **边界**:① 100 节点为外推(localhost 最大测到 42 节点),减速趋势上方,需半实物复核;
  ② 单事务跨多表的"行级精度"留后续(scoped bookie 高风险,经评估暂不做,因多表事务少见)。

## 3. 跨平台查询(4.4.2):部分复制的必要配套

部分复制后本地缺非关心表 → **查询路由**:任意节点 `/v1/queries` 本地有则本地答(快路径),
没有则**经 QUIC 双向流路由到持有者**(持有者本地执行,结果流式回传)。按 gossip 地址寻址
(绕开 api_addr 不 gossip)。实测 `query_routing_test.py`:侦察节点查本地 flight ✓、
路由 battlefield/target 拿回正确计数 ✓。
**边界**:JOIN/子查询表提取、多持有者重试留后续(本期做对 correctness)。

### 3.1 1M QPS 可达性(已实测 + 外推,与 Codex 联合分析)
1M = 100 节点 × 10K/节点(全局聚合)。核心机制:**1M 必须靠本地命中**(部分复制 + interest 让高频查询本地化),
跨节点路由查询被 500Kbps 带宽卡死,撑不起 1M。三层证据(`qps_bench.py` 可复现):

- **单节点本地命中实测**:`/v1/queries` HTTP 全路径点查 **≥25K QPS**(14 核单机,ab 与 corrosion 抢 CPU,
  故是**下界**;独立节点更高)。**远超需要的 10K/节点**。SQLite 执行本身仅 **125ns**(可忽略),
  瓶颈在 HTTP/JSON 序列化/tokio 路径(~38μs/查询)。
- **100 节点聚合外推**:单节点 25K × 100 ≈ **2.5M ≥ 1M**(同构独立节点 + 高本地命中率假设)。
- **500Kbps 路由上限(为何本地命中是必然)**:单链路按 700B/查询估 ≈ **89 QPS/link**,100 节点全连通
  4950 链路 → 路由聚合上限 **≈442K(单向)/884K(双向),均 < 1M**。即跨节点路由扛不起 1M,
  **本地命中不是优化而是数学必然** —— 这正是部分复制 placement 的价值。
- **优化(prepare→prepare_cached)**:省重复 SQL 解析,但端到端增益微乎其微(HTTP 主导,简单点查解析占比小);
  保留为标准实践(复杂/参数化查询才显价值)。诚实记录:真正的 QPS 杠杆在 HTTP/序列化路径,非 SQL 解析。

**诚实边界**:① 无 100 节点半实物,聚合 ×100 是外推;② localhost 多进程受**共享 CPU**限(实测 2 节点
同机合计仍 ~26K 非 2×),无法验证聚合线性,真实独立硬件才线性;③ 路由上限为带宽数学推导;
④ 假设业务查询高本地命中率(placement 跟访问模式走);⑤ 高并发缓存/连接池调优(读池 20、HTTP 并发 128)留后续。

## 4. 智能推送模型(4.3.3):GNN + 深度强化学习

placement(谁存/收哪类数据)此前是人工规则。4.3.3 要用 **GNN+DRL 学适配度评分函数**
`score(数据类型 d, 平台 p | 态势)`,由分数导出 placement,在硬约束下最小化传输。

### 4.1 形式化(Codex 评审后)
- **状态 = 态势图**:平台为节点(角色/链路均值/链路方差/...),链路为边;数据-平台需要/critical 关系。
- **动作 = 适配度评分 → Top-K placement**(路径/行级暂缓)。
- **奖励 = −传输成本**(push=Σ持有者链路成本×写量 + query=非本地需要者×查询×路由);
  覆盖/critical/最低副本做**硬约束 mask+repair**(防"学全不发骗降量")。
- **训练 = 规则/greedy 监督预训练 → RL 微调**(防 PPO 从零不稳)。

### 4.2 实现与结果(`research/rl/`,torch CPU,二部图 GNN)
- **监督学到评分函数**:二部图 GNN 在 **30 个未见态势**上达 greedy 近最优、**比规则基线省 65%**,
  且"一次前向"即出(greedy 是逐态势穷搜)。→ GNN 确实挖到了适配度评分函数并泛化。
- **DRL 的价值(鲁棒性)**:引入**链路方差**(便宜但不稳的平台),真实成本含风险项。greedy/监督 GNN
  方差盲;RL 用风险成本当奖励微调(每态势自评基线稳定化)→ 学会**避开高方差链路**,
  **比 greedy 省 4%、比规则省 69%**(消融:random 1154 > rule 1011 > greedy 330 ≈ 监督 332 > GNN+RL 317)。
- **证据图**:`fig_score_heatmap`(评分函数)/`fig_variance_aware`(RL 避高方差,监督不会)/`fig_ablation`(归因)。
- **诚实点**:首次随机对称扰动 RL 没赢(无结构性鲁棒空间),改"链路方差"风险模型 + 稳定基线后才显出价值。

### 4.3 sim-to-real 端到端验证(已做,与 Codex 联合;含重要诚实校准)
RL 的优势**分两部分**,各自对应不同度量,分别端到端验证:

- **① placement 基数(高写数据少存 → holder 少 → 字节少)** → **已由 4.4.3 降量端到端验证**
  (§2.3 的 54~77% 降量本质就是它:少存 = 少推送 + 少对账)。这是 RL/placement 的主优势。
- **② 链路方差规避(查询密集数据放稳定链路 holder)** → **延迟/可靠性优势**,本次用注入实测验证。

**关键 sim-to-real 校准(诚实、反直觉)**:起初想在真 corrosion 上用**字节量**显示②,实测发现**显示不出**——
app 层给坏链路注入丢包,corrosion 的 anti-entropy 补传**比逐条 broadcast 更省字节**(实测丢 60% 反而总量 ↓5%),
且**多路径冗余(broadcast+gossip+对账)绕过单链路故障**(延迟 broadcast,sync 照样秒收敛)。
即 corrosion 太鲁棒,链路质量不放大 app 字节量。**真实可观测的是查询路由延迟**:查询密集数据只存少数 holder,
消费者查它要路由过去,holder 在坏链路则慢(无冗余可绕)。

**端到端实测(`research/harness/rl_e2e_latency.py` + `transport.rs` 的 `CORRO_LINK_FAULTS` 应用层注入)**:
- 段 A(真 RL 模型决策):对查询密集数据 target,RL 选的 holder 链路方差 **0.11** vs greedy **1.69**——RL 选了远更稳的 holder。
- 段 B(真 corrosion):把 target 放在 RL 选的稳定 holder(注入 13ms)vs greedy 选的高方差 holder(注入 203ms),
  消费者路由查询 **P50 16ms vs 207ms** → **RL 的方差感知 placement 使真实查询延迟低 13×**。
- **结论**:RL 主优势(基数→字节)已由 4.4.3 验证;次优势(方差→查询延迟)由本实验端到端验证。两者都落地。

**诚实边界**:① 链路方差→延迟为线性映射(合成),真实战场链路更复杂;② 单 holder 简化;
③ 应用层注入延迟≠QUIC 子包级重传(更高保真需 macOS dummynet,需 sudo,留作附录);④ 12 平台/小规模。

### 4.4 集群级归因:RL 价值的「三处吸收」与安全归位(★核心诚实结论)

§4.3 的 13× 是在**受控单 holder**场景取得的。把 RL 接进**真集群**逐层验证后(`rl_placement_e2e.py` + Codex 对抗复核),得到一个反直觉但扎实的结论:**RL 的"智能"超出平凡基线的部分,在当前 corrosion 集群里处处被现成机制吸收**——三个维度同构:

| 维度 | RL 想加的价值 | 被谁吸收 | 实测 |
|---|---|---|---|
| **字节** | placement 少存高写数据 | **cardinality(副本数)** | RL vs 同副本预算随机砍(random_cut)推送字节 **↓0.0%**(完全相同):同副本数→推送字节数学上相同,与"砍哪张表"无关 |
| **查询延迟** | 把数据放稳定 holder | **resolve(改后按 RTT 选最优)** | 改 `resolve_table_holder` 选 RTT 最优 holder 后,RL vs random_cut 延迟平手:两者共享 critical holder,resolve 选其中好的→同一 holder,RL 的非 critical 精选选不到 |
| **广播选路** | 发给稳定链路 peer | **ring(RTT 分桶已排序)** | `BroadcastStrategy::Rl` 方差感知选目标,e2e 仅 ↓5%:高方差⟹高均值 RTT⟹高 ring⟹已被 ring0-flood 降优先级,纯方差信号空间极小 |

**为什么三处同构**:corrosion 在**副本管理、RTT 路由、链路分桶**这些维度本身已经做得扎实(cardinality 决定字节、resolve/ring 决定链路偏好),RL 试图注入的"把数据/流量放对地方"的智能,恰好落在这些已被现成机制覆盖的维度上 → 被吸收。**这不是 RL 失败,是 corrosion 设计扎实**;RL 干净显价值只在"绕开这些机制"的受控场景(§4.3 单 holder 13×)。

**副产品(独立真改进)**:为解锁 RL 而做的 `resolve_table_holder` 从「选 SQL 首个」→「选 RTT 最优 holder」,本身是独立于 RL 的正确生产改进(异构网络查询路由到最近 holder);合成对调探针证明其生效(target 放 {快+慢},对调角色后 `{both}` 恒命中快 holder)。

**RL 的安全归位(关键设计决策)**:RL 有两种可能落点——
- **动态 placement(谁长期持有,durability 层)**:❌ **不安全**。实测(`dynamic_interest_hole.py`)坐实一个正确性 bug:节点动态"重新关心"某表时,对账早先把该表版本发 `Changeset::Empty`→标 `Cleared` 关 gap→**永不重取**→本地永久缺历史数据,却以 holder 身份对外服务不完整结果(NodeR 重新关心后 0/15,对照 15/15)。Codex 复核确认真 bug + 推送/对账读两套 interest 状态。
- **瞬态选路(这条广播发给谁/扇出/路径)**:✅ **安全**。合法范围由 interest 规则圈定,RL 只在集内优化,**推错自愈**(anti-entropy 兜底,不改 placement/不碰 durability)。已实现为 `BroadcastStrategy::Rl`(方差感知,单测+e2e)。

**故 RL 归位在瞬态层**(符合合同 4.3.3"目标平台/路径"的措辞,"谁长期持有"=interest 规则,静态安全)。这也界定了未尽工程:若未来要动态 placement,须先补"重新关心→scoped 回填 + handoff 纪律 + 推送/对账 interest 口径统一"的安全协议(见 §6 未尽事项)。

### 4.5 critical 表传输优先级(「优先…及时推送」的落地,已实验验证)

合同 4.3.3 要求「**优先**将作战任务目标态势数据**及时**、精准推送」。"精准"由 interest+GNN
承担(发给谁);"优先/及时"此前无传输层机制——所有表的广播共用攒批缓冲,scope 不变时最多等
`bcast_interval`(500ms)tick 才发,**每一跳 rebroadcast 都要再等一次** → 多跳时延
O(跳数×攒批间隔)。

**机制**:`gossip.critical_tables`(如 `["target"]`)里的表触发广播缓冲**立即 flush**
(本地全局路径 + 每跳 rebroadcast 路径),当轮发送 → critical 数据多跳时延降为 O(跳数×RTT)。
空配置=行为不变(全表同等攒批)。与 `graphrl.critical_tables`(GNN 特征,模型面)独立,
这里是传输优先级(数据面)。

**实测**(`critical_latency_test.py`,复用多跳场景:writer→needer 直连切死逼数据走中继
rebroadcast——攒批延迟正住在那条路径;sync 拉长隔离;逐行写、needer 直读细粒度轮询):

| 表 | needer 单行到达时延(≥2 跳) |
|---|---|
| flight(普通,攒批) | 中位 **182ms**,P90 333ms(≈uniform(0,500ms) tick 等待) |
| target(critical,立即 flush) | 中位 **13ms**,P90 16ms(**↓93%**) |

证据:直连丢弃计数 53(切断生效)、critical flush 计数 90(机制真在触发)。
**诚实边界**:localhost RTT≈0,真实网络时延下限=跳数×真 RTT;critical 表写入频繁时立即
flush 会牺牲攒批吞吐(时效换吞吐,正是"优先"的语义);半实物复核待里程碑 2。

## 5. 不确定链路通断下的多跳路由(4.2.3,已实验验证)

**机制论证**:本方案的多跳路由 = 两条互补路径,均保留 corrosion 原生机制、interest 过滤叠加其上:
- **快路径 = rebroadcast 疫情式多跳**:每个收到广播的节点按 `max_transmissions` 递减再转发,
  转发同样经 selector 按 interest 过滤 → 多跳扩散被限制在关心者子图内。单条链路断,数据经
  其他关心者中继绕行,跳数自适应(无需显式路由表)。
- **兜底 = anti-entropy sync**:周期随机对账版本区间(按 interest 版本级过滤),快路径整体
  失效(如接收方所有入向广播链路断)时保证最终收敛。
- **控制面(SWIM/foca)独立于数据面**:成员协议走 `send_datagram`,数据广播走 `send_uni`,
  数据链路断不等于节点失联——成员/RTT 信息仍在,选路信号不丢。

**实验**(`research/harness/multihop_test.py`,应用层链路故障注入 `CORRO_LINK_FAULTS`,
只切广播数据面;6 节点全关心 flight,writer=node0,needer=node5,写 20 行,needer 直读本地 db 计收齐):

| 场景 | 注入 | sync 间隔 | 结果 |
|---|---|---|---|
| 对照(直连可用) | 无 | 拉长 60~120s | 0.4s 收齐,丢弃计数 0 |
| **A:直连断→多跳快路径** | writer→needer drop_p=1.0 | 拉长 60~120s(**隔离对账**) | **0.4s 收齐**(丢弃 26 次=直连真在切)——sync 被隔离,数据必经中继 rebroadcast **≥2 跳**到达,**绕行代价≈0** |
| **B:快路径全断→sync 兜底** | 全部发送方→needer drop_p=1.0 | 默认 | **1.5s 收齐**(丢弃 52 次)——广播颗粒无存,anti-entropy 补齐,最终一致 |

9 节点复跑结论一致(对照 0.2s / A 0.4s 绕行 +0.2s / B 3.3s),非单次运气。

**实验暴露并修复的真问题(控制面鸡生蛋)**:interest 声明表 `node_interest` 自身的广播曾被
interest 过滤——没有节点「声明关心 node_interest」→ 合法集为空 → 声明广播零目标,传播完全
依赖 sync 兜底(默认间隔短未暴露;间隔拉长到 60s 时 interest 图分钟级不完整)。对账侧早有恒
豁免(`interest_for_sync` 恒加入 `node_interest`),推送侧缺失。**修复**:selector `interest_pool`
对触及控制面表的广播返回不过滤(全网扩散),与对账侧口径对齐(`selector.rs::CONTROL_TABLE`,
单测 `control_table_broadcast_bypasses_interest_filter`)。教训:元数据(谁关心什么)必须比
数据(被关心的内容)有更强的传播保证。

**诚实边界**:应用层注入(丢弃发生在 QUIC 之上,非真实网络丢包/重传行为);localhost 拓扑
RTT 均匀,真实异构网络多跳绕行代价不会是 0;debug 构建 sync 默认间隔短(1~2s),场景 B 的
兜底时延随 `min/max_sync_backoff` 配置伸缩;半实物 100 节点通断验证在里程碑 2。

## 6. 总体结论与交付

| 考核项 | 状态 | 关键证据 |
|---|---|---|
| 4.4.3 全局传输降 30% | ✅ localhost 54~77%(42 节点 54%,外推 100 节点≈44%,均>30%;待半实物复核) | `sweep.py` 多规模重复均值+误差带;`verify_phase2.py` 部分复制(业务表+buffered 双层)+无死锁 |
| 4.4.2 任意节点查 + 1M QPS | 查询路由 ✅ correctness;1M=单节点微基准外推(**未半实物验证**) | `query_routing_test.py` 路由正确;`qps_bench.py` 单节点≥25K→×100 外推 2.5M,500Kbps 路由上限442K |
| 4.2.3 数据需求模版 + gossip 多跳 + 模型 | ✅(多跳已实验验证,§5) | `node_interest` 模版(节点启动**自写**);`multihop_test.py` 直连断→中继多跳 0.4s 收齐 / 快路径全断→sync 兜底;GNN+DRL 模型 |
| 4.3.3 GNN+DRL 适配度评分函数 | ✅ 核心验证 + 端到端;集群级归因诚实(§4.4);「优先/及时」传输优先级 ✅(§4.5) | 监督省 65% + RL 鲁棒省 69%;3 张证据图;`rl_e2e_latency.py` 受控 16 vs 207ms(13×);`rl_placement_e2e.py` 集群逐层证「三处吸收」;RL 安全归位瞬态选路 `BroadcastStrategy::Rl`;`critical_latency_test.py` critical 表多跳时延 13 vs 182ms(↓93%) |

**生产侧 interest 写入(本轮收口)**:corrosion 启动时从 `gossip.interest` 配置**自写** `node_interest`
(`run_root.rs::write_own_interest`),不再依赖外部/harness 写入,且启动即可见(减轻传播竞态)。
`SKIP_WRITE_INTEREST=1 verify_phase2.py` 验证:全靠 corrosion 自写,部分复制仍 PASS。

**interest wildcard(本轮收口)**:`interest=["*"]` = 关心全部(= 全量节点),与精确表名两种语义。
无需模式匹配,本质是「把该节点当全量节点处理」:推送端 `interested_set` 把 `*` 关心者并入每张表;
对账 `interest_for_sync` 见 `*` 返回 None(不过滤);查询端 `*` 节点全本地查、且可做任意表持有者
(`resolve_table_holder` 匹配 `table=? OR '*'`)。`wildcard_test.py` 实测:`*` 节点本地收全 3 表、
共存的 jam 节点仍只有 target(部分复制不被破坏)、jam 经路由从 `*` 持有者取回 flight。

**动态 interest 安全边界(本轮收口,★正确性)**:实测坐实(`dynamic_interest_hole.py` + Codex 复核)一个**动态 placement 的正确性 bug**——
- **机制**:节点先不关心表 T 时,对账把 T 的版本发 `Changeset::Empty` → `process_empty_version` 标 `KnownDbVersion::Cleared` → 从 `__corro_bookkeeping_gaps` 删该 gap;`generate_sync` 的 need 计算里 `Cleared` 与「真有完整数据」**不区分**(`bookie.rs` `contains_version`)→ 节点动态"重新关心"T 后**永不重取**那些版本 → 本地永久缺 T 的历史数据,却以 holder 身份对查询本地答不完整结果(实测 NodeR 重新关心后 0/15,对照 NodeS 15/15)。另 Codex 发现推送侧读运行时 `node_interest`、对账侧读静态 `gossip.interest` 配置,**两套 interest 状态分裂**。
- **收口结论**:**静态 interest(本项目所有考核/实验用法)完全安全**——CRDT 值不分叉、不删本地数据、部分复制正确。bug **仅在动态变更 interest 时触发**(见 §4.4:这也是 RL 归位瞬态层、不做动态 placement 的原因之一)。
- **若未来需动态 placement**,最小安全协议:① 重新关心 T 时,把 T 相关的已 `Cleared` 版本重新插回 gaps 触发 scoped 回填(需给 `Changeset::Empty` 带表集合,或保守全段重开);② 失去 interest 的 handoff 纪律(≥min_replicas 当前 holder 已持有再摘除路由);③ 推送/对账 interest 口径统一(都读 `node_interest`);④ 回填需集群有全量/wildcard holder 留完整历史作源。列为独立立项。

**未尽事项(诚实)**:① 100 节点半实物真聚合(本期单节点实测 + ×100 外推);② 高并发缓存/连接池调优;
③ RL 链路优势的 **dummynet/QUIC 重传**高保真复核(本期已用应用层延迟注入端到端验证查询延迟优势,§4.3);
④ 单事务多表的行级精度(scoped bookie,高风险);⑤ **动态 interest/placement 安全协议**(上段:静态安全,动态有已坐实数据洞 bug,须补 scoped 回填+handoff+口径统一,独立立项)。

**方法学**:全程"设计 → Codex 对抗复核 → 实现 → 实测(重复均值)→ 诚实记录(含失败)",
多处靠 Codex 复核纠偏(payload 混表根因、一进程约束下路线选择、RL 奖励 hacking 防护、稳定化、1M QPS 可达性骨架)。
