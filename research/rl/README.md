# 基于图强化学习的态势数据智能推送(4.3.3)

学一个**适配度评分函数** `score(数据类型 d, 平台 p | 态势)`，由分数导出 placement
(谁存/收哪类数据)，在硬约束(覆盖/时效/最低副本)下最小化全局传输成本。
设计见 `docs/research/active-push-rl-design.md`。

## 依赖

```bash
pip3 install torch --index-url https://download.pytorch.org/whl/cpu   # CPU 即可
# numpy 已有；不需要 PyG(二部图消息传递 hand-roll)
```

## 文件

- `env.py` —— 仿真环境：态势(平台角色/链路成本 + 数据写量/查询量/需要者/critical)→ placement
  成本模型(push=Σ持有者链路成本×写量 + query=非本地需要者×查询×路由)+ 硬约束 repair(防奖励 hacking)。
- `baselines.py` —— rule(存所有需要者=人工 interest)/ random / greedy(启发式近最优)。
- `model.py` —— **二部图 GNN 评分器**(数据↔平台消息传递)，greedy 标签监督预训练。

## 跑

```bash
cd research/rl
python3 baselines.py   # 基线对照：greedy 比 rule 省 ~65%(规则基线明显次优)
python3 model.py       # GNN 监督训练 + 在新态势上对比
```

## 已得结果

**里程碑 4(监督学评分函数)**：GNN 在 **30 个未见态势**上成本 152.3 = greedy 152.3 ≪ rule 437.9
→ 比规则基线省 65%、精确达近最优 greedy 上限。GNN 学到适配度评分函数、泛化到新态势,
且"一次前向"即出(greedy 逐态势穷搜)。`python3 model.py`

**里程碑 5(RL 微调,DRL 的价值)**：引入**链路方差**(有些平台便宜但不稳),真实成本含风险项
`均值 + RISK×方差`。greedy/监督 GNN 只看均值(方差盲)。`rl.py` 用风险成本当奖励 REINFORCE 微调:
- rule 1011 / greedy(方差盲) 330 / 监督 GNN 332 / **GNN+RL 317**
- **RL 比 greedy 省 4%、比 rule 省 69%** —— RL 学会用方差特征避开"便宜但不稳"的链路,
  这是方差盲的 greedy 和模仿它的监督 GNN 都做不到的。**DRL 在不确定态势下的鲁棒性价值被证明。**
`python3 rl.py`

## 下一步(收口/报告)

- **score heatmap 可视化**(交付证据)：数据-平台适配度热图 + 态势/方差变化前后 score 变化。
- **消融**(无GNN / 规则only / GNN监督 / GNN+RL)归因提升来源。
- **sim-to-real**：学到的 placement 注入 `node_interest` 跑 harness，核对 sim 与真机字节趋势。
- 技术研究报告(对接合同 4.2.3/4.3.3)。
