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

## 已得结果(里程碑 4)

GNN 在 **30 个未见过的态势**上：成本 152.3 = greedy 152.3 ≪ rule 437.9 →
**比规则基线省 65%，精确达到近最优 greedy 上限**。即 GNN 学到了适配度评分函数、
泛化到新态势，且是"一次前向"的快速评分(greedy 是逐态势穷搜，慢)。

## 下一步(里程碑 5+)

- **RL 微调**(PPO/REINFORCE)：在**扰动/动态态势**(链路通断、任务重指派、不确定性)下，
  学出比静态 greedy 更鲁棒的策略——greedy 是逐态势穷搜、不抗扰动，RL 该在这胜出。
- **score heatmap 可视化**(交付证据)：数据-平台适配度热图 + 态势变化前后 score 变化。
- **消融**(无GNN/规则only/GNN监督/GNN+RL)归因。
- **sim-to-real**：学到的 placement 注入 `node_interest` 跑 harness，核对 sim 与真机。
