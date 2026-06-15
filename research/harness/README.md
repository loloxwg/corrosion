# 实验 harness（mission 拓扑 + 降量对照）

对接考核 **4.4.3**：构建 侦察/打击/干扰 3 类异构节点 + 飞行状态/战场环境/目标 3 类数据，
**分别统计「广播式」(`random`) 与「智能主动推送」(`scored_reduce`) 的全局数据传输总量，算降幅**。

## 任务→数据需求映射（数据需求模版，对应 4.2.3）

| 任务 | 关心的数据(表) |
|---|---|
| 侦察 recon | 飞行状态 flight |
| 打击 strike | 战场环境 battlefield + 目标 target |
| 干扰 jam | 目标 target |

节点按 `i % 3` 轮转分配角色；`interest_routing`(表→关心它的 peer)据此生成并写进每个节点配置。

## 用法

```bash
cargo build -p corrosion          # 先编 binary
python3 research/harness/run.py --nodes 18 --rows 15 --strategies random scored_reduce
```

参数：
- `--nodes N`：节点数（节点 0 为种子兼写入端，其余 bootstrap 到它）
- `--rows R`：每张表写 R 行（共 R×3 行），测每个节点看到**它关心的表**的全部行的耗时
- `--strategies ...`：依次跑哪些策略（`random` / `scored` / `scored_reduce` / `rl`）

## 测量什么

| 输出列 | 来源指标 | 含义 |
|---|---|---|
| 收敛(s) | 外部轮询 | 每节点看到**其关心表**的全部行的耗时（按任务定义的相关收敛，越小越好） |
| 推送字节 | `corro.broadcast.sent.bytes` | ★推送轴传输字节（降量主战场） |
| 对账字节 | `corro.sync.chunk.sent.bytes` | anti-entropy 兜底传输字节 |
| 总传输 | 推送字节 + 对账字节 | 全局数据传输总量（4.4.3 考核对象） |
| 广播重复 | `corro.broadcast.duplicate.count` | 收到的重复广播（冗余，越小越好） |

每节点开 Prometheus（端口 9400+i），harness 抓取并对各节点求差求和。
指标全集与按轴分类见 `docs/research/active-push-plan.md §5.1`。

## 降量来自哪里：两条推送路径都过评分函数

corrosion 推送分两路，1a 让 **两条都过 selector / 评分函数**（对应 4.3.3「评分函数决策目标平台」）：
- **ring0 快速 flood**（最近一圈，原本无脑全发）→ `scored_reduce` 只发**关心该数据**的 ring0 + 1 个覆盖配额。
- **远端 selector**（非 ring0 + rebroadcast）→ 按 interest 打分取 Top-K。

`random` 等价改造前行为（flood 全发 + 远端随机），作为「广播式」基线。

## 实测：1a 单独不能稳定降量（重要，诚实结论）

⚠ **单次 localhost 测量方差极大，不可信**（同一 18 节点配置见过 +47% 和 -33%）。
用 `sweep.py` 每点重复 3 次取均值后（每表 20 行，random vs scored_reduce）：

| 节点数 | 推送字节降幅(均值[min~max]) | 总传输降幅(均值) |
|---|---|---|
| 6  | 21.8% [12~29] | 21.2% |
| 12 |  1.9% [-18~31] | 1.4% |
| 18 | 18.1% [1~30] | 17.8% |
| 24 |  8.5% [-14~43] | 8.2% |
| 30 | **-20.4% [-44~-1]** | -20.6% |

**结论：没有单调趋势，误差带横跨 [-44%, +43%]，30 节点甚至净增 20%。1a 推送减量
单独并不能可靠降低全局传输，规模越大越可能反噬。** 趋势图见 `reduction_vs_nodes.png`。

### 为什么 reduce 会反噬

reduce 跳过非关心 peer 的直推 → 那些节点拿不到 → 数据靠 **(a)** anti-entropy 全量兜底
补回来；**(b)** 收敛变慢/不均触发更多 `max_transmissions` 重传轮次 + 多跳 rebroadcast。
节点越多，这种放大越压过省下的直推量。**推送减量机制本身正确（确实少推），但在
corrosion 的 epidemic + 全量 anti-entropy 模型里，省下的被补传/重传吃掉。**

## 下一步：1b 是必需项，不是可选

- **1b：对账按 interest 过滤**——`generate_sync` 只 need/拉本节点关心的表（解决版本→表映射），
  断掉 anti-entropy 的全量兜底；配合「查得到≠每节点都存」(4.4.2 查询路由)。这是让全局总量
  真正、稳定下降的关键。
- **同时要抑制重传/rebroadcast 放大**：reduce 下收敛判定与 `max_transmissions` 需重新审视，
  避免"少直推→多重传"把账抵消。
- **`scored`(链路打分) 在 localhost 无差异**：RTT≈0 全 ring0。要看链路优势需注入延迟
  (`tc`/`dummynet`)。`scored_reduce` 的相关度过滤不依赖延迟，但如上所述单独不足以降量。

## 工具

- `run.py`：单次对照（一组节点数/策略）。**注意单次方差大，仅供快速 smoke test。**
- `sweep.py`：扫多个节点数、每点重复取均值 + 画误差带趋势图（结论以此为准）。
  ```bash
  python3 research/harness/sweep.py --sizes 6 12 18 24 30 --rows 20 --repeats 3
  ```
