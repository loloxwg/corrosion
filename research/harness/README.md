# 实验 harness

对主动推送传播策略做对照实验：编排 N 个本地 Corrosion 节点 → 跑写入负载 →
测「收敛延迟」+ 抓 broadcast/gossip 指标，对比 `random`（基线）与 `scored`（链路感知）。

## 用法

```bash
cargo build -p corrosion          # 先编 binary
python3 research/harness/run.py --nodes 5 --rows 50 --strategies random scored
```

参数：
- `--nodes N`：节点数（节点 0 为种子，其余 bootstrap 到它）
- `--rows R`：在节点 0 写入 R 行后，测全集群都看到这 R 行的耗时
- `--strategies ...`：依次跑哪些策略（`random` / `scored` / `rl`）

## 测量什么

| 指标 | 来源 | 含义 |
|---|---|---|
| 收敛延迟(s) | 外部轮询所有节点 | 写入到全集群可见的耗时（越小越好） |
| 广播发送 | `corro.broadcast.spawn` | 广播传输次数（带宽代理，越小越省） |
| 广播重复 | `corro.broadcast.duplicate.count` | 收到的重复广播（冗余，越小越好） |
| 对账字节 | `corro.sync.chunk.sent.bytes` | anti-entropy 兜底传输字节 |

每节点开 Prometheus（端口 9400+i），harness 抓取并对各节点求差求和。

## 重要：localhost 看不出 scored 优势（预期）

本机多节点的 RTT≈0，所有 peer 都落进 `ring0`，于是 `scored` 的链路打分**无差异化**，
结果与 `random` 几乎相同（已实测：收敛 0.30s vs 0.30s，广播 96 vs 99）。**这是正确的**——
没有链路差异，链路感知策略当然没区别。

要看出 `scored` 的优势，需要制造差异化：

1. **注入链路延迟**：让节点间 RTT 有梯度 → `ring` 分化 → scored 偏好近邻。
   实现方式（待办）：传输层按目的地加合成延迟，或用 `tc`/`dummynet` 做网络仿真。
2. **mission-aware 数据相关度**（阶段2）：在 localhost 即可看出差异——
   只把数据推给"关心它"的节点，无关节点收到更少 → 带宽/冗余下降。这条不依赖延迟仿真。

harness 的测量逻辑是通用的，两种差异化方式都能直接复用它出对照数据。

## 输出示例

```
策略        收敛(s)     广播发送    广播重复    对账字节
random    0.30        99          0           1097
scored    0.30        96          0           0
```
