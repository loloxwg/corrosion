# 2026-07-15 远端 Linux 100 节点聚合验证

## Context

- 代码快照：`research/active-push`，`bd45fe3`；部署目录
  `/home/xwg/dev/corrosion-active-push-validation`。
- 机台：`192.168.3.214`（`tzdb-ThinkCentre-M930t-N000`），Linux
  6.8.0-134-generic，20 CPU，62 GiB 内存。
- 拓扑：单台物理机上运行 100 个真实 Corrosion agent 进程，使用独立端口和 SQLite DB。
  这比本地开发机外推更强，但仍不是 100 台独立物理机。
- 构建偏差：机台缺少仓库默认的 `clang + mold`，本轮使用已有 Rust 1.92、GCC 和
  等价必要 `RUSTFLAGS` 构建；仓库锁定的 Rust 1.89 因远端下载重试未用。100-agent
  传输与第一轮聚合使用 debug；后续承载密度使用 release。
- 网络边界：节点走同机 loopback，未注入每链路 500 Kbps；QPS 查询全部为 wildcard
  本地命中，因此未用跨节点查询规避带宽限制。

## Decision

本轮分三层验证，避免把外推继续包装成实测：

1. 100 agent 广播式与 `scored_reduce` 同拓扑对照，直接汇总每个 agent 的
   `broadcast.sent.bytes + sync.chunk.sent.bytes`。
2. 直读 100 个本地 SQLite DB 和 buffered 层，验证关心表收齐、非关心表不泄漏；等待
   bookkeeping gap 与 `needed.v2` gauge 连续两次归零。
3. 让 100 个 agent 同时承受查询流量，以共同墙钟时间计算真实聚合 QPS；另跑单 agent
   多连接对照，分开“单节点能力”和“100 agent 共享一台机的资源上限”。

## Validation

### 4.4.3 传输总量

命令：

```bash
ACTIVE_TIMEOUT=180 SETTLE=45 \
python3 research/harness/run.py \
  --nodes 100 --rows 5 --strategies random scored_reduce
```

| 策略 | 收敛 | 推送字节 | 对账字节 | 总传输 |
|---|---:|---:|---:|---:|
| `random` | 0.84 s | 2,882,115 | 32,788 | 2,914,903 |
| `scored_reduce` | 0.82 s | 1,388,370 | 74,993 | 1,463,363 |

结果：推送字节下降 **51.8%**，总传输下降 **49.8%**，超过 30% 指标。

### 部分复制正确性与收敛

命令：

```bash
python3 research/harness/verify_phase2.py \
  --nodes 100 --rows 5 --settle 20 \
  --startup-timeout 180 --quiesce-timeout 120
```

结果：

- 100/100 节点的关心表全部收齐；
- 除写入节点外，非关心表在业务表和 `__corro_buffered_changes` 两层均为 0；
- 初始瞬态 gap（部分节点为 5）最终全部归零；
- `corro.sync.client.needed.v2` 最终连续两次归零；
- 对账过滤触发 490 个版本、保留 3,996 个版本；总判定 `Phase 2 PASS`。

验证中修复了两个 harness 观测错误：内部 gap 表应直读 SQLite，不能经公开 query API；
`needed.v2` 在源码中是 gauge，不能把一次 `0 -> 1` 瞬态当累计重试增长。

### 4.4.2 查询聚合

100 agent 同步起压：

```bash
python3 research/harness/qps_bench.py \
  --nodes 100 --aggregate-concurrent --driver python \
  --ab-parallel 1 --n 5000 --startup-timeout 180 --seed-timeout 180 \
  --json-out research/results/2026-07-15-remote-214/qps-100nodes.json
# 阶梯复跑：--ab-parallel 2 --n 2500，以及 --ab-parallel 4 --n 1250
# 三轮总请求量均为 100 * ab-parallel * n = 500,000
```

- 每节点 1/2/4 条持久连接均执行总计 500,000 次查询，三轮均 0 错误；
- 聚合 QPS 分别为 **23,084 / 23,090 / 20,635**；峰值 **23,090 QPS**；
- 2 连接后已平台化，继续提高到 4 连接反而下降，排除“并发不足”解释；
- 作业平均使用约 13.8--14.2 CPU 核（`time -v` 为 1379%--1415%）。

单 agent、8 个持久连接对照：160,000 次查询，0 错误，**24,348 QPS**。因此单节点
超过“1M / 100 = 10K QPS/节点”的能力门槛，独立硬件线性外推为 2.43M QPS；但本轮
100 agent 实际聚合峰值只有 23,090 QPS，不能用外推替换这一负结果。

### Release 每节点 10K 与承载密度

release 二进制 SHA-256：
`3e74a5b4440ed296af95901a3de5ceea45de901478f1c450eb97c67056378a2d`。
持续时间模式让所有节点在同一个 10 秒窗口内持续起压，并按节点输出
min/median/max 与达标数：

```bash
CORRO_BIN=target/release/corrosion \
python3 research/harness/qps_bench.py \
  --nodes 4 --aggregate-concurrent --driver python \
  --ab-parallel 4 --duration 10 --json-out result.json
```

| agent 数 | 重复 | 聚合 QPS | 每节点最小 QPS | ≥10K 节点 | 结论 |
|---:|---:|---:|---:|---:|---|
| 1 | 1 | 36,808 | — | 单节点通过 | 3.68× 余量 |
| 4 | 1 | 48,975 | 12,221 | 4/4 | 通过 |
| 4 | 2 | 48,369 | 12,074 | 4/4 | 通过 |
| 4 | 3 | 48,751 | 12,164 | 4/4 | 通过 |
| 5 | 1 | 51,581 | 10,277 | 5/5 | 临界通过 |
| 5 | 2 | 49,261 | 9,799 | 0/5 | 失败 |
| 5 | 3 | 50,557 | 10,084 | 5/5 | 临界通过 |
| 10 | 1 | 52,049 | 5,119 | 0/10 | 失败 |
| 20 | 1 | 51,426 | 2,470 | 0/20 | 失败 |

结论：release 单节点代码能力满足 10K，不需要立即开发查询热路径；当前 20 核机台在
agent 与 Python 起压器同机时，**稳定承载密度为 4，5 为临界密度**，总平台上限约
49K--52K QPS。按保守密度承载 100 节点需要 25 台同规格机；若起压器独立部署，应重新
测量并可能提高每台 agent 密度。

### 独立起压器经局域网复核

为排除 agent 与起压器争抢 `.214` CPU，agent API 改绑 `0.0.0.0:8600+`，本机 Mac
（Darwin arm64，14 CPU）使用 `oha 1.15.0` 经 `192.168.3.0/24` 局域网起压。oha
SHA-256 为
`ca53b088d4bc79778948ba36a4766ca0ebc55a3b92b2c090e07161184380a737`。

| 远端 agent 数 | 聚合 QPS | 每节点最小/中位/最大 QPS | ≥10K | 成功率 | 最差 P95/P99 |
|---:|---:|---:|---:|---:|---:|
| 1 | 39,234 | 39,234 | 1/1 | 100% | 1.88/2.01 ms |
| 4 | 46,476 | 11,602/11,623/11,629 | 4/4 | 100% | 8.78/9.96 ms |
| 5 | 46,520 | 9,266/9,307/9,354 | 0/5 | 100% | 11.42/13.16 ms |
| 10 | 46,844 | 4,680/4,683/4,693 | 0/10 | 100% | 25.32/26.69 ms |

独立起压器没有提高稳定密度：**4 agent 仍是安全边界，5 agent 仍不满足每节点 10K**；
但它把测量链路澄清为 server 侧约 46.5K--46.8K 的平台化吞吐。Python 多进程外部起压
上限仅约 29K；ApacheBench 无 keep-alive 测成 TCP 握手吞吐，启用 keep-alive 后又与流式
响应发生 connection reset，均只保留为工具失败证据，不用于验收结论。

## Failure handling

- `192.168.3.212`、`.215` 从本机和 `.214` 均不可达；`.245`、`.248` 的 SSH 端口开放，
  但现有密钥无登录权限。本轮没有绕过授权边界。
- 远端缺 `ab` 和 matplotlib。传输实验直接输出文本；查询压测新增无外部依赖的 Python
  多进程持久 HTTP 驱动，并保存逐进程 JSON。
- 外部负载机最终使用临时安装在 `/tmp/corrosion-oha/bin/oha` 的 oha 1.15.0；服务端与
  起压器完全分离。`qps_bench.py --server-only/--load-only` 支持复现该拓扑。
- 单节点没有 peer，不会出现成员 `considered ACTIVE`。QPS harness 已改为单节点等待
  API listener，多节点仍等待全部成员 ACTIVE；启动异常会清理已拉起进程。

## Operational plan

当前可以确认：**4.4.3 在远端 100 agent 规模通过；4.4.2 release 单节点能力与 4-agent
稳定密度已由独立起压器复核通过，但 100 节点 1M QPS 尚未得到半实物实证。** 下一次应
至少提供 25 台同规格机台（或更高单机算力），把 agent 按稳定密度分散，并保持起压器独立。
物理 CPU 分散，并为每个节点/容器配置 500 Kbps 网络命名空间。验收必须同时满足：

- 100 个独立节点身份全部在线；
- 共同时间窗成功查询数不少于 1M/s，错误率为 0；
- server、client CPU 未饱和，或明确给出饱和点；
- 传输对照继续保持至少 30% 降幅；
- gap、needed、buffered 和非关心表在规定收敛窗内归零。

原始结果位于 `research/results/2026-07-15-remote-214/`。其中 `.log` 受仓库
`*.log` 忽略规则保护，JSON 保存逐起压进程数据。
