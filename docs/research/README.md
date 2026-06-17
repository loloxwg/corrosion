# 研究 fork：任务感知主动推送（Mission-aware Active Push）

本仓库是 [superfly/corrosion](https://github.com/superfly/corrosion) 的研究 fork。
研究目标：把 Corrosion 的**随机 gossip 传播**升级为**任务感知的定向主动推送**，
对比验证送达率↑ / 收敛延迟↓ / 带宽↓。

## 仓库布局

```
crates/                       Corrosion 原代码（基线）
  corro-agent/src/broadcast/mod.rs   ★ 传播选 peer 的核心，研究改造点
docs/research/
  README.md                   本文件
  active-push-plan.md         主动推送改造方案 + 对照实验设计（§8 = 1b 前提）
  active-push-1b-design.md     1b 详细设计：对账按 interest 过滤（含 §7.5 根因复盘=payload 混表）
  active-push-cluster-design.md 圈子(cluster)分群实现部分复制——推荐主线，不改 corrosion 内部
  gossipdb-design.md          教学最小实现的设计文档（原理推导）
sandbox/gossipdb/             自研 200 行教学原型（HLC/CRDT/SWIM/对账全透明）
                              用于吃透 Corrosion 每块在干嘛，非生产代码
```

## 分支与上游

- 工作分支：`research/active-push`
- 上游：`git remote` 名为 `upstream`，`git fetch upstream` 拉 Corrosion 更新
- 当前是浅克隆（`--depth 1`）；需要完整历史时 `git fetch --unshallow upstream`
- 推到自己的 GitHub fork：`gh repo fork superfly/corrosion --remote-name origin` 后 push

## 为什么是 Corrosion

Corrosion = Rust + SWIM(foca) + CRDT(cr-sqlite) + SQL(SQLite) + QUIC + 最终一致 masterless，
正是"无中心、最终一致、带 SQL"的成品。它已有的随机传播是天然基线；它**没有**的
"任务感知定向传播"正是本研究的增量。详见 `active-push-plan.md`。

## sandbox/gossipdb 的作用

自研教学原型，把 Corrosion 内部概念用最小代码摊开：混合逻辑时钟、CRDT 合并、
SWIM 故障检测、anti-entropy 对账、UDP/TCP 分流。读它再读 Corrosion 源码无黑盒。
不参与生产构建，仅作学习与对照参考。
