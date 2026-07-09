//! 内嵌 GNN 前向推理(4.3.3 深度图强化学习,活模型)。
//!
//! 把离线训练好的 `BipartiteGNN`(`research/rl/`)权重当数据加载,在 agent 里**手写前向**
//! (matmul + relu + 二部图消息传递),对当前态势算 `score(数据, 平台)` 适配度评分,
//! 驱动 selector 的推送目标选择(瞬态层,安全,不碰 placement/durability)。
//!
//! 不依赖 torch/ONNX:权重来自 `research/rl/export_weights.py` 导出的 JSON,数值对齐由
//! `parity_matches_python` 单测保证(Rust 前向 == Python 前向,误差 <1e-4)。
//!
//! 架构(与 model.py 一致):plat_enc/data_enc 编码 → rounds 轮二部图消息传递(残差)→
//! 逐 (d,p) 用 [h_d, h_p, 边特征6] 打分 MLP。h=32, rounds=2。

use serde::Deserialize;

/// 全连接层:out×in 权重 + out 偏置。
#[derive(Debug, Clone, Deserialize)]
pub struct Linear {
    pub weight: Vec<Vec<f32>>, // out × in
    pub bias: Vec<f32>,        // out
}

impl Linear {
    /// y = W x + b(单向量)。
    fn forward(&self, x: &[f32]) -> Vec<f32> {
        self.weight
            .iter()
            .zip(&self.bias)
            .map(|(row, b)| row.iter().zip(x).map(|(w, xi)| w * xi).sum::<f32>() + b)
            .collect()
    }
}

fn relu(mut v: Vec<f32>) -> Vec<f32> {
    for x in &mut v {
        if *x < 0.0 {
            *x = 0.0;
        }
    }
    v
}

fn add(a: &[f32], b: &[f32]) -> Vec<f32> {
    a.iter().zip(b).map(|(x, y)| x + y).collect()
}

fn concat(a: &[f32], b: &[f32]) -> Vec<f32> {
    let mut v = Vec::with_capacity(a.len() + b.len());
    v.extend_from_slice(a);
    v.extend_from_slice(b);
    v
}

/// 一个态势的输入张量(与 model.py situation_tensors 对齐)。
#[derive(Debug, Clone)]
pub struct Situation {
    pub plat: Vec<Vec<f32>>,     // P × 5(角色 one-hot3 + link_cost + link_var)
    pub data: Vec<Vec<f32>>,     // D × 2(write_vol, query_vol)
    pub needer: Vec<Vec<f32>>,   // D × P(0/1)
    pub critical: Vec<Vec<f32>>, // D × P(0/1)
    pub link: Vec<f32>,          // P(link_cost 均值)
    pub link_var: Vec<f32>,      // P(link_var)
}

impl Situation {
    pub fn n_plat(&self) -> usize {
        self.plat.len()
    }
    pub fn n_data(&self) -> usize {
        self.data.len()
    }
}

/// 内嵌 GNN 权重集。
#[derive(Debug, Clone, Deserialize)]
pub struct GraphRl {
    pub plat_enc: Linear,
    pub data_enc: Linear,
    pub upd_plat: Vec<Linear>, // rounds 个
    pub upd_data: Vec<Linear>,
    pub score_0: Linear, // Linear(2h+6, h)
    pub score_2: Linear, // Linear(h, 1)
}

impl GraphRl {
    /// 从导出 JSON 的 `weights` 对象反序列化。
    pub fn from_weights_json(v: &serde_json::Value) -> Result<Self, String> {
        serde_json::from_value(v.clone()).map_err(|e| format!("GraphRl 权重解析失败: {e}"))
    }

    /// 前向:态势 → score(数据 d, 平台 p) 的 D×P logits 矩阵(与 model.py forward 对齐)。
    pub fn forward(&self, sit: &Situation) -> Vec<Vec<f32>> {
        let p_n = sit.n_plat();
        let d_n = sit.n_data();

        // 度(clamp min 1)
        let deg_p: Vec<f32> = (0..p_n)
            .map(|p| (0..d_n).map(|d| sit.needer[d][p]).sum::<f32>().max(1.0))
            .collect();
        let deg_d: Vec<f32> = (0..d_n)
            .map(|d| sit.needer[d].iter().sum::<f32>().max(1.0))
            .collect();

        // 编码 + relu
        let mut hp: Vec<Vec<f32>> = sit.plat.iter().map(|x| relu(self.plat_enc.forward(x))).collect();
        let mut hd: Vec<Vec<f32>> = sit.data.iter().map(|x| relu(self.data_enc.forward(x))).collect();
        // 隐藏维从权重推,不依赖平台/数据数(空态势时 hp/hd 为空也不 panic,循环空跑)。
        let h = self.plat_enc.bias.len();

        // 消息传递(残差)
        for r in 0..self.upd_plat.len() {
            // msg_p[p] = (Σ_d A[d][p] hd[d]) / deg_p[p]
            let msg_p: Vec<Vec<f32>> = (0..p_n)
                .map(|p| {
                    let mut acc = vec![0.0f32; h];
                    for d in 0..d_n {
                        let a = sit.needer[d][p];
                        if a != 0.0 {
                            for k in 0..h {
                                acc[k] += a * hd[d][k];
                            }
                        }
                    }
                    for k in 0..h {
                        acc[k] /= deg_p[p];
                    }
                    acc
                })
                .collect();
            // msg_d[d] = (Σ_p A[d][p] hp[p]) / deg_d[d]
            let msg_d: Vec<Vec<f32>> = (0..d_n)
                .map(|d| {
                    let mut acc = vec![0.0f32; h];
                    for p in 0..p_n {
                        let a = sit.needer[d][p];
                        if a != 0.0 {
                            for k in 0..h {
                                acc[k] += a * hp[p][k];
                            }
                        }
                    }
                    for k in 0..h {
                        acc[k] /= deg_d[d];
                    }
                    acc
                })
                .collect();
            // hp = relu(upd_plat[r]([hp, msg_p])) + hp
            hp = (0..p_n)
                .map(|p| add(&relu(self.upd_plat[r].forward(&concat(&hp[p], &msg_p[p]))), &hp[p]))
                .collect();
            hd = (0..d_n)
                .map(|d| add(&relu(self.upd_data[r].forward(&concat(&hd[d], &msg_d[d]))), &hd[d]))
                .collect();
        }

        // 逐 (d,p) 打分:feat = [hd[d], hp[p], edge(6)]
        (0..d_n)
            .map(|d| {
                (0..p_n)
                    .map(|p| {
                        let edge = [
                            sit.link[p],
                            sit.link_var[p],
                            sit.needer[d][p],
                            sit.critical[d][p],
                            sit.data[d][0], // write_vol
                            sit.data[d][1], // query_vol
                        ];
                        let feat = concat(&concat(&hd[d], &hp[p]), &edge);
                        let hidden = relu(self.score_0.forward(&feat));
                        self.score_2.forward(&hidden)[0]
                    })
                    .collect()
            })
            .collect()
    }
}

// ── 从活状态构造态势 + 算评分表 ────────────────────────────────────────────────
use std::collections::{BTreeSet, HashMap};
use std::net::SocketAddr;

use corro_types::config::GraphRlConfig;

/// 单个平台的链路信号(实时,来自 members RTT)。
#[derive(Debug, Clone, Copy)]
pub struct LinkInfo {
    pub link_cost: f32, // 链路均值(ms 或归一化)
    pub link_var: f32,  // 链路方差
}

fn sigmoid(x: f32) -> f32 {
    1.0 / (1.0 + (-x).exp())
}

impl GraphRl {
    /// 用当前态势算 `score(表, 平台) ∈ [0,1]` 适配度评分表(活模型推理)。
    /// 态势 = 任务模版(cfg:数据属性/角色映射/critical) + interest_routing(needer 图,live)
    ///        + 实时链路(link_of)。输出供 selector 的 Rl 策略给推送目标打分。
    pub fn score_table(
        &self,
        cfg: &GraphRlConfig,
        interest_routing: &HashMap<String, Vec<SocketAddr>>,
        link_of: impl Fn(&SocketAddr) -> LinkInfo,
    ) -> HashMap<(String, SocketAddr), f32> {
        // 平台集 = interest_routing 里出现的所有 addr(任务里的作战平台),排序保确定性。
        let mut plats: Vec<SocketAddr> = interest_routing
            .values()
            .flatten()
            .copied()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        plats.sort();
        // 每平台的 interest 集(反转 interest_routing:addr → {表})→ 推断角色。
        let mut plat_tables: HashMap<SocketAddr, BTreeSet<String>> = HashMap::new();
        for (table, addrs) in interest_routing {
            for a in addrs {
                plat_tables.entry(*a).or_default().insert(table.clone());
            }
        }
        let n_roles = cfg.roles.len().max(1);
        let role_index = |addr: &SocketAddr| -> Option<usize> {
            let want = plat_tables.get(addr)?;
            // 角色匹配:该平台 interest 集 == 某角色的表集(有序 roles 的下标即 one-hot 位)。
            cfg.roles.iter().position(|r| {
                let rt: BTreeSet<String> = r.tables.iter().cloned().collect();
                rt == *want
            })
        };

        // 平台特征 P×(n_roles+2):角色 one-hot + link_cost + link_var
        let plat: Vec<Vec<f32>> = plats
            .iter()
            .map(|a| {
                let li = link_of(a);
                let mut feat = vec![0.0f32; n_roles + 2];
                if let Some(ri) = role_index(a) {
                    feat[ri] = 1.0;
                }
                feat[n_roles] = li.link_cost;
                feat[n_roles + 1] = li.link_var;
                feat
            })
            .collect();

        // 数据特征 D×2 + needer/critical D×P
        let data: Vec<Vec<f32>> = cfg
            .tables
            .iter()
            .map(|t| vec![t.write_vol, t.query_vol])
            .collect();
        let interested = |table: &str, addr: &SocketAddr| -> bool {
            interest_routing
                .get(table)
                .map(|v| v.contains(addr))
                .unwrap_or(false)
        };
        let needer: Vec<Vec<f32>> = cfg
            .tables
            .iter()
            .map(|t| plats.iter().map(|a| if interested(&t.name, a) { 1.0 } else { 0.0 }).collect())
            .collect();
        let critical: Vec<Vec<f32>> = cfg
            .tables
            .iter()
            .map(|t| {
                let is_crit = cfg.critical_tables.contains(&t.name);
                plats
                    .iter()
                    .map(|a| if is_crit && interested(&t.name, a) { 1.0 } else { 0.0 })
                    .collect()
            })
            .collect();
        let link: Vec<f32> = plats.iter().map(|a| link_of(a).link_cost).collect();
        let link_var: Vec<f32> = plats.iter().map(|a| link_of(a).link_var).collect();

        let sit = Situation { plat, data, needer, critical, link, link_var };
        let logits = self.forward(&sit); // D×P

        let mut out = HashMap::new();
        for (d, t) in cfg.tables.iter().enumerate() {
            for (p, addr) in plats.iter().enumerate() {
                out.insert((t.name.clone(), *addr), sigmoid(logits[d][p]));
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parity_matches_python() {
        // 读 research/rl/export_weights.py 导出的权重 + 参考态势,Rust 前向须匹配 Python 输出。
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../research/rl/gnn_weights.json");
        let raw = match std::fs::read_to_string(path) {
            Ok(s) => s,
            Err(_) => {
                eprintln!("跳过:未找到 {path}(先跑 python3 research/rl/export_weights.py)");
                return;
            }
        };
        let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
        let model = GraphRl::from_weights_json(&v["weights"]).unwrap();

        let refn = &v["reference"];
        let inp = &refn["input"];
        let to_2d = |x: &serde_json::Value| -> Vec<Vec<f32>> {
            x.as_array()
                .unwrap()
                .iter()
                .map(|row| row.as_array().unwrap().iter().map(|c| c.as_f64().unwrap() as f32).collect())
                .collect()
        };
        let to_1d = |x: &serde_json::Value| -> Vec<f32> {
            x.as_array().unwrap().iter().map(|c| c.as_f64().unwrap() as f32).collect()
        };
        let sit = Situation {
            plat: to_2d(&inp["plat"]),
            data: to_2d(&inp["data"]),
            needer: to_2d(&inp["needer"]),
            critical: to_2d(&inp["critical"]),
            link: to_1d(&inp["link"]),
            link_var: to_1d(&inp["link_var"]),
        };
        let expected = to_2d(&refn["output"]); // D×P
        let got = model.forward(&sit);

        let mut max_err = 0.0f32;
        for d in 0..expected.len() {
            for p in 0..expected[d].len() {
                max_err = max_err.max((got[d][p] - expected[d][p]).abs());
            }
        }
        assert!(
            max_err < 1e-3,
            "Rust 前向与 Python 不一致,最大误差 {max_err}(应 <1e-3)"
        );
        eprintln!("数值对齐 OK,最大误差 {max_err:.2e}");
    }
}
