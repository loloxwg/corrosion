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
    /// 输入维度(要求所有权重行等宽,否则 None)。
    fn in_dim(&self) -> Option<usize> {
        let w = self.weight.first()?.len();
        (self.weight.iter().all(|r| r.len() == w) && self.weight.len() == self.bias.len())
            .then_some(w)
    }

    fn out_dim(&self) -> usize {
        self.bias.len()
    }

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

    /// 权重 shape 校验(加载时调,配错即拒载退回启发式)。
    /// 必须做:`Linear::forward` 的 zip 在维度不匹配时**静默截断**而非报错——
    /// 配置的角色数与训练时不一致(plat 特征=n_roles+2)会悄悄算出错位的垃圾分。
    pub fn validate(&self, n_roles: usize) -> Result<(), String> {
        let dim = |l: &Linear, name: &str| {
            l.in_dim()
                .ok_or_else(|| format!("{name} 权重行宽不一致或与 bias 长度不符"))
        };
        let h = self.plat_enc.out_dim();
        let want_plat_in = n_roles + 2;
        let plat_in = dim(&self.plat_enc, "plat_enc")?;
        if plat_in != want_plat_in {
            return Err(format!(
                "plat_enc 输入维 {plat_in} ≠ 角色数+2={want_plat_in}(config roles 数与训练不一致?)"
            ));
        }
        if dim(&self.data_enc, "data_enc")? != 2 || self.data_enc.out_dim() != h {
            return Err("data_enc 应为 2 → h".into());
        }
        if self.upd_plat.is_empty() || self.upd_plat.len() != self.upd_data.len() {
            return Err(format!(
                "消息传递轮数不符: upd_plat={} upd_data={}",
                self.upd_plat.len(),
                self.upd_data.len()
            ));
        }
        for (r, (up, ud)) in self.upd_plat.iter().zip(&self.upd_data).enumerate() {
            for (l, name) in [(up, "upd_plat"), (ud, "upd_data")] {
                if dim(l, name)? != 2 * h || l.out_dim() != h {
                    return Err(format!("{name}[{r}] 应为 2h → h(h={h})"));
                }
            }
        }
        if dim(&self.score_0, "score_0")? != 2 * h + 6 {
            return Err(format!("score_0 输入维应为 2h+6={}", 2 * h + 6));
        }
        if dim(&self.score_2, "score_2")? != self.score_0.out_dim() || self.score_2.out_dim() != 1 {
            return Err("score_2 应为 score_0 输出维 → 1".into());
        }
        Ok(())
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
        // wildcard:声明 `*` 的 peer 关心全部表 → 对每张表 needer=1。
        // 与 selector::interested_set 口径对齐,否则全量节点在合法集内却被 GNN 主信号压到队尾。
        let interested = |table: &str, addr: &SocketAddr| -> bool {
            interest_routing
                .get(table)
                .map(|v| v.contains(addr))
                .unwrap_or(false)
                || interest_routing
                    .get("*")
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
    use corro_types::config::{GraphRlRole, GraphRlTable};

    fn zeros(out: usize, inp: usize) -> Linear {
        Linear { weight: vec![vec![0.0; inp]; out], bias: vec![0.0; out] }
    }

    /// 手造最小合法模型(h=2,n_roles=1):编码/消息传递全零,
    /// 打分头只透传边特征里的 needer(feat[2h+2]=needer)→ logit = needer。
    /// 用于可控地观察某特征是否进了模型。
    fn needer_probe_model() -> GraphRl {
        let h = 2;
        let mut score_0 = zeros(1, 2 * h + 6);
        score_0.weight[0][2 * h + 2] = 1.0; // edge[2] = needer
        let mut score_2 = zeros(1, 1);
        score_2.weight[0][0] = 1.0;
        GraphRl {
            plat_enc: zeros(h, 1 + 2), // n_roles=1
            data_enc: zeros(h, 2),
            upd_plat: vec![zeros(h, 2 * h)],
            upd_data: vec![zeros(h, 2 * h)],
            score_0,
            score_2,
        }
    }

    #[test]
    fn validate_rejects_role_mismatch() {
        let m = needer_probe_model(); // 按 n_roles=1 造
        assert!(m.validate(1).is_ok(), "匹配的角色数应通过");
        let err = m.validate(3).unwrap_err();
        assert!(err.contains("plat_enc"), "角色数不符须在 plat_enc 处拒载: {err}");
    }

    #[test]
    fn validate_rejects_ragged_weights() {
        let mut m = needer_probe_model();
        m.score_0.weight[0].pop(); // 行宽破坏
        assert!(m.validate(1).is_err(), "行宽不一致须拒载");
    }

    #[test]
    fn wildcard_peer_counts_as_needer() {
        // B 声明 "*"(关心全部):GNN needer 特征应=1(与 selector interested_set 口径对齐),
        // 探针模型 logit=needer → score(flight,B)=sigmoid(1)>0.7;不关心的 C 应=sigmoid(0)=0.5。
        let m = needer_probe_model();
        let cfg = GraphRlConfig {
            weights_path: String::new(),
            tables: vec![GraphRlTable { name: "flight".into(), write_vol: 0.0, query_vol: 0.0 }],
            roles: vec![GraphRlRole { name: "recon".into(), tables: vec!["flight".into()] }],
            critical_tables: vec![],
        };
        let a: SocketAddr = "[::1]:9000".parse().unwrap(); // 精确关心 flight
        let b: SocketAddr = "[::1]:9001".parse().unwrap(); // wildcard
        let c: SocketAddr = "[::1]:9002".parse().unwrap(); // 不关心(只出现在别的表)
        let mut routing = HashMap::new();
        routing.insert("flight".to_string(), vec![a]);
        routing.insert("*".to_string(), vec![b]);
        routing.insert("other".to_string(), vec![c]);
        let zero_link = |_: &SocketAddr| LinkInfo { link_cost: 0.0, link_var: 0.0 };
        let scores = m.score_table(&cfg, &routing, zero_link);
        let s = |addr| scores[&("flight".to_string(), addr)];
        assert!(s(a) > 0.7, "精确关心者 needer=1: {}", s(a));
        assert!(s(b) > 0.7, "wildcard 关心者 needer 应=1: {}", s(b));
        assert!((s(c) - 0.5).abs() < 1e-4, "不关心者 needer=0: {}", s(c));
    }

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
        // 真权重须过 shape 校验(训练 ROLES=3:recon/strike/jam)——防 validate 误杀生产权重。
        model.validate(3).expect("导出的真权重应通过 shape 校验");

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
