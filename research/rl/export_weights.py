#!/usr/bin/env python3
"""导出训练好的 BipartiteGNN 权重 + 一个参考态势的输入/输出,供 Rust 内嵌推理做数值对齐。

产物 research/rl/gnn_weights.json:
  - meta: 架构维度(h, rounds, 输入维度)
  - weights: 各层 weight/bias(嵌套数组)
  - reference: 一个态势的输入张量 + Python 前向输出 score(D×P),供 Rust 单测比对(数值对齐)。

Rust 侧加载 weights 手写前向,喂 reference.input 应得到 reference.output(误差<1e-4)→ 移植正确。
用法: python3 research/rl/export_weights.py
"""
import json
import os

import numpy as np
import torch

from env import Situation
from model import BipartiteGNN, situation_tensors
from rl import pretrain, rl_finetune


def dump_linear(lin):
    return {"weight": lin.weight.detach().numpy().tolist(),
            "bias": lin.bias.detach().numpy().tolist()}


def main():
    torch.manual_seed(0)
    train = [Situation(n_platforms=9, seed=s) for s in range(40)]
    model = BipartiteGNN(h=32, rounds=2)
    print("训练(pretrain + rl_finetune)...")
    pretrain(model, train, epochs=50)
    rl_finetune(model, train, iters=60)
    model.eval()

    weights = {
        "plat_enc": dump_linear(model.plat_enc),
        "data_enc": dump_linear(model.data_enc),
        "upd_plat": [dump_linear(l) for l in model.upd_plat],
        "upd_data": [dump_linear(l) for l in model.upd_data],
        "score_0": dump_linear(model.score[0]),   # Linear(2h+6, h)
        "score_2": dump_linear(model.score[2]),   # Linear(h, 1)
    }

    # 参考态势(固定 seed)→ 输入张量 + Python 前向输出,供 Rust 对齐
    ref = Situation(n_platforms=6, seed=123)
    t = situation_tensors(ref)
    with torch.no_grad():
        out = model(t).numpy()  # D×P logits

    reference = {
        "n_platforms": ref.n,
        "n_data": ref.D,
        "input": {
            "plat": t["plat"].numpy().tolist(),        # P×5
            "data": t["data"].numpy().tolist(),        # D×2
            "needer": t["needer"].numpy().tolist(),    # D×P
            "critical": t["critical"].numpy().tolist(),# D×P
            "link": t["link"].numpy().tolist(),        # P
            "link_var": t["link_var"].numpy().tolist(),# P
        },
        "output": out.tolist(),                        # D×P logits
    }

    payload = {
        "meta": {"h": 32, "rounds": 2, "n_roles": 3,
                 "plat_in": 5, "data_in": 2, "edge_in": 6,
                 "note": "BipartiteGNN(4.3.3) 权重 + 参考态势,供 Rust 内嵌前向数值对齐"},
        "weights": weights,
        "reference": reference,
    }
    out_path = os.path.join(os.path.dirname(__file__), "gnn_weights.json")
    with open(out_path, "w") as f:
        json.dump(payload, f)
    sz = os.path.getsize(out_path)
    print(f"导出 {out_path}({sz} 字节)")
    print(f"参考态势: {ref.n} 平台 × {ref.D} 数据;输出 logits 范围 "
          f"[{out.min():.3f}, {out.max():.3f}]")
    # 打印每层形状,便于 Rust 侧核对
    print("层形状:")
    for k, v in weights.items():
        if isinstance(v, list):
            for i, l in enumerate(v):
                print(f"  {k}[{i}].weight {np.array(l['weight']).shape}")
        else:
            print(f"  {k}.weight {np.array(v['weight']).shape}")


if __name__ == "__main__":
    main()
