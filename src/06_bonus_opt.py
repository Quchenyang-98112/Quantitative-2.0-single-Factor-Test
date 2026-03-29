#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
06_bonus_opt.py
组合优化模块（加分题）

功能：
- 基于预测收益率进行组合优化
- 个股权重上下限约束
- 行业权重偏离约束
- L2正则化惩罚

输入文件：
- results/panel_standardized_neutralized.parquet: 标准化和中性化后的因子数据

输出文件：
- results/bonus_weights.csv: 优化后的权重分配
- results/bonus_nav.csv: 组合净值序列
- results/bonus_perf.csv: 组合绩效指标
- results/bonus_nav.png: 净值走势图
- results/bonus_summary.txt: 优化结果摘要
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

try:
    import cvxpy as cp
except Exception:
    cp = None


# 优化参数配置
BONUS_FACTOR_COL = "factor_neu_mad"              # 使用的因子列
BONUS_PRED_RET_COL = "pred_ret_A2_mad_neutralized"  # 预测收益列
BONUS_POOL_SIZE = 100                            # 股票池大小
MAX_WEIGHT = 0.03                                # 个股最大权重
MIN_WEIGHT = 0.0                                 # 个股最小权重
INDUSTRY_DEV = 0.10                              # 行业偏离上限
L2_PENALTY = 0.05                                # L2正则化系数
TOP_HOLDINGS_TO_SHOW = 20                        # 显示前N大持仓


def calc_drawdown(nav: pd.Series) -> float:
    """计算最大回撤
    
    参数:
        nav: 净值序列
    
    返回:
        最大回撤值（负数）
    """
    running_max = nav.cummax()
    dd = nav / running_max - 1
    return dd.min()


def calc_perf_metrics(ret: pd.Series, periods_per_year: int = 252) -> dict:
    """计算绩效指标
    
    计算年化收益、年化波动、夏普比率、最大回撤等指标
    
    参数:
        ret: 收益率序列
        periods_per_year: 年化周期数，默认252（日度）
    
    返回:
        包含各项绩效指标的字典
    """
    ret = ret.dropna()
    if len(ret) == 0:
        return {
            "ann_return": np.nan,
            "ann_vol": np.nan,
            "sharpe": np.nan,
            "max_drawdown": np.nan,
            "win_rate": np.nan,
            "cum_return": np.nan,
            "n_periods": 0,
        }
    nav = (1 + ret).cumprod()
    ann_return = nav.iloc[-1] ** (periods_per_year / len(ret)) - 1
    ann_vol = ret.std(ddof=0) * np.sqrt(periods_per_year)
    sharpe = ann_return / ann_vol if pd.notna(ann_vol) and ann_vol != 0 else np.nan
    mdd = calc_drawdown(nav)
    return {
        "ann_return": ann_return,
        "ann_vol": ann_vol,
        "sharpe": sharpe,
        "max_drawdown": mdd,
        "win_rate": (ret > 0).mean(),
        "cum_return": nav.iloc[-1] - 1,
        "n_periods": len(ret),
    }


def solve_qp_for_one_day(sub: pd.DataFrame) -> pd.DataFrame:
    """单日组合优化
    
    使用二次规划求解最优权重分配
    
    目标函数：最大化 (mu @ w - L2_PENALTY * ||w||^2)
    约束条件：
    - 权重之和为1
    - 个股权重在[MIN_WEIGHT, MAX_WEIGHT]范围内
    - 行业权重偏离基准不超过INDUSTRY_DEV
    
    参数:
        sub: 当日股票数据
    
    返回:
        包含优化权重的DataFrame
    """
    if cp is None:
        raise ImportError("未安装 cvxpy，请先安装：pip install cvxpy")

    sub = sub.dropna(subset=[BONUS_FACTOR_COL, BONUS_PRED_RET_COL, "ret_fwd", "industry_l1"]).copy()
    if len(sub) == 0:
        return pd.DataFrame()

    sub = sub.sort_values(BONUS_FACTOR_COL, ascending=True).head(BONUS_POOL_SIZE).copy()
    if len(sub) == 0:
        return pd.DataFrame()

    n = len(sub)
    max_weight_eff = max(MAX_WEIGHT, 1.0 / n)

    mu = sub[BONUS_PRED_RET_COL].astype(float).values
    industries = sub["industry_l1"].astype(str).values

    w = cp.Variable(n)
    objective = cp.Maximize(mu @ w - L2_PENALTY * cp.sum_squares(w))

    constraints = [
        cp.sum(w) == 1,
        w >= MIN_WEIGHT,
        w <= max_weight_eff,
    ]

    industry_benchmark = pd.Series(industries).value_counts(normalize=True)
    for ind in sorted(pd.unique(industries)):
        mask = (industries == ind).astype(float)
        base_w = float(industry_benchmark.get(ind, 0.0))
        constraints += [
            cp.sum(cp.multiply(mask, w)) <= min(1.0, base_w + INDUSTRY_DEV),
            cp.sum(cp.multiply(mask, w)) >= max(0.0, base_w - INDUSTRY_DEV),
        ]

    prob = cp.Problem(objective, constraints)
    try:
        prob.solve(solver=cp.SCS, verbose=False)
    except Exception:
        prob.solve(verbose=False)

    if w.value is None:
        return pd.DataFrame()

    out = sub[["date", "code", "industry_l1", BONUS_FACTOR_COL, BONUS_PRED_RET_COL, "ret_fwd"]].copy()
    out["opt_weight"] = np.array(w.value).flatten()
    out = out[out["opt_weight"] > 1e-8].sort_values("opt_weight", ascending=False).reset_index(drop=True)
    return out


def run(root: Optional[Path] = None) -> None:
    """执行组合优化流程"""
    project_root = Path(root) if root else Path(__file__).resolve().parents[1]
    results_dir = project_root / "results"

    panel = pd.read_parquet(results_dir / "panel_standardized_neutralized.parquet")
    panel = panel[panel["valid_t1"]].copy()
    panel = panel.sort_values(["date", "code"]).reset_index(drop=True)

    all_weights = []
    ret_records = []

    # 逐日进行组合优化
    for dt, sub in panel.groupby("date"):
        try:
            wdf = solve_qp_for_one_day(sub)
        except Exception as e:
            print(f"[跳过] {dt} 优化失败: {e}")
            continue

        if wdf.empty:
            continue

        port_ret = float((wdf["opt_weight"] * wdf["ret_fwd"]).sum())
        ret_records.append({
            "date": dt,
            "portfolio_ret": port_ret,
            "n_holdings": len(wdf),
        })
        all_weights.append(wdf)

    if not ret_records:
        raise RuntimeError("加分题优化未生成任何有效结果，请检查 cvxpy 是否已安装，或检查样本约束是否过严。")

    weights_df = pd.concat(all_weights, axis=0, ignore_index=True)
    perf_df = pd.DataFrame(ret_records).sort_values("date").reset_index(drop=True)
    perf_df["nav"] = (1 + perf_df["portfolio_ret"]).cumprod()

    metrics = calc_perf_metrics(perf_df["portfolio_ret"])
    metrics["factor_used"] = BONUS_FACTOR_COL
    metrics["pred_ret_used"] = BONUS_PRED_RET_COL
    metrics["pool_size"] = BONUS_POOL_SIZE
    metrics["max_weight"] = MAX_WEIGHT
    metrics["industry_dev"] = INDUSTRY_DEV
    metrics_df = pd.DataFrame([metrics])

    weights_df.to_csv(results_dir / "bonus_weights.csv", index=False, encoding="utf-8-sig")
    perf_df.to_csv(results_dir / "bonus_nav.csv", index=False, encoding="utf-8-sig")
    metrics_df.to_csv(results_dir / "bonus_perf.csv", index=False, encoding="utf-8-sig")

    latest_date = perf_df["date"].max()
    latest_weights = weights_df[weights_df["date"] == latest_date].sort_values("opt_weight", ascending=False)
    summary_lines = [
        "========== Bonus Optimization Summary ==========",
        f"使用因子: {BONUS_FACTOR_COL}",
        f"预测收益代理: {BONUS_PRED_RET_COL}",
        f"股票池: 因子值最小的 {BONUS_POOL_SIZE} 只股票",
        f"个股权重上限: {MAX_WEIGHT:.2%}",
        f"行业权重偏离上限: ±{INDUSTRY_DEV:.2%}",
        "",
        "[组合绩效]",
        metrics_df.to_string(index=False),
        "",
        f"[最新调仓日 Top {TOP_HOLDINGS_TO_SHOW}] {latest_date}",
        latest_weights.head(TOP_HOLDINGS_TO_SHOW).to_string(index=False),
    ]
    (results_dir / "bonus_summary.txt").write_text("\n".join(summary_lines), encoding="utf-8")

    plt.figure(figsize=(10, 5), dpi=140)
    plt.plot(pd.to_datetime(perf_df["date"]), perf_df["nav"])
    plt.title("Bonus Optimization NAV")
    plt.xlabel("Date")
    plt.ylabel("Net Value")
    plt.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(results_dir / "bonus_nav.png", bbox_inches="tight")
    plt.close()

    print("06_bonus_opt 完成。")
    print("输出：results/bonus_weights.csv, bonus_perf.csv, bonus_nav.csv, bonus_nav.png")


if __name__ == "__main__":
    run()
