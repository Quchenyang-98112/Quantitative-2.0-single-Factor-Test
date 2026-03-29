#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
04_ic_test.py
IC测试模块

功能：
- 计算因子的Pearson IC和Spearman Rank IC
- 对IC序列进行统计检验
- 生成IC汇总表和累计IC走势图

输入文件：
- results/panel_standardized_neutralized.parquet: 标准化和中性化后的因子数据

输出文件：
- results/ic_summary.csv: IC统计汇总表
- results/ic_series_*.csv: 各因子的IC时间序列
- results/cum_ic.png: Pearson IC累计走势图
- results/cum_rank_ic.png: Rank IC累计走势图
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional, Dict

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy import stats


# 因子字典：名称 -> 列名
FACTOR_DICT: Dict[str, str] = {
    "A1_mad_raw": "factor_std_mad",
    "A2_mad_neutralized": "factor_neu_mad",
    "B1_rank_raw": "factor_std_rank",
    "B2_rank_neutralized": "factor_neu_rank",
}


def calc_ic_series(panel: pd.DataFrame, factor_col: str, ret_col: str = "ret_fwd") -> pd.DataFrame:
    """计算因子的IC时间序列
    
    对每个交易日计算因子值与未来收益的相关系数
    
    参数:
        panel: 包含因子值和收益的DataFrame
        factor_col: 因子列名
        ret_col: 收益列名，默认为"ret_fwd"
    
    返回:
        包含日期、Pearson IC、Rank IC和样本数的DataFrame
    """
    rows = []
    for dt, sub in panel.groupby("date"):
        valid = sub[factor_col].notna() & sub[ret_col].notna()
        sub2 = sub.loc[valid]
        if len(sub2) < 5:
            rows.append({"date": dt, "pearson_ic": np.nan, "rank_ic": np.nan, "n": len(sub2)})
            continue
        pearson_ic = sub2[factor_col].corr(sub2[ret_col], method="pearson")
        rank_ic = sub2[factor_col].corr(sub2[ret_col], method="spearman")
        rows.append({"date": dt, "pearson_ic": pearson_ic, "rank_ic": rank_ic, "n": len(sub2)})
    return pd.DataFrame(rows).sort_values("date").reset_index(drop=True)


def summarize_ic(ic_df: pd.DataFrame, ic_col: str) -> dict:
    """对IC序列进行统计汇总
    
    计算IC的均值、标准差、IR比率、正IC比例、t统计量和p值
    
    参数:
        ic_df: 包含IC序列的DataFrame
        ic_col: IC列名
    
    返回:
        包含各项统计指标的字典
    """
    s = ic_df[ic_col].dropna()
    if len(s) == 0:
        return {
            "ic_mean": np.nan,
            "ic_std": np.nan,
            "ic_ir": np.nan,
            "abs_ic_mean": np.nan,
            "positive_ratio": np.nan,
            "t_stat": np.nan,
            "p_value": np.nan,
            "n_dates": 0,
        }
    t_stat, p_value = stats.ttest_1samp(s, popmean=0.0, nan_policy="omit")
    ic_std = s.std(ddof=1)
    return {
        "ic_mean": s.mean(),
        "ic_std": ic_std,
        "ic_ir": s.mean() / ic_std if pd.notna(ic_std) and ic_std != 0 else np.nan,
        "abs_ic_mean": s.abs().mean(),
        "positive_ratio": (s > 0).mean(),
        "t_stat": t_stat,
        "p_value": p_value,
        "n_dates": len(s),
    }


def run(root: Optional[Path] = None) -> None:
    """执行IC测试流程"""
    project_root = Path(root) if root else Path(__file__).resolve().parents[1]
    results_dir = project_root / "results"

    panel = pd.read_parquet(results_dir / "panel_standardized_neutralized.parquet")
    panel = panel[panel["valid_t1"]].copy()

    summary_rows = []
    pearson_store = {}
    rank_store = {}

    # 对每个因子计算IC
    for name, factor_col in FACTOR_DICT.items():
        ic_df = calc_ic_series(panel, factor_col)
        ic_df.to_csv(results_dir / f"ic_series_{name}.csv", index=False, encoding="utf-8-sig")

        pearson_summary = summarize_ic(ic_df, "pearson_ic")
        pearson_summary.update({"factor_name": name, "ic_type": "pearson_ic"})
        rank_summary = summarize_ic(ic_df, "rank_ic")
        rank_summary.update({"factor_name": name, "ic_type": "rank_ic"})
        summary_rows.extend([pearson_summary, rank_summary])

        pearson_store[name] = ic_df.set_index("date")["pearson_ic"].fillna(0).cumsum()
        rank_store[name] = ic_df.set_index("date")["rank_ic"].fillna(0).cumsum()

    # 保存汇总结果
    summary_df = pd.DataFrame(summary_rows)
    summary_df.to_csv(results_dir / "ic_summary.csv", index=False, encoding="utf-8-sig")

    # 绘制Pearson IC累计走势图
    fig, axes = plt.subplots(2, 2, figsize=(14, 8), dpi=140)
    axes = axes.flatten()
    for ax, (name, ser) in zip(axes, pearson_store.items()):
        ax.plot(ser.index, ser.values)
        ax.set_title(f"{name} - Pearson IC Cumsum")
        ax.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(results_dir / "cum_ic.png", bbox_inches="tight")
    plt.close()

    # 绘制Rank IC累计走势图
    fig, axes = plt.subplots(2, 2, figsize=(14, 8), dpi=140)
    axes = axes.flatten()
    for ax, (name, ser) in zip(axes, rank_store.items()):
        ax.plot(ser.index, ser.values)
        ax.set_title(f"{name} - Rank IC Cumsum")
        ax.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(results_dir / "cum_rank_ic.png", bbox_inches="tight")
    plt.close()

    print("04_ic_test 完成。")
    print("输出：results/ic_summary.csv, results/cum_ic.png, results/cum_rank_ic.png")


if __name__ == "__main__":
    run()
