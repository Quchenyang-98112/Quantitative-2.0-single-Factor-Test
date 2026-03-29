#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
05_layer_backtest.py
分层回测模块

功能：
- 全市场横截面分层回测（5层）
- 行业内部分层后再聚合回测
- 计算分层组合绩效指标
- 绘制分层净值走势图

输入文件：
- results/panel_standardized_neutralized.parquet: 标准化和中性化后的因子数据

输出文件：
- results/layer_perf.csv: 分层组合绩效汇总表
- results/layer_backtest.png: 分层净值走势图
- results/group_returns_*.csv: 各分层组合的收益率序列
- results/layer_nav_*.csv: 各分层组合的净值序列
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional, Dict, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


# 分层数量
N_GROUPS = 5

# 加权方式：equal（等权）或 float_mcap（流通市值加权）
WEIGHTING = "equal"

# 因子字典
FACTOR_DICT: Dict[str, str] = {
    "A1_mad_raw": "factor_std_mad",
    "A2_mad_neutralized": "factor_neu_mad",
    "B1_rank_raw": "factor_std_rank",
    "B2_rank_neutralized": "factor_neu_rank",
}


def safe_rankcut(values: pd.Series, n_groups: int) -> pd.Series:
    """安全分层函数
    
    根据因子值进行分层，处理边界情况
    
    参数:
        values: 因子值序列
        n_groups: 分层数量
    
    返回:
        分层标记序列（1到n_groups）
    """
    out = pd.Series(np.nan, index=values.index)
    valid = values.notna()
    if valid.sum() < n_groups:
        return out
    rank = values[valid].rank(method="first")
    try:
        group = pd.qcut(rank, q=n_groups, labels=False) + 1
    except Exception:
        group = pd.cut(rank, bins=n_groups, labels=False, include_lowest=True) + 1
    out.loc[valid] = group.astype(float)
    return out


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


def calc_group_return(gsub: pd.DataFrame) -> float:
    """计算分层组合收益
    
    根据加权方式计算分层内组合收益
    
    参数:
        gsub: 分层内股票数据
    
    返回:
        组合收益
    """
    if WEIGHTING == "float_mcap":
        w = gsub["float_mcap"].clip(lower=0)
        if w.sum() <= 0:
            return float(gsub["ret_fwd"].mean())
        w = w / w.sum()
        return float((w * gsub["ret_fwd"]).sum())
    return float(gsub["ret_fwd"].mean())


def post_process_group_returns(group_ret_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """后处理分层收益数据
    
    转换数据格式，计算多空组合，生成绩效指标
    
    参数:
        group_ret_df: 分层收益DataFrame
    
    返回:
        (分层收益表, 绩效指标表, 净值表)
    """
    ret_wide = group_ret_df.pivot(index="date", columns="group", values="group_ret").sort_index()
    ret_wide.columns = [f"G{int(c)}" for c in ret_wide.columns]
    g1 = "G1"
    gN = f"G{N_GROUPS}"
    mean_g1 = ret_wide[g1].mean() if g1 in ret_wide.columns else np.nan
    mean_gN = ret_wide[gN].mean() if gN in ret_wide.columns else np.nan
    if pd.notna(mean_gN) and pd.notna(mean_g1) and mean_gN >= mean_g1:
        ret_wide["LongShort"] = ret_wide[gN] - ret_wide[g1]
        long_short_def = f"{gN}-{g1}"
    else:
        ret_wide["LongShort"] = ret_wide[g1] - ret_wide[gN]
        long_short_def = f"{g1}-{gN}"
    nav_wide = (1 + ret_wide.fillna(0)).cumprod()
    perf_rows = []
    for col in ret_wide.columns:
        metric = calc_perf_metrics(ret_wide[col])
        metric["portfolio"] = col
        metric["long_short_definition"] = long_short_def
        perf_rows.append(metric)
    perf_df = pd.DataFrame(perf_rows)
    return group_ret_df, perf_df, nav_wide


def layer_backtest_standard(panel: pd.DataFrame, factor_col: str):
    """标准分层回测（全市场）
    
    在全市场范围内按因子值分层，计算各层收益
    
    参数:
        panel: 包含因子值和收益的DataFrame
        factor_col: 因子列名
    
    返回:
        (分层收益表, 绩效指标表, 净值表)
    """
    records = []
    for dt, sub in panel.groupby("date"):
        sub = sub[sub[factor_col].notna() & sub["ret_fwd"].notna()].copy()
        if len(sub) < N_GROUPS:
            continue
        sub["group"] = safe_rankcut(sub[factor_col], N_GROUPS)
        sub = sub.dropna(subset=["group"])
        for g, gsub in sub.groupby("group"):
            records.append({
                "date": dt,
                "group": int(g),
                "group_ret": calc_group_return(gsub),
                "n_stocks": len(gsub),
            })
    group_ret_df = pd.DataFrame(records).sort_values(["date", "group"]).reset_index(drop=True)
    if group_ret_df.empty:
        return group_ret_df, pd.DataFrame(), pd.DataFrame()
    return post_process_group_returns(group_ret_df)


def layer_backtest_industry_neutral(panel: pd.DataFrame, factor_col: str):
    """行业中性的分层回测
    
    在每个行业内部进行分层，再按行业市值加权聚合
    
    参数:
        panel: 包含因子值和收益的DataFrame
        factor_col: 因子列名
    
    返回:
        (分层收益表, 绩效指标表, 净值表)
    """
    records = []
    for dt, sub in panel.groupby("date"):
        sub = sub[sub[factor_col].notna() & sub["ret_fwd"].notna() & sub["industry_l1"].notna()].copy()
        if len(sub) < N_GROUPS:
            continue
        industry_parts = []
        for ind, ind_sub in sub.groupby("industry_l1"):
            if len(ind_sub) < N_GROUPS:
                continue
            ind_sub = ind_sub.copy()
            ind_sub["group"] = safe_rankcut(ind_sub[factor_col], N_GROUPS)
            ind_sub = ind_sub.dropna(subset=["group"])
            if ind_sub.empty:
                continue
            ind_weight = ind_sub["float_mcap"].clip(lower=0).sum() if WEIGHTING == "float_mcap" else len(ind_sub)
            if ind_weight <= 0:
                continue
            grp_ret = (
                ind_sub.groupby("group")
                .apply(lambda x: calc_group_return(x))
                .rename("group_ret")
                .reset_index()
            )
            grp_ret["date"] = dt
            grp_ret["industry_l1"] = ind
            grp_ret["industry_weight_raw"] = ind_weight
            grp_ret["n_stocks"] = len(ind_sub)
            industry_parts.append(grp_ret)
        if not industry_parts:
            continue
        day_ind = pd.concat(industry_parts, axis=0, ignore_index=True)
        day_ind["industry_weight"] = day_ind["industry_weight_raw"] / day_ind["industry_weight_raw"].sum()
        day_group = (
            day_ind.groupby(["date", "group"])
            .apply(lambda x: pd.Series({
                "group_ret": float((x["group_ret"] * x["industry_weight"]).sum()),
                "n_stocks": int(x["n_stocks"].sum()),
            }))
            .reset_index()
        )
        records.append(day_group)
    if not records:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    group_ret_df = pd.concat(records, axis=0, ignore_index=True).sort_values(["date", "group"]).reset_index(drop=True)
    return post_process_group_returns(group_ret_df)


def run(root: Optional[Path] = None) -> None:
    """执行分层回测流程"""
    project_root = Path(root) if root else Path(__file__).resolve().parents[1]
    results_dir = project_root / "results"

    panel = pd.read_parquet(results_dir / "panel_standardized_neutralized.parquet")
    panel = panel[panel["valid_t1"]].copy()

    all_perf = []
    nav_store = {}

    # 执行两种分层方法
    for method_name, backtest_func in {
        "standard": layer_backtest_standard,
        "industry_neutral": layer_backtest_industry_neutral,
    }.items():
        for factor_name, factor_col in FACTOR_DICT.items():
            group_ret_df, perf_df, nav_wide = backtest_func(panel, factor_col)
            if not group_ret_df.empty:
                group_ret_df["layering_method"] = method_name
                group_ret_df.to_csv(
                    results_dir / f"group_returns_{method_name}_{factor_name}.csv",
                    index=False,
                    encoding="utf-8-sig"
                )
            if not perf_df.empty:
                perf_df["factor_name"] = factor_name
                perf_df["layering_method"] = method_name
                all_perf.append(perf_df)
            if not nav_wide.empty:
                nav_wide.to_csv(results_dir / f"layer_nav_{method_name}_{factor_name}.csv", encoding="utf-8-sig")
                nav_store[(method_name, factor_name)] = nav_wide

    # 保存绩效汇总
    if all_perf:
        layer_perf = pd.concat(all_perf, axis=0, ignore_index=True)
        layer_perf.to_csv(results_dir / "layer_perf.csv", index=False, encoding="utf-8-sig")

    # 绘制分层净值图
    fig, axes = plt.subplots(2, 4, figsize=(22, 10), dpi=140)
    axes = np.array(axes)
    row_map = {"standard": 0, "industry_neutral": 1}
    col_map = {name: i for i, name in enumerate(FACTOR_DICT.keys())}
    for (method_name, factor_name), nav in nav_store.items():
        ax = axes[row_map[method_name], col_map[factor_name]]
        for col in nav.columns:
            ax.plot(nav.index, nav[col], label=col)
        ax.set_title(f"{method_name} | {factor_name}")
        ax.set_xlabel("Date")
        ax.set_ylabel("Net Value")
        ax.grid(alpha=0.3)
        ax.legend(fontsize=8, ncol=2)
    plt.tight_layout()
    plt.savefig(results_dir / "layer_backtest.png", bbox_inches="tight")
    plt.close()

    print("05_layer_backtest 完成。")
    print("输出：results/layer_perf.csv, results/layer_backtest.png")
    print("已同时完成：")
    print("1) 全市场分层回测")
    print("2) 行业内部分层后再聚合（可选项）")


if __name__ == "__main__":
    run()
