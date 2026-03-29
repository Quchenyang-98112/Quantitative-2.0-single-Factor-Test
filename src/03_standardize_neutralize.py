#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
03_standardize_neutralize.py
因子标准化与中性化模块

功能：
- 对原始因子进行两种标准化处理
  1. MAD去极值 + Z-Score标准化
  2. 排序标准化 + Z-Score标准化
- 对标准化后的因子进行市值和行业中性化处理
- 对中性化残差再次进行截面标准化

输入文件：
- results/panel_factor.parquet: 包含因子值和未来收益的宽表

输出文件：
- results/panel_standardized_neutralized.parquet: 标准化和中性化后的因子数据
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd


# MAD去极值参数
MAD_N = 5.0


def winsorize_mad(series: pd.Series, n: float = 5.0) -> pd.Series:
    """MAD去极值处理
    
    使用中位数绝对偏差(MAD)进行去极值，将数据限制在[median-n*MAD, median+n*MAD]范围内
    
    参数:
        series: 输入数据序列
        n: MAD倍数，默认5倍
    
    返回:
        去极值后的序列
    """
    x = series.astype(float).copy()
    med = x.median()
    mad = (x - med).abs().median()
    if pd.isna(mad) or mad == 0:
        return x
    lower = med - n * mad
    upper = med + n * mad
    return x.clip(lower=lower, upper=upper)


def zscore(series: pd.Series) -> pd.Series:
    """Z-Score标准化
    
    将数据标准化为均值为0，标准差为1的分布
    
    参数:
        series: 输入数据序列
    
    返回:
        标准化后的序列
    """
    x = series.astype(float)
    std = x.std(ddof=0)
    if pd.isna(std) or std == 0:
        return pd.Series(np.nan, index=series.index)
    return (x - x.mean()) / std


def neutralize_one_cross_section(y: pd.Series, float_mcap: pd.Series, industry: pd.Series) -> pd.Series:
    """单期截面中性化处理
    
    通过线性回归去除市值和行业对因子的影响，返回回归残差
    
    参数:
        y: 因子值序列
        float_mcap: 流通市值序列
        industry: 行业分类序列
    
    返回:
        中性化后的残差序列
    """
    idx = y.index
    tmp = pd.DataFrame({
        "y": y,
        "float_mcap": float_mcap,
        "industry_l1": industry,
    }, index=idx).dropna()

    tmp = tmp[tmp["float_mcap"] > 0].copy()
    if len(tmp) < 10:
        return pd.Series(np.nan, index=idx)

    tmp["log_float_mcap"] = np.log(tmp["float_mcap"].astype(float))

    x = pd.get_dummies(tmp[["industry_l1"]], columns=["industry_l1"], drop_first=True)
    x.insert(0, "log_float_mcap", tmp["log_float_mcap"].values)
    x.insert(0, "const", 1.0)

    xv = x.astype(float).values
    yv = tmp["y"].astype(float).values.reshape(-1, 1)

    try:
        beta = np.linalg.lstsq(xv, yv, rcond=None)[0]
        fitted = (xv @ beta).flatten()
        resid = tmp["y"].values - fitted
        out = pd.Series(np.nan, index=idx)
        out.loc[tmp.index] = resid
        return out
    except Exception:
        return pd.Series(np.nan, index=idx)


def run(root: Optional[Path] = None) -> None:
    """执行因子标准化与中性化流程"""
    project_root = Path(root) if root else Path(__file__).resolve().parents[1]
    results_dir = project_root / "results"

    panel = pd.read_parquet(results_dir / "panel_factor.parquet")
    df = panel.copy()

    valid_mask = df["valid_t1"]

    # MAD去极值 + Z-Score标准化
    def _mad_std(cs: pd.Series) -> pd.Series:
        return zscore(winsorize_mad(cs, n=MAD_N))

    df["factor_std_mad"] = np.nan
    df.loc[valid_mask, "factor_std_mad"] = (
        df.loc[valid_mask]
        .groupby("date")["factor_raw"]
        .transform(_mad_std)
    )

    # 排序标准化 + Z-Score标准化
    def _rank_std(cs: pd.Series) -> pd.Series:
        ranked = cs.rank(method="average")
        return zscore(ranked)

    df["factor_std_rank"] = np.nan
    df.loc[valid_mask, "factor_std_rank"] = (
        df.loc[valid_mask]
        .groupby("date")["factor_raw"]
        .transform(_rank_std)
    )

    # 市值和行业中性化
    df["factor_neu_mad"] = np.nan
    df["factor_neu_rank"] = np.nan

    for dt, idx in df.loc[valid_mask].groupby("date").groups.items():
        sub = df.loc[idx]
        resid_mad = neutralize_one_cross_section(
            y=sub["factor_std_mad"],
            float_mcap=sub["float_mcap"],
            industry=sub["industry_l1"],
        )
        resid_rank = neutralize_one_cross_section(
            y=sub["factor_std_rank"],
            float_mcap=sub["float_mcap"],
            industry=sub["industry_l1"],
        )
        df.loc[idx, "factor_neu_mad"] = zscore(resid_mad).values
        df.loc[idx, "factor_neu_rank"] = zscore(resid_rank).values

    # 添加预测收益列（因子值的负值，因为因子是反转因子）
    df["pred_ret_A1_mad_raw"] = -df["factor_std_mad"]
    df["pred_ret_A2_mad_neutralized"] = -df["factor_neu_mad"]
    df["pred_ret_B1_rank_raw"] = -df["factor_std_rank"]
    df["pred_ret_B2_rank_neutralized"] = -df["factor_neu_rank"]

    df.to_parquet(results_dir / "panel_standardized_neutralized.parquet", index=False)

    print("03_standardize_neutralize 完成。")
    print("已生成四组因子：")
    print("1) factor_std_mad")
    print("2) factor_neu_mad")
    print("3) factor_std_rank")
    print("4) factor_neu_rank")


if __name__ == "__main__":
    run()
