#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
01_data_prepare.py
数据预处理模块

功能：
- 加载原始数据文件
- 统一日期和股票代码格式
- 保留核心字段并重命名
- 生成清洗后的数据文件

输入文件：
- basic_data/daily_data.parquet: 日度行情数据
- basic_data/停牌.parquet: 停牌信息
- basic_data/st.parquet: ST股票标记
- basic_data/industry.parquet: 行业分类数据

输出文件：
- results/daily_prepared.parquet: 清洗后的日度数据
- results/suspend_prepared.parquet: 清洗后的停牌数据
- results/st_prepared.parquet: 清洗后的ST标记数据
- results/industry_prepared.parquet: 清洗后的行业数据
- results/data_prepare_report.txt: 数据检查报告
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd


# 日度数据必需字段
REQUIRED_DAILY_FIELDS = [
    "DY-ADJ_AF-CLOSE_PRICE_2",
    "DY-ADJ_AF-OPEN_PRICE_2",
    "DY-ADJ_AF-HIGHEST_PRICE_2",
    "DY-ADJ_AF-LOWEST_PRICE_2",
    "DY-ADJ_AF-TURNOVER_VOL",
    "DY-BASIC-DEAL_AMOUNT",
    "DY-BASIC-MARKET_VALUE",
    "DY-BASIC-NEG_MARKET_VALUE",
    "DY-BASIC-TURNOVER_RATE",
    "DY-IND-CHG_STATUS",
]


def standardize_code(x: object) -> str:
    """标准化股票代码格式
    
    将各种格式的股票代码统一为6位数字格式
    例如：000001.SZ -> 000001
    """
    if pd.isna(x):
        return np.nan
    s = str(x).strip()
    s = s.replace(".SZ", "").replace(".SH", "")
    s = s.replace("SZ", "").replace("SH", "")
    s = s.replace(" ", "")
    if s.isdigit():
        return s.zfill(6)
    digits = "".join(ch for ch in s if ch.isdigit())
    return digits.zfill(6) if digits else s


def to_datetime_series(s: pd.Series) -> pd.Series:
    """将序列转换为日期时间格式
    
    支持多种日期格式，优先尝试YYYYMMDD格式
    """
    if np.issubdtype(s.dtype, np.datetime64):
        return pd.to_datetime(s)
    s_str = s.astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
    dt = pd.to_datetime(s_str, format="%Y%m%d", errors="coerce")
    if dt.notna().mean() >= 0.8:
        return dt
    return pd.to_datetime(s_str, errors="coerce")


def detect_index_columns(df: pd.DataFrame) -> tuple[str, str]:
    """自动识别DataFrame中的日期列和股票代码列
    
    通过列名关键字匹配来识别日期列和代码列
    返回: (日期列名, 代码列名)
    """
    date_col = None
    code_col = None
    for col in df.columns:
        col_upper = str(col).upper()
        if date_col is None and any(key in col_upper for key in ["DATE", "DATES"]):
            date_col = col
        if code_col is None and any(key in col_upper for key in ["CODE", "CODES", "STOCK", "SYMBOL"]):
            code_col = col
    if date_col is None:
        for col in df.columns:
            if col in ["date", "DATE", "Date"]:
                date_col = col
                break
    if code_col is None:
        for col in df.columns:
            if col in ["code", "CODE", "Code", "stock", "STOCK", "symbol", "SYMBOL"]:
                code_col = col
                break
    if date_col is None or code_col is None:
        raise ValueError("未能自动识别 daily_data.parquet 中的日期列或股票代码列，请检查原始数据结构。")
    return date_col, code_col


def run(root: Optional[Path] = None) -> None:
    """执行数据预处理流程"""
    project_root = Path(root) if root else Path(__file__).resolve().parents[1]
    basic_data_dir = project_root / "basic_data"
    results_dir = project_root / "results"
    results_dir.mkdir(parents=True, exist_ok=True)

    daily_path = basic_data_dir / "daily_data.parquet"
    suspend_path = basic_data_dir / "停牌.parquet"
    st_path = basic_data_dir / "st.parquet"
    industry_path = basic_data_dir / "industry.parquet"

    daily_all = pd.read_parquet(daily_path)
    daily = daily_all.reset_index()
    date_col, code_col = detect_index_columns(daily)

    keep_cols = [date_col, code_col] + [c for c in REQUIRED_DAILY_FIELDS if c in daily.columns]
    missing_daily = [c for c in REQUIRED_DAILY_FIELDS if c not in daily.columns]
    if missing_daily:
        raise ValueError(f"daily_data.parquet 缺少必要字段: {missing_daily}")

    daily = daily[keep_cols].rename(columns={
        date_col: "date",
        code_col: "code",
        "DY-ADJ_AF-CLOSE_PRICE_2": "close_adj",
        "DY-ADJ_AF-OPEN_PRICE_2": "open_adj",
        "DY-ADJ_AF-HIGHEST_PRICE_2": "high_adj",
        "DY-ADJ_AF-LOWEST_PRICE_2": "low_adj",
        "DY-ADJ_AF-TURNOVER_VOL": "turnover_vol",
        "DY-BASIC-DEAL_AMOUNT": "deal_amount",
        "DY-BASIC-MARKET_VALUE": "total_mcap",
        "DY-BASIC-NEG_MARKET_VALUE": "float_mcap",
        "DY-BASIC-TURNOVER_RATE": "turnover_rate",
        "DY-IND-CHG_STATUS": "limit_status",
    })
    daily["code"] = daily["code"].map(standardize_code)
    daily["date"] = to_datetime_series(daily["date"])
    daily = daily.dropna(subset=["date", "code"]).sort_values(["code", "date"]).reset_index(drop=True)

    suspend = pd.read_parquet(suspend_path).rename(columns={
        "股票代码": "code",
        "日期": "date",
        "是否停牌": "is_suspend",
    })
    suspend["code"] = suspend["code"].map(standardize_code)
    suspend["date"] = to_datetime_series(suspend["date"])
    suspend["is_suspend"] = pd.to_numeric(suspend["is_suspend"], errors="coerce").fillna(0).astype(int)
    suspend = suspend[["date", "code", "is_suspend"]].dropna(subset=["date", "code"]).reset_index(drop=True)

    st_wide = pd.read_parquet(st_path)
    st_index_series = pd.Series(st_wide.index, index=st_wide.index)
    st_wide.index = to_datetime_series(st_index_series)
    st_wide.index.name = "date"

    st_long = st_wide.stack(dropna=False).reset_index()
    st_long.columns = ["date", "code", "st_status"]
    st_long["code"] = st_long["code"].map(standardize_code)
    st_long["is_st"] = st_long["st_status"].notna().astype(int)
    st_long = st_long[["date", "code", "is_st", "st_status"]].reset_index(drop=True)

    industry = pd.read_parquet(industry_path)
    if not {"CODE", "DATE"}.issubset(industry.columns):
        industry = industry.reset_index()

    industry = industry.rename(columns={
        "CODE": "code",
        "DATE": "date",
        "TYPE_ID": "type_id",
        "LEVEL1_NAME": "industry_l1",
        "LEVEL2_NAME": "industry_l2",
        "LEVEL3_NAME": "industry_l3",
    })
    industry["code"] = industry["code"].map(standardize_code)
    industry["date"] = to_datetime_series(industry["date"])
    industry = industry[["date", "code", "type_id", "industry_l1", "industry_l2", "industry_l3"]]
    industry = industry.dropna(subset=["date", "code"]).sort_values(["date", "code"]).reset_index(drop=True)

    industry_dup_count_before = int(industry[["date", "code"]].duplicated().sum())
    industry = industry.drop_duplicates(subset=["date", "code"], keep="last").reset_index(drop=True)
    industry_dup_count_after = int(industry[["date", "code"]].duplicated().sum())

    daily.to_parquet(results_dir / "daily_prepared.parquet", index=False)
    suspend.to_parquet(results_dir / "suspend_prepared.parquet", index=False)
    st_long.to_parquet(results_dir / "st_prepared.parquet", index=False)
    industry.to_parquet(results_dir / "industry_prepared.parquet", index=False)

    limit_values = list(pd.Series(daily["limit_status"].dropna().unique()).tolist())
    limit_freq = daily["limit_status"].value_counts(dropna=False).head(20)
    report = [
        "========== 数据准备检查报告 ==========",
        f"daily_prepared 行数: {len(daily):,}",
        f"suspend_prepared 行数: {len(suspend):,}",
        f"st_prepared 行数: {len(st_long):,}",
        f"industry_prepared 行数: {len(industry):,}",
        "",
        f"industry 去重前重复键数(date, code): {industry_dup_count_before:,}",
        f"industry 去重后重复键数(date, code): {industry_dup_count_after:,}",
        "",
        "DY-IND-CHG_STATUS 唯一值（前 100 个）:",
        str(limit_values[:100]),
        "",
        "[DY-IND-CHG_STATUS 频数 Top 20]",
        limit_freq.to_string(),
        "",
        "说明：",
        "02_factor_build.py 中已经按当前定性固定 LIMIT_STATUS_MAP；",
       
    ]
    (results_dir / "data_prepare_report.txt").write_text("\n".join(report), encoding="utf-8")

    print("01_data_prepare 完成。")
    print("输出：results/daily_prepared.parquet 等基础清洗文件。")


if __name__ == "__main__":
    run()
