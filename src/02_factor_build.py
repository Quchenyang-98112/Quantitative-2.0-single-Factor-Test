#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
02_factor_build.py
因子构建模块

功能：
- 构造动量因子：factor(t0) = close(t0) / close(t-5) - 1
- 构造未来收益：ret_fwd(t0) = open(t2) / open(t1) - 1
- 按 t1 时点进行样本过滤（涨跌停、停牌、ST、上市60天内）
- 过滤无效价格数据

输入文件：
- results/daily_prepared.parquet: 清洗后的日度数据
- results/suspend_prepared.parquet: 停牌数据
- results/st_prepared.parquet: ST标记数据
- results/industry_prepared.parquet: 行业数据

输出文件：
- results/panel_factor.parquet: 包含因子值和未来收益的宽表
- results/factor_build_diagnostics.txt: 因子构建诊断报告
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional, Dict, List, Tuple

import pandas as pd


# 根据对DY-IND-CHG_STATUS的定性分析，定义涨跌停状态映射
# UP: 涨停状态值 [2, 3]，其中，2：涨停但非一字板；3：一字涨停
# DOWN: 跌停状态值 [5, 6]，其中，5：跌停但非一字板；6：一字跌停
LIMIT_STATUS_MAP: Dict[str, List[object]] = {
    "UP": [2, 3],
    "DOWN": [5, 6],
}

# 上市天数阈值（过滤上市60天内的股票）
LIST_DAYS_THRESHOLD = 60


def infer_limit_flags(series: pd.Series) -> Tuple[pd.Series, pd.Series]:
    """根据状态值识别涨跌停
    
    参数:
        series: 包含涨跌停状态值的序列
    
    返回:
        (涨停标记序列, 跌停标记序列)
    """
    s = series.copy()
    up_values = set(LIMIT_STATUS_MAP.get("UP", []))
    down_values = set(LIMIT_STATUS_MAP.get("DOWN", []))
    return s.isin(up_values).fillna(False), s.isin(down_values).fillna(False)


def build_listing_proxy(daily: pd.DataFrame) -> pd.DataFrame:
    """构建上市日期代理变量
    
    通过每只股票最早出现的日期作为上市日期代理
    
    参数:
        daily: 日度数据DataFrame
    
    返回:
        包含股票代码和上市日期代理的DataFrame
    """
    return (
        daily.groupby("code", as_index=False)["date"]
        .min()
        .rename(columns={"date": "proxy_list_date"})
    )


def quantile_text(s: pd.Series) -> str:
    """计算序列的分位数统计
    
    计算0, 0.1%, 1%, 5%, 50%, 95%, 99%, 99.9%, 100%分位数
    """
    q = s.quantile([0, 0.001, 0.01, 0.05, 0.5, 0.95, 0.99, 0.999, 1.0])
    return q.to_string()


def run(root: Optional[Path] = None) -> None:
    """执行因子构建流程"""
    project_root = Path(root) if root else Path(__file__).resolve().parents[1]
    results_dir = project_root / "results"

    daily = pd.read_parquet(results_dir / "daily_prepared.parquet")
    suspend = pd.read_parquet(results_dir / "suspend_prepared.parquet")
    st_df = pd.read_parquet(results_dir / "st_prepared.parquet")
    industry = pd.read_parquet(results_dir / "industry_prepared.parquet")

    industry_dup = int(industry[["date", "code"]].duplicated().sum())
    if industry_dup > 0:
        raise ValueError(f"industry_prepared.parquet 存在重复键(date, code): {industry_dup}")

    daily = daily.sort_values(["code", "date"]).copy()
    g = daily.groupby("code", group_keys=False)

    # 计算动量因子：过去5日收益率
    daily["factor_raw"] = g["close_adj"].transform(lambda x: x / x.shift(5) - 1)

    # 计算未来收益：t+2开盘价 / t+1开盘价 - 1
    daily["open_t1"] = g["open_adj"].shift(-1)
    daily["open_t2"] = g["open_adj"].shift(-2)
    daily["ret_fwd"] = daily["open_t2"] / daily["open_t1"] - 1

    # t+1日期
    daily["date_t1"] = g["date"].shift(-1)

    # t+1涨跌停标记
    daily["limit_status_t1"] = g["limit_status"].shift(-1)
    daily["is_limit_up_t1"], daily["is_limit_down_t1"] = infer_limit_flags(daily["limit_status_t1"])
    daily["is_limit_t1"] = (daily["is_limit_up_t1"] | daily["is_limit_down_t1"]).astype(int)

    # 合并t+1停牌标记
    suspend_t1 = suspend.rename(columns={"date": "date_t1"})
    daily = daily.merge(suspend_t1, on=["date_t1", "code"], how="left")
    daily["is_suspend"] = daily["is_suspend"].fillna(0).astype(int)

    # 合并t+1 ST标记
    st_t1 = st_df.rename(columns={"date": "date_t1"})
    daily = daily.merge(st_t1[["date_t1", "code", "is_st"]], on=["date_t1", "code"], how="left")
    daily["is_st"] = daily["is_st"].fillna(0).astype(int)

    # 合并行业信息
    daily = daily.merge(industry[["date", "code", "industry_l1"]], on=["date", "code"], how="left")

    # 计算上市天数并标记新股
    listing_proxy = build_listing_proxy(daily)
    daily = daily.merge(listing_proxy, on="code", how="left")
    daily["listed_days_at_t1"] = (
        pd.to_datetime(daily["date_t1"]) - pd.to_datetime(daily["proxy_list_date"])
    ).dt.days
    daily["is_new"] = (daily["listed_days_at_t1"] < LIST_DAYS_THRESHOLD).astype(int)

    # 标记有效样本（满足所有过滤条件）
    daily["valid_t1"] = (
        daily["factor_raw"].notna()
        & daily["ret_fwd"].notna()
        & daily["open_t1"].notna()
        & daily["open_t2"].notna()
        & (daily["close_adj"] > 0)
        & (daily["open_t1"] > 0)
        & (daily["open_t2"] > 0)
        & (daily["is_limit_t1"] == 0)
        & (daily["is_suspend"] == 0)
        & (daily["is_st"] == 0)
        & (daily["is_new"] == 0)
    )

    # 选择输出字段
    panel = daily[[
        "date", "code", "date_t1",
        "factor_raw", "ret_fwd",
        "close_adj", "open_adj", "open_t1", "open_t2",
        "high_adj", "low_adj",
        "turnover_vol", "deal_amount", "turnover_rate",
        "total_mcap", "float_mcap", "industry_l1",
        "limit_status", "limit_status_t1",
        "is_limit_t1", "is_suspend", "is_st", "is_new", "valid_t1",
    ]].copy()

    panel = panel.sort_values(["date", "code"]).reset_index(drop=True)
    panel.to_parquet(results_dir / "panel_factor.parquet", index=False)

    # 生成诊断报告
    valid = panel["valid_t1"]
    text = [
        "========== 02_factor_build 诊断报告 ==========",
        f"panel_factor 总样本数: {len(panel):,}",
        f"valid_t1 样本数: {int(valid.sum()):,}",
        f"valid_t1 占比: {valid.mean():.4%}",
        "",
        "[过滤条件命中率]",
        f"is_limit_t1 占比: {panel['is_limit_t1'].mean():.4%}",
        f"is_suspend 占比: {panel['is_suspend'].mean():.4%}",
        f"is_st 占比: {panel['is_st'].mean():.4%}",
        f"is_new 占比: {panel['is_new'].mean():.4%}",
        f"open_t1 <= 0 数量: {(panel['open_t1'] <= 0).sum():,}",
        f"open_t2 <= 0 数量: {(panel['open_t2'] <= 0).sum():,}",
        f"close_adj <= 0 数量: {(panel['close_adj'] <= 0).sum():,}",
        "",
        "[t1 limit_status 频数 Top 20]",
        panel["limit_status_t1"].value_counts(dropna=False).head(20).to_string(),
        "",
        "[ret_fwd 分位数：全样本]",
        quantile_text(panel["ret_fwd"].dropna()),
        "",
        "[ret_fwd 分位数：valid_t1 样本]",
        quantile_text(panel.loc[valid, "ret_fwd"].dropna()),
    ]
    (results_dir / "factor_build_diagnostics.txt").write_text("\n".join(text), encoding="utf-8")

    print("02_factor_build 完成。")
    print(f"panel_factor 总样本数: {len(panel):,}")
    print(f"有效样本占比(valid_t1): {panel['valid_t1'].mean():.2%}")


if __name__ == "__main__":
    run()
