#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

"""
analyze_dy_ind_chg_status_qualitative.py
----------------------------------------
对 DY-IND-CHG_STATUS 指标进行定性分析，特别是 2/3/5/6 取值。
"""

from pathlib import Path
import numpy as np
import pandas as pd

def analyze_qualitative():
    project_root = Path(__file__).resolve().parent
    results_dir = project_root / "results"
    basic_data_dir = project_root / "basic_data"
    
    # 读取原始数据
    print("读取原始数据...")
    daily = pd.read_parquet(basic_data_dir / "daily_data.parquet")
    
    # 找到相关列（不区分大小写）
    print("识别相关列...")
    columns_map = {}
    for col in daily.columns:
        col_upper = str(col).upper()
        if col_upper == "DY-IND-CHG_STATUS":
            columns_map["status"] = col
        elif col_upper == "DY-ADJ_AF-HIGHEST_PRICE_2":
            columns_map["high"] = col
        elif col_upper == "DY-ADJ_AF-LOWEST_PRICE_2":
            columns_map["low"] = col
        elif col_upper == "DY-ADJ_AF-OPEN_PRICE_2":
            columns_map["open"] = col
        elif col_upper == "DY-ADJ_AF-CLOSE_PRICE_2":
            columns_map["close"] = col
        elif col_upper == "DY-ADJ_AF-TURNOVER_VOL":
            columns_map["volume"] = col
        elif col_upper == "DY-BASIC-DEAL_AMOUNT":
            columns_map["amount"] = col
        elif col_upper == "DY-BASIC-TURNOVER_RATE":
            columns_map["turnover"] = col
    
    # 检查必要列是否存在
    required_cols = ["status", "high", "low", "open", "close"]
    for col in required_cols:
        if col not in columns_map:
            print(f"警告：未找到 {col} 列")
    
    # 重置索引，将日期和代码作为列
    print("重置索引...")
    daily = daily.reset_index()
    daily.rename(columns={"DATE": "date", "CODE": "code"}, inplace=True)
    
    # 计算前一天的收盘价
    print("计算前一天收盘价...")
    daily = daily.sort_values(["code", "date"])
    daily["close_prev"] = daily.groupby("code")[columns_map["close"]].shift(1)
    
    # 计算当日收益率
    daily["ret"] = daily[columns_map["close"]] / daily["close_prev"] - 1
    
    # 生成分析报告
    print("生成定性分析报告...")
    report_lines = []
    report_lines.append("DY-IND-CHG_STATUS 指标定性分析报告")
    report_lines.append("=" * 80)
    report_lines.append("")
    
    # 1. 价格特征分析
    report_lines.append("1. 价格特征分析")
    report_lines.append("-" * 40)
    
    # 计算价格特征
    daily["all_equal"] = (
        daily[columns_map["open"]] == daily[columns_map["high"]]) & \
        (daily[columns_map["high"]] == daily[columns_map["low"]]) & \
        (daily[columns_map["low"]] == daily[columns_map["close"]])
    
    daily["close_eq_high"] = daily[columns_map["close"]] == daily[columns_map["high"]]
    daily["close_eq_low"] = daily[columns_map["close"]] == daily[columns_map["low"]]
    
    # 按状态分组分析
    status_groups = daily.groupby(columns_map["status"])
    
    report_lines.append("各状态价格特征统计:")
    report_lines.append("状态 | 样本数 | 一字板比例 | close==high比例 | close==low比例")
    report_lines.append("-" * 80)
    
    for status, group in status_groups:
        if pd.isna(status):
            continue
        n = len(group)
        all_equal_pct = group["all_equal"].mean() * 100
        close_eq_high_pct = group["close_eq_high"].mean() * 100
        close_eq_low_pct = group["close_eq_low"].mean() * 100
        
        report_lines.append(f"{status} | {n:,} | {all_equal_pct:.2f}% | {close_eq_high_pct:.2f}% | {close_eq_low_pct:.2f}%")
    
    report_lines.append("")
    
    # 2. 当日收益方向分析
    report_lines.append("2. 当日收益方向分析")
    report_lines.append("-" * 40)
    
    report_lines.append("各状态收益率统计:")
    report_lines.append("状态 | 样本数 | 收益率均值 | 收益率中位数 | 正收益比例 | 负收益比例")
    report_lines.append("-" * 80)
    
    for status, group in status_groups:
        if pd.isna(status):
            continue
        n = len(group)
        ret_mean = group["ret"].mean() * 100
        ret_median = group["ret"].median() * 100
        positive_pct = (group["ret"] > 0).mean() * 100
        negative_pct = (group["ret"] < 0).mean() * 100
        
        report_lines.append(f"{status} | {n:,} | {ret_mean:.2f}% | {ret_median:.2f}% | {positive_pct:.2f}% | {negative_pct:.2f}%")
    
    report_lines.append("")
    
    # 3. 成交量/换手率特征分析
    report_lines.append("3. 成交量/换手率特征分析")
    report_lines.append("-" * 40)
    
    # 检查成交量相关列是否存在
    if "volume" in columns_map:
        report_lines.append("各状态成交量统计:")
        report_lines.append("状态 | 样本数 | 成交量均值 | 成交量中位数")
        report_lines.append("-" * 80)
        
        for status, group in status_groups:
            if pd.isna(status):
                continue
            n = len(group)
            volume_mean = group[columns_map["volume"]].mean()
            volume_median = group[columns_map["volume"]].median()
            
            report_lines.append(f"{status} | {n:,} | {volume_mean:.2f} | {volume_median:.2f}")
        
        report_lines.append("")
    
    if "amount" in columns_map:
        report_lines.append("各状态成交额统计:")
        report_lines.append("状态 | 样本数 | 成交额均值 | 成交额中位数")
        report_lines.append("-" * 80)
        
        for status, group in status_groups:
            if pd.isna(status):
                continue
            n = len(group)
            amount_mean = group[columns_map["amount"]].mean()
            amount_median = group[columns_map["amount"]].median()
            
            report_lines.append(f"{status} | {n:,} | {amount_mean:.2f} | {amount_median:.2f}")
        
        report_lines.append("")
    
    if "turnover" in columns_map:
        report_lines.append("各状态换手率统计:")
        report_lines.append("状态 | 样本数 | 换手率均值 | 换手率中位数")
        report_lines.append("-" * 80)
        
        for status, group in status_groups:
            if pd.isna(status):
                continue
            n = len(group)
            turnover_mean = group[columns_map["turnover"]].mean() * 100
            turnover_median = group[columns_map["turnover"]].median() * 100
            
            report_lines.append(f"{status} | {n:,} | {turnover_mean:.2f}% | {turnover_median:.2f}%")
        
        report_lines.append("")
    
    # 4. 重点分析 2/3/5/6
    report_lines.append("4. 重点分析: 2/3/5/6 状态")
    report_lines.append("-" * 40)
    
    focus_statuses = [2, 3, 5, 6]
    for status in focus_statuses:
        if status not in status_groups.groups:
            continue
        group = status_groups.get_group(status)
        n = len(group)
        
        # 价格特征
        all_equal_pct = group["all_equal"].mean() * 100
        close_eq_high_pct = group["close_eq_high"].mean() * 100
        close_eq_low_pct = group["close_eq_low"].mean() * 100
        
        # 收益特征
        ret_mean = group["ret"].mean() * 100
        ret_median = group["ret"].median() * 100
        
        # 成交量特征
        volume_stats = "N/A"
        if "volume" in columns_map:
            volume_mean = group[columns_map["volume"]].mean()
            volume_median = group[columns_map["volume"]].median()
            volume_stats = f"均值: {volume_mean:.2f}, 中位数: {volume_median:.2f}"
        
        # 换手率特征
        turnover_stats = "N/A"
        if "turnover" in columns_map:
            turnover_mean = group[columns_map["turnover"]].mean() * 100
            turnover_median = group[columns_map["turnover"]].median() * 100
            turnover_stats = f"均值: {turnover_mean:.2f}%, 中位数: {turnover_median:.2f}%"
        
        report_lines.append(f"状态 {status}:")
        report_lines.append(f"  样本数: {n:,}")
        report_lines.append(f"  价格特征: 一字板 {all_equal_pct:.2f}%, close==high {close_eq_high_pct:.2f}%, close==low {close_eq_low_pct:.2f}%")
        report_lines.append(f"  收益特征: 均值 {ret_mean:.2f}%, 中位数 {ret_median:.2f}%")
        report_lines.append(f"  成交量: {volume_stats}")
        report_lines.append(f"  换手率: {turnover_stats}")
        report_lines.append("")
    
    # 5. 5 vs 6 对比分析
    report_lines.append("5. 5 vs 6 对比分析")
    report_lines.append("-" * 40)
    
    if 5 in status_groups.groups and 6 in status_groups.groups:
        group_5 = status_groups.get_group(5)
        group_6 = status_groups.get_group(6)
        
        report_lines.append("5 vs 6 对比:")
        report_lines.append(f"  样本数: 5={len(group_5):,}, 6={len(group_6):,}")
        
        # 价格特征对比
        all_equal_5 = group_5["all_equal"].mean() * 100
        all_equal_6 = group_6["all_equal"].mean() * 100
        report_lines.append(f"  一字板比例: 5={all_equal_5:.2f}%, 6={all_equal_6:.2f}%")
        
        # 收益特征对比
        ret_mean_5 = group_5["ret"].mean() * 100
        ret_mean_6 = group_6["ret"].mean() * 100
        report_lines.append(f"  收益率均值: 5={ret_mean_5:.2f}%, 6={ret_mean_6:.2f}%")
        
        # 成交量对比
        if "volume" in columns_map:
            volume_mean_5 = group_5[columns_map["volume"]].mean()
            volume_mean_6 = group_6[columns_map["volume"]].mean()
            volume_ratio = volume_mean_6 / volume_mean_5 if volume_mean_5 > 0 else 0
            report_lines.append(f"  成交量均值: 5={volume_mean_5:.2f}, 6={volume_mean_6:.2f}, 6/5={volume_ratio:.2f}")
        
        # 换手率对比
        if "turnover" in columns_map:
            turnover_mean_5 = group_5[columns_map["turnover"]].mean() * 100
            turnover_mean_6 = group_6[columns_map["turnover"]].mean() * 100
            turnover_ratio = turnover_mean_6 / turnover_mean_5 if turnover_mean_5 > 0 else 0
            report_lines.append(f"  换手率均值: 5={turnover_mean_5:.2f}%, 6={turnover_mean_6:.2f}%, 6/5={turnover_ratio:.2f}")
        
        report_lines.append("")
    
    # 6. 结论
    report_lines.append("6. 结论")
    report_lines.append("-" * 40)
    report_lines.append("基于以上分析，可以得出以下结论：")
    
    # 保存报告
    report_path = results_dir / "DY-IND-CHG_STATUS_定性分析报告.txt"
    report_path.write_text("\n".join(report_lines), encoding="utf-8")
    
    print(f"定性分析完成！报告已保存至: {report_path}")
    print("\n分析重点关注了 2/3/5/6 四个状态的价格特征、收益方向和成交量/换手率特征")

if __name__ == "__main__":
    analyze_qualitative()
