#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
提取 DY-IND-CHG_STATUS 指标的所有唯一值及其出现次数
"""

from pathlib import Path
import pandas as pd


def extract_dy_ind_chg_status():
    project_root = Path(__file__).resolve().parent
    daily_path = project_root / "basic_data" / "daily_data.parquet"
    
    print(f"读取文件: {daily_path}")
    
    # 读取日线数据
    daily = pd.read_parquet(daily_path)
    
    # 处理索引，确保我们能访问到所有列
    daily = daily.reset_index()
    
    # 找到 DY-IND-CHG_STATUS 列（不区分大小写）
    dy_ind_chg_status_col = None
    for col in daily.columns:
        if str(col).upper() == "DY-IND-CHG_STATUS":
            dy_ind_chg_status_col = col
            break
    
    if dy_ind_chg_status_col is None:
        print("未找到 DY-IND-CHG_STATUS 列")
        return
    
    print(f"找到列: {dy_ind_chg_status_col}")
    
    # 提取唯一值及其出现次数
    value_counts = daily[dy_ind_chg_status_col].value_counts(dropna=False)
    
    # 生成报告
    report_lines = [
        "# DY-IND-CHG_STATUS 指标完整取值报告",
        "",
        "## 1. 基本信息",
        f"- 总样本数: {len(daily):,}",
        f"- 唯一值数量: {len(value_counts):,}",
        "",
        "## 2. 所有取值及其出现次数",
        "",
        "| 取值 | 出现次数 | 占比 |",
        "|------|----------|------|",
    ]
    
    total = len(daily)
    for value, count in value_counts.items():
        percentage = (count / total) * 100
        # 处理空值的显示
        if pd.isna(value):
            value_str = "(空值)"
        else:
            value_str = str(value)
        report_lines.append(f"| {value_str} | {count:,} | {percentage:.4f}% |")
    
    # 保存报告
    report_path = project_root / "DY-IND-CHG_STATUS_完整取值报告.txt"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(report_lines))
    
    print(f"\n报告已生成: {report_path}")
    print(f"唯一值数量: {len(value_counts)}")
    print("前10个取值:")
    print(value_counts.head(10))


if __name__ == "__main__":
    extract_dy_ind_chg_status()
