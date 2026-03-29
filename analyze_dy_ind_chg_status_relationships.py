#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

"""
analyze_dy_ind_chg_status_relationships.py
----------------------------------------
分析 DY-IND-CHG_STATUS 指标不同取值与其他指标数量的关系。
"""

from pathlib import Path
import numpy as np
import pandas as pd

def analyze_relationships():
    project_root = Path(__file__).resolve().parent
    results_dir = project_root / "results"
    basic_data_dir = project_root / "basic_data"
    
    # 读取原始数据
    print("读取原始数据...")
    daily = pd.read_parquet(basic_data_dir / "daily_data.parquet")
    
    # 找到 DY-IND-CHG_STATUS 列（不区分大小写）
    dy_ind_chg_status_col = None
    for col in daily.columns:
        if str(col).upper() == "DY-IND-CHG_STATUS":
            dy_ind_chg_status_col = col
            break
    
    if not dy_ind_chg_status_col:
        print("未找到 DY-IND-CHG_STATUS 列")
        return
    
    print(f"找到 DY-IND-CHG_STATUS 列: {dy_ind_chg_status_col}")
    
    # 读取处理后的数据
    print("读取处理后的数据...")
    panel = pd.read_parquet(results_dir / "panel_factor.parquet")
    
    # 分析 DY-IND-CHG_STATUS 的所有取值
    print("分析 DY-IND-CHG_STATUS 取值...")
    dy_values = daily[dy_ind_chg_status_col].unique()
    dy_values = sorted([v for v in dy_values if pd.notna(v)])
    
    # 收集各取值的数量
    dy_counts = daily[dy_ind_chg_status_col].value_counts(dropna=False)
    
    # 分析面板数据中的各种指标数量
    print("分析面板数据中的指标数量...")
    panel_analysis = {
        "total_rows": len(panel),
        "open_t1_leq_0": int((panel['open_t1'] <= 0).fillna(False).sum()),
        "open_t2_leq_0": int((panel['open_t2'] <= 0).fillna(False).sum()),
        "close_adj_leq_0": int((panel['close_adj'] <= 0).fillna(False).sum()),
        "is_limit_t1": int(panel['is_limit_t1'].sum()),
        "is_suspend": int(panel['is_suspend'].sum()),
        "is_st": int(panel['is_st'].sum()),
        "is_new": int(panel['is_new'].sum()),
        "is_price_valid_false": int((~panel['is_price_valid']).sum()),
        "valid_t1_false": int((~panel['valid_t1']).sum()),
        "factor_raw_na": int(panel['factor_raw'].isna().sum()),
        "ret_fwd_na": int(panel['ret_fwd'].isna().sum()),
    }
    
    # 分析 limit_status_t1 的取值数量
    limit_status_t1_counts = panel['limit_status_t1'].value_counts(dropna=False)
    
    # 生成报告
    print("生成分析报告...")
    report_lines = []
    report_lines.append("DY-IND-CHG_STATUS 指标与其他指标数量关系分析报告")
    report_lines.append("=" * 80)
    report_lines.append("")
    
    # 源数据中 DY-IND-CHG_STATUS 的基本统计
    report_lines.append("1. 源数据中 DY-IND-CHG_STATUS 取值统计")
    report_lines.append("-" * 40)
    total_samples = len(daily)
    report_lines.append(f"总样本数: {total_samples:,}")
    report_lines.append(f"唯一取值数量: {len(dy_values)}")
    report_lines.append("")
    report_lines.append("各取值数量:")
    for value, count in dy_counts.items():
        percentage = (count / total_samples) * 100
        report_lines.append(f"  {value}: {count:,} ({percentage:.4f}%)")
    report_lines.append("")
    
    # 面板数据中的指标数量
    report_lines.append("2. 面板数据中的指标数量")
    report_lines.append("-" * 40)
    for key, value in panel_analysis.items():
        report_lines.append(f"{key}: {value:,}")
    report_lines.append("")
    
    # limit_status_t1 取值数量
    report_lines.append("3. limit_status_t1 取值数量")
    report_lines.append("-" * 40)
    for value, count in limit_status_t1_counts.items():
        report_lines.append(f"  {value}: {count:,}")
    report_lines.append("")
    
    # 关系分析
    report_lines.append("4. 关系分析")
    report_lines.append("-" * 40)
    
    # 检查 DY-IND-CHG_STATUS 各取值与面板指标的数量关系
    for value, count in dy_counts.items():
        report_lines.append(f"DY-IND-CHG_STATUS = {value} (数量: {count:,})")
        
        # 检查是否与面板指标数量匹配
        matches = []
        for key, panel_count in panel_analysis.items():
            if count == panel_count:
                matches.append(f"  - 与 {key} 数量相同: {panel_count:,}")
        
        # 检查是否与 limit_status_t1 取值数量匹配
        for ls_value, ls_count in limit_status_t1_counts.items():
            if count == ls_count:
                matches.append(f"  - 与 limit_status_t1 = {ls_value} 数量相同: {ls_count:,}")
        
        if matches:
            for match in matches:
                report_lines.append(match)
        else:
            report_lines.append("  - 无匹配的指标数量")
        report_lines.append("")
    
    # 特别分析 -1 的情况
    if -1 in dy_counts:
        report_lines.append("5. 特别分析: DY-IND-CHG_STATUS = -1")
        report_lines.append("-" * 40)
        count_minus_1 = dy_counts[-1]
        report_lines.append(f"数量: {count_minus_1:,}")
        
        # 检查 open_t1 <= 0 的情况
        open_t1_leq_0_count = panel_analysis["open_t1_leq_0"]
        report_lines.append(f"open_t1 <= 0 数量: {open_t1_leq_0_count:,}")
        report_lines.append(f"是否相同: {'是' if count_minus_1 == open_t1_leq_0_count else '否'}")
        report_lines.append("")
    
    # 保存报告
    report_path = results_dir / "DY-IND-CHG_STATUS_关系分析报告.txt"
    report_path.write_text("\n".join(report_lines), encoding="utf-8")
    
    print(f"分析完成！报告已保存至: {report_path}")
    print("\n关键发现:")
    if -1 in dy_counts:
        count_minus_1 = dy_counts[-1]
        open_t1_leq_0_count = panel_analysis["open_t1_leq_0"]
        print(f"- DY-IND-CHG_STATUS = -1 的数量: {count_minus_1:,}")
        print(f"- open_t1 <= 0 的数量: {open_t1_leq_0_count:,}")
        print(f"- 两者是否相同: {'是' if count_minus_1 == open_t1_leq_0_count else '否'}")
    
    # 检查其他可能的匹配
    for value, count in dy_counts.items():
        for key, panel_count in panel_analysis.items():
            if count == panel_count and value != -1:
                print(f"- DY-IND-CHG_STATUS = {value} 与 {key} 数量相同: {count:,}")

if __name__ == "__main__":
    analyze_relationships()
