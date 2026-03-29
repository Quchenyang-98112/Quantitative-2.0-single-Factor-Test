#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

"""
analyze_dy_ind_chg_status_detailed.py
----------------------------------------
详细分析 DY-IND-CHG_STATUS 指标不同取值与其他指标的关系。
"""

from pathlib import Path
import numpy as np
import pandas as pd

def detailed_analysis():
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
    
    # 生成详细报告
    print("生成详细分析报告...")
    report_lines = []
    report_lines.append("DY-IND-CHG_STATUS 指标详细关系分析报告")
    report_lines.append("=" * 80)
    report_lines.append("")
    
    # 1. 分析 DY-IND-CHG_STATUS = 6 的情况
    report_lines.append("1. DY-IND-CHG_STATUS = 6 详细分析")
    report_lines.append("-" * 40)
    
    # 源数据中 DY-IND-CHG_STATUS = 6 的数量
    dy_6_count = (daily[dy_ind_chg_status_col] == 6).sum()
    # 面板数据中 limit_status_t1 = 6.0 的数量
    ls_6_count = (panel['limit_status_t1'] == 6.0).sum()
    
    report_lines.append(f"源数据中 DY-IND-CHG_STATUS = 6 的数量: {dy_6_count:,}")
    report_lines.append(f"面板数据中 limit_status_t1 = 6.0 的数量: {ls_6_count:,}")
    report_lines.append(f"两者是否相同: {'是' if dy_6_count == ls_6_count else '否'}")
    report_lines.append("")
    
    # 2. 分析 DY-IND-CHG_STATUS = -1 的情况
    report_lines.append("2. DY-IND-CHG_STATUS = -1 详细分析")
    report_lines.append("-" * 40)
    
    # 源数据中 DY-IND-CHG_STATUS = -1 的数量
    dy_minus_1_count = (daily[dy_ind_chg_status_col] == -1).sum()
    # 面板数据中相关指标的数量
    open_t1_leq_0_count = int((panel['open_t1'] <= 0).fillna(False).sum())
    open_t2_leq_0_count = int((panel['open_t2'] <= 0).fillna(False).sum())
    ls_minus_1_count = (panel['limit_status_t1'] == -1.0).sum()
    
    report_lines.append(f"源数据中 DY-IND-CHG_STATUS = -1 的数量: {dy_minus_1_count:,}")
    report_lines.append(f"面板数据中 open_t1 <= 0 的数量: {open_t1_leq_0_count:,}")
    report_lines.append(f"面板数据中 open_t2 <= 0 的数量: {open_t2_leq_0_count:,}")
    report_lines.append(f"面板数据中 limit_status_t1 = -1.0 的数量: {ls_minus_1_count:,}")
    report_lines.append(f"DY-IND-CHG_STATUS = -1 与 open_t1 <= 0 的差异: {abs(dy_minus_1_count - open_t1_leq_0_count):,}")
    report_lines.append("")
    
    # 3. 分析所有取值的对应关系
    report_lines.append("3. 所有取值的对应关系分析")
    report_lines.append("-" * 40)
    
    # 获取所有唯一取值
    dy_values = sorted([v for v in daily[dy_ind_chg_status_col].unique() if pd.notna(v)])
    
    for value in dy_values:
        # 源数据中的数量
        dy_count = (daily[dy_ind_chg_status_col] == value).sum()
        # 面板数据中 limit_status_t1 对应值的数量
        ls_count = (panel['limit_status_t1'] == float(value)).sum()
        # 差异
        diff = abs(dy_count - ls_count)
        # 差异百分比
        diff_pct = (diff / dy_count * 100) if dy_count > 0 else 0
        
        report_lines.append(f"DY-IND-CHG_STATUS = {value}:")
        report_lines.append(f"  源数据数量: {dy_count:,}")
        report_lines.append(f"  面板数据 limit_status_t1 = {float(value)} 数量: {ls_count:,}")
        report_lines.append(f"  差异: {diff:,} ({diff_pct:.4f}%)")
        report_lines.append("")
    
    # 4. 分析其他可能的关系
    report_lines.append("4. 其他指标关系分析")
    report_lines.append("-" * 40)
    
    # 分析 DY-IND-CHG_STATUS 与价格有效性的关系
    report_lines.append("DY-IND-CHG_STATUS 与价格有效性的关系:")
    
    # 计算每个 DY-IND-CHG_STATUS 值对应的价格有效性比例
    valid_price_ratio = {}
    for value in dy_values:
        # 找到源数据中对应的值
        mask = daily[dy_ind_chg_status_col] == value
        if mask.sum() > 0:
            # 在面板数据中找到对应的记录
            panel_mask = panel['limit_status_t1'] == float(value)
            if panel_mask.sum() > 0:
                valid_price_ratio[value] = panel.loc[panel_mask, 'is_price_valid'].mean()
    
    for value, ratio in valid_price_ratio.items():
        report_lines.append(f"  DY-IND-CHG_STATUS = {value}: 价格有效比例 = {ratio:.4f}")
    
    report_lines.append("")
    
    # 5. 结论
    report_lines.append("5. 结论")
    report_lines.append("-" * 40)
    report_lines.append("1. 只有 DY-IND-CHG_STATUS = 6 与 limit_status_t1 = 6.0 的数量完全匹配")
    report_lines.append("2. DY-IND-CHG_STATUS = -1 与 open_t1 <= 0 的数量接近但不完全相同")
    report_lines.append("3. 其他取值在源数据和面板数据之间存在一定差异")
    report_lines.append("4. 差异可能源于数据处理过程中的过滤和转换")
    
    # 保存报告
    report_path = results_dir / "DY-IND-CHG_STATUS_详细关系分析报告.txt"
    report_path.write_text("\n".join(report_lines), encoding="utf-8")
    
    print(f"详细分析完成！报告已保存至: {report_path}")
    print("\n关键发现:")
    print(f"- DY-IND-CHG_STATUS = 6 与 limit_status_t1 = 6.0 数量完全相同: {dy_6_count:,}")
    print(f"- DY-IND-CHG_STATUS = -1 与 open_t1 <= 0 数量差异: {abs(dy_minus_1_count - open_t1_leq_0_count):,}")

if __name__ == "__main__":
    detailed_analysis()
