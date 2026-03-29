#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
筛选最后一次全过程实验的结果文件
"""

import shutil
from pathlib import Path
from datetime import datetime

def filter_final_results():
    """筛选最后一次全过程实验的结果文件"""
    project_root = Path(__file__).resolve().parent
    results_dir = project_root / "results"
    final_dir = project_root / "final"
    
    # 创建final目录
    final_dir.mkdir(parents=True, exist_ok=True)
    
    # 定义最后一次全过程实验的核心文件
    core_files = {
        # 步骤1：数据预处理
        "daily_prepared.parquet": "数据预处理 - 日度数据",
        "suspend_prepared.parquet": "数据预处理 - 停牌数据",
        "st_prepared.parquet": "数据预处理 - ST数据",
        "industry_prepared.parquet": "数据预处理 - 行业数据",
        "data_prepare_report.txt": "数据预处理 - 报告",
        
        # 步骤2：因子构建
        "panel_factor.parquet": "因子构建 - 因子面板",
        "factor_build_diagnostics.txt": "因子构建 - 诊断报告",
        
        # 步骤3：因子标准化与中性化
        "panel_standardized_neutralized.parquet": "因子标准化 - 标准化因子面板",
        
        # 步骤4：IC测试
        "ic_summary.csv": "IC测试 - IC汇总",
        "ic_series_A1_mad_raw.csv": "IC测试 - A1因子IC序列",
        "ic_series_A2_mad_neutralized.csv": "IC测试 - A2因子IC序列",
        "ic_series_B1_rank_raw.csv": "IC测试 - B1因子IC序列",
        "ic_series_B2_rank_neutralized.csv": "IC测试 - B2因子IC序列",
        "cum_ic.png": "IC测试 - Pearson IC累计图",
        "cum_rank_ic.png": "IC测试 - Rank IC累计图",
        
        # 步骤5：分层回测
        "layer_perf.csv": "分层回测 - 绩效汇总",
        "layer_backtest.png": "分层回测 - 净值图",
        "group_returns_standard_A1_mad_raw.csv": "分层回测 - A1标准分层收益",
        "group_returns_standard_A2_mad_neutralized.csv": "分层回测 - A2标准分层收益",
        "group_returns_standard_B1_rank_raw.csv": "分层回测 - B1标准分层收益",
        "group_returns_standard_B2_rank_neutralized.csv": "分层回测 - B2标准分层收益",
        "group_returns_industry_neutral_A1_mad_raw.csv": "分层回测 - A1行业中性分层收益",
        "group_returns_industry_neutral_A2_mad_neutralized.csv": "分层回测 - A2行业中性分层收益",
        "group_returns_industry_neutral_B1_rank_raw.csv": "分层回测 - B1行业中性分层收益",
        "group_returns_industry_neutral_B2_rank_neutralized.csv": "分层回测 - B2行业中性分层收益",
        "layer_nav_standard_A1_mad_raw.csv": "分层回测 - A1标准分层净值",
        "layer_nav_standard_A2_mad_neutralized.csv": "分层回测 - A2标准分层净值",
        "layer_nav_standard_B1_rank_raw.csv": "分层回测 - B1标准分层净值",
        "layer_nav_standard_B2_rank_neutralized.csv": "分层回测 - B2标准分层净值",
        "layer_nav_industry_neutral_A1_mad_raw.csv": "分层回测 - A1行业中性分层净值",
        "layer_nav_industry_neutral_A2_mad_neutralized.csv": "分层回测 - A2行业中性分层净值",
        "layer_nav_industry_neutral_B1_rank_raw.csv": "分层回测 - B1行业中性分层净值",
        "layer_nav_industry_neutral_B2_rank_neutralized.csv": "分层回测 - B2行业中性分层净值",
        
        # 步骤6：组合优化
        "bonus_weights.csv": "组合优化 - 权重分配",
        "bonus_perf.csv": "组合优化 - 绩效指标",
        "bonus_nav.csv": "组合优化 - 净值序列",
        "bonus_nav.png": "组合优化 - 净值图",
        "bonus_summary.txt": "组合优化 - 优化摘要",
        
        # 额外分析文件
        "DY-IND-CHG_STATUS_定性分析报告.txt": "额外分析 - DY-IND-CHG_STATUS定性分析",
        "DY-IND-CHG_STATUS_详细关系分析报告.txt": "额外分析 - DY-IND-CHG_STATUS详细关系分析",
    }
    
    # 复制文件
    copied_count = 0
    missing_files = []
    
    for filename, description in core_files.items():
        src_file = results_dir / filename
        if src_file.exists():
            dst_file = final_dir / filename
            shutil.copy2(src_file, dst_file)
            copied_count += 1
            print(f"[OK] {filename} ({description})")
        else:
            missing_files.append(filename)
            print(f"[MISSING] {filename} ({description}) - 文件不存在")
    
    # 生成筛选报告
    report_lines = [
        "=" * 80,
        "最后一次全过程实验结果文件筛选报告",
        "=" * 80,
        f"筛选时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"源目录: {results_dir}",
        f"目标目录: {final_dir}",
        "",
        f"成功复制: {copied_count} 个文件",
        f"缺失文件: {len(missing_files)} 个文件",
        "",
        "=" * 80,
        "文件清单",
        "=" * 80,
    ]
    
    for filename, description in core_files.items():
        status = "[OK]" if (results_dir / filename).exists() else "[MISSING]"
        report_lines.append(f"{status} {filename:50s} {description}")
    
    if missing_files:
        report_lines.extend([
            "",
            "=" * 80,
            "缺失文件列表",
            "=" * 80,
        ])
        report_lines.extend(missing_files)
    
    report_path = final_dir / "筛选报告.txt"
    report_path.write_text("\n".join(report_lines), encoding="utf-8")
    
    print("\n" + "=" * 80)
    print(f"筛选完成！")
    print(f"成功复制 {copied_count} 个文件到 {final_dir}")
    if missing_files:
        print(f"缺失 {len(missing_files)} 个文件")
    print(f"筛选报告已保存至: {report_path}")
    print("=" * 80)

if __name__ == "__main__":
    filter_final_results()
