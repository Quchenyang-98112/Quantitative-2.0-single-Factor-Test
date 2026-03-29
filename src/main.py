#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
main.py
单因子测试流水线主程序

功能：
- 按顺序执行单因子测试的6个步骤
- 支持从任意步骤开始执行
- 提供执行进度反馈

步骤说明：
1. 数据预处理 (01_data_prepare.py)
2. 因子构建 (02_factor_build.py)
3. 因子标准化与中性化 (03_standardize_neutralize.py)
4. IC测试 (04_ic_test.py)
5. 分层回测 (05_layer_backtest.py)
6. 组合优化 (06_bonus_opt.py)

使用方法：
- 直接运行：python main.py
- 从指定步骤开始：python main.py --start 3
"""

from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path


def load_module(module_path: Path, module_name: str):
    """动态加载Python模块
    
    参数:
        module_path: 模块文件路径
        module_name: 模块名称
    
    返回:
        加载的模块对象
    """
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


def main():
    """主程序入口"""
    parser = argparse.ArgumentParser(description="单因子测试流水线")
    parser.add_argument(
        "--start",
        type=int,
        default=1,
        help="从第几步开始执行 (1-6)，默认从第1步开始"
    )
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[1]
    src_dir = project_root / "src"

    # 定义流水线步骤
    steps = [
        (src_dir / "01_data_prepare.py", "step01_data_prepare", "数据预处理"),
        (src_dir / "02_factor_build.py", "step02_factor_build", "因子构建"),
        (src_dir / "03_standardize_neutralize.py", "step03_standardize_neutralize", "因子标准化与中性化"),
        (src_dir / "04_ic_test.py", "step04_ic_test", "IC测试"),
        (src_dir / "05_layer_backtest.py", "step05_layer_backtest", "分层回测"),
        (src_dir / "06_bonus_opt.py", "step06_bonus_opt", "组合优化"),
    ]

    start_step = max(1, min(args.start, len(steps)))

    print("=" * 60)
    print("单因子测试流水线")
    print("=" * 60)
    print(f"总共 {len(steps)} 个步骤，从第 {start_step} 步开始执行\n")

    # 执行流水线
    for i, (module_path, module_name, step_desc) in enumerate(steps[start_step - 1:], start=start_step):
        print("=" * 60)
        print(f"步骤 {i}/{len(steps)}: {step_desc}")
        print(f"运行: {module_path.name}")
        print("=" * 60)

        try:
            mod = load_module(module_path, module_name)
            mod.run(project_root)
            print(f"步骤 {i} 完成\n")
        except Exception as e:
            print(f"步骤 {i} 执行失败: {e}")
            raise

    print("=" * 60)
    print("流水线执行完成")
    print("=" * 60)


if __name__ == "__main__":
    main()
