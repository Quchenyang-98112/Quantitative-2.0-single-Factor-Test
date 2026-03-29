#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
import pandas as pd

def check_columns():
    project_root = Path(__file__).resolve().parent
    basic_data_dir = project_root / "basic_data"
    
    # 读取数据
    print("读取 daily_data.parquet 文件...")
    daily = pd.read_parquet(basic_data_dir / "daily_data.parquet")
    
    # 打印所有列名
    print("\n所有列名:")
    for i, col in enumerate(daily.columns):
        print(f"{i+1}. {col}")
    
    # 打印数据类型
    print("\n数据类型:")
    print(daily.dtypes)
    
    # 打印前几行数据
    print("\n前5行数据:")
    print(daily.head())

if __name__ == "__main__":
    check_columns()
