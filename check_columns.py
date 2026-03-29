from pathlib import Path
import pandas as pd

def check_parquet_columns(file_path: Path):
    """查看Parquet文件的所有列名"""
    try:
        df = pd.read_parquet(file_path)
        print(f"文件: {file_path.name}")
        print(f"列数: {len(df.columns)}")
        print("列名:")
        for col in df.columns:
            print(f"  - {col}")
        return df.columns
    except Exception as e:
        print(f"读取失败: {e}")
        return []

def main():
    project_root = Path(__file__).resolve().parent
    basic_data_dir = project_root / "basic_data"
    
    # 只查看daily_data.parquet的列名
    file_path = basic_data_dir / "daily_data.parquet"
    print("=" * 60)
    check_parquet_columns(file_path)
    
    # 查看停牌.parquet的列名
    file_path = basic_data_dir / "停牌.parquet"
    print("=" * 60)
    check_parquet_columns(file_path)
    
    # 查看industry.parquet的列名
    file_path = basic_data_dir / "industry.parquet"
    print("=" * 60)
    check_parquet_columns(file_path)

if __name__ == "__main__":
    main()
