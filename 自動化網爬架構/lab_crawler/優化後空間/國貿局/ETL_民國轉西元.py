import re
import pandas as pd
from typing import Optional

# === 民國時間週期轉西元 ===
def convert_minguo_period_to_ad(s: str) -> Optional[str]:
    """
    將像「114   01-06」或「114   01-6」的民國時間週期，轉成西元日期 YYYY-MM-DD。
    若格式不符則返回原值。
    """
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return None
    s = str(s).strip()
    if not s:
        return None

    # 樣式 1: YYY   MM-DD 或 YYY   MM-D
    m = re.match(r'^\s*(\d{2,3})\s+(\d{1,2})\s*[-/．.]\s*(\d{1,2})\s*$', s)
    if m:
        y, mm, dd = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return f'{y + 1911:04d}-{mm:02d}-{dd:02d}'

    # 樣式 2: YYYMMDD
    m = re.match(r'^\s*(\d{7})\s*$', s)
    if m:
        raw = m.group(1)
        y, mm, dd = int(raw[:3]), int(raw[3:5]), int(raw[5:7])
        return f'{y + 1911:04d}-{mm:02d}-{dd:02d}'

    # 樣式 3: YYY/MM/DD 或 YYY.MM.DD
    m = re.match(r'^\s*(\d{2,3})[./-](\d{1,2})[./-](\d{1,2})\s*$', s)
    if m:
        y, mm, dd = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return f'{y + 1911:04d}-{mm:02d}-{dd:02d}'

    # 單純年份
    m = re.match(r'^\s*(\d{2,3})\s*$', s)
    if m:
        return str(int(m.group(1)) + 1911)

    return s


# === 民國年份轉西元 ===
def convert_minguo_year_to_ad(s: str) -> Optional[str]:
    """
    將民國年（例如 114）轉成西元年（例如 2025）
    """
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return None
    s = str(s).strip()
    if not s:
        return None

    m = re.match(r'^\d{2,3}$', s)
    if m:
        return str(int(s) + 1911)
    return s


# === 應用於整個 DataFrame ===
def convert_dataframe_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    同時處理兩欄：
    1. 時間週期 → YYYY-MM-DD
    2. 統計時間年 → YYYY
    """
    if '時間週期' in df.columns:
        df['時間週期'] = df['時間週期'].apply(convert_minguo_period_to_ad)
    if '統計時間年' in df.columns:
        df['統計時間年'] = df['統計時間年'].apply(convert_minguo_year_to_ad)
    return df


# === 主程式入口 ===
def main(input_path: str, output_path: str):
    # 讀取外部 CSV
    df = pd.read_csv(input_path, dtype=str, encoding="utf-8-sig")
    print(f"📥 已讀取 {len(df)} 筆資料")

    # 轉換
    df_converted = convert_dataframe_dates(df)

    # 寫出結果
    df_converted.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"✅ 已轉換完成，輸出至：{output_path}")


# === 執行範例 ===
if __name__ == "__main__":
    input_path = "./output/export_import_grade.csv"      # <-- 你原始的檔案路徑
    output_path = "./output/export_import_grade_converted.csv"  # <-- 轉換後輸出位置
    main(input_path, output_path)


