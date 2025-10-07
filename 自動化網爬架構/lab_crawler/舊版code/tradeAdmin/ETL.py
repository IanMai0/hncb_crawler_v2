import pandas as pd
from typing import Optional

# === 民國轉西元 Function ===
def convert_minguo_to_ad(date_str: str) -> Optional[str]:
    """
    將民國時間轉換為西元時間
    範例：
    114 01-06 -> 2025 01-06
    114       -> 2025
    """
    try:
        if pd.isna(date_str):
            return None

        date_str = str(date_str).strip()
        parts = date_str.split()

        # 民國轉西元
        minguo_year = int(parts[0])
        ad_year = minguo_year + 1911  # 民國1年=1912西元

        if len(parts) == 1:
            return str(ad_year)
        elif len(parts) == 2:
            return f"{ad_year} {parts[1]}"
        else:
            return None
    except Exception as e:
        print("❌ 無法處理:", date_str, "錯誤:", e)
        return None

# === ETL 流程 ===
def etl_convert_csv(input_file: str, output_file: str):
    # 讀取 CSV
    df = pd.read_csv(input_file)

    # 轉換「時間週期」欄位
    df["時間週期_西元"] = df["時間週期"].apply(convert_minguo_to_ad)

    # 輸出新檔案
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"✅ 已完成轉換，輸出檔案：{output_file}")


# === 測試 ===
if __name__ == "__main__":
    etl_convert_csv("input.csv", "output.csv")
