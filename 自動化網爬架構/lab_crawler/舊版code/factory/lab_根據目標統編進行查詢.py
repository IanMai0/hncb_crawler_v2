import pandas as pd

# ========= 檔案路徑 =========
FACTORY_CSV = "output/處理後_20260323_092223_11501.csv"
TARGET_UID_CSV = "137313筆目標統編_400w-1000w.csv"
OUTPUT_CSV = "result_400w-1000w_260302.csv"

# ========= 讀取資料 =========
factory_df = pd.read_csv(FACTORY_CSV, dtype=str)
target_df = pd.read_csv(TARGET_UID_CSV, dtype=str)

# 統一編號標準化
factory_df["統一編號"] = factory_df["統一編號"].astype(str).str.strip()
target_df["統一編號"] = target_df["統一編號"].astype(str).str.strip()

# ========= 篩選目標統編 =========
filtered = factory_df[
    factory_df["統一編號"].isin(target_df["統一編號"])
].copy()

# ========= 異常檢測 =========
# 1. 同統編多工廠
dup_uid = (
    filtered.groupby("統一編號")
    .size()
    .reset_index(name="cnt")
)
multi_uid = set(dup_uid[dup_uid["cnt"] > 1]["統一編號"])

def detect_abnormal(row):
    if row["統一編號"] in multi_uid:
        return "MULTI_FACTORY"
    return ""

filtered["__異常註記__"] = filtered.apply(detect_abnormal, axis=1)

# ========= 欄位映射（依你給的順序） =========
result_cols = {
    "工廠名稱": "工廠名稱",
    "工廠登記編號": "工廠登記編號",
    "工廠設立許可案號": "工廠設立許可案號",
    "工廠地址": "工廠地址",
    "工廠市鎮鄉村里": "工廠市鎮鄉村里",
    "工廠負責人姓名": "工廠負責人姓名",
    "統一編號": "統一編號",
    "工廠組織型態": "工廠組織型態",
    "工廠登記狀態": "工廠登記狀態",
    "產業類別": "產業類別",
    "主要產品": "主要產品",
    "產業類別代號": "產業類別代號",
    "主要產品代號": "主要產品代號",
    "__異常註記__": "__異常註記__",
}

result_df = filtered[list(result_cols.keys())]

# ========= 輸出 =========
result_df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

print(f"✅ 完成：{len(result_df):,} 筆，輸出至 {OUTPUT_CSV}")
