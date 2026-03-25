import random
import pandas as pd
from typing import List, Tuple


# =========================
# Load file
# =========================
def load_file_to_df(path: str) -> pd.DataFrame:
    if path.endswith(".csv"):
        df = pd.read_csv(path)
    else:
        df = pd.read_excel(path, header=0)

    # 去掉全空列
    df = df.dropna(how="all")

    # 去掉 index
    df.reset_index(drop=True, inplace=True)

    return df


# =========================
# normalize（對齊 ETL）
# =========================
def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for col in df.columns:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace("　", " ", regex=False)
            .str.strip()
            .replace({"": None})
        )

    return df

# =========================
# 直接在 pandas 做 mapping：
# =========================
def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    將 PCC 原始欄位轉成 DB schema 欄位
    """

    column_map = {
        df.columns[0]: "Corporation_number",
        df.columns[1]: "Corporation_name",
        df.columns[2]: "Corporation_address",
        df.columns[3]: "Announce_agency_code",
        df.columns[4]: "Announce_agency_name",
        df.columns[5]: "Announce_agency_address",
        df.columns[6]: "Contact_person",
        df.columns[7]: "Contact_phone",
        df.columns[8]: "Announce_agency_mail",
        df.columns[9]: "Judgment_no",
        df.columns[10]: "Effective_date",
        df.columns[11]: "Expire_date",
        df.columns[12]: "Announce_agency_no",
        df.columns[13]: "Remark",
    }

    df = df.rename(columns=column_map)

    return df


# =========================
# diff 核心
# =========================
def compute_diff(
    df_new: pd.DataFrame,
    df_old: pd.DataFrame,
    key_col: str
) -> Tuple[int, pd.DataFrame]:
    """
    模擬 SQL CDC：
    - 新 key → insert
    - 同 key，但欄位不同 → insert
    """

    # normalize
    df_new = normalize_df(df_new)
    df_old = normalize_df(df_old)

    # index by key
    df_new = df_new.set_index(key_col)
    df_old = df_old.set_index(key_col)

    # 找 new keys
    new_keys = df_new.index.difference(df_old.index)

    # 找共同 keys
    common_keys = df_new.index.intersection(df_old.index)

    # 比較內容
    changed_keys = []

    for k in common_keys:
        row_new = df_new.loc[k]
        row_old = df_old.loc[k]

        # 比較整 row（排除 nan 差異）
        # if not row_new.equals(row_old):
        if not row_new.fillna("").equals(row_old.fillna("")):
            changed_keys.append(k)


    diff_keys = list(new_keys) + changed_keys

    diff_df = df_new.loc[diff_keys].reset_index()

    return len(diff_keys), diff_df


# =========================
# 隨機抽三天
# =========================
def sample_3_days(files: List[str]) -> List[str]:
    if len(files) < 3:
        raise ValueError("至少需要 3 個檔案")

    return sorted(random.sample(files, 3))


# =========================
# 主流程（你要用的）
# =========================
def run_random_diff_check(
    file_list: List[str],
    key_col: str = "Corporation_number"
):
    selected = sample_3_days(file_list)

    print("\n🎯 隨機抽樣三天：")
    for f in selected:
        print(f" - {f}")

    df1 = load_file_to_df(selected[0])
    df1 = standardize_columns(df1)
    df2 = load_file_to_df(selected[1])
    df2 = standardize_columns(df2)
    df3 = load_file_to_df(selected[2])
    df3 = standardize_columns(df3)

    print("\n🔍 Diff 結果：")

    diff_12, _ = compute_diff(df2, df1, key_col)
    print(f"Day2 vs Day1 差異筆數: {diff_12}")

    diff_23, _ = compute_diff(df3, df2, key_col)
    print(f"Day3 vs Day2 差異筆數: {diff_23}")

    return {
        "pair_1_2": diff_12,
        "pair_2_3": diff_23,
        "files": selected
    }

files = [
    "./data/pcc_excellent_2026-03-11_15-32-02.csv",
    "./data/pcc_excellent_2026-03-17_16-39-57.csv",
    "./data/pcc_excellent_2026-03-18_13-47-41.csv",
]

result = run_random_diff_check(files)

