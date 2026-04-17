from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd


# ---------------
# USE case
# python compare_tax_ids_v2.py  --target-file "input\企行部\目標檔案_39萬筆統編.csv"  --source-file "input\商工\比對統編檔案含有數據.csv"  --output-dir "output"
# ---------------

def normalize_tax_id(series: pd.Series) -> pd.Series:
    """
    將統編標準化為 8 碼字串：
    - 去除 .0
    - 去空白
    - 只保留數字
    - 左側補 0 到 8 碼
    """
    s = series.astype(str).str.strip()
    s = s.str.replace(r"\.0$", "", regex=True)
    s = s.str.replace(r"\D", "", regex=True)
    s = s.str.zfill(8)
    s = s.where(s.str.len() > 0, "")
    return s


def compare_files(
    target_file: str,
    source_file: str,
    target_tax_id_col: str = "統編",
    source_tax_id_col: str = "統一編號",
    output_dir: str = "output",
) -> None:
    """
    比對 A 檔（目標檔案）中的統編，是否存在 B 檔（比對檔案）。
    若存在：
        匯出 B 檔中所有對應統編的完整資料，但移除原始序號欄位。
    若不存在：
        匯出 A 檔中未匹配到的統編，且只保留一欄統編。
    """

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    print("開始讀取檔案...")

    # 讀取時先全部當字串，避免統編被吃掉前導 0
    df_a = pd.read_csv(target_file, dtype=str, encoding="utf-8-sig")
    df_b = pd.read_csv(source_file, dtype=str, encoding="utf-8-sig")

    if target_tax_id_col not in df_a.columns:
        raise ValueError(f"A 檔找不到欄位：{target_tax_id_col}；實際欄位：{list(df_a.columns)}")

    if source_tax_id_col not in df_b.columns:
        raise ValueError(f"B 檔找不到欄位：{source_tax_id_col}；實際欄位：{list(df_b.columns)}")

    print("開始標準化統編格式...")

    df_a["_tax_id"] = normalize_tax_id(df_a[target_tax_id_col])
    df_b["_tax_id"] = normalize_tax_id(df_b[source_tax_id_col])

    # 排除空統編，避免垃圾值污染結果
    df_a = df_a[df_a["_tax_id"] != ""].copy()
    df_b = df_b[df_b["_tax_id"] != ""].copy()

    print(f"A 檔筆數（有效統編）：{len(df_a):,}")
    print(f"B 檔筆數（有效統編）：{len(df_b):,}")

    a_tax_ids = set(df_a["_tax_id"])
    b_tax_ids = set(df_b["_tax_id"])

    matched_tax_ids = a_tax_ids & b_tax_ids
    unmatched_tax_ids = a_tax_ids - b_tax_ids

    print(f"有匹配到的統編數：{len(matched_tax_ids):,}")
    print(f"A 檔未匹配到的統編數：{len(unmatched_tax_ids):,}")

    # 1. 匯出 B 檔中所有對應到 A 檔統編的完整資料
    matched_b = df_b[df_b["_tax_id"].isin(matched_tax_ids)].copy()

    # 移除 B 檔原本的序號欄位，避免舊排序干擾
    if "序號" in matched_b.columns:
        matched_b.drop(columns=["序號"], inplace=True)

    # 移除內部比對欄位
    if "_tax_id" in matched_b.columns:
        matched_b.drop(columns=["_tax_id"], inplace=True)

    # 2. 匯出 A 檔中未匹配到的統編資料，只保留一欄統編
    unmatched_a = (
        df_a[df_a["_tax_id"].isin(unmatched_tax_ids)][[target_tax_id_col]]
        .drop_duplicates()
        .copy()
    )
    unmatched_a.columns = ["統編"]

    matched_output = output_path / "matched_from_B.csv"
    unmatched_output = output_path / "unmatched_from_A.csv"
    summary_output = output_path / "summary.txt"

    matched_b.to_csv(matched_output, index=False, encoding="utf-8-sig")
    unmatched_a.to_csv(unmatched_output, index=False, encoding="utf-8-sig")

    with open(summary_output, "w", encoding="utf-8-sig") as f:
        f.write("比對結果摘要\n")
        f.write("=" * 40 + "\n")
        f.write(f"A 檔路徑：{target_file}\n")
        f.write(f"B 檔路徑：{source_file}\n")
        f.write(f"A 檔統編欄位：{target_tax_id_col}\n")
        f.write(f"B 檔統編欄位：{source_tax_id_col}\n")
        f.write(f"A 檔有效筆數：{len(df_a):,}\n")
        f.write(f"B 檔有效筆數：{len(df_b):,}\n")
        f.write(f"有匹配到的統編數：{len(matched_tax_ids):,}\n")
        f.write(f"A 檔未匹配到的統編數：{len(unmatched_tax_ids):,}\n")
        f.write(f"匹配結果檔：{matched_output}\n")
        f.write(f"未匹配結果檔：{unmatched_output}\n")

    print("完成。")
    print(f"匹配結果已輸出：{matched_output}")
    print(f"A 檔未匹配結果已輸出：{unmatched_output}")
    print(f"摘要檔已輸出：{summary_output}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="比對 A 檔統編是否存在於 B 檔，並匯出結果")
    parser.add_argument("--target-file", required=True, help="A 檔路徑（目標檔案）")
    parser.add_argument("--source-file", required=True, help="B 檔路徑（比對統編檔案）")
    parser.add_argument("--target-tax-id-col", default="統編", help="A 檔統編欄位名稱，預設：統編")
    parser.add_argument("--source-tax-id-col", default="統一編號", help="B 檔統編欄位名稱，預設：統一編號")
    parser.add_argument("--output-dir", default="output", help="輸出資料夾，預設：output")
    args = parser.parse_args()

    compare_files(
        target_file=args.target_file,
        source_file=args.source_file,
        target_tax_id_col=args.target_tax_id_col,
        source_tax_id_col=args.source_tax_id_col,
        output_dir=args.output_dir,
    )
