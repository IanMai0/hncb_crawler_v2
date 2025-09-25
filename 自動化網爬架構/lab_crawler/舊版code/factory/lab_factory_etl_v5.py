import html
import re
import pandas as pd
import unicodedata
from collections import Counter
from typing import Tuple
import datetime
import os


# === 欄位代碼表 ===
COLUMN_CODE_MAP = {
    "工廠名稱": "C01",
    "工廠登記編號": "C03",
    "工廠設立許可案號": "C04",
    "工廠地址": "C05",
    "工廠市鎮鄉村里": "C06",
    "工廠負責人姓名": "C07",
    "統一編號": "C02",
    "工廠組織型態": "C08",
    "工廠設立核准日期": "C09",
    "工廠登記核准日期": "C10",
    "工廠登記狀態": "C11",
    "產業類別": "C12",
    "主要產品": "C13",
    "產業類別代號": "C14",
    "主要產品代號": "C15",
    "__異常註記__": "C00",
}

# === 異常註記代碼表 ===
NOTE_CODE_MAP = {
    '為空值': 'E01',
    '長度<': 'E02',
    '長度>': 'E03',
    '長度異常（多於允許規則）': 'E04',
    '長度異常（少於允許清單）': 'E05',
    '包含規則不允許的異常關鍵字': 'E06',
    '全數字或符號': 'E07',
    '相同值 跨列重複': 'E08',
    '跨列重複': 'E09',
    '疑似測試值': 'E10',
    '特定無效值': 'E11'
}

# === 資料預處理：讀取、欄位正規化、數據優化處理與代碼/名稱拆分 ===
class DataPreprocessor:
    """
    負責：
      1) CSV 讀取後全形→半形、trim
      2) 拆分「主要產品」「產業類別」為代號與名稱
    """
    def __init__(self):
        self.normalize_cols = list(COLUMN_CODE_MAP.keys())

    # 民國格式轉西元
    @staticmethod
    def convert_roc_to_ad(date_str: str) -> str:
        """將中華民國年格式 (如 1140625) 轉換為西元格式 (2025-06-25)，若格式異常則回傳原值"""
        if pd.isna(date_str) or not re.fullmatch(r"\d{7,8}", date_str):
            return date_str
        try:
            year = int(date_str[:3]) + 1911
            month = int(date_str[3:5])
            day = int(date_str[5:7])
            dt = datetime.date(year, month, day)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            return date_str

    # 數據優化處理，優化 ETL 效率，甚至未來 DB CRUD 效率
    # 分割目標欄位_產業類別、主要產品
    @staticmethod
    def split_codes_and_labels(field: str) -> pd.Series:
        if pd.isna(field) or field.strip() == "":
            return pd.Series({"codes": "", "labels": ""})

        codes = []
        labels = []

        for match in re.finditer(r"(\d+)([^0-9]*)", field):
            code = match.group(1).strip()
            label = match.group(2).strip().replace("、", "")  # 🔧 去除殘留頓號
            if code or label:
                codes.append(code)
                labels.append(label)

        return pd.Series({
            "codes": ";".join(codes),
            "labels": ";".join(labels)
        })

    # 載入 並且規範化
    def load_and_normalize(self, csv_path: str) -> pd.DataFrame:
        df = pd.read_csv(csv_path, encoding="utf-8", dtype=str, keep_default_na=False)
        for col in self.normalize_cols:
            if col not in df.columns:
                continue
            df[col] = (
                df[col].fillna("").astype(str)
                .apply(lambda x: ''.join(
                    chr(ord(c) - 0xFEE0) if 0xFF01 <= ord(c) <= 0xFF5E else
                    (' ' if ord(c) == 0x3000 else c)
                    for c in x
                ))
                .str.strip()
            )
        return df

    # 數據優化處理欄位_產業類別、主要產品、工廠設立核准日期、工廠登記核准日期
    def optimize_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        # 拆分「產業類別」
        if "產業類別" in df.columns:
            ind_split = df["產業類別"].apply(self.split_codes_and_labels)
            df["產業類別代號"] = ind_split["codes"]
            df["產業類別"] = ind_split["labels"]

        # 拆分「主要產品」
        if "主要產品" in df.columns:
            prod_split = df["主要產品"].apply(self.split_codes_and_labels)
            df["主要產品代號"] = prod_split["codes"]
            df["主要產品"] = prod_split["labels"]

        # 日期欄位：民國 → 西元
        for date_col in ["工廠設立核准日期", "工廠登記核准日期"]:
            if date_col in df.columns:
                df[date_col] = df[date_col].apply(self.convert_roc_to_ad)

        return df

    # 預處理
    def preprocess(self, csv_path: str) -> pd.DataFrame:
        df = self.load_and_normalize(csv_path)
        df = self.optimize_fields(df)
        return df

# === 清洗與異常 HTML/全形/標點/難字 處理 ===
class DataCleaner:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    # 轉換並處理錯誤
    def convert_and_handle_errors(self) -> pd.DataFrame:
        def html_half(val: str) -> str:
            text = html.unescape(val)
            return ''.join(
                chr(ord(c) - 0xFEE0) if 0xFF01 <= ord(c) <= 0xFF5E else
                ' ' if ord(c) == 0x3000 else c
                for c in text
            )
        def clean_spaces(val: str) -> str:
            return re.sub(r"\s+", " ", val)
        def strip_punct(val: str) -> str:
            return re.sub(r"^[^\w\u4e00-\u9fff ]+|[^\w\u4e00-\u9fff ]+$", "", val)
        def replace_diff(val: str) -> str:
            norm = unicodedata.normalize('NFC', val)
            out = []
            for c in norm:
                cat = unicodedata.category(c)[0]
                cp = ord(c)
                in_pua = (0xE000 <= cp <= 0xF8FF) or (0xF0000 <= cp <= 0xFFFFD) or (0x100000 <= cp <= 0x10FFFD)
                out.append('　' if cat not in ('L','N','P','Z') or in_pua else c)
            return ''.join(out)

        special = {"工廠名稱","工廠地址","工廠市鎮鄉村里","工廠負責人姓名"}
        for col in self.df.columns:
            s = self.df[col].astype(str).apply(html_half).apply(clean_spaces)
            if col not in special:
                s = s.apply(strip_punct)
            self.df[col] = s
        for col in special:
            if col in self.df.columns:
                self.df[col] = self.df[col].apply(replace_diff)
        return self.df

# === 異常註記與統計 ===
class DataAnomalyReporter:
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
        if '__異常註記__' not in self.df.columns:
            self.df['__異常註記__'] = ''
        self.rules = {
            '工廠登記編號': {'min_len':8,'max_len':10,'repeat_pattern':r'(\d)\1{7}','repeat_limit':2},
            '工廠名稱': {'min_len':2,'keywords':['期限'],'pattern':r'[0-9\W_]+','repeat_limit':2},
            '統一編號': {'min_len':8,'repeat_pattern':r'(\d)\1{7}','repeat_limit':2},
            '工廠設立許可案號': {'min_len':14,'repeat_pattern':r'(\d)\1{7}','repeat_limit':2},
            '工廠地址': {'min_len':9,'repeat_pattern':r'(\d)\1{7}','repeat_limit':2},
            '工廠市鎮鄉村里': {'min_len': 5, 'repeat_pattern': r'(\d)\1{7}'},
            '工廠負責人姓名': {'min_len':2,'repeat_pattern':r'(\d)\1{7}','repeat_limit':2},
            '工廠組織型態': {'min_len':2},
            '工廠設立核准日期': {'min_len':7,'repeat_pattern':r'(\d)\1{7}','invalid_values':['0010101']},
            '工廠登記核准日期': {'min_len': 2, 'repeat_pattern': r'(\d)\1{7}'},
            '工廠登記狀態': {'min_len': 2, 'repeat_pattern': r'(\d)\1{7}'},
            '產業類別': {'min_len_list':[0,1,2]},
            '主要產品': {'min_len':5}
        }

    # 異常註記
    def validate_column(self, col: str, rule: dict) -> None:
        s = self.df[col]
        col_code = COLUMN_CODE_MAP.get(col, col)

        def log(code: str):
            return f'[{col_code}]{code};'

        mask_empty = s.str.strip() == ''
        self.df.loc[mask_empty, '__異常註記__'] += log(NOTE_CODE_MAP['為空值'])

        if 'min_len' in rule:
            mask = s.str.len() < rule['min_len']
            self.df.loc[mask, '__異常註記__'] += log(NOTE_CODE_MAP['長度<'])
        if 'max_len' in rule:
            mask = s.str.len() > rule['max_len']
            self.df.loc[mask, '__異常註記__'] += log(NOTE_CODE_MAP['長度>'])
        if 'min_len_list' in rule:
            mask = s.str.len().isin(rule['min_len_list'])
            self.df.loc[mask, '__異常註記__'] += log(NOTE_CODE_MAP['長度異常（少於允許清單）'])
        if 'keywords' in rule:
            for kw in rule['keywords']:
                mask = s.str.contains(kw, na=False)
                self.df.loc[mask, '__異常註記__'] += log(NOTE_CODE_MAP['包含規則不允許的異常關鍵字'])
        if 'pattern' in rule:
            mask = s.str.fullmatch(rule['pattern'], na=False)
            self.df.loc[mask, '__異常註記__'] += log(NOTE_CODE_MAP['全數字或符號'])
        if 'repeat_pattern' in rule:
            mask = s.str.fullmatch(rule['repeat_pattern'], na=False)
            self.df.loc[mask, '__異常註記__'] += log(NOTE_CODE_MAP['相同值 跨列重複'])
        if 'repeat_limit' in rule:
            bad_vals = s.value_counts()[lambda x: x > rule['repeat_limit']].index
            mask = self.df[col].isin(bad_vals)
            self.df.loc[mask, '__異常註記__'] += log(NOTE_CODE_MAP['跨列重複'])
        if 'invalid_values' in rule:
            mask = s.isin(rule['invalid_values'])
            self.df.loc[mask, '__異常註記__'] += log(NOTE_CODE_MAP['特定無效值'])

        mask_test = s.str.contains(r'(?:null|test|sample|none)', case=False, na=False)
        self.df.loc[mask_test, '__異常註記__'] += log(NOTE_CODE_MAP['疑似測試值'])

    def dedup(self) -> None:
        self.df['__異常註記__'] = self.df['__異常註記__'].apply(
            lambda x: ';'.join(sorted(set(filter(None, x.split(';'))))) if isinstance(x, str) else x
        )

    # 異常統計
    def count_abnormalities(self) -> Counter:
        self.dedup()
        cnt = Counter()
        for note in self.df['__異常註記__']:
            if isinstance(note, str):
                for t in note.split(';'):
                    if t:
                        cnt[t] += 1
        return cnt

    # 執行程序
    def execute(self) -> pd.DataFrame:
        DataCleaner(self.df).convert_and_handle_errors()
        for col, rule in self.rules.items():
            self.validate_column(col, rule)
        stats = self.count_abnormalities()
        print("\n異常統計彙總：")
        for tag, count in stats.items():
            print(f"{tag:<20}:{count}")
        return self.df

# === 敘述性統計 ===
class StatisticalSummaryEngine:
    def __init__(self, df: pd.DataFrame): self.df = df.copy()
    def analyze_text_distribution(self, col: str) -> pd.DataFrame:
        items = self.df[col].dropna().astype(str).str.split("、").explode().str.strip()
        total = len(items)
        counts = items.value_counts().to_frame("數量")
        counts["占比(%)"] = (counts["數量"] / total * 100).round(2)
        return counts.reset_index(names=[col])

    # 抓出前 100 名最多工廠的公司
    def output_top_100_factory_holders(self, output_path: str = "output/top_100_companies.csv") -> pd.DataFrame:
        """
        統計前 50 名工廠數最多的統一編號（即公司）
        """
        # 確保欄位名稱為「統一編號」
        if "統一編號" not in self.df.columns:
            raise ValueError("DataFrame 中缺少『統一編號』欄位")
        # 統計統一編號的工廠數
        top100  = (
            self.df["統一編號"]
            .dropna()
            .astype(str)
            .value_counts()
            .head(100)
            .reset_index()
        )
        top100.columns = ["統一編號", "工廠數"]
        # 抓公司名稱：選取每個統一編號的第一筆工廠名稱
        names = (
            self.df[["統一編號", "工廠名稱"]]
            .drop_duplicates(subset=["統一編號"])
            .rename(columns={"工廠名稱": "公司名稱"})
        )
        # 工廠清單：groupby 彙整工廠登記編號為字串清單
        factory_lists = (
            self.df[["統一編號", "工廠登記編號"]]
            .dropna()
            .astype(str)
            .groupby("統一編號")["工廠登記編號"]
            .apply(lambda x: ";".join(sorted(set(x))))
            .reset_index(name="工廠登記編號清單")
        )
        # 整併所有欄位
        result = (
            top100
            .merge(names, on="統一編號", how="left")
            .merge(factory_lists, on="統一編號", how="left")
            .sort_values(by="工廠數", ascending=False)
        )
        # 建立資料夾並匯出
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        result.to_csv(output_path, index=False, encoding="utf-8-sig")
        print(f"✅ 匯出完成：{output_path}")
        return result

    def execute(self) -> None:
        for col in ["產業類別","工廠組織型態","工廠登記狀態","工廠市鎮鄉村里","主要產品"]:
            print(f"\n{col} 統計報表：")
            print(self.analyze_text_distribution(col))

class Output:
    def __init__(self, df: pd.DataFrame, TargetFileName: str) -> None:
        self.df = df
        self.TargetFileName = TargetFileName

    # 匯出 處理後資料 (純異常註記)
    def output_data_to_csv(self):
        output_dir = "output"
        os.makedirs(output_dir, exist_ok=True)

        output_filename = f"處理後_{self.TargetFileName}"
        output_path = os.path.join(output_dir, output_filename)

        columns_order = [
            "工廠名稱", "工廠登記編號", "工廠設立許可案號",
            "工廠地址", "工廠市鎮鄉村里", "工廠負責人姓名", "統一編號",
            "工廠組織型態", "工廠設立核准日", "工廠登記核准日",
            "工廠登記狀態", "產業類別", "主要產品",
            "產業類別代號", "主要產品代號", "__異常註記__"
        ]

        # 只保留並排序指定欄位（若某些欄位缺失會跳過）
        existing_columns = [col for col in columns_order if col in self.df.columns]
        df_to_output = self.df[existing_columns]

        df_to_output.to_csv(output_path, index=False, encoding="utf-8-sig")
        print(f"✅ 資料已匯出至：{output_path}")

    # 匯出 處理後資料 (含入庫預處裡)
    def output_data_to_DB(self):
        pass

# # === 主流程 (物件導向打包測試使用) ===
# def main(fp: str):
#     # 檔案路徑範例：fp = r"C:/Users/wits/Downloads/工廠data/生產中工廠清冊/11404.csv"
#     # 1. 資料預處理
#     preprocessor = DataPreprocessor()
#     df = preprocessor.preprocess(fp)
#     # 2. 清洗與異常處理
#     df_after = DataCleaner(df).convert_and_handle_errors()
#     df_after = DataAnomalyReporter(df_after).execute()
#     # 3. 敘述性統計報表
#     print("\n" + "=" * 40 + "\n")
#     StatisticalSummaryEngine(df_after).execute()  # 針對處裡好以後的 Data 進行敘述性統計

# if __name__ == '__main__':
#     main()

# === 主流程 (單一測試使用) ===
if __name__ == '__main__':
    fp = r"C:/Users/wits/Downloads/工廠data/11405.csv"
    # 1. 資料預處理
    preprocessor = DataPreprocessor()
    df = preprocessor.preprocess(fp)
    # 2. 清洗與異常處理
    df_after = DataCleaner(df).convert_and_handle_errors()
    df_after = DataAnomalyReporter(df_after).execute()

    # 支線_抓出前 100 名最多工廠數公司
    engine = StatisticalSummaryEngine(df_after)
    # 輸出前 100 名統一編號與工廠數
    top100_df = engine.output_top_100_factory_holders()
    print('\n\n\n')
    print('-'*33)

    # 3. 敘述性統計報表
    # print("\n" + "="*40 + "\n")
    # StatisticalSummaryEngine(df_after).execute()  # 針對處裡好以後的 Data 進行敘述性統計
