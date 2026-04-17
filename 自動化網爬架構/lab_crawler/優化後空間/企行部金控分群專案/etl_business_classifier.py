# 針對對應統編 把對應的稅籍 data 給拉出來
# 針對企業型態為 獨資 合夥的

from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd


# =========================
# 資料結構定義
# =========================

@dataclass
class ColumnMapping:
    """欄位對應設定"""
    tax_id: str = "統一編號"
    enterprise_type: str = "企業型態"
    industry_code: str = "行業代號"
    industry_name: str = "行業別"
    capital_total: str = "資本總額"
    query_time: str = "查詢時間"


@dataclass
class RuleConfig:
    """單筆規則設定"""
    rule_order: int
    rule_name: str
    enabled: bool
    source_type: str
    field_name: str
    operator: str
    value: str
    output_code: int


# =========================
# 工具函式
# =========================

def normalize_tax_id(value: Any) -> str:
    """
    將統編標準化為 8 碼字串
    """
    if pd.isna(value):
        return ""

    text: str = str(value).strip()
    text = re.sub(r"\.0$", "", text)
    text = re.sub(r"\D", "", text)

    if not text:
        return ""

    return text.zfill(8)


def to_int_safe(value: Any, default: int = 0) -> int:
    """
    安全轉換為整數，無法轉換時回傳 default
    """
    if pd.isna(value):
        return default

    text: str = str(value).strip().replace(",", "")
    text = re.sub(r"[^\d\-]", "", text)

    if text == "":
        return default

    try:
        return int(text)
    except ValueError:
        return default


def to_bool(value: Any) -> bool:
    """
    將設定值轉為布林
    """
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


# =========================
# 稅籍資料介面
# =========================

class TaxProfileService:
    """
    稅籍資料查詢服務介面

    目前先提供 placeholder。
    後續可替換成：
    1. API 查詢
    2. DB 查詢
    3. 本地 CSV 對照
    """

    def get_tax_profile(self, tax_id: str) -> Dict[str, Any]:
        """
        根據統編取得稅籍資料
        """
        # TODO: 後續在這裡串接真實稅籍資料
        # 範例回傳：
        # {
        #     "party_type": "獨資",
        #     "business_type": "獨資",
        #     "tax_status": "營業中"
        # }
        return {}


# =========================
# 規則載入器
# =========================

class RuleLoader:
    """
    載入外部 CSV 規則設定
    """

    @staticmethod
    def load_rules(rule_csv_path: str) -> list[RuleConfig]:
        rules: list[RuleConfig] = []

        with open(rule_csv_path, "r", encoding="utf-8-sig", newline="") as file:
            reader = csv.DictReader(file)
            for row in reader:
                rules.append(
                    RuleConfig(
                        rule_order=int(row["rule_order"]),
                        rule_name=row["rule_name"].strip(),
                        enabled=to_bool(row["enabled"]),
                        source_type=row["source_type"].strip(),
                        field_name=row["field_name"].strip(),
                        operator=row["operator"].strip(),
                        value=row["value"].strip(),
                        output_code=int(row["output_code"]),
                    )
                )

        # 規則依優先序排序
        rules.sort(key=lambda x: x.rule_order)
        return rules


# =========================
# 規則引擎
# =========================

class RuleEvaluator:
    """
    規則判斷器
    """

    def __init__(self, tax_service: TaxProfileService) -> None:
        self.tax_service = tax_service

    def evaluate(self, record: Dict[str, Any], rules: list[RuleConfig]) -> Optional[int]:
        """
        依序套用規則，命中即回傳 output_code
        """
        tax_profile_cache: Optional[Dict[str, Any]] = None

        for rule in rules:
            if not rule.enabled:
                continue

            if rule.source_type == "business":
                actual_value: Any = record.get(rule.field_name)
            elif rule.source_type == "tax":
                if tax_profile_cache is None:
                    tax_profile_cache = self.tax_service.get_tax_profile(record.get("tax_id", ""))
                actual_value = tax_profile_cache.get(rule.field_name)
            else:
                continue

            if self._match(actual_value, rule.operator, rule.value):
                return rule.output_code

        return None

    def _match(self, actual_value: Any, operator: str, expected_value: str) -> bool:
        """
        通用比對函式
        """
        if operator == "eq":
            return str(actual_value).strip() == expected_value

        if operator == "in":
            expected_list: list[str] = [x.strip() for x in expected_value.split("|") if x.strip()]
            return str(actual_value).strip() in expected_list

        if operator == "ge":
            actual_num: int = to_int_safe(actual_value, default=0)
            expected_num: int = to_int_safe(expected_value, default=0)
            return actual_num >= expected_num

        if operator == "gt":
            actual_num = to_int_safe(actual_value, default=0)
            expected_num = to_int_safe(expected_value, default=0)
            return actual_num > expected_num

        if operator == "lt":
            actual_num = to_int_safe(actual_value, default=0)
            expected_num = to_int_safe(expected_value, default=0)
            return actual_num < expected_num

        if operator == "le":
            actual_num = to_int_safe(actual_value, default=0)
            expected_num = to_int_safe(expected_value, default=0)
            return actual_num <= expected_num

        if operator == "contains":
            return expected_value in str(actual_value)

        return False


# =========================
# ETL 主流程
# =========================

class BusinessClassifierETL:
    """
    商工資料分類 ETL
    """

    def __init__(
        self,
        column_mapping: ColumnMapping,
        tax_service: TaxProfileService,
    ) -> None:
        self.column_mapping = column_mapping
        self.rule_evaluator = RuleEvaluator(tax_service)

    def run(
        self,
        input_csv_path: str,
        rule_csv_path: str,
        output_csv_path: str,
    ) -> pd.DataFrame:
        """
        執行 ETL
        """
        df: pd.DataFrame = pd.read_csv(input_csv_path, dtype=str, encoding="utf-8-sig")
        rules: list[RuleConfig] = RuleLoader.load_rules(rule_csv_path)

        # 欄位標準化
        df["tax_id"] = df[self.column_mapping.tax_id].apply(normalize_tax_id)
        df["enterprise_type_var"] = df[self.column_mapping.enterprise_type].fillna("").astype(str).str.strip()
        df["industry_code_var"] = df[self.column_mapping.industry_code].fillna("").astype(str).str.strip()
        df["industry_name_var"] = df[self.column_mapping.industry_name].fillna("").astype(str).str.strip()
        df["capital_total_var"] = df[self.column_mapping.capital_total].apply(to_int_safe)

        # 單筆規則判斷
        result_codes: list[Optional[int]] = []
        for _, row in df.iterrows():
            record: Dict[str, Any] = {
                "tax_id": row["tax_id"],
                "企業型態": row["enterprise_type_var"],
                "行業代號": row["industry_code_var"],
                "行業別": row["industry_name_var"],
                "資本總額": row["capital_total_var"],
            }

            result_code: Optional[int] = self.rule_evaluator.evaluate(record, rules)
            result_codes.append(result_code)

        df["result_code"] = result_codes

        # 輸出結果
        df.to_csv(output_csv_path, index=False, encoding="utf-8-sig")

        print("ETL 執行完成")
        print(f"輸入筆數：{len(df):,}")
        print(f"輸出檔案：{output_csv_path}")

        return df


# =========================
# 主程式入口
# =========================

def main() -> None:
    input_csv_path: str = "business_input.csv"
    rule_csv_path: str = "rule_config.csv"
    output_csv_path: str = "business_output.csv"

    column_mapping = ColumnMapping(
        tax_id="統一編號",
        enterprise_type="企業型態",
        industry_code="行業代號",
        industry_name="行業別",
        capital_total="資本總額",
        query_time="查詢時間",
    )

    tax_service = TaxProfileService()

    etl = BusinessClassifierETL(
        column_mapping=column_mapping,
        tax_service=tax_service,
    )

    etl.run(
        input_csv_path=input_csv_path,
        rule_csv_path=rule_csv_path,
        output_csv_path=output_csv_path,
    )


if __name__ == "__main__":
    main()