# -*- coding: utf-8 -*-
"""
動產擔保交易平台 crawler - 批次版（多案件 Page2 修正版）
功能：
1. 讀取 company_list_*.csv
2. 依統編批次查詢
3. Page 1 / Page 2 各自彙整到單一 CSV
4. Page 1 避免抓到 footer table
5. Page 2 改為固定欄位 schema
6. 同一統編若有多筆案件，逐筆抓取所有 Page 2 detail
7. 若查無資料，寫入「查無資料」
8. 寫入 logs
9. 寫入 progress.csv，方便斷跑續跑
"""

from __future__ import annotations

import io
import logging
import re
import time
from dataclasses import dataclass
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib3.exceptions import InsecureRequestWarning


requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)


@dataclass
class DetailRequest:
    reg_unit_code: str
    certificate_no: str
    page1_case_index: int
    page1_case_no: str


class PropertyCrawler:
    QUERY_URL = "https://ppstrq.nat.gov.tw/pps/pubQuery/PropertyQuery/propertyQuery.do"
    DETAIL_URL = "https://ppstrq.nat.gov.tw/pps/pubQuery/PropertyQuery/propertyDetail.do"

    PAGE1_FIXED_COLUMNS = [
        "query_debtor_no",
        "項次",
        "登記機關",
        "案件類別",
        "債務人(買受人、受託人)名稱",
        "抵押權人(出賣人、信託人)名稱",
        "登記編號",
        "案件狀態",
        "各動產擔保機關聯絡電話",
        "page",
        "result",
    ]

    PAGE2_FIXED_COLUMNS = [
        "query_debtor_no",
        "page1_case_index",
        "page1_case_no",
        "登記機關",
        "案件類別",
        "登記編號",
        "登記核准日期",
        "變更文號",
        "變更核准日期",
        "註銷文號",
        "註銷日期",
        "名稱",
        "統編",
        "代理人名稱",
        "代理人統編",
        "契約啟始日期",
        "契約終止日期",
        "標的物所有人名稱",
        "擔保債權金額",
        "標的物所有人統編",
        "動產明細項數",
        "標的物所在地",
        "是否最高限額",
        "是否為浮動擔保",
        "標的物種類",
        "page",
        "result",
    ]

    PAGE1_RESULT_HEADERS = [
        "項次",
        "登記機關",
        "案件類別",
        "債務人(買受人、受託人)名稱",
        "抵押權人(出賣人、信託人)名稱",
        "登記編號",
        "案件狀態",
        "各動產擔保機關聯絡電話",
    ]

    def __init__(
        self,
        output_dir: str = "output",
        log_dir: str = "logs",
        timeout: int = 20,
        sleep_seconds: int = 3,
    ) -> None:
        self.output_dir = Path(output_dir)
        self.log_dir = Path(log_dir)
        self.timeout = timeout
        self.sleep_seconds = sleep_seconds

        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)

        self.page1_result_path = self.output_dir / "page1_results.csv"
        self.page2_result_path = self.output_dir / "page2_results.csv"
        self.progress_path = self.output_dir / "progress.csv"

        self.logger = self._setup_logger()

        self.session = requests.Session()
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/146.0.0.0 Safari/537.36"
            ),
            "Origin": "https://ppstrq.nat.gov.tw",
            "Referer": self.QUERY_URL,
        }

    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger("PropertyCrawler")
        logger.setLevel(logging.INFO)

        if logger.handlers:
            return logger

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )

        file_handler = RotatingFileHandler(
            self.log_dir / "property_crawler.log",
            maxBytes=2 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        return logger

    def _safe_request(self, method: str, url: str, **kwargs) -> requests.Response:
        try:
            response = self.session.request(
                method=method,
                url=url,
                headers=self.headers,
                verify=False,
                timeout=self.timeout,
                **kwargs,
            )
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            self.logger.exception(
                "HTTP 請求失敗 | method=%s | url=%s | error=%s",
                method,
                url,
                exc,
            )
            raise

    def get_struts_token(self) -> str:
        response = self._safe_request("GET", self.QUERY_URL)

        soup = BeautifulSoup(response.text, "html.parser")
        token_tag = soup.find("input", {"name": "struts.token"})
        token = token_tag.get("value", "").strip() if token_tag else ""

        if not token:
            self.logger.warning("未取得 struts.token，後續查詢可能失敗")

        return token

    def build_query_payload(self, company_no: str, token: str) -> dict[str, str]:
        return {
            "method": "query",
            "regUnitCode": "",
            "certificateAppNoWord": "",
            "currentPage": "1",
            "totalPage": "1",
            "debtorType": "1",
            "creditorType": "1",
            "scrollTop": "0",
            "debtorTypeRadio": "1",
            "queryDebtorName": "",
            "queryDebtorNo": company_no,
            "creditorTypeRadio": "1",
            "queryCreditorName": "",
            "queryCreditorNo": "",
            "struts.token.name": "struts.token",
            "struts.token": token,
            "pagingModel.currentPage": "1",
            "monthCount": "",
            "totalCount": "",
        }

    def query_page_1(self, company_no: str) -> str:
        token = self.get_struts_token()
        payload = self.build_query_payload(company_no=company_no, token=token)
        response = self._safe_request("POST", self.QUERY_URL, data=payload)
        return response.text

    def query_page_2(self, company_no: str, reg_unit_code: str, certificate_no: str) -> str:
        token = self.get_struts_token()

        payload = {
            "method": "query",
            "regUnitCode": reg_unit_code,
            "certificateAppNoWord": certificate_no,
            "queryDebtorNo": company_no,
            "struts.token.name": "struts.token",
            "struts.token": token,
            "debtorType": "1",
            "creditorType": "1",
        }

        response = self._safe_request("POST", self.DETAIL_URL, data=payload)
        return response.text

    def build_page1_no_data_df(self, company_no: str) -> pd.DataFrame:
        return pd.DataFrame([{
            "query_debtor_no": company_no,
            "項次": "",
            "登記機關": "",
            "案件類別": "",
            "債務人(買受人、受託人)名稱": "",
            "抵押權人(出賣人、信託人)名稱": "",
            "登記編號": "",
            "案件狀態": "",
            "各動產擔保機關聯絡電話": "",
            "page": "page1",
            "result": "查無資料",
        }], columns=self.PAGE1_FIXED_COLUMNS)

    def build_page2_no_data_df(
        self,
        company_no: str,
        page1_case_index: str = "",
        page1_case_no: str = "",
    ) -> pd.DataFrame:
        return pd.DataFrame([{
            "query_debtor_no": company_no,
            "page1_case_index": page1_case_index,
            "page1_case_no": page1_case_no,
            "登記機關": "",
            "案件類別": "",
            "登記編號": "",
            "登記核准日期": "",
            "變更文號": "",
            "變更核准日期": "",
            "註銷文號": "",
            "註銷日期": "",
            "名稱": "",
            "統編": "",
            "代理人名稱": "",
            "代理人統編": "",
            "契約啟始日期": "",
            "契約終止日期": "",
            "標的物所有人名稱": "",
            "擔保債權金額": "",
            "標的物所有人統編": "",
            "動產明細項數": "",
            "標的物所在地": "",
            "是否最高限額": "",
            "是否為浮動擔保": "",
            "標的物種類": "",
            "page": "page2",
            "result": "查無資料",
        }], columns=self.PAGE2_FIXED_COLUMNS)

    def normalize_page1_df(self, df: pd.DataFrame, company_no: str) -> pd.DataFrame:
        df = df.copy()
        df.columns = [str(col).strip() for col in df.columns]

        for col in self.PAGE1_RESULT_HEADERS:
            if col not in df.columns:
                df[col] = ""

        df = df[self.PAGE1_RESULT_HEADERS].copy()
        df.insert(0, "query_debtor_no", company_no)
        df["page"] = ""
        df["result"] = ""

        return df[self.PAGE1_FIXED_COLUMNS]

    def is_valid_page1_result_df(self, df: pd.DataFrame) -> bool:
        df = df.copy()
        df.columns = [str(col).strip() for col in df.columns]

        matched_headers = sum(1 for col in self.PAGE1_RESULT_HEADERS if col in df.columns)
        if matched_headers < 6:
            return False

        for _, row in df.iterrows():
            case_no = str(row.get("登記編號", "")).strip()
            debtor_name = str(row.get("債務人(買受人、受託人)名稱", "")).strip()
            case_type = str(row.get("案件類別", "")).strip()
            if case_no or debtor_name or case_type:
                return True

        return False

    def parse_page_1_table(self, html: str, company_no: str) -> pd.DataFrame:
        if "查無資料" in html:
            return self.build_page1_no_data_df(company_no)

        candidate_dfs: list[pd.DataFrame] = []

        try:
            dataframes = pd.read_html(io.StringIO(html))
            for df in dataframes:
                if isinstance(df, pd.DataFrame) and not df.empty:
                    candidate_dfs.append(df)
        except Exception as exc:
            self.logger.warning("Page 1 read_html 解析異常 | company_no=%s | error=%s", company_no, exc)

        for df in candidate_dfs:
            if self.is_valid_page1_result_df(df):
                return self.normalize_page1_df(df, company_no)

        soup = BeautifulSoup(html, "html.parser")
        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            parsed_rows = []

            for row in rows:
                cells = row.find_all(["th", "td"])
                values = [cell.get_text(" ", strip=True) for cell in cells]
                if values:
                    parsed_rows.append(values)

            if len(parsed_rows) < 2:
                continue

            max_len = max(len(r) for r in parsed_rows)
            normalized_rows = [r + [""] * (max_len - len(r)) for r in parsed_rows]
            header = normalized_rows[0]
            body = normalized_rows[1:]

            df = pd.DataFrame(body, columns=header)

            if self.is_valid_page1_result_df(df):
                return self.normalize_page1_df(df, company_no)

        return self.build_page1_no_data_df(company_no)

    def extract_all_detail_params(self, html: str, page1_df: pd.DataFrame) -> list[DetailRequest]:
        """
        從 Page 1 HTML 抽出所有案件 detail 所需參數。
        若 regex 抓不到完整明細參數，至少會依 Page1 的登記編號回填 page1_case_no，
        方便後續 debug。
        """
        requests_found: list[DetailRequest] = []

        # 先整理 page1 的案件編號順序
        case_nos = []
        if "登記編號" in page1_df.columns:
            for idx, value in enumerate(page1_df["登記編號"].tolist(), start=1):
                case_no = str(value).strip()
                if case_no:
                    case_nos.append((idx, case_no))

        # 常見 JS / URL pattern
        patterns = [
            r"propertyDetail\.do[^\"']*?[?&]regUnitCode=([^&\"']+)[^\"']*?[?&]certificateAppNoWord=([^&\"']+)",
            r"detail\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\)",
            r"detail\(\s*\"([^\"]+)\"\s*,\s*\"([^\"]+)\"\s*\)",
            r"doQueryDetail\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\)",
            r"doQueryDetail\(\s*\"([^\"]+)\"\s*,\s*\"([^\"]+)\"\s*\)",
        ]

        pairs: list[tuple[str, str]] = []
        for pattern in patterns:
            matches = re.findall(pattern, html, flags=re.IGNORECASE)
            for g1, g2 in matches:
                g1 = g1.strip()
                g2 = g2.strip()

                if "(" in g1 or "-" in g1:
                    certificate_no, reg_unit_code = g1, g2
                elif "(" in g2 or "-" in g2:
                    reg_unit_code, certificate_no = g1, g2
                else:
                    reg_unit_code, certificate_no = g1, g2

                if reg_unit_code and certificate_no:
                    pairs.append((reg_unit_code, certificate_no))

        # 去重保序
        seen = set()
        unique_pairs = []
        for pair in pairs:
            if pair not in seen:
                seen.add(pair)
                unique_pairs.append(pair)

        # 依 page1 順序組裝
        for i, (reg_unit_code, certificate_no) in enumerate(unique_pairs, start=1):
            page1_case_no = ""
            if i <= len(case_nos):
                page1_case_no = case_nos[i - 1][1]

            requests_found.append(
                DetailRequest(
                    reg_unit_code=reg_unit_code,
                    certificate_no=certificate_no,
                    page1_case_index=i,
                    page1_case_no=page1_case_no,
                )
            )

        # 如果 regex 完全抓不到，但 page1 有登記編號，至少記 log
        if not requests_found and case_nos:
            self.logger.warning("Page 1 有案件，但未抽到任何 Page 2 參數，需檢查 HTML link pattern")

        return requests_found

    def parse_page_2_details(
        self,
        html: str,
        company_no: str,
        page1_case_index: int,
        page1_case_no: str,
    ) -> pd.DataFrame:
        if "查無資料" in html:
            return self.build_page2_no_data_df(
                company_no=company_no,
                page1_case_index=str(page1_case_index),
                page1_case_no=page1_case_no,
            )

        soup = BeautifulSoup(html, "html.parser")
        labels = soup.find_all(class_="pubDetailLabel")
        values = soup.find_all(class_="pubDetailValue")

        detail_map: dict[str, str] = {}
        for label, value in zip(labels, values):
            key = label.get_text(" ", strip=True).replace("：", "").strip()
            val = value.get_text(" ", strip=True)
            if key:
                detail_map[key] = val

        if not detail_map:
            return self.build_page2_no_data_df(
                company_no=company_no,
                page1_case_index=str(page1_case_index),
                page1_case_no=page1_case_no,
            )

        row = {
            "query_debtor_no": company_no,
            "page1_case_index": str(page1_case_index),
            "page1_case_no": page1_case_no,
            "登記機關": detail_map.get("登記機關", ""),
            "案件類別": detail_map.get("案件類別", ""),
            "登記編號": detail_map.get("登記編號", ""),
            "登記核准日期": detail_map.get("登記核准日期", ""),
            "變更文號": detail_map.get("變更文號", ""),
            "變更核准日期": detail_map.get("變更核准日期", ""),
            "註銷文號": detail_map.get("註銷文號", ""),
            "註銷日期": detail_map.get("註銷日期", ""),
            "名稱": detail_map.get("名稱", ""),
            "統編": detail_map.get("統編", ""),
            "代理人名稱": detail_map.get("代理人名稱", ""),
            "代理人統編": detail_map.get("代理人統編", ""),
            "契約啟始日期": detail_map.get("契約啟始日期", ""),
            "契約終止日期": detail_map.get("契約終止日期", ""),
            "標的物所有人名稱": detail_map.get("標的物所有人名稱", ""),
            "擔保債權金額": detail_map.get("擔保債權金額", ""),
            "標的物所有人統編": detail_map.get("標的物所有人統編", ""),
            "動產明細項數": detail_map.get("動產明細項數", ""),
            "標的物所在地": detail_map.get("標的物所在地", ""),
            "是否最高限額": detail_map.get("是否最高限額", ""),
            "是否為浮動擔保": detail_map.get("是否為浮動擔保", ""),
            "標的物種類": detail_map.get("標的物種類", ""),
            "page": "",
            "result": "",
        }

        return pd.DataFrame([row], columns=self.PAGE2_FIXED_COLUMNS)

    def append_result_csv(self, df: pd.DataFrame, file_path: Path, fixed_columns: list[str]) -> None:
        if df is None or df.empty:
            return

        df = df.copy()
        for col in fixed_columns:
            if col not in df.columns:
                df[col] = ""
        df = df[fixed_columns]

        if file_path.exists():
            df.to_csv(
                file_path,
                mode="a",
                header=False,
                index=False,
                encoding="utf-8-sig",
            )
        else:
            df.to_csv(
                file_path,
                index=False,
                encoding="utf-8-sig",
            )

    def append_progress_record(
        self,
        company_no: str,
        seq_no: int,
        total_count: int,
        status: str,
        message: str = "",
    ) -> None:
        row = pd.DataFrame([{
            "seq_no": seq_no,
            "total_count": total_count,
            "company_no": company_no,
            "status": status,
            "message": message,
        }])

        file_exists = self.progress_path.exists()
        row.to_csv(
            self.progress_path,
            mode="a",
            header=not file_exists,
            index=False,
            encoding="utf-8-sig",
        )

    def load_completed_company_set(self) -> set[str]:
        if not self.progress_path.exists():
            return set()

        try:
            df = pd.read_csv(self.progress_path, dtype=str, encoding="utf-8-sig")
            if df.empty or "company_no" not in df.columns or "status" not in df.columns:
                return set()

            completed_status = {"success", "no_data"}
            completed_df = df[df["status"].isin(completed_status)]

            return set(
                completed_df["company_no"]
                .dropna()
                .astype(str)
                .str.strip()
            )
        except Exception as exc:
            self.logger.exception("讀取 progress.csv 失敗 | error=%s", exc)
            return set()

    def run_one(self, company_no: str) -> str:
        self.logger.info("========== 開始查詢 | company_no=%s ==========", company_no)

        page1_html = self.query_page_1(company_no)
        page1_df = self.parse_page_1_table(page1_html, company_no)
        self.append_result_csv(page1_df, self.page1_result_path, self.PAGE1_FIXED_COLUMNS)

        # Page1 無資料
        if "result" in page1_df.columns and (page1_df["result"] == "查無資料").all():
            page2_df = self.build_page2_no_data_df(company_no)
            self.append_result_csv(page2_df, self.page2_result_path, self.PAGE2_FIXED_COLUMNS)
            return "no_data"

        # 抽所有 detail 參數
        detail_requests = self.extract_all_detail_params(page1_html, page1_df)

        # 抽不到 detail 參數：至少補一筆查無資料
        if not detail_requests:
            self.logger.warning("統編有 Page1 結果，但 Page2 參數未抽出 | company_no=%s", company_no)
            page2_df = self.build_page2_no_data_df(company_no)
            self.append_result_csv(page2_df, self.page2_result_path, self.PAGE2_FIXED_COLUMNS)
            return "success"

        # 逐案件查 Page2
        for detail_req in detail_requests:
            self.logger.info(
                "開始抓取 Page2 明細 | company_no=%s | case_index=%s | case_no=%s",
                company_no,
                detail_req.page1_case_index,
                detail_req.page1_case_no,
            )

            page2_html = self.query_page_2(
                company_no=company_no,
                reg_unit_code=detail_req.reg_unit_code,
                certificate_no=detail_req.certificate_no,
            )

            page2_df = self.parse_page_2_details(
                html=page2_html,
                company_no=company_no,
                page1_case_index=detail_req.page1_case_index,
                page1_case_no=detail_req.page1_case_no,
            )

            self.append_result_csv(page2_df, self.page2_result_path, self.PAGE2_FIXED_COLUMNS)

            # 同一統編下多案件之間也稍微休息
            time.sleep(1)

        self.logger.info("========== 查詢完成 | company_no=%s ==========", company_no)
        return "success"


def load_company_list(csv_path: str) -> list[str]:
    df = pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig")
    df.columns = [str(col).strip() for col in df.columns]

    if "統編" not in df.columns:
        raise ValueError(f"CSV 缺少必要欄位：統編 | columns={df.columns.tolist()}")

    company_series = (
        df["統編"]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
    )

    company_list = [x for x in company_series if x]

    seen = set()
    unique_list = []
    for company_no in company_list:
        if company_no not in seen:
            seen.add(company_no)
            unique_list.append(company_no)

    return unique_list


def main() -> None:
    company_list_csv = "company_list.csv"

    crawler = PropertyCrawler(
        output_dir="output",
        log_dir="logs",
        timeout=20,
        sleep_seconds=3,
    )

    company_list = load_company_list(company_list_csv)
    total_count = len(company_list)

    crawler.logger.info("載入公司清單完成 | total_count=%s | file=%s", total_count, company_list_csv)

    completed_company_set = crawler.load_completed_company_set()
    crawler.logger.info("已完成筆數=%s", len(completed_company_set))

    for idx, company_no in enumerate(company_list, start=1):
        if company_no in completed_company_set:
            crawler.logger.info("[%s/%s] 略過已完成統編 | company_no=%s", idx, total_count, company_no)
            continue

        crawler.logger.info("[%s/%s] 開始處理 | company_no=%s", idx, total_count, company_no)

        try:
            final_status = crawler.run_one(company_no)

            crawler.append_progress_record(
                company_no=company_no,
                seq_no=idx,
                total_count=total_count,
                status=final_status,
                message="",
            )

            crawler.logger.info(
                "[%s/%s] 處理完成 | company_no=%s | status=%s",
                idx,
                total_count,
                company_no,
                final_status,
            )

        except Exception as exc:
            crawler.logger.exception(
                "[%s/%s] 處理失敗 | company_no=%s | error=%s",
                idx,
                total_count,
                company_no,
                exc,
            )

            crawler.append_progress_record(
                company_no=company_no,
                seq_no=idx,
                total_count=total_count,
                status="failed",
                message=str(exc),
            )

        if idx < total_count:
            crawler.logger.info("休息 %s 秒後處理下一筆", crawler.sleep_seconds)
            time.sleep(crawler.sleep_seconds)


if __name__ == "__main__":
    main()