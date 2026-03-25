import requests
from bs4 import BeautifulSoup
import pandas as pd
import io
import time
import os
from urllib3.exceptions import InsecureRequestWarning

# 1. 抑制 SSL 安全警告
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)


class PropertyCrawler:
    def __init__(self):
        self.session = requests.Session()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Origin": "https://ppstrq.nat.gov.tw"
        }
        self.query_url = "https://ppstrq.nat.gov.tw/pps/pubQuery/PropertyQuery/propertyQuery.do"

    def get_token(self):
        """獲取最新的 struts.token"""
        try:
            res = self.session.get(self.query_url, headers=self.headers, verify=False, timeout=10)
            soup = BeautifulSoup(res.text, 'html.parser')
            token_tag = soup.find("input", {"name": "struts.token"})
            return token_tag.get("value") if token_tag else ""
        except:
            return ""

    def query_list_only(self, debtor_no):
        """僅執行 Page 1 查詢並回傳結果表格"""
        token = self.get_token()
        if not token:
            return "Token 獲取失敗", None

        payload = {
            "method": "query",
            "regUnitCode": "5001",
            "certificateAppNoWord": "",  # 預留為空，查詢該統編所有案件
            "queryDebtorNo": debtor_no,
            "struts.token.name": "struts.token",
            "struts.token": token,
            "pagingModel.currentPage": "1",
            "monthCount": "6228052",
            "totalCount": "950967596"
        }

        try:
            self.headers["Referer"] = self.query_url
            res = self.session.post(self.query_url, data=payload, headers=self.headers, verify=False, timeout=15)

            if "查無資料" in res.text:
                return "查無資料", None

            # 解析表格
            dfs = pd.read_html(io.StringIO(res.text))
            for df in dfs:
                if "登記編號" in df.columns:
                    df["查詢來源統編"] = debtor_no  # 標記這筆資料屬於哪個統編
                    return "成功", df

            return "無法解析表格", None
        except Exception as e:
            return f"連線異常: {str(e)}", None


# --- 執行主程式 ---
if __name__ == "__main__":
    csv_input = "company_list.csv"

    if not os.path.exists(csv_input):
        print(f"錯誤：找不到輸入檔 {csv_input}")
        exit()

    # 1. 讀取並限制前 100 筆
    df_raw = pd.read_csv(csv_input, dtype={'統編': str})
    debtor_list = df_raw['統編'].tolist()[:100]  # <--- 控制只取前 100 筆
    print(f"預計處理筆數: {len(debtor_list)}")

    crawler = PropertyCrawler()
    all_table_data = []  # 存儲抓到的表格
    execution_status = []  # 存儲狀態記錄

    # 2. 開始批次執行
    for i, debtor_no in enumerate(debtor_list, 1):
        status, result_df = crawler.query_list_only(debtor_no)

        # 記錄狀態
        execution_status.append({
            "序號": i,
            "統編": debtor_no,
            "查詢狀態": status,
            "案件數量": len(result_df) if result_df is not None else 0
        })

        # 如果有資料，加入總表
        if result_df is not None:
            all_table_data.append(result_df)
            print(f"[{i}/100] 統編 {debtor_no}: 成功抓取 {len(result_df)} 筆")
        else:
            print(f"[{i}/100] 統編 {debtor_no}: {status}")

        time.sleep(1.2)  # 延遲避免被封鎖

    # 3. 產出結果
    # --- 狀態報表 ---
    df_status = pd.DataFrame(execution_status)
    df_status.to_csv("查詢執行狀態表.csv", index=False, encoding='utf-8-sig')

    # --- 數據總表 ---
    if all_table_data:
        df_final = pd.concat(all_table_data, ignore_index=True)
        df_final.to_csv("Page1_查詢結果彙整.csv", index=False, encoding='utf-8-sig')
        print(f"\n數據彙整完成，共計 {len(df_final)} 筆案件。")

    print("\n--- 任務結束 ---")
    print(df_status)  # 在螢幕列印簡單的狀態清單