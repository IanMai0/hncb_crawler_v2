import requests
from bs4 import BeautifulSoup
import pandas as pd
import io
from urllib3.exceptions import InsecureRequestWarning

# 1. 抑制 SSL 安全警告
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)  # 因應目標網站跟不上最新 SSL


class PropertyCrawler:
    def __init__(self):
        self.session = requests.Session()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Origin": "https://ppstrq.nat.gov.tw"
        }
        # 通用的查詢參數範本
        self.base_payload = {
            "method": "query",
            "regUnitCode": "5001",
            "certificateAppNoWord": "60-111-424-1(27904)",  # 登記編號
            "currentPage": "1",
            "totalPage": "1",
            "debtorType": "1",
            "creditorType": "1",
            "scrollTop": "0",
            "debtorTypeRadio": "1",
            "queryDebtorName": "",
            "queryDebtorNo": "57108207",  # 南亞工業股份有限公司
            "creditorTypeRadio": "1",
            "queryCreditorName": "",
            "queryCreditorNo": "",
            "struts.token.name": "struts.token",
            "pagingModel.currentPage": "1",
            "monthCount": "6228052",  # 本查詢項目本月共累計查詢6,228,052筆
            "totalCount": "950967596"  # 自中華民國103年3月26日起共累計查詢950,967,596筆
        }
        self.query_url = "https://ppstrq.nat.gov.tw/pps/pubQuery/PropertyQuery/propertyQuery.do"  # 第一頁查詢
        self.detail_url = "https://ppstrq.nat.gov.tw/pps/pubQuery/PropertyQuery/propertyDetail.do"  # 第二頁查詢


    def get_token_page1(self, url):
        """從目標頁面獲取最新的 struts.token"""
        try:
            res = self.session.get(url, headers=self.headers, verify=False, timeout=10)
            soup = BeautifulSoup(res.text, 'html.parser')
            token_tag = soup.find("input", {"name": "struts.token"})
            return token_tag.get("value") if token_tag else ""
        except:
            return ""

    def get_token_page2(self):
        try:
            res = self.session.get(self.query_url, headers=self.headers, verify=False, timeout=10)
            soup = BeautifulSoup(res.text, 'html.parser')
            token_tag = soup.find("input", {"name": "struts.token"})
            return token_tag.get("value") if token_tag else ""
        except:
            return ""

    def query_page_1(self):
        """執行 propertyQuery.do 的查詢"""
        url = "https://ppstrq.nat.gov.tw/pps/pubQuery/PropertyQuery/propertyQuery.do"
        print(f"\n--- 正在執行 Page 1 查詢 ({url}) ---")

        token = self.get_token_page1(url)
        payload = self.base_payload.copy()
        payload["struts.token"] = token

        self.headers["Referer"] = url
        res = self.session.post(url, data=payload, headers=self.headers, verify=False)
        return self.parse_table(res.text, "Page 1")

    def query_page_2(self):
        """Page 2: 抓取詳細資料 (DIV 結構解析)"""
        print(f"\n>>> 執行 Page 2 (詳細頁) <<<")
        token = self.get_token_page2()
        payload = {
            "method": "query",
            "regUnitCode": "5001",
            "certificateAppNoWord": "60-111-424-1(27904)",
            "queryDebtorNo": "57108207",
            "struts.token.name": "struts.token",
            "struts.token": token,
            "debtorType": "1",
            "creditorType": "1"
        }
        self.headers["Referer"] = self.query_url
        res = self.session.post(self.detail_url, data=payload, headers=self.headers, verify=False)

        return self.parse_detail_divs(res.text)

    def parse_table(self, html, label):
        """針對 page1 的解決方案, 解析 HTML 表格並轉換為 DataFrame"""
        if "查無資料" in html:
            print(f"[{label}] 結果：查無資料。")
            return None

        try:
            # 優先用 Pandas，失敗則手動解析
            dfs = pd.read_html(io.StringIO(html))
            valid_dfs = [d for d in dfs if len(d.columns) > 1 and len(d) > 0]
            if valid_dfs:
                print(f"[{label}] 成功解析出表格！")
                return valid_dfs[0]
        except:
            pass

        # 手動 BeautifulSoup 提取 (容錯處理)
        soup = BeautifulSoup(html, 'html.parser')
        for table in soup.find_all('table'):
            rows = table.find_all('tr')
            data = [[c.get_text(strip=True) for c in r.find_all(['th', 'td'])] for r in rows]
            if len(data) > 1 and len(data[0]) > 1:
                return pd.DataFrame(data)

        print(f"[{label}] 無法解析出有效資料表。")
        return None

    def parse_detail_divs(self, html):
        """針對 Page 2 的 pubDetailLabel 和 pubDetailValue 進行解析"""
        soup = BeautifulSoup(html, 'html.parser')
        details = {}

        # 尋找所有的標籤與對應的值
        labels = soup.find_all(class_='pubDetailLabel')
        values = soup.find_all(class_='pubDetailValue')

        for label, value in zip(labels, values):
            key = label.get_text(strip=True).replace("：", "")
            val = value.get_text(strip=True)
            if key:
                details[key] = val

        if not details:
            return None

        # 轉成 DataFrame 方便閱讀 (Key 為 Index, Value 為內容)
        df = pd.DataFrame(list(details.items()), columns=['欄位名稱', '內容值'])
        return df


# --- 執行 ---
if __name__ == "__main__":
    crawler = PropertyCrawler()

    # 執行第一個頁面的查詢
    df1 = crawler.query_page_1()
    if df1 is not None:
        print("\n--- [Page 1 查詢結果] ---")
        print(df1.to_string(index=False))

    print("\n" + "=" * 60)

    # 執行第二個頁面的查詢
    df2 = crawler.query_page_2()
    if df2 is not None:
        print("\n--- [Page 2 詳細內容 (由 DIV 提取)] ---")
        print(df2.to_string(index=False))
    else:
        print("Page 2 數據提取失敗。")


