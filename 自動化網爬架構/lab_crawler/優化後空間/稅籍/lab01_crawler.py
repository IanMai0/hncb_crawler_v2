import ssl
import os
import csv
import json
import time
import urllib
import requests
import pymysql
import pandas as pd

from zipfile import ZipFile
from pandas import DataFrame
from datetime import datetime
from time import sleep
from tqdm import trange

# 主要下載稅籍日檔網址：
# url = "https://eip.fia.gov.tw/data/BGMOPEN1.zip"
PATH_DATA = "C:/Users/wits/PycharmProjects/hncb_crawler/自動化網爬架構/lab_crawler/優化後空間/稅籍/data"

# 下載稅籍批次日檔
# 直接將該 Data 寫入 Tmp table (raw_data)
# 根據 raw_data 進行 ETL
# ETL 後的 Data 寫入 Tmp table
# 數據比對來源沒問題後, 寫入 main table


