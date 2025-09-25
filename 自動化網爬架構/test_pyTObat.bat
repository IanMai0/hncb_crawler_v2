@echo on
REM 進入專案資料夾
cd /d C:\Users\wits\Downloads\HNCB\tests\crawler_hncb\自動化網爬架構

REM 啟動 venv
CALL C:\Users\wits\Downloads\HNCB\tests\crawler_hncb\.crawler\Scripts\activate.bat

REM === 取得日期與時間，格式化為 YYYYMMDD_HHMM ===
for /f "tokens=2 delims==" %%I in ('wmic os get localdatetime /value') do set ldt=%%I
set logdate=%ldt:~0,8%_%ldt:~8,4%

REM 執行爬蟲
python test_pyTObat.py

REM === 執行 Python 腳本並輸出到 log ===
python test_pyTObat.py >> logs\crawler_%logdate%.log 2>&1
pause
