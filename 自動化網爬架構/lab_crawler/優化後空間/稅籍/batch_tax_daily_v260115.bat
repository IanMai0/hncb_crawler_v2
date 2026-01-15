@echo off
cd /d C:\your_project\gcis_pipeline
set MYSQL_HOST=YOUR_HOST
set MYSQL_USER=YOUR_USER
set MYSQL_PASSWORD=YOUR_PASSWORD
set MYSQL_DB=crawlerdb
set MYSQL_PORT=3306
set LOG_DIR=C:\your_project\gcis_pipeline\logs
set WORK_DIR=C:\your_project\gcis_pipeline\work

python run_daily_job.py
