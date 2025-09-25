import os
import uvicorn
import logging
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

# 復用 main.py 內的函式（你也可以把它們抽成 common.py）
from main import download_and_extract_zip, run_etl_from_csv  # 下載與 ETL 主流程  :contentReference[oaicite:5]{index=5}  :contentReference[oaicite:6]{index=6}

app = FastAPI(title="Factory ETL API", version="1.0.0")

logger = logging.getLogger("uvicorn.error")

class RunFromZipRequest(BaseModel):
    zip_url: str = Field(..., description="遠端 ZIP 連結")
    download_dir: str = Field(default="./data")
    make_top100: bool = Field(default=True)

class RunFromCsvRequest(BaseModel):
    csv_path: str = Field(..., description="已存在的 CSV 檔路徑")
    make_top100: bool = Field(default=True)

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/etl/run-from-zip")
def etl_run_from_zip(req: RunFromZipRequest):
    try:
        csv_basename = download_and_extract_zip(req.zip_url, extract_to=req.download_dir)
        csv_path = str(Path(req.download_dir) / csv_basename)
        output_path = run_etl_from_csv(csv_path, make_top100=req.make_top100)
        return {"ok": True, "csv_path": csv_path, "output": output_path}
    except Exception as e:
        logger.exception("run-from-zip failed")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/etl/run-from-csv")
def etl_run_from_csv(req: RunFromCsvRequest):
    try:
        output_path = run_etl_from_csv(req.csv_path, make_top100=req.make_top100)
        return {"ok": True, "csv_path": req.csv_path, "output": output_path}
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception("run-from-csv failed")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    # 直接 python app.py 就能啟動 API
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=False)
