"""
FastAPI REST API — Bijlipay Reconciliation Engine
"""

import os
import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, BackgroundTasks, Query
from pydantic import BaseModel

from db.repository import ReconRepository
from main import run_pipeline

logger = logging.getLogger(**name**)

# This is the IMPORTANT line your server.py expects

router = APIRouter()

_db = os.getenv("DATABASE_URL", "postgresql://bijlipay:bijlipay@localhost:5432/recon")
DB_URL = _db.replace("postgres://", "postgresql://", 1) if _db.startswith("postgres://") else _db
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "./output")

def get_repo():
return ReconRepository(DB_URL)

# -----------------------------

# REQUEST MODELS

# -----------------------------

class RunRequest(BaseModel):
recon_date: str
input_dir: str
output_dir: Optional[str] = OUTPUT_DIR
force: Optional[bool] = False

class RunResponse(BaseModel):
recon_date: str
status: str
message: str

# -----------------------------

# ROOT

# -----------------------------

@router.get("/")
def root():
return {
"service": "Bijlipay Reconciliation Engine",
"version": "1.0.0",
"docs": "/docs",
}

# -----------------------------

# RUN RECON

# -----------------------------

@router.post("/recon/run", response_model=RunResponse)
async def trigger_recon(req: RunRequest, background_tasks: BackgroundTasks):

```
try:
    recon_date = datetime.strptime(req.recon_date, "%Y-%m-%d").date()
except ValueError:
    raise HTTPException(400, f"Invalid date format: {req.recon_date}")

background_tasks.add_task(
    run_pipeline,
    db_url=DB_URL,
    recon_date=recon_date,
    input_dir=req.input_dir,
    output_dir=req.output_dir or OUTPUT_DIR,
    force=req.force or False,
)

return RunResponse(
    recon_date=req.recon_date,
    status="QUEUED",
    message=f"Recon started for {req.recon_date}",
)
```

# -----------------------------

# STATUS

# -----------------------------

@router.get("/recon/status/{recon_date}")
def get_status(recon_date: str):

```
try:
    d = datetime.strptime(recon_date, "%Y-%m-%d").date()
except ValueError:
    raise HTTPException(400, "Invalid date")

repo = get_repo()
from sqlalchemy import text

with repo.get_session() as s:
    rows = s.execute(
        text("SELECT step, status FROM recon_run_log WHERE recon_date=:d"),
        {"d": d}
    ).fetchall()

return {
    "recon_date": recon_date,
    "steps": [{"step": r[0], "status": r[1]} for r in rows],
}
```

# -----------------------------

# HEALTH CHECK

# -----------------------------

@router.get("/health")
def health():
return {
"status": "ok",
"service": "bijlipay-recon-engine"
}
