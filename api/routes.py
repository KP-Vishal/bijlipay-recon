"""
FastAPI REST API — Bijlipay Reconciliation Engine
"""
import os
import logging
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException, BackgroundTasks, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Bijlipay Reconciliation Engine",
    description="Financial reconciliation pipeline for CARD, DQR, and SQR transactions",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_db = os.getenv("DATABASE_URL", "postgresql://bijlipay:bijlipay@localhost:5432/recon")
DB_URL = _db.replace("postgres://", "postgresql://", 1) if _db.startswith("postgres://") else _db
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/app/output")

def get_repo():
    from db.repository import ReconRepository
    return ReconRepository(DB_URL)

class RunRequest(BaseModel):
    recon_date: str
    input_dir: str
    output_dir: Optional[str] = None
    force: Optional[bool] = False

class RunResponse(BaseModel):
    recon_date: str
    status: str
    message: str

@app.get("/")
def root():
    return {"service": "Bijlipay Reconciliation Engine", "version": "1.0.0", "docs": "/docs"}

@app.get("/health")
def health():
    return {"status": "ok", "service": "bijlipay-recon-engine"}

@app.post("/recon/run", response_model=RunResponse)
async def trigger_recon(req: RunRequest, background_tasks: BackgroundTasks):
    try:
        recon_date = datetime.strptime(req.recon_date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(400, f"Invalid date format: {req.recon_date}. Use YYYY-MM-DD.")
    return RunResponse(
        recon_date=req.recon_date,
        status="QUEUED",
        message=f"Pipeline queued for {req.recon_date}.",
    )

@app.get("/recon/status/{recon_date}")
def get_status(recon_date: str):
    try:
        d = datetime.strptime(recon_date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(400, f"Invalid date: {recon_date}")
    try:
        from sqlalchemy import text
        repo = get_repo()
        with repo.get_session() as s:
            rows = s.execute(
                text("SELECT step, status, rows_processed, started_at, completed_at, error_msg FROM recon_run_log WHERE recon_date=:d ORDER BY started_at"),
                {"d": d}
            ).fetchall()
    except Exception as e:
        return {"recon_date": recon_date, "status": "DB_ERROR", "error": str(e)}
    if not rows:
        return {"recon_date": recon_date, "status": "NOT_STARTED", "steps": []}
    steps = [{"step": r[0], "status": r[1], "rows_processed": r[2],
               "started_at": str(r[3]), "completed_at": str(r[4]), "error": r[5]} for r in rows]
    overall = "COMPLETE" if all(s["status"] == "COMPLETE" for s in steps) else \
              ("FAILED" if any(s["status"] == "FAILED" for s in steps) else "IN_PROGRESS")
    return {"recon_date": recon_date, "status": overall, "steps": steps}

@app.get("/recon/errors/{recon_date}")
def get_errors(recon_date: str, error_code: Optional[str] = Query(None), rail: Optional[str] = Query(None)):
    try:
        d = datetime.strptime(recon_date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(400, f"Invalid date: {recon_date}")
    try:
        from sqlalchemy import text
        repo = get_repo()
        with repo.get_session() as s:
            summary = s.execute(
                text("SELECT error_code, COUNT(*) as cnt FROM transaction_errors WHERE recon_date=:d GROUP BY error_code ORDER BY cnt DESC"),
                {"d": d}
            ).fetchall()
    except Exception as e:
        return {"recon_date": recon_date, "error": str(e)}
    return {"recon_date": recon_date, "summary": [{"error_code": r[0], "count": r[1]} for r in summary]}

@app.get("/recon/payouts/{recon_date}")
def get_payouts(recon_date: str, rail: Optional[str] = Query(None)):
    try:
        d = datetime.strptime(recon_date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(400, f"Invalid date: {recon_date}")
    try:
        from sqlalchemy import text
        repo = get_repo()
        q = "SELECT rail, COUNT(*) as count, SUM(amount) as total FROM payouts WHERE recon_date=:d"
        p = {"d": d}
        if rail:
            q += " AND rail=:rail"; p["rail"] = rail
        q += " GROUP BY rail"
        with repo.get_session() as s:
            rows = s.execute(text(q), p).fetchall()
    except Exception as e:
        return {"recon_date": recon_date, "error": str(e)}
    return {"recon_date": recon_date,
            "payouts": [{"rail": r[0], "count": r[1], "total_amount": float(r[2] or 0)} for r in rows],
            "grand_total": sum(float(r[2] or 0) for r in rows)}

@app.post("/recon/reset/{recon_date}")
def reset_recon(recon_date: str):
    try:
        d = datetime.strptime(recon_date, "%Y-%m-%d").date()
        get_repo().clear_recon_date(d)
    except Exception as e:
        raise HTTPException(500, str(e))
    return {"message": f"All data cleared for {recon_date}."}
