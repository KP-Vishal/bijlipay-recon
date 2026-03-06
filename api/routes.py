"""
FastAPI REST API — Bijlipay Reconciliation Engine

Endpoints:
  POST /recon/run          — Trigger a reconciliation run
  GET  /recon/status/{date} — Get run status for a date
  GET  /recon/errors/{date} — Get errors for a date
  GET  /recon/payouts/{date} — Get payout summary
  GET  /recon/credit/{date}  — Get credit validation results
  POST /recon/reset/{date}   — Reset a date (destructive)
"""
import os
import logging
from datetime import date, datetime
from typing import Optional

from fastapi import FastAPI, HTTPException, BackgroundTasks, Query
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from db.repository import ReconRepository
from main import run_pipeline

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
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "./output")

def get_repo():
    return ReconRepository(DB_URL)


# ------------------------------------------------------------------ #
#  REQUEST / RESPONSE MODELS                                          #
# ------------------------------------------------------------------ #

class RunRequest(BaseModel):
    recon_date:  str           # YYYY-MM-DD
    input_dir:   str           # path to input files
    output_dir:  Optional[str] = OUTPUT_DIR
    force:       Optional[bool] = False


class RunResponse(BaseModel):
    recon_date: str
    status:     str
    message:    str


# ------------------------------------------------------------------ #
#  ENDPOINTS                                                          #
# ------------------------------------------------------------------ #

@app.get("/")
def root():
    return {
        "service": "Bijlipay Reconciliation Engine",
        "version": "1.0.0",
        "docs": "/docs",
    }


@app.post("/recon/run", response_model=RunResponse)
async def trigger_recon(req: RunRequest, background_tasks: BackgroundTasks):
    """
    Trigger a full reconciliation pipeline run for a given date.
    Runs asynchronously in the background.
    """
    try:
        recon_date = datetime.strptime(req.recon_date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(400, f"Invalid date format: {req.recon_date}. Use YYYY-MM-DD.")

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
        message=f"Reconciliation pipeline queued for {req.recon_date}. Check /recon/status/{req.recon_date} for progress.",
    )


@app.get("/recon/status/{recon_date}")
def get_status(recon_date: str):
    """Get step-by-step pipeline status for a recon date."""
    try:
        d = datetime.strptime(recon_date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(400, f"Invalid date: {recon_date}")

    repo = get_repo()
    from sqlalchemy import text
    with repo.get_session() as s:
        rows = s.execute(
            text("SELECT step, status, rows_processed, started_at, completed_at, error_msg FROM recon_run_log WHERE recon_date=:d ORDER BY started_at"),
            {"d": d}
        ).fetchall()

    if not rows:
        return {"recon_date": recon_date, "status": "NOT_STARTED", "steps": []}

    steps = [
        {
            "step": r[0], "status": r[1], "rows_processed": r[2],
            "started_at": str(r[3]), "completed_at": str(r[4]),
            "error": r[5],
        }
        for r in rows
    ]

    all_complete = all(s["status"] == "COMPLETE" for s in steps)
    any_failed   = any(s["status"] == "FAILED" for s in steps)
    overall = "COMPLETE" if all_complete else ("FAILED" if any_failed else "IN_PROGRESS")

    return {"recon_date": recon_date, "status": overall, "steps": steps}


@app.get("/recon/errors/{recon_date}")
def get_errors(
    recon_date: str,
    error_code: Optional[str] = Query(None, description="Filter by error code"),
    rail: Optional[str] = Query(None, description="Filter by rail: CARD/DQR/SQR"),
):
    """Get all errors for a recon date, optionally filtered."""
    try:
        d = datetime.strptime(recon_date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(400, f"Invalid date: {recon_date}")

    repo = get_repo()
    from sqlalchemy import text
    query = "SELECT * FROM transaction_errors WHERE recon_date=:d"
    params = {"d": d}
    if error_code:
        query += " AND error_code=:ec"
        params["ec"] = error_code
    if rail:
        query += " AND rail=:rail"
        params["rail"] = rail

    with repo.get_session() as s:
        rows = s.execute(text(query), params).fetchall()
        keys = s.execute(text(query), params).keys() if rows else []

    # Get summary
    summary_q = "SELECT error_code, COUNT(*) as cnt FROM transaction_errors WHERE recon_date=:d GROUP BY error_code ORDER BY cnt DESC"
    with repo.get_session() as s:
        summary = s.execute(text(summary_q), {"d": d}).fetchall()

    from models.error import ERROR_CODES
    return {
        "recon_date": recon_date,
        "total_errors": len(rows),
        "summary": [
            {"error_code": r[0], "count": r[1], "description": ERROR_CODES.get(r[0], "Unknown")}
            for r in summary
        ],
    }


@app.get("/recon/payouts/{recon_date}")
def get_payouts(recon_date: str, rail: Optional[str] = Query(None)):
    """Get payout summary for a recon date."""
    try:
        d = datetime.strptime(recon_date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(400, f"Invalid date: {recon_date}")

    repo = get_repo()
    from sqlalchemy import text
    query = "SELECT rail, COUNT(*) as count, SUM(amount) as total FROM payouts WHERE recon_date=:d"
    params = {"d": d}
    if rail:
        query += " AND rail=:rail"
        params["rail"] = rail
    query += " GROUP BY rail"

    with repo.get_session() as s:
        rows = s.execute(text(query), params).fetchall()

    return {
        "recon_date": recon_date,
        "payouts": [
            {"rail": r[0], "count": r[1], "total_amount": float(r[2] or 0)}
            for r in rows
        ],
        "grand_total": sum(float(r[2] or 0) for r in rows),
    }


@app.get("/recon/credit/{recon_date}")
def get_credit_validation(recon_date: str):
    """Get credit validation results for a recon date."""
    try:
        d = datetime.strptime(recon_date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(400, f"Invalid date: {recon_date}")

    repo = get_repo()
    df = repo.get_credit_validation_df(d)
    if df.empty:
        return {"recon_date": recon_date, "validations": []}

    return {
        "recon_date": recon_date,
        "validations": df.to_dict(orient="records"),
    }


@app.post("/recon/reset/{recon_date}")
def reset_recon(recon_date: str):
    """
    DESTRUCTIVE: Reset all data for a recon date.
    Use only when you need to completely re-run from scratch.
    """
    try:
        d = datetime.strptime(recon_date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(400, f"Invalid date: {recon_date}")

    repo = get_repo()
    repo.clear_recon_date(d)
    return {"message": f"All data cleared for {recon_date}. You can now re-run reconciliation."}


@app.get("/health")
def health():
    return {"status": "ok", "service": "bijlipay-recon-engine"}
