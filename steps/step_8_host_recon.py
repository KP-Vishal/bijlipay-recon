"""
STEP 8 — Credit Validation (CARD)
Inputs: Host base, Axis MPR, Bank statement
Error 16: Base missing in MPR
Error 17: MPR missing in base → add transaction
Credit validation: SUM(success) vs bank statement
"""
import logging
from datetime import date
from uuid import uuid4

import pandas as pd

from db.repository import ReconRepository
from ingestion.file_loader import load_axis_mpr, load_bank_statement
from ingestion.schema_normalizer import normalize_axis_mpr, normalize_bank_statement

logger = logging.getLogger(__name__)
STEP_NAME = "STEP_8_CREDIT_VALIDATION_CARD"


def run(
    repo: ReconRepository,
    recon_date: date,
    axis_mpr_path: str,
    bank_statement_path: str,
    force: bool = False,
) -> dict:
    if not force and repo.step_already_complete(recon_date, STEP_NAME):
        logger.info(f"{STEP_NAME} already complete for {recon_date}, skipping.")
        return {}

    repo.mark_step_start(recon_date, STEP_NAME)
    logger.info(f"[{STEP_NAME}] Starting for {recon_date}")

    try:
        host_df = repo.get_host_transactions_df(recon_date)

        raw_mpr  = load_axis_mpr(axis_mpr_path)
        raw_stmt = load_bank_statement(bank_statement_path)

        mpr_df  = normalize_axis_mpr(raw_mpr, recon_date) if not raw_mpr.empty else pd.DataFrame()
        stmt_df = normalize_bank_statement(raw_stmt, recon_date) if not raw_stmt.empty else pd.DataFrame()

        host_keys = _build_keys(host_df, "rrn") if not host_df.empty else set()
        mpr_keys  = _build_keys(mpr_df, "rrn") if not mpr_df.empty else set()

        errors = []
        new_txns = []

        # Error 16: Host base missing in MPR
        if not host_df.empty:
            missing_in_mpr = host_df[~host_df["rrn"].astype(str).isin(mpr_keys)]
            for _, row in missing_in_mpr.iterrows():
                errors.append({
                    "id": str(uuid4()), "recon_date": recon_date,
                    "transaction_id": None, "rrn": _s(row.get("rrn")),
                    "tid": _s(row.get("tid")), "amount": row.get("amount"), "rail": "CARD",
                    "error_code": "16", "error_message": "Base missing in MPR",
                    "step": STEP_NAME, "source_file": "HOST_BASE",
                })

        # Error 17: MPR missing in host base → add transaction
        if not mpr_df.empty:
            missing_in_base = mpr_df[~mpr_df["rrn"].astype(str).isin(host_keys)]
            for _, row in missing_in_base.iterrows():
                txn_id = str(uuid4())
                new_txns.append({
                    "id": txn_id, "recon_date": recon_date,
                    "rrn": _s(row.get("rrn")), "tid": _s(row.get("tid")),
                    "txn_date": row.get("txn_date"), "rail": "CARD", "status": "SUCCESS",
                    "amount": row.get("gross_amount"), "merchant_id": _s(row.get("merchant_id")),
                    "terminal_id": _s(row.get("tid")), "auth_id": _s(row.get("auth_id")),
                    "source_file": "AXIS_MPR",
                })
                errors.append({
                    "id": str(uuid4()), "recon_date": recon_date,
                    "transaction_id": txn_id, "rrn": _s(row.get("rrn")),
                    "amount": row.get("gross_amount"), "rail": "CARD",
                    "error_code": "17", "error_message": "MPR missing in host base",
                    "step": STEP_NAME, "source_file": "AXIS_MPR",
                })

        # Credit validation: SUM(SUCCESS CARD) vs bank statement
        success_card = repo.get_success_transactions_df(recon_date, rail="CARD")
        txn_total = float(success_card["amount"].sum()) if not success_card.empty else 0.0
        stmt_total = float(stmt_df["amount"].sum()) if not stmt_df.empty else 0.0
        diff = round(txn_total - stmt_total, 2)

        repo.upsert_credit_validation({
            "recon_date": recon_date, "rail": "CARD",
            "validation_type": "CARD_MPR",
            "total_txn_count": len(success_card),
            "total_txn_amount": txn_total,
            "statement_amount": stmt_total,
            "difference": diff,
            "status": "MATCHED" if diff == 0 else "MISMATCH",
            "notes": f"Diff={diff}",
        })

        inserted_txns = repo.batch_upsert_transactions(new_txns)
        inserted_errors = repo.batch_insert_errors(errors)
        logger.info(f"[{STEP_NAME}] Errors={inserted_errors}, Credit diff={diff}")

        repo.mark_step_complete(recon_date, STEP_NAME, inserted_errors)
        return {"errors": inserted_errors, "credit_diff": diff, "status": "MATCHED" if diff == 0 else "MISMATCH"}

    except Exception as e:
        logger.error(f"[{STEP_NAME}] FAILED: {e}", exc_info=True)
        repo.mark_step_failed(recon_date, STEP_NAME, str(e))
        raise


def _build_keys(df, col) -> set:
    return set(df[col].astype(str).values)


def _s(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    return s if s and s.lower() not in ("nan", "none") else None
