"""
STEP 2 — DQR Middleware Consistency Check
Compare transaction base vs UPI settlement files.
Error 4: Settlement exists but base missing → add transaction
Error 5: Base exists but settlement missing
"""
import logging
from datetime import date
from uuid import uuid4

import pandas as pd

from db.repository import ReconRepository
from ingestion.file_loader import load_upi_settlement_jupiter, load_upi_settlement_ogs
from ingestion.schema_normalizer import (
    normalize_upi_settlement_jupiter, normalize_upi_settlement_ogs
)

logger = logging.getLogger(__name__)
STEP_NAME = "STEP_2_DQR_CONSISTENCY"


def run(
    repo: ReconRepository,
    recon_date: date,
    upi_settlement_jupiter_path: str,
    upi_settlement_ogs_path: str,
    force: bool = False,
) -> dict:
    if not force and repo.step_already_complete(recon_date, STEP_NAME):
        logger.info(f"{STEP_NAME} already complete for {recon_date}, skipping.")
        return {}

    repo.mark_step_start(recon_date, STEP_NAME)
    logger.info(f"[{STEP_NAME}] Starting for {recon_date}")

    try:
        # Load base (DQR/SQR only)
        base_df = repo.get_transactions_df(recon_date)
        upi_base = base_df[base_df["rail"].isin(["DQR", "SQR"])].copy()

        # Load UPI settlements
        frames = []
        for path, loader, normalizer in [
            (upi_settlement_jupiter_path, load_upi_settlement_jupiter, normalize_upi_settlement_jupiter),
            (upi_settlement_ogs_path, load_upi_settlement_ogs, normalize_upi_settlement_ogs),
        ]:
            raw = loader(path)
            if not raw.empty:
                frames.append(normalizer(raw, recon_date))

        if not frames:
            repo.mark_step_complete(recon_date, STEP_NAME, 0)
            return {}

        sett_df = pd.concat(frames, ignore_index=True)

        # Build natural keys for comparison
        upi_base_keys = _build_upi_keys(upi_base)
        sett_keys = _build_upi_keys(sett_df)

        errors = []
        new_txns = []

        # Error 4: Settlement exists but base missing
        missing_in_base = sett_df[~sett_df["_nk"].isin(upi_base_keys)]
        logger.info(f"  Error 4 candidates: {len(missing_in_base)}")
        for _, row in missing_in_base.iterrows():
            txn_id = str(uuid4())
            new_txns.append({
                "id": txn_id, "recon_date": recon_date,
                "rrn": _s(row.get("rrn")), "order_id": _s(row.get("order_id")),
                "txn_id": _s(row.get("txn_id")), "txn_date": row.get("txn_date"),
                "rail": str(row.get("rail", "DQR")), "status": str(row.get("status", "SUCCESS")),
                "amount": row.get("amount"), "merchant_id": _s(row.get("merchant_id")),
                "creditvpa": _s(row.get("creditvpa")), "source_file": "DQR_SETTLEMENT",
            })
            errors.append({
                "id": str(uuid4()), "recon_date": recon_date,
                "transaction_id": txn_id, "rrn": _s(row.get("rrn")),
                "order_id": _s(row.get("order_id")), "amount": row.get("amount"),
                "error_code": "4", "error_message": "Settlement exists but base missing",
                "step": STEP_NAME, "source_file": "UPI_SETTLEMENT",
            })

        # Error 5: Base exists but settlement missing
        missing_in_sett = upi_base[~upi_base["_nk"].isin(sett_keys)]
        logger.info(f"  Error 5 candidates: {len(missing_in_sett)}")
        for _, row in missing_in_sett.iterrows():
            errors.append({
                "id": str(uuid4()), "recon_date": recon_date,
                "transaction_id": str(row.get("id")), "rrn": _s(row.get("rrn")),
                "order_id": _s(row.get("order_id")), "amount": row.get("amount"),
                "error_code": "5", "error_message": "Base exists but settlement missing",
                "step": STEP_NAME, "source_file": "BASE",
            })

        inserted_txns = repo.batch_upsert_transactions(new_txns)
        inserted_errors = repo.batch_insert_errors(errors)
        logger.info(f"[{STEP_NAME}] New txns={inserted_txns}, Errors={inserted_errors}")

        repo.mark_step_complete(recon_date, STEP_NAME, inserted_errors)
        return {"new_transactions": inserted_txns, "errors": inserted_errors}

    except Exception as e:
        logger.error(f"[{STEP_NAME}] FAILED: {e}", exc_info=True)
        repo.mark_step_failed(recon_date, STEP_NAME, str(e))
        raise


def _build_upi_keys(df: pd.DataFrame) -> set:
    if df.empty:
        return set()
    df = df.copy()
    df["_order_id"] = df["order_id"].fillna("") if "order_id" in df.columns else ""
    df["_nk"] = df["rrn"].astype(str) + "|" + df["amount"].astype(str) + "|" + df["_order_id"].astype(str)
    return set(df["_nk"].values)


def _s(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    return s if s and s.lower() not in ("nan", "none") else None
