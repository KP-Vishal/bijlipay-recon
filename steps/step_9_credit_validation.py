"""
STEP 9 — Credit Validation DQR/SQR (Bulk)
Compare Axis MIS vs Bulk Settlement
Error 18: MIS missing in bulk
Error 19: Bulk missing in MIS
Only SUCCESS transactions participate.
"""
import logging
from datetime import date
from uuid import uuid4

import pandas as pd

from db.repository import ReconRepository
from ingestion.file_loader import load_mis_dynamic, load_mis_static, load_bulk_settlement
from ingestion.schema_normalizer import normalize_mis, normalize_bulk_settlement

logger = logging.getLogger(__name__)
STEP_NAME = "STEP_9_CREDIT_VALIDATION_DQR_SQR"


def run(
    repo: ReconRepository,
    recon_date: date,
    mis_dynamic_path: str,
    mis_static_path: str,
    bulk_settlement_path: str,
    force: bool = False,
) -> dict:
    if not force and repo.step_already_complete(recon_date, STEP_NAME):
        logger.info(f"{STEP_NAME} already complete for {recon_date}, skipping.")
        return {}

    repo.mark_step_start(recon_date, STEP_NAME)
    logger.info(f"[{STEP_NAME}] Starting for {recon_date}")

    try:
        raw_dyn    = load_mis_dynamic(mis_dynamic_path)
        raw_stat   = load_mis_static(mis_static_path)
        raw_bulk   = load_bulk_settlement(bulk_settlement_path)

        mis_dyn_df  = normalize_mis(raw_dyn, recon_date, "DQR") if not raw_dyn.empty else pd.DataFrame()
        mis_stat_df = normalize_mis(raw_stat, recon_date, "SQR") if not raw_stat.empty else pd.DataFrame()
        bulk_df     = normalize_bulk_settlement(raw_bulk, recon_date) if not raw_bulk.empty else pd.DataFrame()

        # Combine MIS files
        mis_frames = [f for f in [mis_dyn_df, mis_stat_df] if not f.empty]
        mis_df = pd.concat(mis_frames, ignore_index=True) if mis_frames else pd.DataFrame()

        # Only SUCCESS
        if not mis_df.empty:
            mis_df = mis_df[mis_df["status"] == "SUCCESS"]
        if not bulk_df.empty and "status" in bulk_df.columns:
            bulk_df = bulk_df[bulk_df["status"] == "SUCCESS"]

        mis_keys  = _upi_keys(mis_df)
        bulk_keys = _upi_keys(bulk_df)

        errors = []

        # Error 18: MIS missing in bulk
        if not mis_df.empty:
            mis_df["_nk"] = _nk_series(mis_df)
            missing = mis_df[~mis_df["_nk"].isin(bulk_keys)]
            for _, row in missing.iterrows():
                errors.append({
                    "id": str(uuid4()), "recon_date": recon_date,
                    "transaction_id": None, "rrn": _s(row.get("rrn")),
                    "order_id": _s(row.get("order_id")), "amount": row.get("amount"),
                    "rail": str(row.get("rail", "DQR")),
                    "error_code": "18", "error_message": "MIS missing in bulk settlement",
                    "step": STEP_NAME, "source_file": "MIS",
                })

        # Error 19: Bulk missing in MIS
        if not bulk_df.empty:
            bulk_df["_nk"] = _nk_series(bulk_df)
            missing = bulk_df[~bulk_df["_nk"].isin(mis_keys)]
            for _, row in missing.iterrows():
                errors.append({
                    "id": str(uuid4()), "recon_date": recon_date,
                    "transaction_id": None, "rrn": _s(row.get("rrn")),
                    "order_id": _s(row.get("order_id")), "amount": row.get("amount"),
                    "rail": str(row.get("rail", "DQR")),
                    "error_code": "19", "error_message": "Bulk settlement missing in MIS",
                    "step": STEP_NAME, "source_file": "BULK_SETTLEMENT",
                })

        inserted_errors = repo.batch_insert_errors(errors)
        logger.info(f"[{STEP_NAME}] Errors={inserted_errors}")

        repo.mark_step_complete(recon_date, STEP_NAME, inserted_errors)
        return {"errors": inserted_errors}

    except Exception as e:
        logger.error(f"[{STEP_NAME}] FAILED: {e}", exc_info=True)
        repo.mark_step_failed(recon_date, STEP_NAME, str(e))
        raise


def _upi_keys(df: pd.DataFrame) -> set:
    if df.empty:
        return set()
    return set(_nk_series(df).values)


def _nk_series(df: pd.DataFrame) -> pd.Series:
    order = df["order_id"].fillna("") if "order_id" in df.columns else pd.Series("", index=df.index)
    return df["rrn"].astype(str) + "|" + df["amount"].astype(str) + "|" + order.astype(str)


def _s(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    return s if s and s.lower() not in ("nan", "none") else None
