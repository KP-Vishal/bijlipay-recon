"""
STEP 7 — Build Host Transaction Base
Compare WL ATF vs WL Aggregator.
Error 14: ATF missing in aggregator
Error 15: Aggregator missing in ATF
Creates host_transaction_base.
"""
import logging
from datetime import date
from uuid import uuid4

import pandas as pd

from db.repository import ReconRepository
from ingestion.file_loader import load_wl_atf, load_wl_aggregator
from ingestion.schema_normalizer import normalize_wl_atf, normalize_wl_aggregator

logger = logging.getLogger(__name__)
STEP_NAME = "STEP_7_BUILD_HOST_BASE"


def run(
    repo: ReconRepository,
    recon_date: date,
    wl_atf_path: str,
    wl_aggregator_path: str,
    force: bool = False,
) -> dict:
    if not force and repo.step_already_complete(recon_date, STEP_NAME):
        logger.info(f"{STEP_NAME} already complete for {recon_date}, skipping.")
        return {}

    repo.mark_step_start(recon_date, STEP_NAME)
    logger.info(f"[{STEP_NAME}] Starting for {recon_date}")

    try:
        raw_atf  = load_wl_atf(wl_atf_path)
        raw_aggr = load_wl_aggregator(wl_aggregator_path)

        atf_df  = normalize_wl_atf(raw_atf, recon_date) if not raw_atf.empty else pd.DataFrame()
        aggr_df = normalize_wl_aggregator(raw_aggr, recon_date) if not raw_aggr.empty else pd.DataFrame()

        atf_keys  = _card_keys(atf_df)
        aggr_keys = _card_keys(aggr_df)

        errors = []
        host_records = []

        # Build host base from ATF
        for _, row in atf_df.iterrows():
            host_records.append({
                "id": str(uuid4()), "recon_date": recon_date,
                "rrn": _s(row.get("rrn")), "tid": _s(row.get("tid")),
                "amount": row.get("amount"), "status": str(row.get("status", "SUCCESS")),
                "source": "ATF", "merchant_id": _s(row.get("merchant_id")),
                "terminal_id": _s(row.get("tid")), "auth_id": _s(row.get("auth_id")),
                "txn_date": row.get("txn_date"),
            })

        # Error 14: ATF missing in aggregator
        if not atf_df.empty:
            atf_df["_nk"] = _nk_series(atf_df)
            missing = atf_df[~atf_df["_nk"].isin(aggr_keys)]
            for _, row in missing.iterrows():
                errors.append({
                    "id": str(uuid4()), "recon_date": recon_date,
                    "transaction_id": None, "rrn": _s(row.get("rrn")),
                    "tid": _s(row.get("tid")), "amount": row.get("amount"), "rail": "CARD",
                    "error_code": "14", "error_message": "WL ATF missing in aggregator",
                    "step": STEP_NAME, "source_file": "WL_ATF",
                })

        # Error 15: Aggregator missing in ATF
        if not aggr_df.empty:
            aggr_df["_nk"] = _nk_series(aggr_df)
            missing = aggr_df[~aggr_df["_nk"].isin(atf_keys)]
            for _, row in missing.iterrows():
                # Also add to host base from aggregator side
                host_records.append({
                    "id": str(uuid4()), "recon_date": recon_date,
                    "rrn": _s(row.get("rrn")), "tid": _s(row.get("tid")),
                    "amount": row.get("amount"), "status": "SUCCESS",
                    "source": "AGGREGATOR", "merchant_id": _s(row.get("merchant_id")),
                    "terminal_id": _s(row.get("tid")), "auth_id": _s(row.get("auth_id")),
                    "txn_date": row.get("txn_date"),
                })
                errors.append({
                    "id": str(uuid4()), "recon_date": recon_date,
                    "transaction_id": None, "rrn": _s(row.get("rrn")),
                    "tid": _s(row.get("tid")), "amount": row.get("amount"), "rail": "CARD",
                    "error_code": "15", "error_message": "WL Aggregator missing in ATF",
                    "step": STEP_NAME, "source_file": "WL_AGGREGATOR",
                })

        inserted_host = repo.batch_insert_host_transactions(host_records)
        inserted_errors = repo.batch_insert_errors(errors)
        logger.info(f"[{STEP_NAME}] Host records={inserted_host}, Errors={inserted_errors}")

        repo.mark_step_complete(recon_date, STEP_NAME, inserted_host)
        return {"host_records": inserted_host, "errors": inserted_errors}

    except Exception as e:
        logger.error(f"[{STEP_NAME}] FAILED: {e}", exc_info=True)
        repo.mark_step_failed(recon_date, STEP_NAME, str(e))
        raise


def _card_keys(df: pd.DataFrame) -> set:
    if df.empty:
        return set()
    return set(_nk_series(df).values)


def _nk_series(df: pd.DataFrame) -> pd.Series:
    return df["rrn"].astype(str) + "|" + df["tid"].astype(str) + "|" + df["amount"].astype(str)


def _s(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    return s if s and s.lower() not in ("nan", "none") else None
