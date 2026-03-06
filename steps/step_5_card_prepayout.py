"""
STEP 5 — Card Aggregator Reconciliation
Compare base vs Worldline Aggregator Report.

Error B: Aggregator exists but base missing (direct TID)
Error 12: Aggregator exists but base missing (non-direct TID)
Error 13: Base exists but aggregator missing
"""
import logging
from datetime import date
from uuid import uuid4
from pathlib import Path

import pandas as pd
import yaml

from db.repository import ReconRepository
from ingestion.file_loader import load_wl_aggregator
from ingestion.schema_normalizer import normalize_wl_aggregator

logger = logging.getLogger(__name__)
STEP_NAME = "STEP_5_CARD_AGGREGATOR_RECON"

CONFIG_PATH = Path(__file__).parent.parent / "config" / "column_mappings.yaml"


def _load_direct_tids() -> set:
    """
    Load direct TIDs from config file.
    These are TIDs where Bijlipay has a direct settlement relationship.
    """
    try:
        with open(CONFIG_PATH) as f:
            cfg = yaml.safe_load(f)
        return set(cfg.get("direct_tids", []))
    except Exception:
        return set()


def run(
    repo: ReconRepository,
    recon_date: date,
    wl_aggregator_path: str,
    force: bool = False,
) -> dict:
    if not force and repo.step_already_complete(recon_date, STEP_NAME):
        logger.info(f"{STEP_NAME} already complete for {recon_date}, skipping.")
        return {}

    repo.mark_step_start(recon_date, STEP_NAME)
    logger.info(f"[{STEP_NAME}] Starting for {recon_date}")

    try:
        direct_tids = _load_direct_tids()

        raw_aggr = load_wl_aggregator(wl_aggregator_path)
        if raw_aggr.empty:
            logger.warning(f"{STEP_NAME}: Aggregator file is empty.")
            repo.mark_step_complete(recon_date, STEP_NAME, 0)
            return {}

        aggr_df = normalize_wl_aggregator(raw_aggr, recon_date)
        base_df = repo.get_transactions_df(recon_date, rail="CARD")

        base_keys = _build_card_keys(base_df)
        aggr_keys = _build_card_keys(aggr_df)

        errors = []
        new_txns = []

        # Error B / 12: In aggregator but not in base
        missing_in_base = aggr_df[~aggr_df["_nk"].isin(base_keys)]
        for _, row in missing_in_base.iterrows():
            tid = _s(row.get("tid"))
            is_direct = tid in direct_tids if tid else False
            error_code = "B" if is_direct else "12"
            error_msg  = (
                "Aggregator exists but base missing (direct TID)" if is_direct
                else "Aggregator exists but base missing (non-direct TID)"
            )
            txn_id = str(uuid4())
            new_txns.append({
                "id": txn_id, "recon_date": recon_date,
                "rrn": _s(row.get("rrn")), "tid": tid,
                "txn_date": row.get("txn_date"), "rail": "CARD",
                "status": "SUCCESS", "amount": row.get("amount"),
                "merchant_id": _s(row.get("merchant_id")),
                "merchant_name": _s(row.get("merchant_name")),
                "terminal_id": tid, "auth_id": _s(row.get("auth_id")),
                "interchange_type": _s(row.get("interchange_type")),
                "source_file": "WL_AGGREGATOR",
            })
            errors.append({
                "id": str(uuid4()), "recon_date": recon_date,
                "transaction_id": txn_id, "rrn": _s(row.get("rrn")),
                "tid": tid, "amount": row.get("amount"), "rail": "CARD",
                "error_code": error_code, "error_message": error_msg,
                "step": STEP_NAME, "source_file": "WL_AGGREGATOR",
            })

        # Error 13: In base but not in aggregator
        if not base_df.empty:
            base_df["_nk"] = _nk_series(base_df)
            missing_in_aggr = base_df[~base_df["_nk"].isin(aggr_keys)]
            for _, row in missing_in_aggr.iterrows():
                errors.append({
                    "id": str(uuid4()), "recon_date": recon_date,
                    "transaction_id": str(row["id"]),
                    "rrn": _s(row.get("rrn")), "tid": _s(row.get("tid")),
                    "amount": row.get("amount"), "rail": "CARD",
                    "error_code": "13",
                    "error_message": "Base exists but missing in aggregator",
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


def _build_card_keys(df: pd.DataFrame) -> set:
    if df.empty:
        return set()
    df = df.copy()
    df["_nk"] = _nk_series(df)
    return set(df["_nk"].values)


def _nk_series(df: pd.DataFrame) -> pd.Series:
    return (
        df["rrn"].astype(str) + "|" +
        df["tid"].astype(str) + "|" +
        df["amount"].astype(str)
    )


def _s(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    return s if s and s.lower() not in ("nan", "none") else None
