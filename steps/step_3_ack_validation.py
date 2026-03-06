"""
STEP 3 — ACK Validation
Uses axis_aggre_*.csv which has ack_enabled and ack_received columns.

Rules:
  Error 6:  ack_enabled=Y, ack_received=N
  Error 7:  ack_enabled=N, ack_received=Y
  Error A:  ack_enabled=N, ack_received=N  (not applicable — eligible for payout)
  Error 8:  Transaction in base but missing in ACK file
  Error 9:  ACK exists but base missing → add transaction
"""
import logging
from datetime import date
from uuid import uuid4

import pandas as pd

from db.repository import ReconRepository
from ingestion.file_loader import load_axis_aggregator_ack
from ingestion.schema_normalizer import normalize_axis_aggregator_ack

logger = logging.getLogger(__name__)
STEP_NAME = "STEP_3_ACK_VALIDATION"


def run(
    repo: ReconRepository,
    recon_date: date,
    axis_aggregator_ack_path: str,
    force: bool = False,
) -> dict:
    if not force and repo.step_already_complete(recon_date, STEP_NAME):
        logger.info(f"{STEP_NAME} already complete for {recon_date}, skipping.")
        return {}

    repo.mark_step_start(recon_date, STEP_NAME)
    logger.info(f"[{STEP_NAME}] Starting for {recon_date}")

    try:
        raw = load_axis_aggregator_ack(axis_aggregator_ack_path)
        if raw.empty:
            logger.warning(f"{STEP_NAME}: ACK file is empty.")
            repo.mark_step_complete(recon_date, STEP_NAME, 0)
            return {}

        ack_df = normalize_axis_aggregator_ack(raw, recon_date)

        # Get CARD transactions from base
        base_df = repo.get_transactions_df(recon_date, rail="CARD")

        base_keys = _build_card_keys(base_df)
        ack_keys  = _build_card_keys(ack_df)

        errors = []
        new_txns = []

        # --- ACK file processing ---
        for _, row in ack_df.iterrows():
            ack_enabled  = str(row.get("ack_enabled", "N")).strip().upper()
            ack_received = str(row.get("ack_received", "N")).strip().upper()
            nk = _card_nk(row)

            if nk in base_keys:
                # Transaction exists in base — apply ACK rules
                base_row = base_df[base_df["_nk"] == nk].iloc[0] if "_nk" in base_df.columns else None
                txn_id = str(base_df[_nk_mask(base_df, row)]["id"].values[0]) if not base_df.empty else None

                if ack_enabled == "Y" and ack_received == "N":
                    errors.append(_make_error("6", "Ack enabled but not received", recon_date, row, txn_id))
                elif ack_enabled == "N" and ack_received == "Y":
                    errors.append(_make_error("7", "Ack received but not enabled", recon_date, row, txn_id))
                elif ack_enabled == "N" and ack_received == "N":
                    errors.append(_make_error("A", "Ack not applicable", recon_date, row, txn_id))
            else:
                # Error 9: ACK exists but base missing → add transaction
                txn_id = str(uuid4())
                new_txns.append({
                    "id": txn_id, "recon_date": recon_date,
                    "rrn": _s(row.get("rrn")), "tid": _s(row.get("tid")),
                    "txn_date": row.get("txn_date"), "rail": "CARD",
                    "status": str(row.get("status", "SUCCESS")),
                    "amount": row.get("amount"),
                    "merchant_id": _s(row.get("merchant_id")),
                    "terminal_id": _s(row.get("tid")),
                    "auth_id": _s(row.get("auth_id")),
                    "ack_enabled": ack_enabled,
                    "ack_received": ack_received,
                    "source_file": "AXIS_AGGREGATOR_ACK",
                })
                errors.append(_make_error("9", "Ack exists but base missing", recon_date, row, txn_id))

                # Also apply ACK rule for newly added transaction
                if ack_enabled == "N" and ack_received == "N":
                    errors.append(_make_error("A", "Ack not applicable", recon_date, row, txn_id))

        # Error 8: Base CARD txn missing in ACK file
        if not base_df.empty:
            base_df["_nk"] = (
                base_df["rrn"].astype(str) + "|" +
                base_df["tid"].astype(str) + "|" +
                base_df["amount"].astype(str)
            )
            missing_in_ack = base_df[~base_df["_nk"].isin(ack_keys)]
            for _, row in missing_in_ack.iterrows():
                errors.append({
                    "id": str(uuid4()), "recon_date": recon_date,
                    "transaction_id": str(row["id"]),
                    "rrn": _s(row.get("rrn")), "tid": _s(row.get("tid")),
                    "amount": row.get("amount"), "rail": "CARD",
                    "error_code": "8", "error_message": "Transaction missing in ACK file",
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


def _card_nk(row) -> str:
    return f"{row.get('rrn')}|{row.get('tid')}|{row.get('amount')}"


def _build_card_keys(df: pd.DataFrame) -> set:
    if df.empty:
        return set()
    return set(
        (df["rrn"].astype(str) + "|" + df["tid"].astype(str) + "|" + df["amount"].astype(str)).values
    )


def _nk_mask(df: pd.DataFrame, row) -> pd.Series:
    return (
        (df["rrn"].astype(str) == str(row.get("rrn"))) &
        (df["tid"].astype(str) == str(row.get("tid"))) &
        (df["amount"].astype(str) == str(row.get("amount")))
    )


def _make_error(code, msg, recon_date, row, txn_id) -> dict:
    return {
        "id": str(uuid4()), "recon_date": recon_date,
        "transaction_id": txn_id,
        "rrn": _s(row.get("rrn")), "tid": _s(row.get("tid")),
        "amount": row.get("amount"), "rail": "CARD",
        "error_code": code, "error_message": msg,
        "step": STEP_NAME, "source_file": "AXIS_AGGREGATOR_ACK",
    }


def _s(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    return s if s and s.lower() not in ("nan", "none") else None
