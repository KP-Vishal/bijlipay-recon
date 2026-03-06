"""
STEP 4 — Refund / Cancel Processing

Rules:
  - Same-day refund → Error 10
  - Refund not found in base:
      → Search previous 2 days payouts
      → If found → generate Adjustment record
      → Else → Error 11
"""
import logging
from datetime import date
from uuid import uuid4

import pandas as pd

from db.repository import ReconRepository
from ingestion.file_loader import load_offline_refund, load_upi_refund, load_upi_cancel
from ingestion.schema_normalizer import (
    normalize_offline_refund, normalize_upi_refund, normalize_upi_cancel
)

logger = logging.getLogger(__name__)
STEP_NAME = "STEP_4_REFUND_CANCEL"


def run(
    repo: ReconRepository,
    recon_date: date,
    offline_refund_path: str,
    upi_refund_path: str,
    upi_cancel_path: str,
    force: bool = False,
) -> dict:
    if not force and repo.step_already_complete(recon_date, STEP_NAME):
        logger.info(f"{STEP_NAME} already complete for {recon_date}, skipping.")
        return {}

    repo.mark_step_start(recon_date, STEP_NAME)
    logger.info(f"[{STEP_NAME}] Starting for {recon_date}")

    try:
        base_df = repo.get_transactions_df(recon_date)
        base_rrns = set(base_df["rrn"].astype(str).values) if not base_df.empty else set()

        errors = []
        adjustments = []

        # --- Offline Card Refunds ---
        raw_offline = load_offline_refund(offline_refund_path)
        if not raw_offline.empty:
            refunds = normalize_offline_refund(raw_offline, recon_date)
            _process_refunds(
                refunds, base_df, base_rrns, recon_date,
                repo, errors, adjustments,
                rrn_col="rrn", amount_col="refund_amount", rail="CARD"
            )

        # --- UPI Refunds ---
        raw_upi_refund = load_upi_refund(upi_refund_path)
        if not raw_upi_refund.empty:
            upi_refunds = normalize_upi_refund(raw_upi_refund, recon_date)
            _process_refunds(
                upi_refunds, base_df, base_rrns, recon_date,
                repo, errors, adjustments,
                rrn_col="rrn", amount_col="refund_amount", rail="DQR"
            )

        # --- UPI Cancels ---
        raw_cancel = load_upi_cancel(upi_cancel_path)
        if not raw_cancel.empty:
            cancels = normalize_upi_cancel(raw_cancel, recon_date)
            _process_cancels(cancels, base_df, recon_date, errors)

        inserted_errors = repo.batch_insert_errors(errors)
        inserted_adj    = repo.batch_insert_adjustments(adjustments)
        logger.info(f"[{STEP_NAME}] Errors={inserted_errors}, Adjustments={inserted_adj}")

        repo.mark_step_complete(recon_date, STEP_NAME, inserted_errors + inserted_adj)
        return {"errors": inserted_errors, "adjustments": inserted_adj}

    except Exception as e:
        logger.error(f"[{STEP_NAME}] FAILED: {e}", exc_info=True)
        repo.mark_step_failed(recon_date, STEP_NAME, str(e))
        raise


def _process_refunds(
    refunds, base_df, base_rrns, recon_date,
    repo, errors, adjustments, rrn_col, amount_col, rail
):
    for _, row in refunds.iterrows():
        rrn = _s(row.get(rrn_col))
        if not rrn:
            continue

        refund_date = row.get("txn_date") or row.get("refund_date")
        refund_amount = row.get(amount_col)

        # Check if original transaction exists in today's base
        if rrn in base_rrns:
            # Same-day refund
            base_row = base_df[base_df["rrn"].astype(str) == rrn]
            txn_id = str(base_row["id"].values[0]) if not base_row.empty else None
            errors.append({
                "id": str(uuid4()), "recon_date": recon_date,
                "transaction_id": txn_id, "rrn": rrn,
                "amount": refund_amount, "rail": rail,
                "error_code": "10",
                "error_message": f"Same-day refund for RRN {rrn}",
                "step": STEP_NAME, "source_file": "REFUND_FILE",
            })
        else:
            # Search previous 2 days payouts
            prev_payouts = repo.get_payouts_by_rrn(rrn, days_back=2)
            if not prev_payouts.empty:
                payout_row = prev_payouts.iloc[0]
                adjustments.append({
                    "id": str(uuid4()), "recon_date": recon_date,
                    "rrn": rrn,
                    "tid": _s(row.get("tid")),
                    "order_id": _s(row.get("order_id")),
                    "rail": rail,
                    "original_amount": float(payout_row.get("amount", 0)),
                    "refund_amount": refund_amount,
                    "payout_date": payout_row.get("recon_date"),
                    "payout_ref": str(payout_row.get("id", "")),
                    "source_file": "REFUND_FILE",
                })
            else:
                # Error 11: Refund not found
                errors.append({
                    "id": str(uuid4()), "recon_date": recon_date,
                    "transaction_id": None, "rrn": rrn,
                    "amount": refund_amount, "rail": rail,
                    "error_code": "11",
                    "error_message": f"Refund RRN {rrn} not found in base or previous payouts",
                    "step": STEP_NAME, "source_file": "REFUND_FILE",
                })


def _process_cancels(cancels, base_df, recon_date, errors):
    """UPI cancels: if order_id found in base → Error 10 (same-day cancel)."""
    if base_df.empty:
        return

    base_orders = set(base_df["order_id"].dropna().astype(str).values)

    for _, row in cancels.iterrows():
        order_id = _s(row.get("order_id"))
        if not order_id:
            continue

        if order_id in base_orders:
            base_row = base_df[base_df["order_id"].astype(str) == order_id]
            txn_id = str(base_row["id"].values[0]) if not base_row.empty else None
            errors.append({
                "id": str(uuid4()), "recon_date": recon_date,
                "transaction_id": txn_id, "rrn": None,
                "order_id": order_id, "amount": row.get("amount"),
                "rail": "DQR",
                "error_code": "10",
                "error_message": f"Same-day UPI cancel for order {order_id}",
                "step": STEP_NAME, "source_file": "UPI_CANCEL",
            })


def _s(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    return s if s and s.lower() not in ("nan", "none") else None
