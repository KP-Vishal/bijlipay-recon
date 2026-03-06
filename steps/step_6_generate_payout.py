"""
STEP 6 — Generate Payout Files

Payout eligibility:
  - status = SUCCESS
  - AND error_codes are ONLY in {A, B} OR no errors at all

All transactions with blocking error codes are EXCLUDED.
Generates payout records per rail (CARD / DQR / SQR).
"""
import logging
from datetime import date
from pathlib import Path
from uuid import uuid4

import pandas as pd

from db.repository import ReconRepository
from models.error import PAYOUT_ELIGIBLE_ERRORS, PAYOUT_BLOCKING_ERRORS

logger = logging.getLogger(__name__)
STEP_NAME = "STEP_6_GENERATE_PAYOUT"


def run(
    repo: ReconRepository,
    recon_date: date,
    output_dir: str = "./output",
    force: bool = False,
) -> dict:
    if not force and repo.step_already_complete(recon_date, STEP_NAME):
        logger.info(f"{STEP_NAME} already complete for {recon_date}, skipping.")
        return {}

    repo.mark_step_start(recon_date, STEP_NAME)
    logger.info(f"[{STEP_NAME}] Starting for {recon_date}")

    try:
        Path(output_dir).mkdir(parents=True, exist_ok=True)

        # Get all SUCCESS transactions
        success_df = repo.get_success_transactions_df(recon_date)
        if success_df.empty:
            logger.warning(f"{STEP_NAME}: No SUCCESS transactions found.")
            repo.mark_step_complete(recon_date, STEP_NAME, 0)
            return {}

        # Get all transactions with blocking errors
        blocking_ids = _get_blocking_transaction_ids(repo, recon_date)
        logger.info(f"  Blocking transaction IDs: {len(blocking_ids)}")

        # Filter to payout eligible
        eligible_df = success_df[~success_df["id"].astype(str).isin(blocking_ids)].copy()
        logger.info(f"  Payout eligible: {len(eligible_df)} / {len(success_df)} SUCCESS txns")

        if eligible_df.empty:
            repo.mark_step_complete(recon_date, STEP_NAME, 0)
            return {"payout_count": 0}

        # Generate payout records
        payout_records = []
        for _, row in eligible_df.iterrows():
            payout_records.append({
                "id":           str(uuid4()),
                "recon_date":   recon_date,
                "transaction_id": str(row["id"]),
                "rrn":          _s(row.get("rrn")),
                "tid":          _s(row.get("tid")),
                "order_id":     _s(row.get("order_id")),
                "rail":         str(row.get("rail")),
                "amount":       row.get("amount"),
                "merchant_id":  _s(row.get("merchant_id")),
                "terminal_id":  _s(row.get("terminal_id")),
                "payout_status": "PENDING",
            })

        inserted = repo.batch_insert_payouts(payout_records)
        logger.info(f"  Inserted payout records: {inserted}")

        # Write payout CSV files per rail
        payout_df = pd.DataFrame(payout_records)
        files_written = []
        for rail in ["CARD", "DQR", "SQR"]:
            rail_df = payout_df[payout_df["rail"] == rail]
            if not rail_df.empty:
                fname = Path(output_dir) / f"payout_{rail}_{recon_date}.csv"
                rail_df.to_csv(fname, index=False)
                files_written.append(str(fname))
                logger.info(f"  Written payout file: {fname} ({len(rail_df)} rows, total={rail_df['amount'].sum():.2f})")

        repo.mark_step_complete(recon_date, STEP_NAME, inserted)
        return {
            "payout_count": inserted,
            "files": files_written,
            "total_amount": float(payout_df["amount"].sum()),
        }

    except Exception as e:
        logger.error(f"[{STEP_NAME}] FAILED: {e}", exc_info=True)
        repo.mark_step_failed(recon_date, STEP_NAME, str(e))
        raise


def _get_blocking_transaction_ids(repo: ReconRepository, recon_date: date) -> set:
    """
    Returns set of transaction IDs that have at least one BLOCKING error code.
    Error codes A and B are NOT blocking.
    """
    errors_df = repo.get_transactions_with_errors_df(recon_date)
    if errors_df.empty:
        return set()

    blocking_ids = set()
    for _, row in errors_df.iterrows():
        codes = row.get("error_codes", []) or []
        # Convert postgres array format if needed
        if isinstance(codes, str):
            codes = codes.strip("{}").split(",")
        codes = [c.strip().strip('"') for c in codes]

        # If any error code is blocking, exclude this transaction
        has_blocking = any(c in PAYOUT_BLOCKING_ERRORS for c in codes)
        if has_blocking:
            blocking_ids.add(str(row["transaction_id"]))

    return blocking_ids


def _s(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    return s if s and s.lower() not in ("nan", "none") else None
