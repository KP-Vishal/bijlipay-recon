"""
STEP 1 — Build Bijlipay Transaction Base
Combines File1 (Jupiter ATF) + File2 (OGS ATF) + File3 (Axis Settlement SQR)
Deduplicates using natural key, creates unified base.
"""
import logging
from datetime import date
from uuid import uuid4

import pandas as pd

from db.repository import ReconRepository
from ingestion.file_loader import (
    load_jupiter_atf, load_ogs_atf, load_axis_settlement_sqr
)
from ingestion.schema_normalizer import (
    normalize_middleware_atf, normalize_axis_settlement_sqr
)

logger = logging.getLogger(__name__)
STEP_NAME = "STEP_1_BUILD_BASE"


def run(
    repo: ReconRepository,
    recon_date: date,
    jupiter_atf_path: str,
    ogs_atf_path: str,
    axis_settlement_sqr_path: str,
    force: bool = False,
) -> int:
    """
    Returns number of transactions inserted into base.
    Idempotent: skips if already complete unless force=True.
    """
    if not force and repo.step_already_complete(recon_date, STEP_NAME):
        logger.info(f"{STEP_NAME} already complete for {recon_date}, skipping.")
        return 0

    repo.mark_step_start(recon_date, STEP_NAME)
    logger.info(f"[{STEP_NAME}] Starting for {recon_date}")

    try:
        frames = []

        # --- File 1: Jupiter ATF ---
        raw_jupiter = load_jupiter_atf(jupiter_atf_path)
        if not raw_jupiter.empty:
            norm = normalize_middleware_atf(raw_jupiter, recon_date, "JUPITER_ATF")
            frames.append(norm)
            logger.info(f"  Jupiter ATF: {len(norm)} rows")

        # --- File 2: OGS ATF ---
        raw_ogs = load_ogs_atf(ogs_atf_path)
        if not raw_ogs.empty:
            norm = normalize_middleware_atf(raw_ogs, recon_date, "OGS_ATF")
            frames.append(norm)
            logger.info(f"  OGS ATF: {len(norm)} rows")

        # --- File 3: Axis Settlement (SQR) ---
        raw_sqr = load_axis_settlement_sqr(axis_settlement_sqr_path)
        if not raw_sqr.empty:
            norm = normalize_axis_settlement_sqr(raw_sqr, recon_date)
            frames.append(norm)
            logger.info(f"  Axis Settlement SQR: {len(norm)} rows")

        if not frames:
            logger.warning(f"{STEP_NAME}: No data loaded from any source file.")
            repo.mark_step_complete(recon_date, STEP_NAME, 0)
            return 0

        combined = pd.concat(frames, ignore_index=True)
        logger.info(f"  Combined rows before dedup: {len(combined)}")

        # --- Deduplication using natural key ---
        combined = _deduplicate(combined)
        logger.info(f"  After dedup: {len(combined)} rows")

        # --- Convert to DB records ---
        records = _to_db_records(combined, recon_date)

        # --- Batch upsert ---
        inserted = repo.batch_upsert_transactions(records)
        logger.info(f"[{STEP_NAME}] Inserted {inserted} transactions.")

        repo.mark_step_complete(recon_date, STEP_NAME, inserted)
        return inserted

    except Exception as e:
        logger.error(f"[{STEP_NAME}] FAILED: {e}", exc_info=True)
        repo.mark_step_failed(recon_date, STEP_NAME, str(e))
        raise


def _deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicates using natural key.
    CARD: (rrn, tid, amount)
    DQR/SQR: (rrn, order_id, amount) or (rrn, amount, txn_date)
    Keep first occurrence, flag subsequent ones with error code 1.
    """
    card_mask = df["rail"] == "CARD"
    upi_mask  = ~card_mask

    card_df = df[card_mask].copy()
    upi_df  = df[upi_mask].copy()

    # CARD dedup
    if not card_df.empty:
        card_df["_nk"] = (
            card_df["rrn"].astype(str) + "|" +
            card_df["tid"].astype(str) + "|" +
            card_df["amount"].astype(str)
        )
        card_df = card_df.drop_duplicates(subset=["_nk"], keep="first")
        card_df = card_df.drop(columns=["_nk"])

    # UPI dedup
    if not upi_df.empty:
        upi_df["_order_id"] = upi_df["order_id"].fillna("")
        upi_df["_nk"] = (
            upi_df["rrn"].astype(str) + "|" +
            upi_df["amount"].astype(str) + "|" +
            upi_df["_order_id"].astype(str)
        )
        upi_df = upi_df.drop_duplicates(subset=["_nk"], keep="first")
        upi_df = upi_df.drop(columns=["_nk", "_order_id"])

    return pd.concat([card_df, upi_df], ignore_index=True)


def _to_db_records(df: pd.DataFrame, recon_date: date) -> list:
    """Convert DataFrame rows to list of dicts for DB insertion."""
    records = []
    for _, row in df.iterrows():
        rec = {
            "id":               str(uuid4()),
            "recon_date":       recon_date,
            "rrn":              _safe_str(row.get("rrn")),
            "tid":              _safe_str(row.get("tid")),
            "order_id":         _safe_str(row.get("order_id")),
            "txn_date":         row.get("txn_date"),
            "rail":             str(row.get("rail", "CARD")),
            "status":           str(row.get("status", "FAILURE")),
            "source_file":      str(row.get("source_file", "")),
            "amount":           row.get("amount"),
            "merchant_id":      _safe_str(row.get("merchant_id")),
            "merchant_name":    _safe_str(row.get("merchant_name")),
            "terminal_id":      _safe_str(row.get("tid")),
            "card_number":      _safe_str(row.get("card_number")),
            "auth_id":          _safe_str(row.get("auth_id")),
            "response_code":    _safe_str(row.get("response_code")),
            "interchange_type": _safe_str(row.get("interchange_type")),
            "stan":             _safe_str(row.get("stan")),
            "pos_entry_mode":   _safe_str(row.get("pos_entry_mode")),
            "credit_debit_flag":_safe_str(row.get("credit_debit_flag")),
            "txn_id":           _safe_str(row.get("txn_id")),
            "vpa":              _safe_str(row.get("vpa")),
            "creditvpa":        _safe_str(row.get("creditvpa")),
            "bankname":         _safe_str(row.get("bankname")),
            "ack_enabled":      _safe_str(row.get("ack_enabled")),
            "ack_received":     _safe_str(row.get("ack_received")),
        }
        records.append(rec)
    return records


def _safe_str(val) -> str | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    return s if s and s.lower() not in ("nan", "none", "") else None
