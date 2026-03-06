"""
STEP 10 — Instant Settlement Validation (DQR/SQR)
Compare host base vs bank statement.
Error 20: Host missing in statement
Error 21: Statement missing in base
"""
import logging
from datetime import date
from uuid import uuid4

import pandas as pd

from db.repository import ReconRepository
from ingestion.file_loader import load_bank_statement
from ingestion.schema_normalizer import normalize_bank_statement

logger = logging.getLogger(__name__)
STEP_NAME = "STEP_10_INSTANT_SETTLEMENT"


def run(
    repo: ReconRepository,
    recon_date: date,
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
        raw_stmt = load_bank_statement(bank_statement_path)
        stmt_df = normalize_bank_statement(raw_stmt, recon_date) if not raw_stmt.empty else pd.DataFrame()

        errors = []

        # We match by amount since bank statements don't carry RRN
        host_amounts = set(host_df["amount"].astype(float).round(2).values) if not host_df.empty else set()
        stmt_amounts = set(stmt_df["amount"].astype(float).round(2).values) if not stmt_df.empty else set()

        # Error 20: Host missing in statement
        if not host_df.empty:
            missing = host_df[~host_df["amount"].astype(float).round(2).isin(stmt_amounts)]
            for _, row in missing.iterrows():
                errors.append({
                    "id": str(uuid4()), "recon_date": recon_date,
                    "transaction_id": None, "rrn": _s(row.get("rrn")),
                    "amount": row.get("amount"), "rail": "CARD",
                    "error_code": "20", "error_message": "Host transaction missing in bank statement",
                    "step": STEP_NAME, "source_file": "HOST_BASE",
                })

        # Error 21: Statement missing in base
        # For UPI statements, cross-check against bijlipay transaction base
        base_df = repo.get_success_transactions_df(recon_date)
        base_amounts = set(base_df["amount"].astype(float).round(2).values) if not base_df.empty else set()
        if not stmt_df.empty:
            upi_stmt = stmt_df[stmt_df["remarks"].str.contains("UPI", case=False, na=False)]
            missing = upi_stmt[~upi_stmt["amount"].astype(float).round(2).isin(base_amounts)]
            for _, row in missing.iterrows():
                errors.append({
                    "id": str(uuid4()), "recon_date": recon_date,
                    "transaction_id": None, "rrn": None,
                    "amount": row.get("amount"), "rail": "DQR",
                    "error_code": "21", "error_message": "Statement entry missing in base",
                    "step": STEP_NAME, "source_file": "BANK_STATEMENT",
                })

        # Credit validation for instant settlement
        upi_success = repo.get_success_transactions_df(recon_date)
        upi_total = float(upi_success["amount"].sum()) if not upi_success.empty else 0.0
        stmt_total = float(stmt_df["amount"].sum()) if not stmt_df.empty else 0.0
        diff = round(upi_total - stmt_total, 2)

        repo.upsert_credit_validation({
            "recon_date": recon_date, "rail": "DQR",
            "validation_type": "DQR_INSTANT",
            "total_txn_count": len(upi_success),
            "total_txn_amount": upi_total,
            "statement_amount": stmt_total,
            "difference": diff,
            "status": "MATCHED" if diff == 0 else "MISMATCH",
            "notes": f"Diff={diff}",
        })

        inserted_errors = repo.batch_insert_errors(errors)
        logger.info(f"[{STEP_NAME}] Errors={inserted_errors}, Credit diff={diff}")

        repo.mark_step_complete(recon_date, STEP_NAME, inserted_errors)
        return {"errors": inserted_errors, "credit_diff": diff}

    except Exception as e:
        logger.error(f"[{STEP_NAME}] FAILED: {e}", exc_info=True)
        repo.mark_step_failed(recon_date, STEP_NAME, str(e))
        raise


def _s(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    return s if s and s.lower() not in ("nan", "none") else None


# ============================================================
"""
STEP 11 — Bulk vs Statement Validation
Compare Bulk Settlement File vs Bank Statement for DQR/SQR.
"""
# ============================================================

STEP_NAME_11 = "STEP_11_BULK_VS_STATEMENT"


def run_step11(
    repo: ReconRepository,
    recon_date: date,
    bulk_settlement_path: str,
    bank_statement_path: str,
    force: bool = False,
) -> dict:
    from ingestion.file_loader import load_bulk_settlement
    from ingestion.schema_normalizer import normalize_bulk_settlement

    if not force and repo.step_already_complete(recon_date, STEP_NAME_11):
        logger.info(f"{STEP_NAME_11} already complete for {recon_date}, skipping.")
        return {}

    repo.mark_step_start(recon_date, STEP_NAME_11)
    logger.info(f"[{STEP_NAME_11}] Starting for {recon_date}")

    try:
        raw_bulk = load_bulk_settlement(bulk_settlement_path)
        raw_stmt = load_bank_statement(bank_statement_path)

        bulk_df = normalize_bulk_settlement(raw_bulk, recon_date) if not raw_bulk.empty else pd.DataFrame()
        stmt_df = normalize_bank_statement(raw_stmt, recon_date) if not raw_stmt.empty else pd.DataFrame()

        # Filter SUCCESS only
        if not bulk_df.empty and "status" in bulk_df.columns:
            bulk_df = bulk_df[bulk_df["status"] == "SUCCESS"]

        bulk_total = float(bulk_df["amount"].sum()) if not bulk_df.empty else 0.0
        stmt_total = float(stmt_df["amount"].sum()) if not stmt_df.empty else 0.0
        diff = round(bulk_total - stmt_total, 2)

        repo.upsert_credit_validation({
            "recon_date": recon_date, "rail": "DQR",
            "validation_type": "DQR_BULK_VS_STATEMENT",
            "total_txn_count": len(bulk_df),
            "total_txn_amount": bulk_total,
            "statement_amount": stmt_total,
            "difference": diff,
            "status": "MATCHED" if diff == 0 else "MISMATCH",
            "notes": f"Bulk={bulk_total}, Statement={stmt_total}, Diff={diff}",
        })

        logger.info(f"[{STEP_NAME_11}] Bulk={bulk_total}, Stmt={stmt_total}, Diff={diff}")
        repo.mark_step_complete(recon_date, STEP_NAME_11, 1)
        return {"bulk_total": bulk_total, "stmt_total": stmt_total, "diff": diff}

    except Exception as e:
        logger.error(f"[{STEP_NAME_11}] FAILED: {e}", exc_info=True)
        repo.mark_step_failed(recon_date, STEP_NAME_11, str(e))
        raise


# ============================================================
"""
STEP 12 — Bijlipay vs Host Reconciliation
Compare entire bijlipay_transaction_base vs host_transaction_base.

HR-001: Missing in host
HR-002: Missing in bijlipay
HR-003: Status mismatch
HR-004: Amount mismatch
HR-005: Identifier mismatch
HR-006: Duplicate in host
"""
# ============================================================

STEP_NAME_12 = "STEP_12_BIJLIPAY_VS_HOST"


def run_step12(
    repo: ReconRepository,
    recon_date: date,
    force: bool = False,
) -> dict:
    if not force and repo.step_already_complete(recon_date, STEP_NAME_12):
        logger.info(f"{STEP_NAME_12} already complete for {recon_date}, skipping.")
        return {}

    repo.mark_step_start(recon_date, STEP_NAME_12)
    logger.info(f"[{STEP_NAME_12}] Starting for {recon_date}")

    try:
        base_df = repo.get_transactions_df(recon_date, rail="CARD")
        host_df = repo.get_host_transactions_df(recon_date)

        errors = []

        if base_df.empty and host_df.empty:
            repo.mark_step_complete(recon_date, STEP_NAME_12, 0)
            return {}

        # Build maps keyed by RRN
        base_map = _build_rrn_map(base_df) if not base_df.empty else {}
        host_map = _build_rrn_map_host(host_df) if not host_df.empty else {}

        # Detect duplicates in host
        host_rrn_counts = host_df["rrn"].value_counts() if not host_df.empty else pd.Series()
        host_duplicates = set(host_rrn_counts[host_rrn_counts > 1].index.astype(str).values)
        for rrn in host_duplicates:
            errors.append({
                "id": str(uuid4()), "recon_date": recon_date,
                "transaction_id": None, "rrn": rrn,
                "amount": None, "rail": "CARD",
                "error_code": "HR-006", "error_message": f"Duplicate RRN {rrn} in host",
                "step": STEP_NAME_12, "source_file": "HOST_BASE",
            })

        # HR-001: In bijlipay base but missing in host
        for rrn, base_row in base_map.items():
            if rrn not in host_map:
                errors.append({
                    "id": str(uuid4()), "recon_date": recon_date,
                    "transaction_id": str(base_row.get("id", "")),
                    "rrn": rrn, "amount": base_row.get("amount"), "rail": "CARD",
                    "error_code": "HR-001", "error_message": "Transaction missing in host",
                    "step": STEP_NAME_12, "source_file": "BASE",
                })
            else:
                host_row = host_map[rrn]
                # HR-003: Status mismatch
                if str(base_row.get("status", "")).upper() != str(host_row.get("status", "")).upper():
                    errors.append({
                        "id": str(uuid4()), "recon_date": recon_date,
                        "transaction_id": str(base_row.get("id", "")),
                        "rrn": rrn, "amount": base_row.get("amount"), "rail": "CARD",
                        "error_code": "HR-003",
                        "error_message": f"Status mismatch: base={base_row.get('status')} host={host_row.get('status')}",
                        "step": STEP_NAME_12, "source_file": "BASE",
                    })
                # HR-004: Amount mismatch
                try:
                    b_amt = round(float(base_row.get("amount") or 0), 2)
                    h_amt = round(float(host_row.get("amount") or 0), 2)
                    if b_amt != h_amt:
                        errors.append({
                            "id": str(uuid4()), "recon_date": recon_date,
                            "transaction_id": str(base_row.get("id", "")),
                            "rrn": rrn, "amount": b_amt, "rail": "CARD",
                            "error_code": "HR-004",
                            "error_message": f"Amount mismatch: base={b_amt} host={h_amt}",
                            "step": STEP_NAME_12, "source_file": "BASE",
                        })
                except (TypeError, ValueError):
                    pass
                # HR-005: TID mismatch
                b_tid = str(base_row.get("tid", "") or "").strip()
                h_tid = str(host_row.get("tid", "") or "").strip()
                if b_tid and h_tid and b_tid != h_tid:
                    errors.append({
                        "id": str(uuid4()), "recon_date": recon_date,
                        "transaction_id": str(base_row.get("id", "")),
                        "rrn": rrn, "amount": base_row.get("amount"), "rail": "CARD",
                        "error_code": "HR-005",
                        "error_message": f"TID mismatch: base={b_tid} host={h_tid}",
                        "step": STEP_NAME_12, "source_file": "BASE",
                    })

        # HR-002: In host but missing in bijlipay base
        for rrn in host_map:
            if rrn not in base_map:
                host_row = host_map[rrn]
                errors.append({
                    "id": str(uuid4()), "recon_date": recon_date,
                    "transaction_id": None, "rrn": rrn,
                    "amount": host_row.get("amount"), "rail": "CARD",
                    "error_code": "HR-002", "error_message": "Transaction missing in Bijlipay base",
                    "step": STEP_NAME_12, "source_file": "HOST_BASE",
                })

        inserted_errors = repo.batch_insert_errors(errors)
        logger.info(f"[{STEP_NAME_12}] Errors={inserted_errors}")

        repo.mark_step_complete(recon_date, STEP_NAME_12, inserted_errors)
        return {"errors": inserted_errors}

    except Exception as e:
        logger.error(f"[{STEP_NAME_12}] FAILED: {e}", exc_info=True)
        repo.mark_step_failed(recon_date, STEP_NAME_12, str(e))
        raise


def _build_rrn_map(df: pd.DataFrame) -> dict:
    return {str(row["rrn"]): row for _, row in df.iterrows()}


def _build_rrn_map_host(df: pd.DataFrame) -> dict:
    result = {}
    for _, row in df.iterrows():
        rrn = str(row["rrn"])
        if rrn not in result:
            result[rrn] = row
    return result
