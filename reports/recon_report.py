"""
Reports — Bijlipay Reconciliation Engine
Generates all output reports as CSV files.
"""
import logging
from datetime import date
from pathlib import Path

import pandas as pd

from db.repository import ReconRepository

logger = logging.getLogger(__name__)


def generate_all_reports(repo: ReconRepository, recon_date: date, output_dir: str = "./output"):
    """Generate all reports for a given recon date."""
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    results = {}

    results["transaction_base"]  = generate_transaction_base_report(repo, recon_date, output_dir)
    results["error_report"]      = generate_error_report(repo, recon_date, output_dir)
    results["payout_report"]     = generate_payout_report(repo, recon_date, output_dir)
    results["credit_validation"] = generate_credit_validation_report(repo, recon_date, output_dir)
    results["adjustment_report"] = generate_adjustment_report(repo, recon_date, output_dir)

    logger.info(f"All reports generated in {output_dir}")
    return results


def generate_transaction_base_report(repo, recon_date, output_dir) -> str:
    df = repo.get_transactions_df(recon_date)
    path = Path(output_dir) / f"transaction_base_{recon_date}.csv"
    df.to_csv(path, index=False)
    logger.info(f"Transaction base report: {path} ({len(df)} rows)")
    return str(path)


def generate_error_report(repo, recon_date, output_dir) -> str:
    df = repo.get_errors_df(recon_date)
    path = Path(output_dir) / f"error_report_{recon_date}.csv"
    df.to_csv(path, index=False)

    # Summary by error code
    if not df.empty:
        summary = df.groupby("error_code").agg(
            count=("id", "count"),
            total_amount=("amount", "sum")
        ).reset_index()
        summary_path = Path(output_dir) / f"error_summary_{recon_date}.csv"
        summary.to_csv(summary_path, index=False)
        logger.info(f"Error report: {path} ({len(df)} errors)")
        _print_error_summary(summary)

    return str(path)


def generate_payout_report(repo, recon_date, output_dir) -> str:
    df = repo.get_payouts_df(recon_date)
    path = Path(output_dir) / f"payout_report_{recon_date}.csv"
    df.to_csv(path, index=False)

    if not df.empty:
        summary = df.groupby("rail").agg(
            count=("id", "count"),
            total_amount=("amount", "sum")
        ).reset_index()
        logger.info(f"\nPayout Summary:")
        for _, row in summary.iterrows():
            logger.info(f"  {row['rail']}: {row['count']} txns, ₹{row['total_amount']:,.2f}")

    return str(path)


def generate_credit_validation_report(repo, recon_date, output_dir) -> str:
    df = repo.get_credit_validation_df(recon_date)
    path = Path(output_dir) / f"credit_validation_{recon_date}.csv"
    df.to_csv(path, index=False)
    logger.info(f"Credit validation report: {path} ({len(df)} rows)")
    return str(path)


def generate_adjustment_report(repo, recon_date, output_dir) -> str:
    df = repo.get_adjustments_df(recon_date)
    path = Path(output_dir) / f"adjustment_report_{recon_date}.csv"
    df.to_csv(path, index=False)
    logger.info(f"Adjustment report: {path} ({len(df)} rows)")
    return str(path)


def _print_error_summary(summary: pd.DataFrame):
    from models.error import ERROR_CODES
    logger.info("\nError Summary:")
    logger.info(f"  {'Code':<10} {'Count':>8} {'Description'}")
    logger.info(f"  {'-'*60}")
    for _, row in summary.sort_values("count", ascending=False).iterrows():
        code = str(row["error_code"])
        desc = ERROR_CODES.get(code, "Unknown")
        logger.info(f"  {code:<10} {int(row['count']):>8}   {desc}")
