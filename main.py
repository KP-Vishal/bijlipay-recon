"""
Bijlipay Reconciliation Engine — Main Pipeline Orchestrator

Usage:
  python main.py --date 2025-10-13 --input-dir /path/to/files

All 12 steps run in sequence. Each step is idempotent.
Re-running for the same date is safe.
"""
import argparse
import logging
import os
import sys
from datetime import date, datetime
from pathlib import Path

from db.repository import ReconRepository
from reports.recon_report import generate_all_reports

import steps.step_1_build_bijlipay_base as step1
import steps.step_2_dqr_consistency     as step2
import steps.step_3_ack_validation      as step3
import steps.step_4_refund_cancel       as step4
import steps.step_5_card_prepayout      as step5
import steps.step_6_generate_payout     as step6
import steps.step_7_build_host_base     as step7
import steps.step_8_host_recon          as step8
import steps.step_9_credit_validation   as step9
from steps.step_10_11_12 import run as run_step10, run_step11, run_step12

# ------------------------------------------------------------------ #
#  LOGGING                                                             #
# ------------------------------------------------------------------ #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("recon_engine.log"),
    ]
)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------ #
#  FILE RESOLVER                                                       #
# ------------------------------------------------------------------ #

def resolve(input_dir: str, pattern: str) -> str:
    """Find a file matching a pattern in the input directory. Returns first match or empty string."""
    base = Path(input_dir)
    import fnmatch
    for f in base.iterdir():
        if fnmatch.fnmatch(f.name, pattern):
            return str(f)
    logger.warning(f"No file matching '{pattern}' found in {input_dir}")
    return ""


def build_file_map(input_dir: str, recon_date: date) -> dict:
    """
    Map each BRD file to an actual path in input_dir.
    Patterns match the naming conventions from your real files.
    """
    d = recon_date.strftime("%Y%m%d")
    d2 = recon_date.strftime("%d%m%Y")

    return {
        "jupiter_atf":              resolve(input_dir, "TransactionList_*"),
        "ogs_atf":                  resolve(input_dir, "Transaction_Report_*"),
        "axis_settlement_sqr":      resolve(input_dir, "AxisSettlement_*"),
        "upi_settlement_jupiter":   resolve(input_dir, "UPI_Settlement-*Jupiter*"),
        "upi_settlement_ogs":       resolve(input_dir, "UPI_Settlement-*.csv"),
        "offline_refund":           resolve(input_dir, "OffilineRefund-*"),
        "upi_refund":               resolve(input_dir, "UPI_Refund_BIJLIPAY_*"),
        "upi_cancel":               resolve(input_dir, "UPI_cancel_Txns-*"),
        "wl_aggregator":            resolve(input_dir, "*Bijlipay_Aggr_Report*"),
        "axis_aggregator_ack":      resolve(input_dir, "axis_aggre_*"),
        "wl_atf":                   resolve(input_dir, "AxisBijliPay_*"),
        "axis_mpr":                 resolve(input_dir, "*_MPR_FILE_*") or resolve(input_dir, "*MPR*FILE*"),
        "bank_statement":           resolve(input_dir, "*BANK_STATMEMNT*"),
        "mis_dynamic":              resolve(input_dir, "*Dynamic*") or resolve(input_dir, "*Dyanmic*"),
        "mis_static":               resolve(input_dir, "*static*"),
        "bulk_settlement":          resolve(input_dir, "UPI_Sett_BIJLIPAY_*"),
    }


# ------------------------------------------------------------------ #
#  MAIN PIPELINE                                                       #
# ------------------------------------------------------------------ #

def run_pipeline(
    db_url: str,
    recon_date: date,
    input_dir: str,
    output_dir: str,
    force: bool = False,
):
    logger.info("=" * 70)
    logger.info(f"  Bijlipay Reconciliation Engine")
    logger.info(f"  Date: {recon_date}  |  Input: {input_dir}")
    logger.info("=" * 70)

    repo = ReconRepository(db_url)
    repo.create_tables()

    files = build_file_map(input_dir, recon_date)
    logger.info(f"\nFile map resolved:")
    for k, v in files.items():
        status = "✓" if v else "✗ MISSING"
        logger.info(f"  {k:<30} {status} {v}")

    results = {}

    # ── STEP 1 ── Build transaction base
    logger.info("\n" + "─"*60)
    logger.info("STEP 1 — Build Bijlipay Transaction Base")
    results["step1"] = step1.run(
        repo, recon_date,
        files["jupiter_atf"],
        files["ogs_atf"],
        files["axis_settlement_sqr"],
        force=force,
    )

    # ── STEP 2 ── DQR consistency
    logger.info("\n" + "─"*60)
    logger.info("STEP 2 — DQR Middleware Consistency Check")
    results["step2"] = step2.run(
        repo, recon_date,
        files["upi_settlement_jupiter"],
        files["upi_settlement_ogs"],
        force=force,
    )

    # ── STEP 3 ── ACK Validation
    logger.info("\n" + "─"*60)
    logger.info("STEP 3 — ACK Validation")
    results["step3"] = step3.run(
        repo, recon_date,
        files["axis_aggregator_ack"],
        force=force,
    )

    # ── STEP 4 ── Refund / Cancel
    logger.info("\n" + "─"*60)
    logger.info("STEP 4 — Refund / Cancel Processing")
    results["step4"] = step4.run(
        repo, recon_date,
        files["offline_refund"],
        files["upi_refund"],
        files["upi_cancel"],
        force=force,
    )

    # ── STEP 5 ── Card Aggregator Recon
    logger.info("\n" + "─"*60)
    logger.info("STEP 5 — Card Aggregator Reconciliation")
    results["step5"] = step5.run(
        repo, recon_date,
        files["wl_aggregator"],
        force=force,
    )

    # ── STEP 6 ── Generate Payout
    logger.info("\n" + "─"*60)
    logger.info("STEP 6 — Generate Payout Files")
    results["step6"] = step6.run(
        repo, recon_date,
        output_dir=output_dir,
        force=force,
    )

    # ── STEP 7 ── Build Host Base
    logger.info("\n" + "─"*60)
    logger.info("STEP 7 — Build Host Transaction Base")
    results["step7"] = step7.run(
        repo, recon_date,
        files["wl_atf"],
        files["wl_aggregator"],
        force=force,
    )

    # ── STEP 8 ── Credit Validation CARD
    logger.info("\n" + "─"*60)
    logger.info("STEP 8 — Credit Validation (CARD)")
    results["step8"] = step8.run(
        repo, recon_date,
        files["axis_mpr"],
        files["bank_statement"],
        force=force,
    )

    # ── STEP 9 ── Credit Validation DQR/SQR
    logger.info("\n" + "─"*60)
    logger.info("STEP 9 — Credit Validation (DQR/SQR Bulk)")
    results["step9"] = step9.run(
        repo, recon_date,
        files["mis_dynamic"],
        files["mis_static"],
        files["bulk_settlement"],
        force=force,
    )

    # ── STEP 10 ── Instant Settlement
    logger.info("\n" + "─"*60)
    logger.info("STEP 10 — Instant Settlement Validation")
    results["step10"] = run_step10(
        repo, recon_date,
        files["bank_statement"],
        force=force,
    )

    # ── STEP 11 ── Bulk vs Statement
    logger.info("\n" + "─"*60)
    logger.info("STEP 11 — Bulk vs Statement Validation")
    results["step11"] = run_step11(
        repo, recon_date,
        files["bulk_settlement"],
        files["bank_statement"],
        force=force,
    )

    # ── STEP 12 ── Bijlipay vs Host
    logger.info("\n" + "─"*60)
    logger.info("STEP 12 — Bijlipay vs Host Reconciliation")
    results["step12"] = run_step12(
        repo, recon_date,
        force=force,
    )

    # ── REPORTS ──
    logger.info("\n" + "─"*60)
    logger.info("Generating Reports...")
    report_files = generate_all_reports(repo, recon_date, output_dir)

    logger.info("\n" + "="*70)
    logger.info("  PIPELINE COMPLETE")
    logger.info(f"  Reports written to: {output_dir}")
    logger.info("="*70)

    return {"steps": results, "reports": report_files}


# ------------------------------------------------------------------ #
#  CLI ENTRY POINT                                                     #
# ------------------------------------------------------------------ #

def main():
    parser = argparse.ArgumentParser(description="Bijlipay Reconciliation Engine")
    parser.add_argument("--date",       required=True,  help="Recon date YYYY-MM-DD")
    parser.add_argument("--input-dir",  required=True,  help="Directory containing input files")
    parser.add_argument("--output-dir", default="./output", help="Directory for output reports")
    parser.add_argument("--db-url",     default=os.getenv("DATABASE_URL", "postgresql://bijlipay:bijlipay@localhost:5432/recon"), help="PostgreSQL connection URL")
    parser.add_argument("--force",      action="store_true", help="Force re-run even if steps are already complete")

    args = parser.parse_args()

    try:
        recon_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    except ValueError:
        print(f"ERROR: Invalid date format '{args.date}'. Use YYYY-MM-DD.")
        sys.exit(1)

    run_pipeline(
        db_url=args.db_url,
        recon_date=recon_date,
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        force=args.force,
    )


if __name__ == "__main__":
    main()
