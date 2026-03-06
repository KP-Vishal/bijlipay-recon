"""
Schema Normalizer — Bijlipay Reconciliation Engine

Converts raw DataFrames from each input file into a unified
transaction schema for the reconciliation pipeline.

Rules:
- response_code "00" → SUCCESS, anything else → FAILURE
- Rail classification: CARD / DQR / SQR
- Amount normalization: strip leading zeros, convert to float
- Date normalization: multiple input formats → date object
- Natural key: (rrn, tid, amount) for CARD; (rrn, amount, order_id) for DQR/SQR
"""
import logging
import re
from datetime import date, datetime
from typing import Optional

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------ #
#  AMOUNT NORMALIZATION                                               #
# ------------------------------------------------------------------ #

def normalize_amount(val) -> Optional[float]:
    """
    Handles: '000000100000' (paisa), '3600.00' (rupees), '174.00', None
    Axis card files store amounts as paisa with 12-digit zero padding.
    UPI files store in rupees directly.
    """
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    s = str(val).strip().replace(",", "")
    if not s or s.lower() in ("nan", "none", ""):
        return None
    try:
        f = float(s)
        # Heuristic: if integer and >= 100000 and no decimal in original → paisa
        if "." not in s and f >= 100:
            # Check if looks like paisa (12-digit Axis format)
            if len(s) >= 8 and f == int(f):
                return round(f / 100, 2)
        return round(f, 2)
    except ValueError:
        return None


def normalize_amount_rupees(val) -> Optional[float]:
    """For files that are already in rupees (UPI settlements, MPR)."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    s = str(val).strip().replace(",", "")
    if not s or s.lower() in ("nan", "none", ""):
        return None
    try:
        return round(float(s), 2)
    except ValueError:
        return None


# ------------------------------------------------------------------ #
#  STATUS NORMALIZATION                                               #
# ------------------------------------------------------------------ #

def normalize_status(response_code) -> str:
    """
    Maps response codes to SUCCESS / FAILURE.
    Only "00" → SUCCESS. Everything else → FAILURE.
    """
    if response_code is None:
        return "FAILURE"
    rc = str(response_code).strip().upper()
    if rc in ("00", "000", "SUCCESS", "APPROVED",
              "TRANSACTION HAS BEEN APPROVED"):
        return "SUCCESS"
    return "FAILURE"


# ------------------------------------------------------------------ #
#  DATE NORMALIZATION                                                 #
# ------------------------------------------------------------------ #

DATE_FORMATS = [
    "%d%m%y",       # 121025  (AxisBijliPay)
    "%d%m%Y",       # 12102025
    "%d%m%y",       # 121025
    "%Y%m%d",       # 20251012
    "%d-%m-%Y",     # 12-10-2025
    "%d-%b-%Y",     # 09-Oct-2025
    "%Y-%m-%d",     # 2025-10-12
    "%d/%m/%Y",     # 12/10/2025
    "%d-%m-%y",     # 12-10-25
    "%m/%d/%Y",     # 10/12/2025
    "%d-%m-%Y %H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S%z",
    "%d-%m-%y %H:%M",
]


def normalize_date(val) -> Optional[date]:
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    s = str(val).strip()
    if not s or s.lower() in ("nan", "none", ""):
        return None

    # Strip timezone info for parsing
    s_clean = re.sub(r"[+-]\d{2}:\d{2}$", "", s).strip()

    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(s_clean[:len(fmt.replace("%Y","0000").replace("%m","00").replace("%d","00").replace("%H","00").replace("%M","00").replace("%S","00"))], fmt).date()
        except (ValueError, TypeError):
            continue

    # pandas fallback
    try:
        return pd.to_datetime(s, dayfirst=True, errors="coerce").date()
    except Exception:
        return None


def _parse_date_series(series: pd.Series) -> pd.Series:
    return series.apply(normalize_date)


# ------------------------------------------------------------------ #
#  RAIL CLASSIFICATION                                                #
# ------------------------------------------------------------------ #

def classify_rail_from_creditvpa(creditvpa: str) -> str:
    """
    Static QR VPAs contain 'staticbp' or 'dqrbp' for dynamic.
    Pattern from actual files:
      dqrbp.40975584@axisbank → DQR
      staticbp.a000000000004568@axisbank → SQR
      bijlipay.10772768@axisbank → DQR (OGS)
    """
    if not creditvpa or pd.isna(creditvpa):
        return "DQR"
    vpa = str(creditvpa).lower()
    if "staticbp" in vpa:
        return "SQR"
    return "DQR"


# ------------------------------------------------------------------ #
#  FILE-SPECIFIC NORMALIZERS                                          #
# ------------------------------------------------------------------ #

def normalize_wl_atf(df: pd.DataFrame, recon_date: date) -> pd.DataFrame:
    """
    AxisBijliPay_*.csv — Worldline ATF (File 9)
    Rail: CARD
    Amount: paisa → rupees (12-digit zero-padded)
    """
    if df.empty:
        return df

    out = pd.DataFrame()
    out["rrn"]              = df["rrn"].astype(str).str.strip()
    out["tid"]              = df["terminal_id"].astype(str).str.strip()
    out["merchant_id"]      = df.get("merchant_id", pd.Series(dtype=str))
    out["merchant_name"]    = df.get("merchant_name", pd.Series(dtype=str))
    out["card_number"]      = df.get("card_number", pd.Series(dtype=str))
    out["auth_id"]          = df.get("auth_id", pd.Series(dtype=str))
    out["response_code"]    = df.get("response_code", pd.Series(dtype=str))
    out["interchange_type"] = df.get("interchange_type", pd.Series(dtype=str))
    out["stan"]             = df.get("stan", pd.Series(dtype=str))
    out["pos_entry_mode"]   = df.get("pos_entry_mode", pd.Series(dtype=str))
    out["credit_debit_flag"]= df.get("credit_debit", pd.Series(dtype=str))
    out["amount"]           = df["transaction_amount"].apply(normalize_amount)
    out["txn_date"]         = _parse_date_series(df["transaction_date"])
    out["status"]           = df["response_code"].apply(normalize_status)
    out["rail"]             = "CARD"
    out["recon_date"]       = recon_date
    out["source_file"]      = "WL_ATF"
    return out.dropna(subset=["rrn", "amount"])


def normalize_wl_aggregator(df: pd.DataFrame, recon_date: date) -> pd.DataFrame:
    """
    00031-Bijlipay_Aggr_Report_*.csv — Worldline Aggregator (File 8)
    Rail: CARD
    No response_code column — all records are settlements → SUCCESS
    """
    if df.empty:
        return df

    out = pd.DataFrame()
    out["rrn"]              = df["RRN"].astype(str).str.strip()
    out["tid"]              = df["TERMINAL_ID"].astype(str).str.strip()
    out["merchant_id"]      = df.get("MERCHANT_ID", pd.Series(dtype=str))
    out["merchant_name"]    = df.get("MERCHANT_NAME", pd.Series(dtype=str))
    out["card_number"]      = df.get("CARD_NUMBER", pd.Series(dtype=str))
    out["auth_id"]          = df.get("AUTH_ID", pd.Series(dtype=str))
    out["interchange_type"] = df.get("INTERCHANGE_TYPE", pd.Series(dtype=str))
    out["stan"]             = df.get("STAN", pd.Series(dtype=str))
    out["pos_entry_mode"]   = df.get("POS_ENTRY_MODE", pd.Series(dtype=str))
    out["credit_debit_flag"]= df.get("CREDIT_DEBIT_FLAG", pd.Series(dtype=str))
    out["amount"]           = df["TRANSACTION_AMOUNT"].apply(normalize_amount)
    out["txn_date"]         = _parse_date_series(df["TRANSACTION_DATE"])
    out["status"]           = "SUCCESS"     # Aggregator report = settled = success
    out["rail"]             = "CARD"
    out["recon_date"]       = recon_date
    out["source_file"]      = "WL_AGGREGATOR"
    return out.dropna(subset=["rrn", "amount"])


def normalize_axis_aggregator_ack(df: pd.DataFrame, recon_date: date) -> pd.DataFrame:
    """
    axis_aggre_*.csv — Internal Axis Aggregator DB export with ACK flags.
    Rail: CARD
    """
    if df.empty:
        return df

    out = pd.DataFrame()
    out["rrn"]              = df["rrn"].astype(str).str.strip()
    out["tid"]              = df["terminal_id"].astype(str).str.strip()
    out["merchant_id"]      = df.get("merchant_id", pd.Series(dtype=str))
    out["merchant_name"]    = df.get("merchant_name", pd.Series(dtype=str))
    out["auth_id"]          = df.get("auth_id", pd.Series(dtype=str))
    out["interchange_type"] = df.get("interchange_type", pd.Series(dtype=str))
    out["stan"]             = df.get("stan", pd.Series(dtype=str))
    out["amount"]           = df["transaction_amount"].apply(normalize_amount)
    out["txn_date"]         = _parse_date_series(df["transaction_date"])
    out["ack_enabled"]      = df.get("ack_enabled", pd.Series(dtype=str))
    out["ack_received"]     = df.get("ack_received", pd.Series(dtype=str))
    out["status"]           = "SUCCESS"
    out["rail"]             = "CARD"
    out["recon_date"]       = recon_date
    out["source_file"]      = "AXIS_AGGREGATOR_ACK"
    return out.dropna(subset=["rrn", "amount"])


def normalize_upi_settlement(df: pd.DataFrame, recon_date: date, source_file: str) -> pd.DataFrame:
    """
    UPI_Settlement-*.csv / AxisSettlement_*.csv — UPI settlements (Files 3,4,5)
    Rail: DQR or SQR based on creditvpa
    Amount: already in rupees
    """
    if df.empty:
        return df

    out = pd.DataFrame()
    out["rrn"]          = df["RRN"].astype(str).str.strip()
    out["txn_id"]       = df.get("TXNID", pd.Series(dtype=str))
    out["order_id"]     = df.get("ORDERID", pd.Series(dtype=str))
    out["amount"]       = df["AMOUNT"].apply(normalize_amount_rupees)
    out["response_code"]= df.get("RESPCODE", pd.Series(dtype=str))
    out["txn_date"]     = _parse_date_series(df.get("TXN_DATE", pd.Series(dtype=str)))
    out["merchant_id"]  = df.get("MERCHANT_ID", df.get("UNQ_CUST ID", pd.Series(dtype=str)))
    out["creditvpa"]    = df.get("CREDITVPA", pd.Series(dtype=str))
    out["vpa"]          = df.get("VPA", pd.Series(dtype=str))
    out["bankname"]     = df.get("BANKNAME", pd.Series(dtype=str))
    out["status"]       = out["response_code"].apply(normalize_status)
    out["rail"]         = out["creditvpa"].apply(classify_rail_from_creditvpa)
    out["recon_date"]   = recon_date
    out["source_file"]  = source_file
    return out.dropna(subset=["rrn", "amount"])


def normalize_axis_settlement_sqr(df: pd.DataFrame, recon_date: date) -> pd.DataFrame:
    return normalize_upi_settlement(df, recon_date, "AXIS_SETTLEMENT_SQR")


def normalize_upi_settlement_jupiter(df: pd.DataFrame, recon_date: date) -> pd.DataFrame:
    return normalize_upi_settlement(df, recon_date, "UPI_SETTLEMENT_JUPITER")


def normalize_upi_settlement_ogs(df: pd.DataFrame, recon_date: date) -> pd.DataFrame:
    return normalize_upi_settlement(df, recon_date, "UPI_SETTLEMENT_OGS")


def normalize_middleware_atf(df: pd.DataFrame, recon_date: date, source: str) -> pd.DataFrame:
    """
    Jupiter/OGS ATF middleware files (Files 1, 2)
    These are xlsx/xlsb exports from middleware systems.
    """
    if df.empty:
        return df

    # Normalize column names to uppercase
    df.columns = df.columns.str.strip().str.upper()

    out = pd.DataFrame()
    out["rrn"]          = df.get("RRN", pd.Series(dtype=str)).astype(str).str.strip()
    out["txn_id"]       = df.get("TXNID", pd.Series(dtype=str))
    out["order_id"]     = df.get("ORDERID", pd.Series(dtype=str))
    out["amount"]       = df.get("AMOUNT", pd.Series(dtype=str)).apply(normalize_amount_rupees)
    out["response_code"]= df.get("RESPCODE", pd.Series(dtype=str))
    out["txn_date"]     = _parse_date_series(df.get("TXN_DATE", pd.Series(dtype=str)))
    out["merchant_id"]  = df.get("MERCHANT_ID", pd.Series(dtype=str))
    out["creditvpa"]    = df.get("CREDITVPA", pd.Series(dtype=str))
    out["vpa"]          = df.get("VPA", pd.Series(dtype=str))
    out["status"]       = out["response_code"].apply(normalize_status)
    out["rail"]         = out["creditvpa"].apply(classify_rail_from_creditvpa)
    out["recon_date"]   = recon_date
    out["source_file"]  = source
    return out.dropna(subset=["rrn", "amount"])


def normalize_offline_refund(df: pd.DataFrame, recon_date: date) -> pd.DataFrame:
    """OffilineRefund-*.csv (File 6)"""
    if df.empty:
        return df

    out = pd.DataFrame()
    out["rrn"]           = df["RRNumber"].astype(str).str.strip()
    out["tid"]           = df.get("TID", pd.Series(dtype=str))
    out["stan"]          = df.get("STAN", pd.Series(dtype=str))
    out["refund_amount"] = df["REFUND AMOUNT"].apply(normalize_amount)
    out["txn_date"]      = _parse_date_series(df["TXN DATE"])
    out["identifier"]    = df.get("Identifier", pd.Series(dtype=str))
    out["recon_date"]    = recon_date
    return out.dropna(subset=["rrn"])


def normalize_upi_refund(df: pd.DataFrame, recon_date: date) -> pd.DataFrame:
    """UPI_Refund_BIJLIPAY_*.csv (pipe-separated)"""
    if df.empty:
        return df

    # Handle pipe-separated single column
    if len(df.columns) == 1:
        header_str = df.columns[0]
        headers = [h.strip() for h in header_str.split("|")]
        rows = df.iloc[:, 0].str.split("|", expand=True)
        rows.columns = headers[:len(rows.columns)]
        df = rows

    out = pd.DataFrame()
    out["rrn"]           = df["RRN"].astype(str).str.strip()
    out["txn_id"]        = df.get("TXNID", pd.Series(dtype=str))
    out["order_id"]      = df.get("ORDERID", pd.Series(dtype=str))
    out["amount"]        = df.get("AMOUNT", pd.Series(dtype=str)).apply(normalize_amount_rupees)
    out["refund_amount"] = df.get("REFUND_AMOUNT", pd.Series(dtype=str)).apply(normalize_amount_rupees)
    out["txn_date"]      = _parse_date_series(df.get("TRANSACTION_DATE", pd.Series(dtype=str)))
    out["refund_date"]   = _parse_date_series(df.get("TXN_REF_DATE", pd.Series(dtype=str)))
    out["merchant_id"]   = df.get("MERCHANT_ID", pd.Series(dtype=str))
    out["response_code"] = df.get("RESPCODE", pd.Series(dtype=str))
    out["refund_type"]   = df.get("REFUND_TYPE", pd.Series(dtype=str))
    out["recon_date"]    = recon_date
    return out.dropna(subset=["rrn"])


def normalize_bulk_settlement(df: pd.DataFrame, recon_date: date) -> pd.DataFrame:
    """UPI_Sett_BIJLIPAY_*.csv (pipe-separated, File 14)"""
    if df.empty:
        return df

    if len(df.columns) == 1:
        header_str = df.columns[0]
        headers = [h.strip() for h in header_str.split("|")]
        rows = df.iloc[:, 0].str.split("|", expand=True)
        rows.columns = headers[:len(rows.columns)]
        df = rows

    return normalize_upi_settlement(df, recon_date, "BULK_SETTLEMENT")


def normalize_upi_cancel(df: pd.DataFrame, recon_date: date) -> pd.DataFrame:
    """UPI_cancel_Txns-*.csv (File 7)"""
    if df.empty:
        return df

    out = pd.DataFrame()
    out["order_id"]     = df.get("ORDERID", pd.Series(dtype=str))
    out["amount"]       = df.get("AMOUNT", pd.Series(dtype=str)).apply(normalize_amount)
    out["terminal_id"]  = df.get("TERMINAL_ID", pd.Series(dtype=str))
    out["merchant_id"]  = df.get("MERCHANT_ID", pd.Series(dtype=str))
    out["txn_date"]     = _parse_date_series(df.get("TXN_DATE", pd.Series(dtype=str)))
    out["cancel_type"]  = df.get("CANCEL_TYPE", pd.Series(dtype=str))
    out["recon_date"]   = recon_date
    return out.dropna(subset=["order_id"])


def normalize_axis_mpr(df: pd.DataFrame, recon_date: date) -> pd.DataFrame:
    """037144000065034_DS_*_MPR_FILE_.csv (File 10)"""
    if df.empty:
        return df

    out = pd.DataFrame()
    out["rrn"]           = df["RRN"].astype(str).str.strip()
    out["tid"]           = df["TERM_ID"].astype(str).str.strip()
    out["merchant_id"]   = df.get("MID", pd.Series(dtype=str))
    out["auth_id"]       = df.get("APPROVE_CODE", pd.Series(dtype=str))
    out["gross_amount"]  = df["GROSS_AMT"].apply(normalize_amount_rupees)
    out["net_amount"]    = df.get("NET_AMT", pd.Series(dtype=str)).apply(normalize_amount_rupees)
    out["txn_date"]      = _parse_date_series(df["TRAN_DATE"])
    out["settlement_date"] = _parse_date_series(df.get("Settlement_Date", pd.Series(dtype=str)))
    out["utr"]           = df.get("UTR", pd.Series(dtype=str))
    out["recon_date"]    = recon_date
    out["source_file"]   = "AXIS_MPR"
    return out.dropna(subset=["rrn"])


def normalize_mis(df: pd.DataFrame, recon_date: date, rail: str) -> pd.DataFrame:
    """MIS_SKILWORTH*_Dynamic.csv and MIS_SKILWORTH*_static.csv (Files 12, 13)"""
    if df.empty:
        return df

    out = pd.DataFrame()
    out["rrn"]          = df["RRN"].astype(str).str.strip()
    out["txn_id"]       = df.get("TXNID", pd.Series(dtype=str))
    out["order_id"]     = df.get("ORDERID", pd.Series(dtype=str))
    out["amount"]       = df["AMOUNT"].apply(normalize_amount_rupees)
    out["response_code"]= df.get("RESPCODE", pd.Series(dtype=str))
    out["txn_date"]     = _parse_date_series(df.get("TXN_DATE", pd.Series(dtype=str)))
    out["creditvpa"]    = df.get("CREDITVPA", pd.Series(dtype=str))
    out["status"]       = out["response_code"].apply(normalize_status)
    out["rail"]         = rail
    out["recon_date"]   = recon_date
    out["source_file"]  = f"MIS_{rail}"
    return out.dropna(subset=["rrn", "amount"])


def normalize_bank_statement(df: pd.DataFrame, recon_date: date) -> pd.DataFrame:
    """922020048692620_*_BANK_STATMEMNT_.csv (File 11)"""
    if df.empty:
        return df

    out = pd.DataFrame()
    out["date"]         = _parse_date_series(df["Date "].str.strip() if "Date " in df.columns else df.iloc[:, 0])
    out["amount"]       = df["Amount"].apply(normalize_amount_rupees)
    out["dr_cr"]        = df.get("DR/CR", pd.Series(dtype=str))
    out["description"]  = df.get("Description", pd.Series(dtype=str))
    out["remarks"]      = df.get("Remarks", pd.Series(dtype=str))
    out["ledger_balance"] = df.get("Ledger Balance", pd.Series(dtype=str)).apply(normalize_amount_rupees)
    out["recon_date"]   = recon_date
    # Only credit entries are relevant for reconciliation
    return out[out["dr_cr"].str.upper().str.strip() == "CR"].dropna(subset=["amount"])
