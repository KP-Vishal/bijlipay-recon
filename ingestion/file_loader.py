"""
File Loader — Bijlipay Reconciliation Engine
Loads all 14 input files into DataFrames.
Handles CSV, Excel, XLSB, pipe-separated formats.
"""
import logging
import os
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd
import yaml

logger = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).parent.parent / "config" / "column_mappings.yaml"


def _load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def _read_file(path: str, sep: str = ",", encoding: str = "utf-8", file_type: str = "csv") -> pd.DataFrame:
    """Universal file reader supporting csv, xlsx, xlsb, xls."""
    path = Path(path)
    if not path.exists():
        logger.warning(f"File not found: {path}")
        return pd.DataFrame()

    ext = path.suffix.lower()

    try:
        if file_type == "csv" or ext == ".csv":
            # Try multiple encodings
            for enc in [encoding, "utf-8", "iso-8859-1", "cp1252"]:
                try:
                    df = pd.read_csv(path, sep=sep, encoding=enc, dtype=str, low_memory=False)
                    df.columns = df.columns.str.strip()
                    return df
                except UnicodeDecodeError:
                    continue
            return pd.DataFrame()

        elif file_type in ("xlsx", "xls") or ext in (".xlsx", ".xls"):
            df = pd.read_excel(path, dtype=str)
            df.columns = df.columns.str.strip()
            return df

        elif file_type == "xlsb" or ext == ".xlsb":
            try:
                import pyxlsb
                df = pd.read_excel(path, engine="pyxlsb", dtype=str)
                df.columns = df.columns.str.strip()
                return df
            except ImportError:
                logger.error("pyxlsb not installed. Install with: pip install pyxlsb")
                return pd.DataFrame()

        else:
            # Fallback to CSV
            df = pd.read_csv(path, sep=sep, dtype=str, low_memory=False)
            df.columns = df.columns.str.strip()
            return df

    except Exception as e:
        logger.error(f"Error reading {path}: {e}")
        return pd.DataFrame()


def load_file(file_key: str, file_path: str) -> pd.DataFrame:
    """
    Load a specific file using its config key.
    Returns raw DataFrame with original column names.
    """
    cfg = _load_config()
    if file_key not in cfg:
        logger.warning(f"Unknown file key: {file_key}")
        return _read_file(file_path)

    fc = cfg[file_key]
    sep = fc.get("separator", ",")
    encoding = fc.get("encoding", "utf-8")
    file_type = fc.get("file_type", "csv")

    df = _read_file(file_path, sep=sep, encoding=encoding, file_type=file_type)
    logger.info(f"Loaded {file_key}: {len(df)} rows from {file_path}")
    return df


# ------------------------------------------------------------------ #
#  Individual loaders by file role                                    #
# ------------------------------------------------------------------ #

def load_jupiter_atf(path: str) -> pd.DataFrame:
    return load_file("jupiter_atf", path)

def load_ogs_atf(path: str) -> pd.DataFrame:
    return load_file("ogs_atf", path)

def load_axis_settlement_sqr(path: str) -> pd.DataFrame:
    return load_file("axis_settlement_sqr", path)

def load_upi_settlement_jupiter(path: str) -> pd.DataFrame:
    return load_file("upi_settlement_jupiter", path)

def load_upi_settlement_ogs(path: str) -> pd.DataFrame:
    return load_file("upi_settlement_ogs", path)

def load_offline_refund(path: str) -> pd.DataFrame:
    return load_file("offline_refund", path)

def load_upi_cancel(path: str) -> pd.DataFrame:
    return load_file("upi_cancel", path)

def load_wl_aggregator(path: str) -> pd.DataFrame:
    return load_file("wl_aggregator", path)

def load_axis_aggregator_ack(path: str) -> pd.DataFrame:
    return load_file("axis_aggregator_ack", path)

def load_wl_atf(path: str) -> pd.DataFrame:
    return load_file("wl_atf", path)

def load_axis_mpr(path: str) -> pd.DataFrame:
    return load_file("axis_mpr", path)

def load_bank_statement(path: str) -> pd.DataFrame:
    return load_file("bank_statement", path)

def load_mis_dynamic(path: str) -> pd.DataFrame:
    return load_file("mis_dynamic", path)

def load_mis_static(path: str) -> pd.DataFrame:
    return load_file("mis_static", path)

def load_bulk_settlement(path: str) -> pd.DataFrame:
    return load_file("bulk_settlement", path)

def load_upi_refund(path: str) -> pd.DataFrame:
    return load_file("upi_refund", path)
