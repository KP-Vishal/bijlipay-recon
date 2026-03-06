"""
Database Repository — Bijlipay Reconciliation Engine
All DB operations go through this layer.
Uses batch inserts for performance (1M+ txn/day).
"""
import logging
from datetime import date, datetime
from typing import List, Dict, Optional, Any
from uuid import UUID, uuid4

import pandas as pd
from sqlalchemy import create_engine, text, select, and_, or_
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.dialects.postgresql import insert as pg_insert

from models.transaction import (
    Base, BijlipayTransactionBase, TransactionError,
    Adjustment, Payout, HostTransactionBase, CreditValidation, ReconRunLog
)

logger = logging.getLogger(__name__)


class ReconRepository:
    def __init__(self, db_url: str):
        self.engine = create_engine(db_url, pool_size=10, max_overflow=20, echo=False)
        self.SessionLocal = sessionmaker(bind=self.engine)

    def create_tables(self):
        Base.metadata.create_all(self.engine)
        logger.info("All tables created/verified.")

    def get_session(self) -> Session:
        return self.SessionLocal()

    # ------------------------------------------------------------------ #
    #  IDEMPOTENCY GUARD                                                   #
    # ------------------------------------------------------------------ #
    def step_already_complete(self, recon_date: date, step: str) -> bool:
        with self.get_session() as s:
            row = s.execute(
                text("SELECT status FROM recon_run_log WHERE recon_date=:d AND step=:s"),
                {"d": recon_date, "s": step}
            ).fetchone()
            return row is not None and row[0] == "COMPLETE"

    def mark_step_start(self, recon_date: date, step: str):
        with self.get_session() as s:
            s.execute(
                text("""
                    INSERT INTO recon_run_log (id, recon_date, step, status, started_at)
                    VALUES (:id, :d, :s, 'RUNNING', NOW())
                    ON CONFLICT (recon_date, step) DO UPDATE
                    SET status='RUNNING', started_at=NOW(), error_msg=NULL
                """),
                {"id": str(uuid4()), "d": recon_date, "s": step}
            )
            s.commit()

    def mark_step_complete(self, recon_date: date, step: str, rows: int = 0):
        with self.get_session() as s:
            s.execute(
                text("""
                    UPDATE recon_run_log
                    SET status='COMPLETE', completed_at=NOW(), rows_processed=:rows
                    WHERE recon_date=:d AND step=:s
                """),
                {"d": recon_date, "s": step, "rows": rows}
            )
            s.commit()

    def mark_step_failed(self, recon_date: date, step: str, error: str):
        with self.get_session() as s:
            s.execute(
                text("""
                    UPDATE recon_run_log
                    SET status='FAILED', completed_at=NOW(), error_msg=:err
                    WHERE recon_date=:d AND step=:s
                """),
                {"d": recon_date, "s": step, "err": error}
            )
            s.commit()

    # ------------------------------------------------------------------ #
    #  TRANSACTION BASE                                                    #
    # ------------------------------------------------------------------ #
    def batch_upsert_transactions(self, records: List[Dict]) -> int:
        """
        Upsert transactions using ON CONFLICT DO NOTHING.
        Financial identifiers (rrn, tid, amount) are NEVER modified.
        Returns count of inserted rows.
        """
        if not records:
            return 0
        with self.get_session() as s:
            stmt = pg_insert(BijlipayTransactionBase.__table__).values(records)
            stmt = stmt.on_conflict_do_nothing()
            result = s.execute(stmt)
            s.commit()
            return result.rowcount

    def get_transaction_by_natural_key(
        self, recon_date: date, rrn: str, rail: str,
        tid: str = None, amount: float = None, order_id: str = None
    ) -> Optional[BijlipayTransactionBase]:
        with self.get_session() as s:
            if rail == "CARD":
                return s.query(BijlipayTransactionBase).filter(
                    BijlipayTransactionBase.recon_date == recon_date,
                    BijlipayTransactionBase.rrn == rrn,
                    BijlipayTransactionBase.tid == tid,
                    BijlipayTransactionBase.amount == amount,
                ).first()
            else:
                if order_id:
                    return s.query(BijlipayTransactionBase).filter(
                        BijlipayTransactionBase.recon_date == recon_date,
                        BijlipayTransactionBase.rrn == rrn,
                        BijlipayTransactionBase.amount == amount,
                        BijlipayTransactionBase.order_id == order_id,
                    ).first()
                else:
                    return s.query(BijlipayTransactionBase).filter(
                        BijlipayTransactionBase.recon_date == recon_date,
                        BijlipayTransactionBase.rrn == rrn,
                        BijlipayTransactionBase.amount == amount,
                    ).first()

    def get_transactions_df(self, recon_date: date, rail: str = None) -> pd.DataFrame:
        query = f"SELECT * FROM bijlipay_transaction_base WHERE recon_date = '{recon_date}'"
        if rail:
            query += f" AND rail = '{rail}'"
        return pd.read_sql(query, self.engine)

    def get_success_transactions_df(self, recon_date: date, rail: str = None) -> pd.DataFrame:
        query = f"SELECT * FROM bijlipay_transaction_base WHERE recon_date='{recon_date}' AND status='SUCCESS'"
        if rail:
            query += f" AND rail='{rail}'"
        return pd.read_sql(query, self.engine)

    # ------------------------------------------------------------------ #
    #  ERRORS                                                              #
    # ------------------------------------------------------------------ #
    def batch_insert_errors(self, error_records: List[Dict]) -> int:
        """Insert errors, skipping duplicates (same txn + error_code)."""
        if not error_records:
            return 0
        with self.get_session() as s:
            stmt = pg_insert(TransactionError.__table__).values(error_records)
            stmt = stmt.on_conflict_do_nothing(constraint="uq_txn_error")
            result = s.execute(stmt)
            s.commit()
            return result.rowcount

    def get_errors_df(self, recon_date: date) -> pd.DataFrame:
        query = f"""
            SELECT e.*, t.rail, t.rrn as base_rrn, t.tid as base_tid
            FROM transaction_errors e
            LEFT JOIN bijlipay_transaction_base t ON e.transaction_id = t.id
            WHERE e.recon_date = '{recon_date}'
        """
        return pd.read_sql(query, self.engine)

    def get_transactions_with_errors_df(self, recon_date: date) -> pd.DataFrame:
        """Get transaction IDs that have blocking error codes."""
        query = f"""
            SELECT DISTINCT transaction_id, array_agg(error_code) as error_codes
            FROM transaction_errors
            WHERE recon_date = '{recon_date}'
            GROUP BY transaction_id
        """
        return pd.read_sql(query, self.engine)

    # ------------------------------------------------------------------ #
    #  ADJUSTMENTS                                                         #
    # ------------------------------------------------------------------ #
    def batch_insert_adjustments(self, records: List[Dict]) -> int:
        if not records:
            return 0
        with self.get_session() as s:
            s.bulk_insert_mappings(Adjustment, records)
            s.commit()
            return len(records)

    def get_adjustments_df(self, recon_date: date) -> pd.DataFrame:
        return pd.read_sql(
            f"SELECT * FROM adjustments WHERE recon_date='{recon_date}'",
            self.engine
        )

    # ------------------------------------------------------------------ #
    #  PAYOUTS                                                             #
    # ------------------------------------------------------------------ #
    def batch_insert_payouts(self, records: List[Dict]) -> int:
        if not records:
            return 0
        with self.get_session() as s:
            stmt = pg_insert(Payout.__table__).values(records)
            stmt = stmt.on_conflict_do_nothing()
            result = s.execute(stmt)
            s.commit()
            return result.rowcount

    def get_payouts_df(self, recon_date: date, rail: str = None) -> pd.DataFrame:
        query = f"SELECT * FROM payouts WHERE recon_date='{recon_date}'"
        if rail:
            query += f" AND rail='{rail}'"
        return pd.read_sql(query, self.engine)

    def get_payouts_by_rrn(self, rrn: str, days_back: int = 2) -> pd.DataFrame:
        """Used in refund step to find original transaction in previous payouts."""
        query = f"""
            SELECT * FROM payouts
            WHERE rrn = '{rrn}'
            AND recon_date >= CURRENT_DATE - INTERVAL '{days_back} days'
        """
        return pd.read_sql(query, self.engine)

    # ------------------------------------------------------------------ #
    #  HOST TRANSACTION BASE                                               #
    # ------------------------------------------------------------------ #
    def batch_insert_host_transactions(self, records: List[Dict]) -> int:
        if not records:
            return 0
        with self.get_session() as s:
            stmt = pg_insert(HostTransactionBase.__table__).values(records)
            stmt = stmt.on_conflict_do_nothing()
            result = s.execute(stmt)
            s.commit()
            return result.rowcount

    def get_host_transactions_df(self, recon_date: date) -> pd.DataFrame:
        return pd.read_sql(
            f"SELECT * FROM host_transaction_base WHERE recon_date='{recon_date}'",
            self.engine
        )

    # ------------------------------------------------------------------ #
    #  CREDIT VALIDATION                                                   #
    # ------------------------------------------------------------------ #
    def upsert_credit_validation(self, record: Dict):
        with self.get_session() as s:
            s.execute(
                text("""
                    INSERT INTO credit_validation
                        (id, recon_date, rail, validation_type, total_txn_count,
                         total_txn_amount, statement_amount, difference, status, notes)
                    VALUES
                        (:id, :rd, :rail, :vtype, :cnt, :txn_amt, :stmt_amt, :diff, :stat, :notes)
                    ON CONFLICT DO NOTHING
                """),
                {
                    "id": str(uuid4()),
                    "rd": record["recon_date"],
                    "rail": record["rail"],
                    "vtype": record["validation_type"],
                    "cnt": record.get("total_txn_count", 0),
                    "txn_amt": record.get("total_txn_amount", 0),
                    "stmt_amt": record.get("statement_amount", 0),
                    "diff": record.get("difference", 0),
                    "stat": record.get("status", "UNKNOWN"),
                    "notes": record.get("notes", ""),
                }
            )
            s.commit()

    def get_credit_validation_df(self, recon_date: date) -> pd.DataFrame:
        return pd.read_sql(
            f"SELECT * FROM credit_validation WHERE recon_date='{recon_date}'",
            self.engine
        )

    # ------------------------------------------------------------------ #
    #  HELPERS                                                             #
    # ------------------------------------------------------------------ #
    def clear_recon_date(self, recon_date: date):
        """Full reset for a recon date — use with caution."""
        with self.get_session() as s:
            s.execute(text("DELETE FROM transaction_errors WHERE recon_date=:d"), {"d": recon_date})
            s.execute(text("DELETE FROM payouts WHERE recon_date=:d"), {"d": recon_date})
            s.execute(text("DELETE FROM adjustments WHERE recon_date=:d"), {"d": recon_date})
            s.execute(text("DELETE FROM host_transaction_base WHERE recon_date=:d"), {"d": recon_date})
            s.execute(text("DELETE FROM credit_validation WHERE recon_date=:d"), {"d": recon_date})
            s.execute(text("DELETE FROM bijlipay_transaction_base WHERE recon_date=:d"), {"d": recon_date})
            s.execute(text("DELETE FROM recon_run_log WHERE recon_date=:d"), {"d": recon_date})
            s.commit()
        logger.warning(f"Cleared all data for recon_date={recon_date}")
