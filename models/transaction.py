"""
SQLAlchemy ORM Models — Bijlipay Reconciliation Engine
"""
from datetime import datetime, date
from uuid import uuid4
from sqlalchemy import (
    Column, String, Numeric, Date, DateTime, Integer,
    ForeignKey, Text, UniqueConstraint
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class BijlipayTransactionBase(Base):
    __tablename__ = "bijlipay_transaction_base"

    id              = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    recon_date      = Column(Date, nullable=False, index=True)

    # Natural key
    rrn             = Column(String(50), index=True)
    tid             = Column(String(50), index=True)
    order_id        = Column(String(100))
    txn_date        = Column(Date)

    # Classification
    rail            = Column(String(10), nullable=False, index=True)   # CARD/DQR/SQR
    status          = Column(String(10), nullable=False, index=True)   # SUCCESS/FAILURE
    source_file     = Column(String(100))

    # Financial — IMMUTABLE
    amount          = Column(Numeric(15, 2), nullable=False)

    # Card fields
    merchant_id     = Column(String(50))
    merchant_name   = Column(String(200))
    terminal_id     = Column(String(50))
    card_number     = Column(String(50))
    auth_id         = Column(String(50))
    response_code   = Column(String(10))
    interchange_type = Column(String(10))
    stan            = Column(String(20))
    pos_entry_mode  = Column(String(10))
    credit_debit_flag = Column(String(5))

    # UPI fields
    txn_id          = Column(String(100))
    vpa             = Column(String(200))
    creditvpa       = Column(String(200))
    bankname        = Column(String(100))

    # ACK
    ack_enabled     = Column(String(5))
    ack_received    = Column(String(5))

    created_at      = Column(DateTime, default=datetime.utcnow)
    updated_at      = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    errors          = relationship("TransactionError", back_populates="transaction", cascade="all, delete-orphan")
    payouts         = relationship("Payout", back_populates="transaction")

    __table_args__ = (
        UniqueConstraint("rrn", "tid", "amount", "recon_date", name="uq_card_txn"),
        UniqueConstraint("rrn", "order_id", "amount", "recon_date", name="uq_upi_txn"),
    )

    def natural_key(self):
        if self.rail == "CARD":
            return (str(self.rrn), str(self.tid), str(self.amount))
        else:
            if self.order_id:
                return (str(self.rrn), str(self.amount), str(self.order_id))
            return (str(self.rrn), str(self.amount), str(self.txn_date))

    def __repr__(self):
        return f"<Txn rrn={self.rrn} rail={self.rail} status={self.status} amount={self.amount}>"


class TransactionError(Base):
    __tablename__ = "transaction_errors"

    id              = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    recon_date      = Column(Date, nullable=False, index=True)
    transaction_id  = Column(UUID(as_uuid=True), ForeignKey("bijlipay_transaction_base.id", ondelete="CASCADE"), index=True)
    rrn             = Column(String(50))
    tid             = Column(String(50))
    order_id        = Column(String(100))
    amount          = Column(Numeric(15, 2))
    rail            = Column(String(10))
    error_code      = Column(String(20), nullable=False, index=True)
    error_message   = Column(Text)
    step            = Column(String(50))
    source_file     = Column(String(100))
    created_at      = Column(DateTime, default=datetime.utcnow)

    transaction     = relationship("BijlipayTransactionBase", back_populates="errors")

    __table_args__ = (
        UniqueConstraint("transaction_id", "error_code", name="uq_txn_error"),
    )


class Adjustment(Base):
    __tablename__ = "adjustments"

    id              = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    recon_date      = Column(Date, nullable=False, index=True)
    rrn             = Column(String(50), index=True)
    tid             = Column(String(50))
    order_id        = Column(String(100))
    rail            = Column(String(10))
    original_amount = Column(Numeric(15, 2))
    refund_amount   = Column(Numeric(15, 2))
    payout_date     = Column(Date)
    payout_ref      = Column(String(100))
    source_file     = Column(String(100))
    created_at      = Column(DateTime, default=datetime.utcnow)


class Payout(Base):
    __tablename__ = "payouts"

    id              = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    recon_date      = Column(Date, nullable=False, index=True)
    transaction_id  = Column(UUID(as_uuid=True), ForeignKey("bijlipay_transaction_base.id"))
    rrn             = Column(String(50))
    tid             = Column(String(50))
    order_id        = Column(String(100))
    rail            = Column(String(10), nullable=False)
    amount          = Column(Numeric(15, 2), nullable=False)
    merchant_id     = Column(String(50))
    terminal_id     = Column(String(50))
    payout_status   = Column(String(20), default="PENDING")
    created_at      = Column(DateTime, default=datetime.utcnow)

    transaction     = relationship("BijlipayTransactionBase", back_populates="payouts")


class HostTransactionBase(Base):
    __tablename__ = "host_transaction_base"

    id              = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    recon_date      = Column(Date, nullable=False, index=True)
    rrn             = Column(String(50), index=True)
    tid             = Column(String(50))
    amount          = Column(Numeric(15, 2))
    status          = Column(String(10))
    source          = Column(String(20))    # ATF | AGGREGATOR
    merchant_id     = Column(String(50))
    terminal_id     = Column(String(50))
    auth_id         = Column(String(50))
    txn_date        = Column(Date)
    created_at      = Column(DateTime, default=datetime.utcnow)


class CreditValidation(Base):
    __tablename__ = "credit_validation"

    id                  = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    recon_date          = Column(Date, nullable=False, index=True)
    rail                = Column(String(10), nullable=False)
    validation_type     = Column(String(50))
    total_txn_count     = Column(Integer)
    total_txn_amount    = Column(Numeric(15, 2))
    statement_amount    = Column(Numeric(15, 2))
    difference          = Column(Numeric(15, 2))
    status              = Column(String(20))
    notes               = Column(Text)
    created_at          = Column(DateTime, default=datetime.utcnow)


class ReconRunLog(Base):
    __tablename__ = "recon_run_log"

    id              = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    recon_date      = Column(Date, nullable=False)
    step            = Column(String(50), nullable=False)
    status          = Column(String(20), default="RUNNING")
    started_at      = Column(DateTime, default=datetime.utcnow)
    completed_at    = Column(DateTime)
    rows_processed  = Column(Integer, default=0)
    error_msg       = Column(Text)

    __table_args__ = (
        UniqueConstraint("recon_date", "step", name="uq_run"),
    )
