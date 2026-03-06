-- ============================================================
-- Bijlipay Reconciliation Engine - PostgreSQL Schema
-- ============================================================

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================
-- BIJLIPAY TRANSACTION BASE
-- ============================================================
CREATE TABLE IF NOT EXISTS bijlipay_transaction_base (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    recon_date          DATE NOT NULL,

    -- Natural key fields
    rrn                 VARCHAR(50),
    tid                 VARCHAR(50),
    order_id            VARCHAR(100),
    txn_date            DATE,

    -- Identity
    rail                VARCHAR(10) NOT NULL CHECK (rail IN ('CARD', 'DQR', 'SQR')),
    status              VARCHAR(10) NOT NULL CHECK (status IN ('SUCCESS', 'FAILURE')),
    source_file         VARCHAR(100),

    -- Financial (IMMUTABLE after creation)
    amount              NUMERIC(15, 2) NOT NULL,

    -- Card fields
    merchant_id         VARCHAR(50),
    merchant_name       VARCHAR(200),
    terminal_id         VARCHAR(50),
    card_number         VARCHAR(50),
    auth_id             VARCHAR(50),
    response_code       VARCHAR(10),
    interchange_type    VARCHAR(10),
    stan                VARCHAR(20),
    pos_entry_mode      VARCHAR(10),
    credit_debit_flag   VARCHAR(5),

    -- UPI fields
    txn_id              VARCHAR(100),
    vpa                 VARCHAR(200),
    creditvpa           VARCHAR(200),
    bankname            VARCHAR(100),

    -- ACK fields
    ack_enabled         VARCHAR(5),
    ack_received        VARCHAR(5),

    -- Meta
    created_at          TIMESTAMP DEFAULT NOW(),
    updated_at          TIMESTAMP DEFAULT NOW(),

    -- Composite natural key constraint
    CONSTRAINT uq_card_txn UNIQUE NULLS NOT DISTINCT (rrn, tid, amount, recon_date),
    CONSTRAINT uq_upi_txn  UNIQUE NULLS NOT DISTINCT (rrn, order_id, amount, recon_date)
);

CREATE INDEX IF NOT EXISTS idx_btb_recon_date ON bijlipay_transaction_base(recon_date);
CREATE INDEX IF NOT EXISTS idx_btb_rrn        ON bijlipay_transaction_base(rrn);
CREATE INDEX IF NOT EXISTS idx_btb_rail       ON bijlipay_transaction_base(rail);
CREATE INDEX IF NOT EXISTS idx_btb_status     ON bijlipay_transaction_base(status);
CREATE INDEX IF NOT EXISTS idx_btb_tid        ON bijlipay_transaction_base(tid);

-- ============================================================
-- TRANSACTION ERRORS
-- ============================================================
CREATE TABLE IF NOT EXISTS transaction_errors (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    recon_date      DATE NOT NULL,
    transaction_id  UUID REFERENCES bijlipay_transaction_base(id) ON DELETE CASCADE,
    rrn             VARCHAR(50),
    tid             VARCHAR(50),
    order_id        VARCHAR(100),
    amount          NUMERIC(15, 2),
    rail            VARCHAR(10),
    error_code      VARCHAR(20) NOT NULL,
    error_message   TEXT,
    step            VARCHAR(50),
    source_file     VARCHAR(100),
    created_at      TIMESTAMP DEFAULT NOW(),

    CONSTRAINT uq_txn_error UNIQUE (transaction_id, error_code)
);

CREATE INDEX IF NOT EXISTS idx_te_recon_date     ON transaction_errors(recon_date);
CREATE INDEX IF NOT EXISTS idx_te_transaction_id ON transaction_errors(transaction_id);
CREATE INDEX IF NOT EXISTS idx_te_error_code     ON transaction_errors(error_code);

-- ============================================================
-- ADJUSTMENTS
-- ============================================================
CREATE TABLE IF NOT EXISTS adjustments (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    recon_date      DATE NOT NULL,
    rrn             VARCHAR(50),
    tid             VARCHAR(50),
    order_id        VARCHAR(100),
    rail            VARCHAR(10),
    original_amount NUMERIC(15, 2),
    refund_amount   NUMERIC(15, 2),
    payout_date     DATE,
    payout_ref      VARCHAR(100),
    source_file     VARCHAR(100),
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_adj_recon_date ON adjustments(recon_date);
CREATE INDEX IF NOT EXISTS idx_adj_rrn        ON adjustments(rrn);

-- ============================================================
-- PAYOUTS
-- ============================================================
CREATE TABLE IF NOT EXISTS payouts (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    recon_date      DATE NOT NULL,
    transaction_id  UUID REFERENCES bijlipay_transaction_base(id),
    rrn             VARCHAR(50),
    tid             VARCHAR(50),
    order_id        VARCHAR(100),
    rail            VARCHAR(10) NOT NULL,
    amount          NUMERIC(15, 2) NOT NULL,
    merchant_id     VARCHAR(50),
    terminal_id     VARCHAR(50),
    payout_status   VARCHAR(20) DEFAULT 'PENDING',
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_payout_recon_date ON payouts(recon_date);
CREATE INDEX IF NOT EXISTS idx_payout_rail       ON payouts(rail);

-- ============================================================
-- HOST TRANSACTION BASE (Worldline)
-- ============================================================
CREATE TABLE IF NOT EXISTS host_transaction_base (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    recon_date      DATE NOT NULL,
    rrn             VARCHAR(50),
    tid             VARCHAR(50),
    amount          NUMERIC(15, 2),
    status          VARCHAR(10),
    source          VARCHAR(20),    -- 'ATF' or 'AGGREGATOR'
    merchant_id     VARCHAR(50),
    terminal_id     VARCHAR(50),
    auth_id         VARCHAR(50),
    txn_date        DATE,
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_htb_recon_date ON host_transaction_base(recon_date);
CREATE INDEX IF NOT EXISTS idx_htb_rrn        ON host_transaction_base(rrn);

-- ============================================================
-- CREDIT VALIDATION
-- ============================================================
CREATE TABLE IF NOT EXISTS credit_validation (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    recon_date          DATE NOT NULL,
    rail                VARCHAR(10) NOT NULL,
    validation_type     VARCHAR(50),    -- 'CARD_MPR', 'DQR_BULK', 'SQR_INSTANT'
    total_txn_count     INTEGER,
    total_txn_amount    NUMERIC(15, 2),
    statement_amount    NUMERIC(15, 2),
    difference          NUMERIC(15, 2),
    status              VARCHAR(20),    -- 'MATCHED', 'MISMATCH'
    notes               TEXT,
    created_at          TIMESTAMP DEFAULT NOW()
);

-- ============================================================
-- RECONCILIATION RUN LOG (idempotency guard)
-- ============================================================
CREATE TABLE IF NOT EXISTS recon_run_log (
    id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    recon_date  DATE NOT NULL,
    step        VARCHAR(50) NOT NULL,
    status      VARCHAR(20) DEFAULT 'RUNNING',  -- RUNNING, COMPLETE, FAILED
    started_at  TIMESTAMP DEFAULT NOW(),
    completed_at TIMESTAMP,
    rows_processed INTEGER DEFAULT 0,
    error_msg   TEXT,
    CONSTRAINT uq_run UNIQUE (recon_date, step)
);
