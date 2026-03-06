# Bijlipay Reconciliation Engine

## Quick Start

### 1. Start the database
```bash
docker-compose up postgres -d
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Run reconciliation via CLI
```bash
python main.py \
  --date 2025-10-13 \
  --input-dir /path/to/your/files \
  --output-dir ./output \
  --db-url postgresql://bijlipay:bijlipay@localhost:5432/recon
```

### 4. Run via Docker (API + DB)
```bash
docker-compose up --build
# Then POST to http://localhost:8000/recon/run
```

---

## Input Files (place in --input-dir)

| File Pattern | BRD Role |
|---|---|
| `TransactionList_*` | Jupiter ATF (File 1) |
| `Transaction_Report_*` | OGS ATF (File 2) |
| `AxisSettlement_*` | Axis Settlement SQR (File 3) |
| `UPI_Settlement-*Jupiter*` | UPI Settlement Jupiter (File 4) |
| `UPI_Settlement-*.csv` | UPI Settlement OGS (File 5) |
| `OffilineRefund-*` | Offline Refund (File 6) |
| `UPI_cancel_Txns-*` | UPI Cancel Transactions (File 7) |
| `*Bijlipay_Aggr_Report*` | Worldline Aggregator (File 8) |
| `AxisBijliPay_*` | Worldline ATF (File 9) |
| `*_MPR_FILE_*` | Axis MPR (File 10) |
| `*BANK_STATMEMNT*` | Bank Statement (File 11) |
| `*Dynamic*` | Axis MIS Bulk/Dynamic (File 12) |
| `*static*` | Axis MIS Static (File 13) |
| `UPI_Sett_BIJLIPAY_*` | Bulk Settlement (File 14) |
| `UPI_Refund_BIJLIPAY_*` | UPI Refund File |
| `axis_aggre_*` | Axis Aggregator with ACK |

---

## API Endpoints

| Method | Path | Description |
|---|---|---|
| POST | `/recon/run` | Trigger pipeline |
| GET | `/recon/status/{date}` | Step-by-step status |
| GET | `/recon/errors/{date}` | Error summary |
| GET | `/recon/payouts/{date}` | Payout summary |
| GET | `/recon/credit/{date}` | Credit validation |
| POST | `/recon/reset/{date}` | Reset (destructive) |

---

## Column Mappings

To remap any input file column, edit `config/column_mappings.yaml`.
No code changes required.

---

## Direct TIDs Configuration

Add direct TID list to `config/column_mappings.yaml`:
```yaml
direct_tids:
  - "71376121"
  - "77371325"
  # ... add more TIDs
```

---

## Pipeline Steps

| Step | Name |
|---|---|
| 1 | Build Bijlipay Transaction Base |
| 2 | DQR Middleware Consistency Check |
| 3 | ACK Validation |
| 4 | Refund / Cancel Processing |
| 5 | Card Aggregator Reconciliation |
| 6 | Generate Payout Files |
| 7 | Build Host Transaction Base |
| 8 | Credit Validation (CARD) |
| 9 | Credit Validation (DQR/SQR Bulk) |
| 10 | Instant Settlement Validation |
| 11 | Bulk vs Statement Validation |
| 12 | Bijlipay vs Host Reconciliation |
