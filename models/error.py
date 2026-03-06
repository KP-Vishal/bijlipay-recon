"""
Error Code Registry — Bijlipay Reconciliation Engine
All error codes and their messages are defined here.
"""

ERROR_CODES = {
    "1":      "Duplicate transaction",
    "4":      "Settlement exists but base missing",
    "5":      "Base exists but settlement missing",
    "6":      "Ack enabled but not received",
    "7":      "Ack received but not enabled",
    "A":      "Ack not applicable",
    "8":      "Transaction missing in Ack file",
    "9":      "Ack exists but base missing",
    "10":     "Same-day refund",
    "11":     "Refund not found in base or previous payouts",
    "12":     "Aggregator mismatch (non-direct TID)",
    "13":     "Missing in aggregator",
    "14":     "ATF missing in aggregator (WL host check)",
    "15":     "Aggregator missing in ATF (WL host check)",
    "16":     "Base missing in MPR",
    "17":     "MPR missing in base",
    "18":     "MIS missing in bulk settlement",
    "19":     "Bulk settlement missing in MIS",
    "20":     "Host missing in statement",
    "21":     "Statement missing in base",
    "B":      "Aggregator exists but base missing (direct TID)",
    "HR-001": "Transaction missing in Host",
    "HR-002": "Transaction missing in Bijlipay base",
    "HR-003": "Status mismatch between Bijlipay and Host",
    "HR-004": "Amount mismatch between Bijlipay and Host",
    "HR-005": "Identifier mismatch between Bijlipay and Host",
    "HR-006": "Duplicate transaction in Host",
}

# Errors that are ELIGIBLE for payout
PAYOUT_ELIGIBLE_ERRORS = {"A", "B"}

# Errors that BLOCK payout
PAYOUT_BLOCKING_ERRORS = set(ERROR_CODES.keys()) - PAYOUT_ELIGIBLE_ERRORS
