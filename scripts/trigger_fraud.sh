#!/usr/bin/env bash
# =============================================================
#  Layer 3 – Helper
#  Triggers a live fraud scenario by inserting 3 fast transactions
#  on Alice's card (**** 1001) within 60 seconds.
#
#  Run this AFTER the pipeline is fully up (all 5 layers).
#  Watch payments.fraud_alerts in the Confluent UI for the alert.
# =============================================================
set -euo pipefail

source .env

echo ""
echo "🚨  Triggering fraud scenario — 3 txns on **** 1001 in < 60 s"

for i in 1 2 3; do
  docker exec payments-postgres psql \
    -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
    "INSERT INTO transactions
       (transaction_id, account_id, card_number, merchant_name,
        merchant_category, amount, currency, status)
     VALUES
       (gen_random_uuid(),
        'a1000000-0000-0000-0000-000000000001',
        '**** **** **** 1001',
        'Live Fraud Shop $i', 'ELECTRONICS', 888.00, 'BRL', 'PENDING')
     RETURNING transaction_id, created_at;" 
  echo "   ✅  Insert $i/3 done"
  sleep 5   # 5 s apart — all 3 land in the same 60-s tumbling window
done

echo ""
echo "✅  3 inserts complete. Check payments.fraud_alerts in ~30 seconds."
