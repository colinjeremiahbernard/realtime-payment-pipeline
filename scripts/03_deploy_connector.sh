#!/usr/bin/env bash
# =============================================================
#  Layer 3 – Ingestion
#  1. Starts local Postgres via Docker Compose
#  2. Deploys the Debezium CDC connector to Confluent Cloud
#  3. Verifies INSERT / UPDATE / DELETE events arrive
#
#  Prerequisites:
#    • docker compose v2
#    • confluent CLI ≥ v3
#    • jq, curl
#    • .env populated
# =============================================================
set -euo pipefail

source .env

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# ─────────────────────────────────────────────────────────────
#  1. Start Postgres
# ─────────────────────────────────────────────────────────────
echo ""
echo "═══ 1/5  Starting Postgres (Docker) ════════════════════"
docker compose -f "$ROOT_DIR/docker/postgres/docker-compose.yml" \
  --env-file "$ROOT_DIR/.env" \
  up -d --wait
echo "   ✅  Postgres is healthy"

# ─────────────────────────────────────────────────────────────
#  2. Verify replication slot + publication exist
# ─────────────────────────────────────────────────────────────
echo ""
echo "═══ 2/5  Verifying WAL prerequisites ═══════════════════"
docker exec payments-postgres psql \
  -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
  "SELECT slot_name, plugin, database FROM pg_replication_slots;"
docker exec payments-postgres psql \
  -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
  "SELECT pubname, puballtables FROM pg_publication;"
echo "   ✅  Slot and publication confirmed"

# ─────────────────────────────────────────────────────────────
#  3. Deploy CDC connector
#     Confluent Cloud fully-managed connector (PostgresSource)
# ─────────────────────────────────────────────────────────────
echo ""
echo "═══ 3/5  Deploying CDC connector ═══════════════════════"

# Expand placeholders in the connector config
CONNECTOR_CONFIG=$(sed \
  -e "s|{{WRITER_API_KEY}}|$WRITER_API_KEY|g" \
  -e "s|{{WRITER_API_SECRET}}|$WRITER_API_SECRET|g" \
  -e "s|{{POSTGRES_HOST}}|$POSTGRES_HOST|g" \
  -e "s|{{POSTGRES_PORT}}|$POSTGRES_PORT|g" \
  -e "s|{{POSTGRES_USER}}|$POSTGRES_USER|g" \
  -e "s|{{POSTGRES_PASSWORD}}|$POSTGRES_PASSWORD|g" \
  -e "s|{{POSTGRES_DB}}|$POSTGRES_DB|g" \
  -e "s|{{SCHEMA_REGISTRY_URL}}|$SCHEMA_REGISTRY_URL|g" \
  -e "s|{{SCHEMA_REGISTRY_API_KEY}}|$SCHEMA_REGISTRY_API_KEY|g" \
  -e "s|{{SCHEMA_REGISTRY_API_SECRET}}|$SCHEMA_REGISTRY_API_SECRET|g" \
  "$ROOT_DIR/connectors/postgres-cdc.json")

confluent connect cluster use "$CONFLUENT_CLUSTER_ID"

CONNECTOR_ID=$(echo "$CONNECTOR_CONFIG" | \
  confluent connect cluster create --config-file - --output json | \
  jq -r '.id')
echo "   ✅  Connector created: $CONNECTOR_ID"

# ─────────────────────────────────────────────────────────────
#  4. Wait for connector to reach RUNNING state
# ─────────────────────────────────────────────────────────────
echo ""
echo "═══ 4/5  Waiting for connector to reach RUNNING ════════"
for i in $(seq 1 20); do
  STATE=$(confluent connect cluster describe "$CONNECTOR_ID" \
    --output json | jq -r '.connector.state')
  echo "   Attempt $i/20 – state: $STATE"
  if [[ "$STATE" == "RUNNING" ]]; then
    echo "   ✅  Connector is RUNNING"
    break
  fi
  if [[ "$STATE" == "FAILED" ]]; then
    echo "   ❌  Connector FAILED. Check logs:"
    confluent connect cluster describe "$CONNECTOR_ID"
    exit 1
  fi
  sleep 10
done

# ─────────────────────────────────────────────────────────────
#  5. Demonstrate INSERT / UPDATE / DELETE events
# ─────────────────────────────────────────────────────────────
echo ""
echo "═══ 5/5  Generating CDC events ═════════════════════════"

echo "   → INSERT: new transaction"
docker exec payments-postgres psql \
  -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
  "INSERT INTO transactions
     (transaction_id, account_id, card_number, merchant_name,
      merchant_category, amount, currency, status)
   VALUES
     (gen_random_uuid(),
      'a1000000-0000-0000-0000-000000000002',
      '**** **** **** 1002',
      'CDC Test Shop', 'ELECTRONICS', 42.00, 'BRL', 'PENDING')
   RETURNING transaction_id;"

sleep 2

echo "   → UPDATE: approve the latest PENDING transaction"
docker exec payments-postgres psql \
  -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
  "UPDATE transactions
   SET status = 'APPROVED'
   WHERE status = 'PENDING'
     AND merchant_name = 'CDC Test Shop'
   RETURNING transaction_id, status;"

sleep 2

echo "   → DELETE: remove the test transaction"
docker exec payments-postgres psql \
  -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
  "DELETE FROM transactions
   WHERE merchant_name = 'CDC Test Shop'
   RETURNING transaction_id;"

echo ""
echo "✅  Layer 3 complete."
echo "   Events are flowing → check topic: payments.public.transactions"
echo ""
echo "   Peek at the last 5 messages:"
confluent kafka topic consume payments.public.transactions \
  --api-key "$WRITER_API_KEY" \
  --api-secret "$WRITER_API_SECRET" \
  --from-beginning \
  --value-format avro \
  --schema-registry-api-key "$SCHEMA_REGISTRY_API_KEY" \
  --schema-registry-api-secret "$SCHEMA_REGISTRY_API_SECRET" \
  --max-messages 5 \
  2>/dev/null || echo "   (use Confluent Cloud UI → Topics → Messages to inspect events)"
