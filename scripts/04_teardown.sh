#!/usr/bin/env bash
# =============================================================
#  Teardown – destroy everything and report cost
#  Order: Flink jobs → Connector → Topics → Cluster → Environment
#
#  ⚠️  This is destructive and irreversible.
#      Data in Confluent Cloud will be permanently deleted.
# =============================================================
set -euo pipefail

source .env

echo ""
echo "⚠️   TEARDOWN — this will delete all Confluent Cloud resources"
read -r -p "   Type 'yes' to confirm: " CONFIRM
if [[ "$CONFIRM" != "yes" ]]; then
  echo "   Aborted."
  exit 0
fi

# ─────────────────────────────────────────────────────────────
#  1. Stop Flink jobs (if any are running)
# ─────────────────────────────────────────────────────────────
echo ""
echo "═══ 1/6  Stopping Flink SQL statements ════════════════"
confluent flink statement list \
  --environment "$CONFLUENT_ENV_ID" \
  --cloud aws --region us-east-1 \
  --output json 2>/dev/null | \
  jq -r '.[].name' | \
  while read -r stmt; do
    echo "   Deleting Flink statement: $stmt"
    confluent flink statement delete "$stmt" \
      --environment "$CONFLUENT_ENV_ID" \
      --cloud aws --region us-east-1 \
      --force 2>/dev/null || true
  done
echo "   ✅  Flink statements stopped"

# ─────────────────────────────────────────────────────────────
#  2. Delete CDC connector
# ─────────────────────────────────────────────────────────────
echo ""
echo "═══ 2/6  Deleting CDC connector ═════════════════════════"
confluent connect cluster list --output json | \
  jq -r '.[].id' | \
  while read -r conn_id; do
    echo "   Deleting connector: $conn_id"
    confluent connect cluster delete "$conn_id" --force 2>/dev/null || true
  done
echo "   ✅  Connectors deleted"

# ─────────────────────────────────────────────────────────────
#  3. Delete topics
# ─────────────────────────────────────────────────────────────
echo ""
echo "═══ 3/6  Deleting topics ════════════════════════════════"
TOPICS=(
  "payments.public.transactions"
  "payments.public.accounts"
  "payments.enriched"
  "payments.fraud_alerts"
)
confluent kafka cluster use "$CONFLUENT_CLUSTER_ID" 2>/dev/null || true
for TOPIC in "${TOPICS[@]}"; do
  confluent kafka topic delete "$TOPIC" --force 2>/dev/null && \
    echo "   ✅  $TOPIC" || \
    echo "   ⚠️  $TOPIC not found"
done

# ─────────────────────────────────────────────────────────────
#  4. Delete API keys
# ─────────────────────────────────────────────────────────────
echo ""
echo "═══ 4/6  Deleting API keys ══════════════════════════════"
for KEY in "$WRITER_API_KEY" "$READER_API_KEY" "$SCHEMA_REGISTRY_API_KEY"; do
  confluent api-key delete "$KEY" --force 2>/dev/null && \
    echo "   ✅  $KEY" || true
done

# ─────────────────────────────────────────────────────────────
#  5. Delete Kafka cluster
# ─────────────────────────────────────────────────────────────
echo ""
echo "═══ 5/6  Deleting Kafka cluster ════════════════════════"
confluent kafka cluster delete "$CONFLUENT_CLUSTER_ID" --force
echo "   ✅  Cluster deleted: $CONFLUENT_CLUSTER_ID"

# ─────────────────────────────────────────────────────────────
#  6. Delete environment
# ─────────────────────────────────────────────────────────────
echo ""
echo "═══ 6/6  Deleting environment ═══════════════════════════"
confluent environment delete "$CONFLUENT_ENV_ID" --force
echo "   ✅  Environment deleted: $CONFLUENT_ENV_ID"

# ─────────────────────────────────────────────────────────────
#  7. Stop Docker
# ─────────────────────────────────────────────────────────────
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
echo ""
echo "   Stopping Docker containers..."
docker compose -f "$ROOT_DIR/docker/postgres/docker-compose.yml" down -v 2>/dev/null || true
echo "   ✅  Docker stopped"

# ─────────────────────────────────────────────────────────────
#  8. Cost report
# ─────────────────────────────────────────────────────────────
echo ""
echo "═══ Cost estimate ═══════════════════════════════════════"
echo "   Run in Confluent Cloud UI:"
echo "   Billing → Usage → select environment 'payments-pipeline'"
echo "   OR use the API:"
echo ""
echo "   curl -sS -u \"\$CONFLUENT_CLOUD_API_KEY:\$CONFLUENT_CLOUD_API_SECRET\" \\"
echo "     'https://api.confluent.cloud/billing/v1/costs?start_date=YYYY-MM-01&end_date=YYYY-MM-DD' \\"
echo "     | jq '{total: .data[].amount}'"
echo ""
echo "✅  Teardown complete."
