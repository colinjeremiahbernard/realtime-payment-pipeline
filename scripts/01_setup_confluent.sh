#!/usr/bin/env bash
# =============================================================
#  Layer 1 – Foundation
#  Creates: Confluent Cloud environment, Basic cluster,
#           Schema Registry, two service accounts (writer/reader),
#           API keys, topic ACLs.
#
#  Prerequisites:
#    • confluent CLI ≥ v3  (https://docs.confluent.io/confluent-cli)
#    • jq
#    • .env file populated from .env.example
# =============================================================
set -euo pipefail

# ── Load environment ──────────────────────────────────────────
if [[ ! -f .env ]]; then
  echo "❌  .env not found. Copy .env.example → .env and fill in your values."
  exit 1
fi
# shellcheck disable=SC1091
source .env

echo "🔐  Logging in to Confluent Cloud..."
confluent login --save \
  --api-key    "$CONFLUENT_CLOUD_API_KEY" \
  --api-secret "$CONFLUENT_CLOUD_API_SECRET"

# ─────────────────────────────────────────────────────────────
#  1. Environment
# ─────────────────────────────────────────────────────────────
echo ""
echo "═══ 1/6  Creating environment ══════════════════════════"
ENV_ID=$(confluent environment create payments-pipeline \
  --output json | jq -r '.id')
echo "   ✅  Environment created: $ENV_ID"
confluent environment use "$ENV_ID"

# ─────────────────────────────────────────────────────────────
#  2. Kafka cluster (Basic, cheapest tier)
# ─────────────────────────────────────────────────────────────
echo ""
echo "═══ 2/6  Creating Kafka cluster ════════════════════════"
CLUSTER_JSON=$(confluent kafka cluster create payments-cluster \
  --cloud aws \
  --region us-east-1 \
  --type basic \
  --output json)
CLUSTER_ID=$(echo "$CLUSTER_JSON" | jq -r '.id')
BOOTSTRAP=$(echo "$CLUSTER_JSON" | jq -r '.endpoint' | sed 's|SASL_SSL://||')
echo "   ✅  Cluster created: $CLUSTER_ID"
echo "   Bootstrap: $BOOTSTRAP"

confluent kafka cluster use "$CLUSTER_ID"

# ─────────────────────────────────────────────────────────────
#  3. Schema Registry (already provisioned per environment)
# ─────────────────────────────────────────────────────────────
echo ""
echo "═══ 3/6  Fetching Schema Registry endpoint ════════════"
SR_URL=$(confluent schema-registry cluster describe \
  --output json | jq -r '.endpoint_url')
echo "   ✅  Schema Registry: $SR_URL"

# ─────────────────────────────────────────────────────────────
#  4. Topics
# ─────────────────────────────────────────────────────────────
echo ""
echo "═══ 4/6  Creating topics ═══════════════════════════════"

create_topic() {
  local name=$1 partitions=$2 retention_ms=$3
  confluent kafka topic create "$name" \
    --partitions "$partitions" \
    --config retention.ms="$retention_ms" 2>/dev/null && \
    echo "   ✅  $name" || echo "   ⚠️  $name already exists"
}

# CDC connector creates these automatically but we pre-create them
# so we can set explicit retention.
create_topic "payments.public.transactions" 3 604800000   # 7 days
create_topic "payments.public.accounts"     3 -1           # infinite (compacted-ish)
create_topic "payments.enriched"            3 604800000
create_topic "payments.fraud_alerts"        3 604800000

# ─────────────────────────────────────────────────────────────
#  5. Service accounts + API keys
# ─────────────────────────────────────────────────────────────
echo ""
echo "═══ 5/6  Creating service accounts ════════════════════"

WRITER_SA_ID=$(confluent iam service-account create payments-writer \
  --description "CDC connector and Flink jobs" \
  --output json | jq -r '.id')
echo "   ✅  Writer SA: $WRITER_SA_ID"

READER_SA_ID=$(confluent iam service-account create payments-reader \
  --description "Consumers, dashboards, read-only" \
  --output json | jq -r '.id')
echo "   ✅  Reader SA: $READER_SA_ID"

# Writer API key
echo "   Generating writer API key..."
WRITER_KEY_JSON=$(confluent api-key create \
  --service-account "$WRITER_SA_ID" \
  --resource "$CLUSTER_ID" \
  --output json)
WRITER_API_KEY=$(echo "$WRITER_KEY_JSON" | jq -r '.api_key')
WRITER_API_SECRET=$(echo "$WRITER_KEY_JSON" | jq -r '.api_secret')
echo "   ✅  Writer API key: $WRITER_API_KEY"

# Reader API key
echo "   Generating reader API key..."
READER_KEY_JSON=$(confluent api-key create \
  --service-account "$READER_SA_ID" \
  --resource "$CLUSTER_ID" \
  --output json)
READER_API_KEY=$(echo "$READER_KEY_JSON" | jq -r '.api_key')
READER_API_SECRET=$(echo "$READER_KEY_JSON" | jq -r '.api_secret')
echo "   ✅  Reader API key: $READER_API_KEY"

# SR keys (shared environment scope)
echo "   Generating Schema Registry API keys..."
SR_WRITER_KEY_JSON=$(confluent api-key create \
  --service-account "$WRITER_SA_ID" \
  --resource "$SR_URL" \
  --output json 2>/dev/null || \
  confluent api-key create \
    --service-account "$WRITER_SA_ID" \
    --resource schema-registry \
    --output json)
SR_API_KEY=$(echo "$SR_WRITER_KEY_JSON" | jq -r '.api_key')
SR_API_SECRET=$(echo "$SR_WRITER_KEY_JSON" | jq -r '.api_secret')
echo "   ✅  SR API key: $SR_API_KEY"

# ─────────────────────────────────────────────────────────────
#  6. ACLs – least-privilege
# ─────────────────────────────────────────────────────────────
echo ""
echo "═══ 6/6  Applying ACLs ══════════════════════════════════"

TOPICS=("payments.public.transactions" "payments.public.accounts" "payments.enriched" "payments.fraud_alerts")

for TOPIC in "${TOPICS[@]}"; do
  # Writer: WRITE + DESCRIBE
  confluent kafka acl create --allow \
    --service-account "$WRITER_SA_ID" \
    --operations WRITE,DESCRIBE \
    --topic "$TOPIC"
  # Reader: READ + DESCRIBE
  confluent kafka acl create --allow \
    --service-account "$READER_SA_ID" \
    --operations READ,DESCRIBE \
    --topic "$TOPIC"
done

# Writer needs CREATE for Kafka Connect to auto-create topics
confluent kafka acl create --allow \
  --service-account "$WRITER_SA_ID" \
  --operations CREATE \
  --topic "*"

# Consumer group ACL for reader
confluent kafka acl create --allow \
  --service-account "$READER_SA_ID" \
  --operations READ \
  --consumer-group "payments-consumers"

echo "   ✅  ACLs applied"

# ─────────────────────────────────────────────────────────────
#  7. Persist IDs to .env (warn, never commit)
# ─────────────────────────────────────────────────────────────
cat >> .env <<EOF

# ── Auto-generated by 01_setup_confluent.sh ──────────────────
CONFLUENT_ENV_ID=$ENV_ID
CONFLUENT_CLUSTER_ID=$CLUSTER_ID
CONFLUENT_BOOTSTRAP=$BOOTSTRAP
SCHEMA_REGISTRY_URL=$SR_URL
SCHEMA_REGISTRY_API_KEY=$SR_API_KEY
SCHEMA_REGISTRY_API_SECRET=$SR_API_SECRET
WRITER_SA_ID=$WRITER_SA_ID
WRITER_API_KEY=$WRITER_API_KEY
WRITER_API_SECRET=$WRITER_API_SECRET
READER_SA_ID=$READER_SA_ID
READER_API_KEY=$READER_API_KEY
READER_API_SECRET=$READER_API_SECRET
EOF

echo ""
echo "✅  Layer 1 complete. Values appended to .env"
echo "   ⚠️  Never commit .env to git!"
echo ""
echo "   ENV_ID:     $ENV_ID"
echo "   CLUSTER_ID: $CLUSTER_ID"
echo "   BOOTSTRAP:  $BOOTSTRAP"
echo "   SR_URL:     $SR_URL"
