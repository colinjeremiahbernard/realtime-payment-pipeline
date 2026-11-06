#!/usr/bin/env bash
# =============================================================
#  Layer 2 – Contract
#  Registers Avro schemas in Schema Registry.
#  Tests BACKWARD compatibility by evolving Transaction to v2.
#
#  Prerequisites:
#    • curl, jq
#    • .env populated (SCHEMA_REGISTRY_URL, SR API key/secret)
# =============================================================
set -euo pipefail

source .env

AUTH="$SCHEMA_REGISTRY_API_KEY:$SCHEMA_REGISTRY_API_SECRET"
BASE="$SCHEMA_REGISTRY_URL/subjects"

# ─────────────────────────────────────────────────────────────
#  Helper: register a schema and return its ID
# ─────────────────────────────────────────────────────────────
register_schema() {
  local subject=$1
  local avsc_file=$2
  # Escape the JSON schema for embedding inside another JSON object
  local schema_json
  schema_json=$(jq -Rs '.' < "$avsc_file")

  echo "   Registering $subject from $avsc_file ..."
  local response
  response=$(curl -sS -X POST \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    -u "$AUTH" \
    "$BASE/$subject/versions" \
    -d "{\"schema\": $schema_json}")

  local schema_id
  schema_id=$(echo "$response" | jq -r '.id // empty')
  if [[ -z "$schema_id" ]]; then
    echo "   ❌  Failed to register $subject:"
    echo "       $response"
    exit 1
  fi
  echo "   ✅  $subject → schema id $schema_id"
  echo "$schema_id"
}

# ─────────────────────────────────────────────────────────────
#  Helper: test compatibility before registering
# ─────────────────────────────────────────────────────────────
test_compatibility() {
  local subject=$1
  local avsc_file=$2
  local schema_json
  schema_json=$(jq -Rs '.' < "$avsc_file")

  echo "   Testing compatibility for $subject ..."
  local response
  response=$(curl -sS -X POST \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    -u "$AUTH" \
    "$BASE/$subject/versions/latest/compatibility" \
    -d "{\"schema\": $schema_json}")

  local is_compatible
  is_compatible=$(echo "$response" | jq -r '.is_compatible // false')
  echo "   is_compatible: $is_compatible"
  if [[ "$is_compatible" != "true" ]]; then
    echo "   ❌  Schema NOT compatible — aborting evolution."
    echo "       $response"
    exit 1
  fi
  echo "   ✅  Compatible"
}

# ─────────────────────────────────────────────────────────────
#  1. Set global compatibility to BACKWARD
# ─────────────────────────────────────────────────────────────
echo ""
echo "═══ 1/4  Setting global compatibility → BACKWARD ═══════"
curl -sS -X PUT \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -u "$AUTH" \
  "$SCHEMA_REGISTRY_URL/config" \
  -d '{"compatibility": "BACKWARD"}' | jq .
echo ""

# ─────────────────────────────────────────────────────────────
#  2. Register v1 schemas
# ─────────────────────────────────────────────────────────────
echo "═══ 2/4  Registering v1 schemas ════════════════════════"
register_schema "payments.public.transactions-value" "schemas/transaction-v1.avsc"
register_schema "payments.public.accounts-value"     "schemas/account-v1.avsc"
echo ""

# ─────────────────────────────────────────────────────────────
#  3. Test BACKWARD compatibility of Transaction v2
#     (adds optional risk_score with null default → compatible)
# ─────────────────────────────────────────────────────────────
echo "═══ 3/4  Testing schema evolution: Transaction v1 → v2 ═"
test_compatibility "payments.public.transactions-value" "schemas/transaction-v2.avsc"
echo ""

# ─────────────────────────────────────────────────────────────
#  4. Register Transaction v2
# ─────────────────────────────────────────────────────────────
echo "═══ 4/4  Registering Transaction v2 ════════════════════"
register_schema "payments.public.transactions-value" "schemas/transaction-v2.avsc"
echo ""

echo "✅  Layer 2 complete."
echo "   Registered subjects:"
curl -sS -u "$AUTH" "$BASE" | jq .
