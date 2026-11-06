# realtime-payment-pipeline

End-to-end streaming pipeline: PostgreSQL → CDC → Confluent Cloud → Flink SQL fraud detection.

```
PostgreSQL (Docker)
      │  WAL / pgoutput
      ▼
Debezium CDC Connector (Confluent Cloud)
      │  Avro + Schema Registry
      ▼
Kafka Topics  payments.public.transactions
              payments.public.accounts
      │
      ▼
Flink SQL — enrichment JOIN + fraud rule (3 txns / card / 60 s)
      │
      ▼
Kafka Topics  payments.enriched
              payments.fraud_alerts
```

---

## Table of contents

1. [Architecture decisions](#architecture-decisions)
2. [Prerequisites](#prerequisites)
3. [Layer 1 — Foundation](#layer-1--foundation)
4. [Layer 2 — Contract](#layer-2--contract)
5. [Layer 3 — Ingestion](#layer-3--ingestion)
6. [Layer 4 — Processing](#layer-4--processing)
7. [Layer 5 — Operations](#layer-5--operations)
8. [How to bring it up from zero](#how-to-bring-it-up-from-zero)
9. [How to tear it all down](#how-to-tear-it-all-down)
10. [Cost](#cost)

---

## Architecture decisions

| Decision | Why |
|---|---|
| **CDC instead of batch** | WAL-based CDC captures INSERT/UPDATE/DELETE at sub-second latency with no query load on the source. Batch polling misses deletes and creates read spikes. |
| **Avro + Schema Registry** | Every produce call is validated against the registered schema. A breaking change fails at registration time, not at runtime, and not silently. |
| **Fully-managed connector (Confluent Cloud)** | No Connect worker to operate. Confluent manages scaling, restarts, and monitoring. The tradeoff is cost — documented in the Cost section. |
| **Flink SQL TUMBLE window (60 s)** | Non-overlapping windows give crisp, deduplicated alerts per bucket. Easy to audit. A SESSION window would be more precise for burst patterns but requires state TTL tuning — a good next iteration. |
| **Two service accounts** | `payments-writer` (connector + Flink) holds WRITE ACLs. `payments-reader` holds READ ACLs. Least privilege limits the blast radius of a leaked key. |
| **Debezium `ExtractNewRecordState` SMT** | Unwraps the Debezium envelope so downstream consumers see a flat record, not `{before, after, op, source}`. The `op` and `ts_ms` fields are promoted to top-level so Flink can filter deletes. |

---

## Prerequisites

| Tool | Version | Purpose |
|---|---|---|
| Docker + Compose v2 | ≥ 24 | Local PostgreSQL |
| Confluent CLI | ≥ v3 | Cluster, connector, Flink management |
| `jq` | any | JSON parsing in scripts |
| `curl` | any | Schema Registry REST calls |
| Confluent Cloud account | — | Free trial sufficient for this pipeline |

---

## Layer 1 — Foundation

**What it does:** creates the Confluent Cloud environment, a Basic Kafka cluster, Schema Registry endpoint, two service accounts with least-privilege ACLs, and the four Kafka topics.

### Steps

```bash
# 1. Copy and fill in secrets
cp .env.example .env
# Edit .env: add your CONFLUENT_CLOUD_API_KEY / API_SECRET

# 2. Run the foundation script
bash scripts/01_setup_confluent.sh
```

### Evidence

```
═══ 1/6  Creating environment ══════════════════════════
   ✅  Environment created: env-abc123

═══ 2/6  Creating Kafka cluster ════════════════════════
   ✅  Cluster created: lkc-xyz789
   Bootstrap: pkc-n98mth.us-east-1.aws.confluent.cloud:9092

═══ 3/6  Fetching Schema Registry endpoint ════════════
   ✅  Schema Registry: https://psrc-kv01d.us-east-1.aws.confluent.cloud

═══ 4/6  Creating topics ═══════════════════════════════
   ✅  payments.public.transactions
   ✅  payments.public.accounts
   ✅  payments.enriched
   ✅  payments.fraud_alerts

═══ 5/6  Creating service accounts ════════════════════
   ✅  Writer SA: sa-w11111
   ✅  Reader SA: sa-r22222
   ✅  Writer API key: ABCDEFGH1234
   ✅  Reader API key: IJKLMNOP5678
   ✅  SR API key: QRSTUVWX9012

═══ 6/6  Applying ACLs ══════════════════════════════════
   ✅  ACLs applied

✅  Layer 1 complete. Values appended to .env
```

> **Replace the evidence block above with the actual output of your run.**

---

## Layer 2 — Contract

**What it does:** registers the Avro schemas for `Transaction` (v1, then v2) and `Account` (v1) in Schema Registry. Verifies that adding an optional field (`risk_score`) is BACKWARD compatible — the check passes before the new version is accepted.

### Steps

```bash
bash scripts/02_register_schemas.sh
```

### Schemas

| Subject | File | Fields added in v2 |
|---|---|---|
| `payments.public.transactions-value` v1 | [`schemas/transaction-v1.avsc`](schemas/transaction-v1.avsc) | — |
| `payments.public.transactions-value` v2 | [`schemas/transaction-v2.avsc`](schemas/transaction-v2.avsc) | `risk_score` (nullable double, default null) |
| `payments.public.accounts-value` v1 | [`schemas/account-v1.avsc`](schemas/account-v1.avsc) | — |

### Why this evolution is BACKWARD compatible

`risk_score` is declared as `["null", "double"]` with `"default": null`.  
Old consumers reading v2 messages simply ignore the new field.  
New consumers reading v1 messages receive `null` for `risk_score`.  
Schema Registry rejects any evolution that breaks this contract.

### Evidence

```
═══ 1/4  Setting global compatibility → BACKWARD ═══════
{"compatibility":"BACKWARD"}

═══ 2/4  Registering v1 schemas ════════════════════════
   Registering payments.public.transactions-value ...
   ✅  payments.public.transactions-value → schema id 1
   Registering payments.public.accounts-value ...
   ✅  payments.public.accounts-value → schema id 2

═══ 3/4  Testing schema evolution: Transaction v1 → v2 ═
   Testing compatibility for payments.public.transactions-value ...
   is_compatible: true
   ✅  Compatible

═══ 4/4  Registering Transaction v2 ════════════════════
   Registering payments.public.transactions-value ...
   ✅  payments.public.transactions-value → schema id 3

✅  Layer 2 complete.
[
  "payments.public.transactions-value",
  "payments.public.accounts-value"
]
```

> **Replace the evidence block above with the actual output of your run.**

---

## Layer 3 — Ingestion

**What it does:** starts PostgreSQL locally via Docker, deploys the fully-managed Debezium CDC connector on Confluent Cloud, and demonstrates INSERT, UPDATE, and DELETE events arriving as Kafka messages.

### Steps

```bash
bash scripts/03_deploy_connector.sh
```

To trigger the live fraud scenario after the pipeline is running:

```bash
bash scripts/trigger_fraud.sh
```

### How CDC works here

1. PostgreSQL is started with `wal_level=logical`.  
2. [`init.sql`](docker/postgres/init.sql) creates a replication slot (`debezium_slot`) and a publication (`debezium_pub`) covering `transactions` and `accounts`.  
3. The connector reads the WAL stream via the `pgoutput` plugin and publishes each change as an Avro message to the matching topic.  
4. The `ExtractNewRecordState` SMT unwraps the Debezium envelope: consumers receive a flat record plus two promoted fields (`__op`, `__ts_ms`).

### Connector config highlights

| Setting | Value | Why |
|---|---|---|
| `plugin.name` | `pgoutput` | Native Postgres logical decoder — no extra extensions needed. |
| `snapshot.mode` | `initial` | Reads the full table on first run, then follows the WAL. |
| `transforms.unwrap.delete.handling.mode` | `rewrite` | Deleted rows arrive as a message with `__op=d` instead of a tombstone — easier to handle in Flink SQL. |
| `heartbeat.interval.ms` | `10000` | Keeps the replication slot alive when the source is idle. |

### Evidence

```
═══ 1/5  Starting Postgres (Docker) ════════════════════
   ✅  Postgres is healthy

═══ 2/5  Verifying WAL prerequisites ═══════════════════
    slot_name    | plugin  | database
-----------------+---------+----------
 debezium_slot   | pgoutput | payments
(1 row)

  pubname       | puballtables
--------------+--------------
 debezium_pub |    f
(1 row)

   ✅  Slot and publication confirmed

═══ 3/5  Deploying CDC connector ═══════════════════════
   ✅  Connector created: lcc-xxxxxx

═══ 4/5  Waiting for connector to reach RUNNING ════════
   Attempt 1/20 – state: PROVISIONING
   Attempt 3/20 – state: RUNNING
   ✅  Connector is RUNNING

═══ 5/5  Generating CDC events ═════════════════════════
   → INSERT: new transaction
   transaction_id
--------------------------------------
 f47ac10b-58cc-4372-a567-0e02b2c3d480
(1 row)

   → UPDATE: approve the latest PENDING transaction
   transaction_id                       | status
--------------------------------------+-----------
 f47ac10b-58cc-4372-a567-0e02b2c3d480 | APPROVED
(1 row)

   → DELETE: remove the test transaction
   transaction_id
--------------------------------------
 f47ac10b-58cc-4372-a567-0e02b2c3d480
(1 row)

✅  Layer 3 complete.
```

> **Replace the evidence block above with the actual output of your run.**

Sample event on `payments.public.transactions` (Avro decoded):

```json
{
  "transaction_id": "f47ac10b-58cc-4372-a567-0e02b2c3d480",
  "account_id":     "a1000000-0000-0000-0000-000000000002",
  "card_number":    "**** **** **** 1002",
  "merchant_name":  "CDC Test Shop",
  "merchant_category": "ELECTRONICS",
  "amount":         42.0,
  "currency":       "BRL",
  "status":         "APPROVED",
  "created_at":     1730000000000,
  "__op":           "u",
  "__ts_ms":        1730000001234
}
```

> **Replace the sample event above with a real message captured from the Confluent UI.**

---

## Layer 4 — Processing

**What it does:** two Flink SQL jobs running in Confluent Cloud for Apache Flink.

| Job | File | What it does |
|---|---|---|
| Enrichment | [`flink/02_enrichment.sql`](flink/02_enrichment.sql) | JOINs every transaction with the latest account snapshot (holder name, card brand, credit limit, account status) and writes to `payments.enriched`. |
| Fraud rule | [`flink/03_fraud_rule.sql`](flink/03_fraud_rule.sql) | Counts transactions per card in a 60-second TUMBLE window; emits an alert to `payments.fraud_alerts` when count ≥ 3. |

### Steps

Open the Confluent Cloud UI → Flink SQL Workspace (or use `confluent flink shell`), then run each file in order:

```sql
-- Step 1: create tables (run once)
-- paste contents of flink/01_create_tables.sql

-- Step 2: start enrichment job
-- paste contents of flink/02_enrichment.sql

-- Step 3: start fraud rule job
-- paste contents of flink/03_fraud_rule.sql
```

Or via the Confluent CLI:

```bash
confluent flink statement create enrichment \
  --sql "$(cat flink/02_enrichment.sql)" \
  --compute-pool <pool-id> \
  --environment "$CONFLUENT_ENV_ID"

confluent flink statement create fraud-rule \
  --sql "$(cat flink/03_fraud_rule.sql)" \
  --compute-pool <pool-id> \
  --environment "$CONFLUENT_ENV_ID"
```

### Why TUMBLE and not HOP or SESSION?

- **TUMBLE (chosen):** fixed non-overlapping 60-second buckets. Every alert covers a distinct time range. Easy to deduplicate downstream. Simple to explain.
- **HOP:** overlapping windows would double-count transactions. Useful for a rolling count but produces redundant alerts per event.
- **SESSION:** closes the window after a gap of inactivity. More precise for burst fraud but requires careful state TTL configuration and produces harder-to-audit alert boundaries.

### Evidence

```
-- Flink SQL workspace output after running 02_enrichment.sql:
[INFO] Statement has been successfully submitted.
Statement name: enrichment
State: RUNNING

-- Flink SQL workspace output after running 03_fraud_rule.sql:
[INFO] Statement has been successfully submitted.
Statement name: fraud-rule
State: RUNNING
```

Enriched transaction event on `payments.enriched`:

```json
{
  "transaction_id":    "b2000000-0000-0000-0000-000000000018",
  "account_id":        "a1000000-0000-0000-0000-000000000001",
  "holder_name":       "Alice Souza",
  "email":             "alice@example.com",
  "card_number":       "**** **** **** 1001",
  "card_brand":        "VISA",
  "credit_limit":      5000.0,
  "account_status":    "ACTIVE",
  "merchant_name":     "Loja Suspeita A",
  "merchant_category": "ELECTRONICS",
  "amount":            999.0,
  "currency":          "BRL",
  "txn_status":        "PENDING",
  "created_at":        1730000010000
}
```

Fraud alert on `payments.fraud_alerts`:

```json
{
  "card_number":   "**** **** **** 1001",
  "account_id":    "a1000000-0000-0000-0000-000000000001",
  "holder_name":   "Alice Souza",
  "email":         "alice@example.com",
  "txn_count":     3,
  "total_amount":  2997.0,
  "window_start":  "2024-10-27T12:00:00",
  "window_end":    "2024-10-27T12:01:00",
  "alert_reason":  "FRAUD_ALERT: 3 transactions on card **** **** **** 1001 in 60 s — total BRL 2997.0"
}
```

> **Replace the evidence blocks above with real output from your Confluent Cloud Flink workspace.**

---

## Layer 5 — Operations

**What it monitors:** connector health, Flink job state, topic throughput, RBAC verification, and cost.

### Connector health

```bash
confluent connect cluster describe <connector-id> --output json | \
  jq '{name: .connector.name, state: .connector.state, tasks: .tasks}'
```

Expected output:

```json
{
  "name":  "payments-postgres-cdc",
  "state": "RUNNING",
  "tasks": [{"id": 0, "state": "RUNNING"}]
}
```

### Flink statement health

```bash
confluent flink statement list \
  --environment "$CONFLUENT_ENV_ID" \
  --cloud aws --region us-east-1 \
  --output json | jq '.[] | {name, status}'
```

Expected output:

```json
{"name": "enrichment",  "status": "RUNNING"}
{"name": "fraud-rule",  "status": "RUNNING"}
```

### Topic throughput

Check in the Confluent Cloud UI: **Topics → payments.public.transactions → Metrics**  
or via the Metrics API:

```bash
curl -sS -u "$CONFLUENT_CLOUD_API_KEY:$CONFLUENT_CLOUD_API_SECRET" \
  "https://api.telemetry.confluent.cloud/v2/metrics/cloud/query" \
  -H "Content-Type: application/json" \
  -d '{
    "aggregations": [{"metric": "io.confluent.kafka.server/received_bytes"}],
    "filter": {"field": "resource.kafka.id", "op": "EQ", "value": "'"$CONFLUENT_CLUSTER_ID"'"},
    "granularity": "PT1M",
    "intervals": ["2024-10-27T12:00:00Z/2024-10-27T12:05:00Z"]
  }' | jq .
```

### RBAC verification

```bash
# Writer can produce, cannot consume
confluent kafka acl list --service-account "$WRITER_SA_ID" --output json | \
  jq '.[] | {topic: .resourceName, operation: .operation, permission: .permission}'

# Reader can consume, cannot produce
confluent kafka acl list --service-account "$READER_SA_ID" --output json | \
  jq '.[] | {topic: .resourceName, operation: .operation, permission: .permission}'
```

Expected output:

```
# Writer
{"topic": "payments.public.transactions", "operation": "WRITE",    "permission": "ALLOW"}
{"topic": "payments.public.transactions", "operation": "DESCRIBE", "permission": "ALLOW"}
...

# Reader
{"topic": "payments.public.transactions", "operation": "READ",     "permission": "ALLOW"}
{"topic": "payments.public.transactions", "operation": "DESCRIBE", "permission": "ALLOW"}
...
```

> **Replace the evidence blocks above with real output from your run.**

### Stream lineage

Open Confluent Cloud UI → **Stream Lineage**.  
You should see:

```
[Postgres] → [payments-postgres-cdc connector]
                → payments.public.transactions
                → payments.public.accounts
                     → [Flink: enrichment]
                           → payments.enriched
                                → [Flink: fraud-rule]
                                      → payments.fraud_alerts
```

> **Add a screenshot of the Stream Lineage graph here.**

---

## How to bring it up from zero

```bash
# 0. Clone the repo
git clone https://github.com/<your-username>/realtime-payment-pipeline.git
cd realtime-payment-pipeline

# 1. Fill in secrets
cp .env.example .env
# Edit .env — add your Confluent Cloud API key/secret and Postgres password

# 2. Layer 1 — Foundation (Confluent resources + ACLs)
bash scripts/01_setup_confluent.sh

# 3. Layer 2 — Contract (register Avro schemas + evolution test)
bash scripts/02_register_schemas.sh

# 4. Layer 3 — Ingestion (start Postgres + deploy CDC connector)
bash scripts/03_deploy_connector.sh

# 5. Layer 4 — Processing (run in Confluent Cloud Flink SQL workspace)
#    Paste flink/01_create_tables.sql → then 02_enrichment.sql → then 03_fraud_rule.sql

# 6. Trigger a live fraud event
bash scripts/trigger_fraud.sh
# Watch payments.fraud_alerts in the Confluent UI
```

Total time from zero to first fraud alert: approximately **15–20 minutes**.

---

## How to tear it all down

```bash
bash scripts/04_teardown.sh
# You will be asked to type 'yes' to confirm.
# Order: Flink jobs → Connector → Topics → Cluster → Environment → Docker
```

---

## Cost

> **Fill this in after running the pipeline.**

Estimated cost for a single session (≈ 2 hours):

| Resource | Tier | Estimated cost |
|---|---|---|
| Kafka cluster (Basic) | 1 CKU | ~$0.36 |
| Fully-managed connector | 1 task | ~$0.18 |
| Flink SQL | compute pool, 2 statements | ~$0.10 |
| Schema Registry | Essentials (included) | $0.00 |
| **Total** | | **~$0.64** |

To get the exact figure from the API after teardown:

```bash
curl -sS \
  -u "$CONFLUENT_CLOUD_API_KEY:$CONFLUENT_CLOUD_API_SECRET" \
  "https://api.confluent.cloud/billing/v1/costs?start_date=YYYY-MM-01&end_date=YYYY-MM-DD" \
  | jq '{total_usd: [.data[].amount | tonumber] | add}'
```

> **Paste the real API response here.**

---

## Repository checklist

- [x] Every layer has an evidence section (replace placeholders with real output)
- [x] Another person can bring up the pipeline following only this README
- [x] No passwords or keys committed — `.env` is in `.gitignore`, `.env.example` has placeholders
- [x] Repository is public, lowercase name, no accents
- [x] Link points to the repository root, not to a file inside it
