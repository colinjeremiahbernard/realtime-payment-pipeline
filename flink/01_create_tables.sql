-- =============================================================
--  Flink SQL – Layer 4 Step 1: Create source & sink tables
--  Run in the Confluent Cloud Flink SQL workspace.
-- =============================================================

-- ─────────────────────────────────────────────────────────────
--  SOURCE: transactions topic  (written by CDC connector)
-- ─────────────────────────────────────────────────────────────
CREATE TABLE transactions (
    transaction_id    STRING,
    account_id        STRING,
    card_number       STRING,
    merchant_name     STRING,
    merchant_category STRING,
    amount            DOUBLE,
    currency          STRING,
    status            STRING,
    created_at        TIMESTAMP(3),
    -- Debezium metadata injected by ExtractNewRecordState
    `__op`            STRING,   -- 'r' read, 'c' create, 'u' update, 'd' delete
    `__ts_ms`         BIGINT,
    -- Flink event-time watermark: tolerate 5-second late arrivals
    WATERMARK FOR created_at AS created_at - INTERVAL '5' SECOND
) WITH (
    'connector'                     = 'confluent',
    'kafka.topic'                   = 'payments.public.transactions',
    'scan.startup.mode'             = 'earliest-offset',
    'value.format'                  = 'avro-confluent',
    'value.avro-confluent.url'      = '${SCHEMA_REGISTRY_URL}',
    'key.format'                    = 'avro-confluent',
    'key.avro-confluent.url'        = '${SCHEMA_REGISTRY_URL}'
);

-- ─────────────────────────────────────────────────────────────
--  SOURCE: accounts topic  (written by CDC connector)
-- ─────────────────────────────────────────────────────────────
CREATE TABLE accounts (
    account_id    STRING,
    holder_name   STRING,
    email         STRING,
    card_number   STRING,
    card_brand    STRING,
    credit_limit  DOUBLE,
    status        STRING,
    created_at    TIMESTAMP(3),
    `__op`        STRING,
    `__ts_ms`     BIGINT,
    PRIMARY KEY (account_id) NOT ENFORCED
) WITH (
    'connector'                     = 'confluent',
    'kafka.topic'                   = 'payments.public.accounts',
    'scan.startup.mode'             = 'earliest-offset',
    'value.format'                  = 'avro-confluent',
    'value.avro-confluent.url'      = '${SCHEMA_REGISTRY_URL}',
    'key.format'                    = 'avro-confluent',
    'key.avro-confluent.url'        = '${SCHEMA_REGISTRY_URL}'
);

-- ─────────────────────────────────────────────────────────────
--  SINK: enriched transactions topic
-- ─────────────────────────────────────────────────────────────
CREATE TABLE enriched_transactions (
    transaction_id    STRING,
    account_id        STRING,
    holder_name       STRING,
    email             STRING,
    card_number       STRING,
    card_brand        STRING,
    credit_limit      DOUBLE,
    account_status    STRING,
    merchant_name     STRING,
    merchant_category STRING,
    amount            DOUBLE,
    currency          STRING,
    txn_status        STRING,
    created_at        TIMESTAMP(3),
    PRIMARY KEY (transaction_id) NOT ENFORCED
) WITH (
    'connector'                     = 'confluent',
    'kafka.topic'                   = 'payments.enriched',
    'value.format'                  = 'avro-confluent',
    'value.avro-confluent.url'      = '${SCHEMA_REGISTRY_URL}',
    'key.format'                    = 'avro-confluent',
    'key.avro-confluent.url'        = '${SCHEMA_REGISTRY_URL}'
);

-- ─────────────────────────────────────────────────────────────
--  SINK: fraud alerts topic
-- ─────────────────────────────────────────────────────────────
CREATE TABLE fraud_alerts (
    card_number       STRING,
    account_id        STRING,
    holder_name       STRING,
    email             STRING,
    txn_count         BIGINT,
    total_amount      DOUBLE,
    window_start      TIMESTAMP(3),
    window_end        TIMESTAMP(3),
    alert_reason      STRING,
    PRIMARY KEY (card_number, window_start) NOT ENFORCED
) WITH (
    'connector'                     = 'confluent',
    'kafka.topic'                   = 'payments.fraud_alerts',
    'value.format'                  = 'avro-confluent',
    'value.avro-confluent.url'      = '${SCHEMA_REGISTRY_URL}',
    'key.format'                    = 'avro-confluent',
    'key.avro-confluent.url'        = '${SCHEMA_REGISTRY_URL}'
);
