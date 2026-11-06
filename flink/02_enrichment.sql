-- =============================================================
--  Flink SQL – Layer 4 Step 2: Enrichment JOIN
--
--  Joins every incoming transaction with the latest account
--  snapshot (changelog table) and writes the merged record
--  to payments.enriched.
--
--  Pattern used: temporal / regular JOIN against a changelog
--  table backed by a Kafka compacted topic (accounts).
-- =============================================================

INSERT INTO enriched_transactions
SELECT
    t.transaction_id,
    t.account_id,
    a.holder_name,
    a.email,
    t.card_number,
    a.card_brand,
    a.credit_limit,
    a.status          AS account_status,
    t.merchant_name,
    t.merchant_category,
    t.amount,
    t.currency,
    t.status          AS txn_status,
    t.created_at
FROM transactions AS t
-- Regular JOIN: Flink keeps the latest account state in state backend.
-- Use LEFT JOIN if you want to forward transactions even when the
-- account row hasn't arrived yet (account_status will be NULL).
JOIN accounts AS a
    ON t.account_id = a.account_id
-- Only forward non-delete events from the CDC stream
WHERE t.`__op` <> 'd';
