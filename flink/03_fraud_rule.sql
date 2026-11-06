-- =============================================================
--  Flink SQL – Layer 4 Step 3: Fraud Detection
--
--  Rule: 3 or more transactions on the same card_number
--        within a TUMBLING window of 60 seconds → FRAUD ALERT.
--
--  Source : enriched_transactions  (output of step 2)
--  Sink   : fraud_alerts
--
--  Why TUMBLE and not HOP/SESSION?
--    • TUMBLE gives a crisp, non-overlapping window so each alert
--      covers a distinct 60-second bucket. Easy to audit and
--      deduplicate downstream.
--    • SESSION windows would be more accurate for "burst" fraud
--      but require state cleanup tuning. TUMBLE is the safer
--      first rule to ship and explain.
-- =============================================================

INSERT INTO fraud_alerts
SELECT
    card_number,
    -- Pick any account_id / holder_name in the window (they're all
    -- from the same card so they're identical).
    MAX(account_id)   AS account_id,
    MAX(holder_name)  AS holder_name,
    MAX(email)        AS email,
    COUNT(*)          AS txn_count,
    SUM(amount)       AS total_amount,
    window_start,
    window_end,
    CONCAT(
        'FRAUD_ALERT: ',
        CAST(COUNT(*) AS STRING),
        ' transactions on card ',
        card_number,
        ' in 60 s — total BRL ',
        CAST(ROUND(SUM(amount), 2) AS STRING)
    )                 AS alert_reason
FROM TABLE(
    TUMBLE(
        TABLE enriched_transactions,
        DESCRIPTOR(created_at),
        INTERVAL '60' SECOND
    )
)
-- Only look at transactions that are still pending or approved
-- (skip already-reversed or declined ones)
WHERE txn_status IN ('PENDING', 'APPROVED')
GROUP BY
    card_number,
    window_start,
    window_end
HAVING COUNT(*) >= 3;

-- =============================================================
--  BONUS RULE (run as a separate INSERT if desired):
--  High-value single transaction on a BLOCKED account → alert.
-- =============================================================
-- INSERT INTO fraud_alerts
-- SELECT
--     et.card_number,
--     et.account_id,
--     et.holder_name,
--     et.email,
--     1                 AS txn_count,
--     et.amount         AS total_amount,
--     et.created_at     AS window_start,
--     et.created_at     AS window_end,
--     CONCAT(
--         'BLOCKED_ACCOUNT_TXN: card ',
--         et.card_number,
--         ' account_status=BLOCKED amount=',
--         CAST(et.amount AS STRING)
--     )                 AS alert_reason
-- FROM enriched_transactions AS et
-- WHERE et.account_status = 'BLOCKED'
--   AND et.amount > 100.0;
