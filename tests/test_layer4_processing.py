"""
Layer 4 — Processing Tests
Validates Flink SQL definitions and executes streaming window & enrichment join simulations.
"""
import os
import sqlite3
from datetime import datetime, timedelta
import pytest

REPO_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def test_flink_sql_files_exist_and_contain_expected_statements():
    f1 = os.path.join(REPO_DIR, "flink", "01_create_tables.sql")
    f2 = os.path.join(REPO_DIR, "flink", "02_enrichment.sql")
    f3 = os.path.join(REPO_DIR, "flink", "03_fraud_rule.sql")
    
    for path in [f1, f2, f3]:
        assert os.path.exists(path), f"File {path} must exist"
        
    with open(f1, "r", encoding="utf-8") as f:
        c1 = f.read()
    assert "CREATE TABLE transactions" in c1
    assert "CREATE TABLE accounts" in c1
    assert "CREATE TABLE enriched_transactions" in c1
    assert "CREATE TABLE fraud_alerts" in c1
    assert "WATERMARK FOR created_at AS created_at - INTERVAL '5' SECOND" in c1

    with open(f2, "r", encoding="utf-8") as f:
        c2 = f.read()
    assert "INSERT INTO enriched_transactions" in c2
    assert "JOIN accounts AS a" in c2
    assert "WHERE t.`__op` <> 'd'" in c2

    with open(f3, "r", encoding="utf-8") as f:
        c3 = f.read()
    assert "INSERT INTO fraud_alerts" in c3
    assert "INTERVAL '60' SECOND" in c3
    assert "HAVING COUNT(*) >= 3" in c3

def test_streaming_enrichment_and_fraud_rule_simulation():
    # In-memory relational database to simulate Flink SQL changelog join and tumbling window
    conn = sqlite3.connect(":memory:")
    cur = conn.cursor()
    
    cur.execute("""
    CREATE TABLE accounts (
        account_id   TEXT PRIMARY KEY,
        holder_name  TEXT NOT NULL,
        email        TEXT NOT NULL,
        card_number  TEXT NOT NULL,
        card_brand   TEXT NOT NULL,
        credit_limit REAL NOT NULL,
        status       TEXT NOT NULL
    )
    """)
    
    cur.execute("""
    CREATE TABLE transactions (
        transaction_id    TEXT PRIMARY KEY,
        account_id        TEXT NOT NULL,
        card_number       TEXT NOT NULL,
        merchant_name     TEXT NOT NULL,
        merchant_category TEXT NOT NULL,
        amount            REAL NOT NULL,
        currency          TEXT NOT NULL,
        status            TEXT NOT NULL,
        created_at        TEXT NOT NULL,
        __op              TEXT NOT NULL
    )
    """)
    
    # Insert Account
    cur.execute("""
    INSERT INTO accounts VALUES 
    ('a1000000-0000-0000-0000-000000000001', 'Alice Souza', 'alice@example.com', '**** **** **** 1001', 'VISA', 5000.00, 'ACTIVE')
    """)
    
    # Insert regular transaction, plus 3 rapid transactions (fraud burst) within 60s, plus a deleted transaction
    transactions_data = [
        # Normal transaction earlier
        ('tx-001', 'a1000000-0000-0000-0000-000000000001', '**** **** **** 1001', 'Store A', 'FOOD', 50.00, 'BRL', 'APPROVED', '2026-11-06 20:00:00', 'c'),
        # Deleted transaction (should be ignored by Flink WHERE __op <> 'd')
        ('tx-002', 'a1000000-0000-0000-0000-000000000001', '**** **** **** 1001', 'Store Del', 'FOOD', 99.00, 'BRL', 'APPROVED', '2026-11-06 21:00:00', 'd'),
        # Fraud Burst within 60s window: 23:09:10, 23:09:30, 23:09:50
        ('tx-003', 'a1000000-0000-0000-0000-000000000001', '**** **** **** 1001', 'Loja Suspeita A', 'ELECTRONICS', 999.00, 'BRL', 'PENDING', '2026-11-06 23:09:10', 'c'),
        ('tx-004', 'a1000000-0000-0000-0000-000000000001', '**** **** **** 1001', 'Loja Suspeita B', 'ELECTRONICS', 999.00, 'BRL', 'PENDING', '2026-11-06 23:09:30', 'c'),
        ('tx-005', 'a1000000-0000-0000-0000-000000000001', '**** **** **** 1001', 'Loja Suspeita C', 'ELECTRONICS', 999.00, 'BRL', 'PENDING', '2026-11-06 23:09:50', 'c')
    ]
    cur.executemany("INSERT INTO transactions VALUES (?,?,?,?,?,?,?,?,?,?)", transactions_data)
    
    # Simulate Flink 02_enrichment.sql
    cur.execute("""
    CREATE TABLE enriched_transactions AS
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
    JOIN accounts AS a
        ON t.account_id = a.account_id
    WHERE t.__op <> 'd'
    """)
    
    # Assert deleted transaction tx-002 was not forwarded
    cur.execute("SELECT transaction_id FROM enriched_transactions WHERE transaction_id = 'tx-002'")
    assert cur.fetchone() is None, "Deleted transaction must be excluded from enriched_transactions"
    
    cur.execute("SELECT count(*) FROM enriched_transactions")
    assert cur.fetchone()[0] == 4, "Expected 4 enriched transactions (5 total minus 1 delete)"
    
    # Simulate Flink 03_fraud_rule.sql
    # Window: 23:09:00 to 23:10:00
    cur.execute("""
    SELECT
        card_number,
        MAX(account_id)   AS account_id,
        MAX(holder_name)  AS holder_name,
        MAX(email)        AS email,
        COUNT(*)          AS txn_count,
        SUM(amount)       AS total_amount,
        '2026-11-06 23:09:00' AS window_start,
        '2026-11-06 23:10:00' AS window_end,
        'FRAUD_ALERT: ' || COUNT(*) || ' transactions on card ' || card_number || ' in 60 s — total BRL ' || ROUND(SUM(amount), 2) AS alert_reason
    FROM enriched_transactions
    WHERE txn_status IN ('PENDING', 'APPROVED')
      AND created_at >= '2026-11-06 23:09:00' AND created_at < '2026-11-06 23:10:00'
    GROUP BY card_number
    HAVING COUNT(*) >= 3
    """)
    alerts = cur.fetchall()
    
    assert len(alerts) == 1, "Expected exactly 1 fraud alert for the 60s tumbling window"
    alert = alerts[0]
    assert alert[0] == '**** **** **** 1001'
    assert alert[1] == 'a1000000-0000-0000-0000-000000000001'
    assert alert[2] == 'Alice Souza'
    assert alert[3] == 'alice@example.com'
    assert alert[4] == 3
    assert alert[5] == 2997.00
    assert "FRAUD_ALERT: 3 transactions on card **** **** **** 1001 in 60 s — total BRL 2997.0" in alert[8]
