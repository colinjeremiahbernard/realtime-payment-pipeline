-- =============================================================
--  payments database – schema + synthetic seed data
--  Run automatically by Docker on first container start.
-- =============================================================

-- ─────────────────────────────────────────────────────────────
--  1. ACCOUNTS table
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS accounts (
    account_id   UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    holder_name  TEXT         NOT NULL,
    email        TEXT         NOT NULL UNIQUE,
    card_number  TEXT         NOT NULL UNIQUE,   -- masked, e.g. **** **** **** 1234
    card_brand   TEXT         NOT NULL,          -- VISA, MASTERCARD, ELO, AMEX
    credit_limit NUMERIC(12,2) NOT NULL,
    status       TEXT         NOT NULL DEFAULT 'ACTIVE', -- ACTIVE | BLOCKED | CLOSED
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- ─────────────────────────────────────────────────────────────
--  2. TRANSACTIONS table
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id  UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id      UUID         NOT NULL REFERENCES accounts(account_id),
    card_number     TEXT         NOT NULL,
    merchant_name   TEXT         NOT NULL,
    merchant_category TEXT       NOT NULL,  -- e.g. FOOD, TRAVEL, ELECTRONICS
    amount          NUMERIC(10,2) NOT NULL,
    currency        TEXT         NOT NULL DEFAULT 'BRL',
    status          TEXT         NOT NULL DEFAULT 'PENDING', -- PENDING | APPROVED | DECLINED | REVERSED
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- ─────────────────────────────────────────────────────────────
--  3. Enable logical replication on both tables
--     Debezium needs a publication or will replicate ALL tables.
-- ─────────────────────────────────────────────────────────────
SELECT pg_create_logical_replication_slot('debezium_slot', 'pgoutput')
WHERE NOT EXISTS (
    SELECT 1 FROM pg_replication_slots WHERE slot_name = 'debezium_slot'
);

CREATE PUBLICATION debezium_pub
    FOR TABLE accounts, transactions;

-- ─────────────────────────────────────────────────────────────
--  4. Synthetic seed – ACCOUNTS (10 records)
-- ─────────────────────────────────────────────────────────────
INSERT INTO accounts (account_id, holder_name, email, card_number, card_brand, credit_limit, status)
VALUES
  ('a1000000-0000-0000-0000-000000000001', 'Alice Souza',    'alice@example.com',   '**** **** **** 1001', 'VISA',       5000.00, 'ACTIVE'),
  ('a1000000-0000-0000-0000-000000000002', 'Bruno Lima',     'bruno@example.com',   '**** **** **** 1002', 'MASTERCARD', 8000.00, 'ACTIVE'),
  ('a1000000-0000-0000-0000-000000000003', 'Carla Mendes',   'carla@example.com',   '**** **** **** 1003', 'ELO',        3000.00, 'ACTIVE'),
  ('a1000000-0000-0000-0000-000000000004', 'Diego Farias',   'diego@example.com',   '**** **** **** 1004', 'VISA',       12000.00,'ACTIVE'),
  ('a1000000-0000-0000-0000-000000000005', 'Elena Costa',    'elena@example.com',   '**** **** **** 1005', 'AMEX',       20000.00,'ACTIVE'),
  ('a1000000-0000-0000-0000-000000000006', 'Felipe Rocha',   'felipe@example.com',  '**** **** **** 1006', 'MASTERCARD', 6000.00, 'BLOCKED'),
  ('a1000000-0000-0000-0000-000000000007', 'Gabi Nunes',     'gabi@example.com',    '**** **** **** 1007', 'VISA',       4500.00, 'ACTIVE'),
  ('a1000000-0000-0000-0000-000000000008', 'Hugo Alves',     'hugo@example.com',    '**** **** **** 1008', 'ELO',        7500.00, 'ACTIVE'),
  ('a1000000-0000-0000-0000-000000000009', 'Iris Batista',   'iris@example.com',    '**** **** **** 1009', 'VISA',       9000.00, 'ACTIVE'),
  ('a1000000-0000-0000-0000-000000000010', 'João Moreira',   'joao@example.com',    '**** **** **** 1010', 'MASTERCARD', 15000.00,'ACTIVE');

-- ─────────────────────────────────────────────────────────────
--  5. Synthetic seed – TRANSACTIONS (20 records)
--     Last 3 rows for Alice use the same card in quick succession
--     → they will trigger the fraud rule in Flink.
-- ─────────────────────────────────────────────────────────────
INSERT INTO transactions (transaction_id, account_id, card_number, merchant_name, merchant_category, amount, currency, status, created_at)
VALUES
  -- Normal transactions spread across accounts
  ('b2000000-0000-0000-0000-000000000001', 'a1000000-0000-0000-0000-000000000001', '**** **** **** 1001', 'Supermercado Extra',  'FOOD',        120.50, 'BRL', 'APPROVED', now() - INTERVAL '2 days'),
  ('b2000000-0000-0000-0000-000000000002', 'a1000000-0000-0000-0000-000000000002', '**** **** **** 1002', 'Posto Shell',         'FUEL',         80.00, 'BRL', 'APPROVED', now() - INTERVAL '2 days'),
  ('b2000000-0000-0000-0000-000000000003', 'a1000000-0000-0000-0000-000000000003', '**** **** **** 1003', 'Amazon',              'ELECTRONICS', 350.00, 'BRL', 'APPROVED', now() - INTERVAL '1 day'),
  ('b2000000-0000-0000-0000-000000000004', 'a1000000-0000-0000-0000-000000000004', '**** **** **** 1004', 'Latam Airlines',      'TRAVEL',     1200.00, 'BRL', 'APPROVED', now() - INTERVAL '1 day'),
  ('b2000000-0000-0000-0000-000000000005', 'a1000000-0000-0000-0000-000000000005', '**** **** **** 1005', 'Hotel Ibis',          'TRAVEL',      500.00, 'BRL', 'APPROVED', now() - INTERVAL '1 day'),
  ('b2000000-0000-0000-0000-000000000006', 'a1000000-0000-0000-0000-000000000006', '**** **** **** 1006', 'iFood',               'FOOD',         45.90, 'BRL', 'DECLINED', now() - INTERVAL '12 hours'),
  ('b2000000-0000-0000-0000-000000000007', 'a1000000-0000-0000-0000-000000000007', '**** **** **** 1007', 'Droga Raia',          'HEALTH',       88.00, 'BRL', 'APPROVED', now() - INTERVAL '12 hours'),
  ('b2000000-0000-0000-0000-000000000008', 'a1000000-0000-0000-0000-000000000008', '**** **** **** 1008', 'Renner',              'RETAIL',      230.00, 'BRL', 'APPROVED', now() - INTERVAL '6 hours'),
  ('b2000000-0000-0000-0000-000000000009', 'a1000000-0000-0000-0000-000000000009', '**** **** **** 1009', 'Submarino',           'ELECTRONICS', 899.00, 'BRL', 'APPROVED', now() - INTERVAL '6 hours'),
  ('b2000000-0000-0000-0000-000000000010', 'a1000000-0000-0000-0000-000000000010', '**** **** **** 1010', 'Booking.com',         'TRAVEL',      950.00, 'BRL', 'APPROVED', now() - INTERVAL '3 hours'),
  ('b2000000-0000-0000-0000-000000000011', 'a1000000-0000-0000-0000-000000000002', '**** **** **** 1002', 'Pão de Açúcar',       'FOOD',         67.40, 'BRL', 'APPROVED', now() - INTERVAL '2 hours'),
  ('b2000000-0000-0000-0000-000000000012', 'a1000000-0000-0000-0000-000000000003', '**** **** **** 1003', 'Netshoes',            'RETAIL',      175.00, 'BRL', 'APPROVED', now() - INTERVAL '2 hours'),
  ('b2000000-0000-0000-0000-000000000013', 'a1000000-0000-0000-0000-000000000004', '**** **** **** 1004', 'Cinemark',            'ENTERTAINMENT', 60.00,'BRL', 'APPROVED', now() - INTERVAL '1 hour'),
  ('b2000000-0000-0000-0000-000000000014', 'a1000000-0000-0000-0000-000000000005', '**** **** **** 1005', 'Apple Store',         'ELECTRONICS',2999.00, 'BRL', 'APPROVED', now() - INTERVAL '1 hour'),
  ('b2000000-0000-0000-0000-000000000015', 'a1000000-0000-0000-0000-000000000007', '**** **** **** 1007', 'Decathlon',           'SPORTS',      320.00, 'BRL', 'APPROVED', now() - INTERVAL '30 minutes'),
  ('b2000000-0000-0000-0000-000000000016', 'a1000000-0000-0000-0000-000000000008', '**** **** **** 1008', 'Americanas',          'RETAIL',      145.00, 'BRL', 'APPROVED', now() - INTERVAL '20 minutes'),
  ('b2000000-0000-0000-0000-000000000017', 'a1000000-0000-0000-0000-000000000009', '**** **** **** 1009', 'Casas Bahia',         'ELECTRONICS', 780.00, 'BRL', 'APPROVED', now() - INTERVAL '15 minutes'),
  -- ⚠ FRAUD SCENARIO: Alice (**** 1001) — 3 transactions in < 60 seconds
  ('b2000000-0000-0000-0000-000000000018', 'a1000000-0000-0000-0000-000000000001', '**** **** **** 1001', 'Loja Suspeita A',     'ELECTRONICS', 999.00, 'BRL', 'PENDING',  now() - INTERVAL '50 seconds'),
  ('b2000000-0000-0000-0000-000000000019', 'a1000000-0000-0000-0000-000000000001', '**** **** **** 1001', 'Loja Suspeita B',     'ELECTRONICS', 999.00, 'BRL', 'PENDING',  now() - INTERVAL '30 seconds'),
  ('b2000000-0000-0000-0000-000000000020', 'a1000000-0000-0000-0000-000000000001', '**** **** **** 1001', 'Loja Suspeita C',     'ELECTRONICS', 999.00, 'BRL', 'PENDING',  now() - INTERVAL '10 seconds');

-- ─────────────────────────────────────────────────────────────
--  6. Helper: simulate a live fraud event by inserting 3 fast txns
--     Run manually after the pipeline is running:
--       psql -f trigger_fraud.sql
-- ─────────────────────────────────────────────────────────────
-- (See scripts/ for the trigger_fraud.sh helper)
