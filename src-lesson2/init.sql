-- ============================================================
-- Lesson 2: CockroachDB schema — mirrors Lesson 1 Postgres
-- ============================================================

CREATE DATABASE IF NOT EXISTS bench;
USE bench;

-- Same table as Lesson 1: same schema, same workload, different engine
CREATE TABLE IF NOT EXISTS orders (
    id          INT8 DEFAULT unique_rowid() PRIMARY KEY,
    customer_id INT4 NOT NULL,
    amount      DECIMAL(10, 2),
    status      STRING DEFAULT 'pending',
    created_at  TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders (customer_id);

-- Accounts table for distributed transaction experiments (debit-credit)
CREATE TABLE IF NOT EXISTS accounts (
    id      INT8 PRIMARY KEY,
    balance DECIMAL(12, 2) NOT NULL DEFAULT 0.00
);

-- Seed 1000 accounts across the key space so they land on different ranges
INSERT INTO accounts (id, balance)
SELECT generate_series(1, 1000), 10000.00
ON CONFLICT (id) DO NOTHING;
