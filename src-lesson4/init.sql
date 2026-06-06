-- Lesson 4: OLTP SOURCE schema (Postgres) for the batch-ETL workshop.
--
-- This is the *source* system. The batch pipeline extracts from here, transforms
-- with DuckDB, and loads an analytical target (data/analytics.duckdb).
--
-- Two tables:
--   orders     — the fact source. Date-partitioned aggregate target (daily_revenue).
--   customers  — a mutable dimension. Addresses change over time → SCD Type 2 demo.
--
-- wal_level=logical is set in docker-compose so this same database can feed
-- Lesson 5's CDC demo without re-provisioning.

CREATE TABLE IF NOT EXISTS customers (
    id          INT PRIMARY KEY,
    name        TEXT        NOT NULL,
    city        TEXT        NOT NULL,
    region      TEXT        NOT NULL,
    signup_date DATE        NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS orders (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id INT         NOT NULL,
    amount      NUMERIC(10,2) NOT NULL,
    status      TEXT        NOT NULL DEFAULT 'pending',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The pipeline filters by created_at::date every run, so this index turns the
-- per-date extract from a seq scan into a range scan. Created here (small table
-- at provision time); for a bulk seed you'd build it after the load.
CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders (created_at);
