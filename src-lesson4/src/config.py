"""Shared connection config + target-schema helpers for the Lesson 4 pipeline.

Every script imports from here so the source/target wiring lives in one place.

Environment overrides (all optional, sane localhost defaults):
    PG_HOST       Postgres host          (default: localhost)
    PG_PORT       Postgres port          (default: 5432)
    DUCKDB_PATH   analytical target file (default: <repo>/data/analytics.duckdb)
"""

import os
from pathlib import Path

import duckdb

# ── Paths ────────────────────────────────────────────────────────────────────
DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)
DUCKDB_PATH = Path(os.environ.get("DUCKDB_PATH", DATA_DIR / "analytics.duckdb"))

# ── Postgres OLTP source ───────────────────────────────────────────────────────
PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = os.environ.get("PG_PORT", "5432")
# psycopg / libpq URI form
PG_DSN = f"postgresql://bench:bench@{PG_HOST}:{PG_PORT}/bench"
# DuckDB postgres-extension ATTACH form (key=value)
PG_CONN_STR = f"host={PG_HOST} port={PG_PORT} user=bench password=bench dbname=bench"


def connect_target() -> duckdb.DuckDBPyConnection:
    """Open the analytical DuckDB target, ensuring its schema exists.

    DuckDB is single-writer: only one process may hold a write handle to the
    file at a time. Run the pipeline, Airflow, and Dagster one at a time.
    """
    con = duckdb.connect(str(DUCKDB_PATH))
    ensure_target_schema(con)
    return con


def ensure_target_schema(con: duckdb.DuckDBPyConnection) -> None:
    """Create the analytical target tables if they don't exist (idempotent DDL)."""
    # NOTE: daily_revenue has NO key on purpose. A naive blind-INSERT load can
    # therefore accumulate duplicates (the Phase-1 lesson), and DELETE + INSERT
    # fixes it WITHOUT needing a key (the stated benefit of partition replacement).
    con.execute("""
        CREATE TABLE IF NOT EXISTS daily_revenue (
            date            DATE          NOT NULL,
            status          TEXT          NOT NULL,
            total_revenue   DECIMAL(18,2) NOT NULL,
            order_count     BIGINT        NOT NULL,
            avg_order_value DECIMAL(10,2) NOT NULL,
            loaded_at       TIMESTAMP     NOT NULL
        )
    """)
    # daily_revenue_keyed DOES have a key — the UPSERT strategy needs one to know
    # which row to replace on conflict.
    con.execute("""
        CREATE TABLE IF NOT EXISTS daily_revenue_keyed (
            date            DATE          NOT NULL,
            status          TEXT          NOT NULL,
            total_revenue   DECIMAL(18,2) NOT NULL,
            order_count     BIGINT        NOT NULL,
            avg_order_value DECIMAL(10,2) NOT NULL,
            loaded_at       TIMESTAMP     NOT NULL,
            PRIMARY KEY (date, status)
        )
    """)
    # Watermark / run-ledger. The load and the mark-complete write to this table
    # must commit in the SAME transaction (see pipeline_watermark.py).
    con.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_metadata (
            table_name TEXT      NOT NULL,
            date       DATE      NOT NULL,
            loaded_at  TIMESTAMP NOT NULL,
            row_count  BIGINT    NOT NULL,
            PRIMARY KEY (table_name, date)
        )
    """)
    # SCD Type 2 customer dimension: one row per (customer, version window).
    con.execute("""
        CREATE TABLE IF NOT EXISTS customers_dim (
            customer_id INT     NOT NULL,
            name        TEXT    NOT NULL,
            city        TEXT    NOT NULL,
            region      TEXT    NOT NULL,
            valid_from  DATE    NOT NULL,
            valid_to    DATE    NOT NULL,   -- 9999-12-31 sentinel for the open record
            is_current  BOOLEAN NOT NULL
        )
    """)
