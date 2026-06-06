"""Dagster version of the Lesson 4 pipeline — SAME logic, different mental model.

Airflow thinks in TASKS ("did extract→transform→load run today?").
Dagster thinks in ASSETS ("is `daily_revenue` up to date with `raw_daily_orders`?").

The dependency is the function signature: daily_revenue(raw_daily_orders) means
"daily_revenue is derived from raw_daily_orders." Dagster draws the lineage graph
from that, tracks partitions, and marks downstream assets stale when an upstream
one is re-materialized.

As in the Airflow DAG, raw_daily_orders stages to Parquet and passes the path
(not 1M rows) downstream. Idempotency is still ours: DELETE + INSERT in one txn.
"""

import os
from datetime import datetime
from pathlib import Path

import duckdb
from dagster import (
    AssetExecutionContext,
    DailyPartitionsDefinition,
    MaterializeResult,
    MetadataValue,
    asset,
)

PG_HOST = os.environ.get("PG_HOST", "postgres")
PG_DSN = f"postgresql://bench:bench@{PG_HOST}:5432/bench"
DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "/app/data/analytics.duckdb")
STAGING_DIR = Path(os.environ.get("STAGING_DIR", "/app/data/staging"))

# One partition per day — the analytical equivalent of Airflow's daily schedule.
daily = DailyPartitionsDefinition(start_date="2024-01-10", end_date="2024-04-01")


@asset(
    partitions_def=daily,
    description="Raw orders for one day, staged from OLTP Postgres to Parquet.",
    metadata={"source": "oltp_postgres", "table": "orders"},
)
def raw_daily_orders(context: AssetExecutionContext) -> str:
    ds = context.partition_key
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    out = STAGING_DIR / f"orders_{ds}.parquet"
    con = duckdb.connect()
    con.execute("INSTALL postgres; LOAD postgres")
    con.execute(f"ATTACH '{PG_DSN}' AS pg (TYPE postgres)")
    con.execute(
        f"""
        COPY (
            SELECT id, customer_id, amount, status, created_at
            FROM pg.orders WHERE created_at::date = DATE '{ds}'
        ) TO '{out}' (FORMAT parquet)
        """
    )
    n = con.execute(f"SELECT COUNT(*) FROM '{out}'").fetchone()[0]
    context.add_output_metadata({"row_count": MetadataValue.int(int(n)), "path": str(out)})
    context.log.info(f"Staged {n} orders for {ds}")
    return str(out)


@asset(
    partitions_def=daily,
    description="Daily revenue aggregated by status, loaded idempotently to DuckDB.",
)
def daily_revenue(context: AssetExecutionContext, raw_daily_orders: str) -> MaterializeResult:
    ds = context.partition_key
    con = duckdb.connect()
    agg = con.execute(
        f"""
        SELECT DATE '{ds}' AS date, status, SUM(amount) AS total_revenue,
               COUNT(*) AS order_count, ROUND(AVG(amount), 2) AS avg_order_value
        FROM '{raw_daily_orders}' GROUP BY status ORDER BY status
        """
    ).fetchall()

    Path(DUCKDB_PATH).parent.mkdir(parents=True, exist_ok=True)
    disk = duckdb.connect(DUCKDB_PATH)
    disk.execute("""
        CREATE TABLE IF NOT EXISTS daily_revenue (
            date DATE, status TEXT, total_revenue DECIMAL(18,2),
            order_count BIGINT, avg_order_value DECIMAL(10,2), loaded_at TIMESTAMP)
    """)
    disk.execute("BEGIN TRANSACTION")
    disk.execute("DELETE FROM daily_revenue WHERE date = ?", (ds,))
    for r in agg:
        disk.execute("INSERT INTO daily_revenue VALUES (?, ?, ?, ?, ?, ?)", (*r, datetime.now()))
    disk.execute("COMMIT")

    preview = "| status | revenue | orders |\n|---|---|---|\n" + "\n".join(
        f"| {r[1]} | {r[2]} | {r[3]} |" for r in agg
    )
    return MaterializeResult(
        metadata={
            "row_count": MetadataValue.int(len(agg)),
            "preview": MetadataValue.md(preview),
        }
    )
