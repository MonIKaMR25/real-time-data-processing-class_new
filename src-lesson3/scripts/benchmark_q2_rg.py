"""Q2: Filtered aggregation — time both engines, show DuckDB row groups."""

import os
import time
from pathlib import Path

import duckdb
import psycopg

DATA_DIR = Path(__file__).parent.parent / "data"
PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_DSN = f"postgresql://bench:bench@{PG_HOST}:{PG_PORT}/bench"
PARQUET_GLOB = str(DATA_DIR / "yellow_tripdata_*.parquet")

SQL = """SELECT DATE_TRUNC('month', pickup_datetime) AS month,
                 payment_type,
                 COUNT(*) AS trips,
                 AVG(fare_amount) AS avg_fare,
                 SUM(tip_amount) AS total_tips
          FROM {table}
          WHERE pickup_datetime >= '2025-02-01'
            AND pickup_datetime < '2025-03-01'
          GROUP BY month, payment_type
          ORDER BY month, payment_type"""

DUCK_TABLE = f"""(
    SELECT
        tpep_pickup_datetime  AS pickup_datetime,
        tpep_dropoff_datetime AS dropoff_datetime,
        PULocationID          AS pickup_location_id,
        DOLocationID          AS dropoff_location_id,
        payment_type,
        fare_amount,
        tip_amount,
        trip_distance
    FROM read_parquet('{PARQUET_GLOB}', union_by_name=true)
)"""


def run_pg():
    with psycopg.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            t0 = time.monotonic()
            cur.execute(SQL.format(table="trips"))
            rows = cur.fetchall()
            elapsed = time.monotonic() - t0
    return elapsed, rows


def run_duck():
    con = duckdb.connect()
    try:
        t0 = time.monotonic()
        result = con.sql(SQL.format(table=DUCK_TABLE)).fetchall()
        elapsed = time.monotonic() - t0
    finally:
        con.close()
    return elapsed, result


def duck_explain_analyze():
    con = duckdb.connect()
    try:
        r = con.sql(f"EXPLAIN ANALYZE {SQL.format(table=DUCK_TABLE)}").fetchall()
    finally:
        con.close()
    return r[0][1]


def main():
    print("=" * 70)
    print("  Query: Q2 — Filtered aggregation (zone maps)")
    print("  WHERE pickup_datetime >= '2025-02-01' AND pickup_datetime < '2025-03-01'")
    print("=" * 70)

    # ── DuckDB timing ──
    print("\n  [DuckDB]  running query...")
    duck_time, duck_result = run_duck()
    print(f"    Result:  {duck_result}")
    print(f"    Time:    {duck_time:.3f}s")

    # ── DuckDB EXPLAIN ANALYZE ──
    print("\n  [DuckDB]  EXPLAIN ANALYZE:")
    print("-" * 70)
    plan = duck_explain_analyze()
    print(plan)
    print("-" * 70)

    # ── Postgres timing ──
    print("\n  [Postgres]  running query...")
    pg_time, pg_result = run_pg()
    print(f"    Result:  {pg_result}")
    print(f"    Time:    {pg_time:.3f}s")

    # ── Summary ──
    print("\n" + "=" * 70)
    print(f"  {'Engine':20} {'Time':>10}")
    print(f"  {'─'*20} {'─'*10}")
    print(f"  {'Postgres':20} {pg_time:>8.3f}s")
    print(f"  {'DuckDB':20} {duck_time:>8.3f}s")
    ratio = pg_time / duck_time
    print(f"\n  DuckDB is {ratio:.0f}× faster")


if __name__ == "__main__":
    main()
