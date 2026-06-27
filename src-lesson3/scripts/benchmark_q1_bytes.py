"""Run Q1 in Postgres and DuckDB, timing each and reporting bytes read."""

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

SQL = """SELECT COUNT(*), AVG(fare_amount), AVG(tip_amount), AVG(trip_distance) FROM {table}"""

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


# ── Postgres ─────────────────────────────────────────────────────────

def run_pg():
    with psycopg.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            # Get buffers with EXPLAIN (ANALYZE, BUFFERS)
            explain_sql = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {SQL.format(table='trips')}"
            cur.execute(explain_sql)
            rows = cur.fetchall()
            explain_json = rows[0][0][0]

            # Time
            t0 = time.monotonic()
            cur.execute(SQL.format(table="trips"))
            result = cur.fetchall()
            elapsed = time.monotonic() - t0

    plan = explain_json["Plan"]

    def sum_buffers(node):
        total = (
            node.get("Shared Hit Blocks", 0) + node.get("Shared Read Blocks", 0)
        ) * 8192
        for child in node.get("Plans", []):
            total += sum_buffers(child)
        return total

    pg_bytes = sum_buffers(plan)

    return elapsed, pg_bytes, result[0]


# ── DuckDB ───────────────────────────────────────────────────────────

def run_duck():
    con = duckdb.connect()
    try:
        t0 = time.monotonic()
        result = con.sql(SQL.format(table=DUCK_TABLE)).fetchall()
        elapsed = time.monotonic() - t0
    finally:
        con.close()

    # DuckDB only reads the 3 projected columns (columnar storage).
    # Sum compressed column chunk sizes for fare_amount, tip_amount, trip_distance.
    con2 = duckdb.connect()
    try:
        col_sizes = con2.sql(f"""
            SELECT SUM(total_compressed_size)
            FROM parquet_metadata('{PARQUET_GLOB}')
            WHERE path_in_schema IN ('fare_amount', 'tip_amount', 'trip_distance')
        """).fetchone()[0] or 0
    finally:
        con2.close()

    return elapsed, col_sizes, result[0]


# ── Main ─────────────────────────────────────────────────────────────

def fmt_bytes(b):
    for unit in ("B", "KiB", "MiB", "GiB"):
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} TiB"


def main():
    print("=" * 70)
    print("  Query: SELECT COUNT(*), AVG(fare_amount), AVG(tip_amount),")
    print("               AVG(trip_distance)")
    print("         FROM trips;")
    print("=" * 70)
    print()

    # DuckDB
    print("  [DuckDB]  reading parquet files...")
    duck_time, duck_bytes, duck_result = run_duck()
    print(f"    Result:   {duck_result}")
    print(f"    Time:     {duck_time:.3f}s")
    print(f"    Bytes:    {fmt_bytes(duck_bytes)}")
    print()

    # Postgres
    print("  [Postgres]  querying table trips (11.2M rows)...")
    pg_time, pg_bytes, pg_result = run_pg()
    print(f"    Result:   {pg_result}")
    print(f"    Time:     {pg_time:.3f}s")
    print(f"    Bytes:    {fmt_bytes(pg_bytes)}")
    print()

    # Comparison
    print("─" * 70)
    print(f"  {'':20} {'Time':>12} {'Bytes Read':>14}")
    print(f"  {'─'*20} {'─'*12} {'─'*14}")
    print(f"  {'Postgres':20} {pg_time:>8.3f}s  {fmt_bytes(pg_bytes):>14}")
    print(f"  {'DuckDB':20} {duck_time:>8.3f}s  {fmt_bytes(duck_bytes):>14}")
    print()
    ratio_time = pg_time / duck_time
    print(f"  DuckDB is {ratio_time:.0f}× faster")
    if pg_bytes > 0:
        ratio_bytes = pg_bytes / max(duck_bytes, 1)
        print(f"  Postgres read {ratio_bytes:.0f}× more bytes than DuckDB")


if __name__ == "__main__":
    main()
