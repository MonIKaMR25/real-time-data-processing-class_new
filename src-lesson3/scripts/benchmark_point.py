"""Point query comparison: Postgres (B+ tree) vs DuckDB (Parquet zone maps)."""

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
TS = "'2025-02-15 14:30:00'"

SQL = f"SELECT * FROM {{table}} WHERE pickup_datetime = {TS}"

DUCK_TABLE = f"""(
    SELECT
        tpep_pickup_datetime  AS pickup_datetime,
        tpep_dropoff_datetime AS dropoff_datetime,
        PULocationID          AS pickup_location_id,
        DOLocationID          AS dropoff_location_id,
        VendorID              AS vendor_id,
        passenger_count,
        trip_distance,
        RatecodeID            AS rate_code_id,
        store_and_fwd_flag,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        Airport_fee           AS airport_fee,
        cbd_congestion_fee
    FROM read_parquet('{PARQUET_GLOB}', union_by_name=true)
)"""


def run_pg():
    with psycopg.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + SQL.format(table="trips"))
            plan = cur.fetchall()
            t0 = time.monotonic()
            cur.execute(SQL.format(table="trips"))
            rows = cur.fetchall()
            elapsed = time.monotonic() - t0
    plan_json = plan[0][0][0]
    return elapsed, rows, plan_json


def run_duck():
    con = duckdb.connect()
    try:
        t0 = time.monotonic()
        result = con.sql(SQL.format(table=DUCK_TABLE)).fetchall()
        elapsed = time.monotonic() - t0
    finally:
        con.close()
    return elapsed, result


def duck_explain():
    con = duckdb.connect()
    try:
        r = con.sql(f"EXPLAIN ANALYZE {SQL.format(table=DUCK_TABLE)}").fetchall()
    finally:
        con.close()
    return r[0][1]


def row_group_analysis():
    con = duckdb.connect()
    try:
        total = con.sql(f"""
            SELECT COUNT(DISTINCT file_name || ':' || row_group_id) AS total_rg
            FROM parquet_metadata('{PARQUET_GLOB}')
            WHERE path_in_schema = 'tpep_pickup_datetime'
        """).fetchone()[0]

        matching = con.sql(f"""
            SELECT COUNT(DISTINCT file_name || ':' || row_group_id) AS matching_rg
            FROM parquet_metadata('{PARQUET_GLOB}')
            WHERE path_in_schema = 'tpep_pickup_datetime'
              AND CAST(stats_min AS TIMESTAMP) <= {TS}::TIMESTAMP
              AND CAST(stats_max AS TIMESTAMP) >= {TS}::TIMESTAMP
        """).fetchone()[0]
    finally:
        con.close()
    return total, matching


def main():
    print("=" * 70)
    print(f"  Point query: SELECT * FROM trips WHERE pickup_datetime = {TS}")
    print("=" * 70)

    # DuckDB
    print("\n  [DuckDB]  running...")
    duck_time, duck_result = run_duck()
    print(f"    Result:  {len(duck_result)} rows")
    print(f"    Time:    {duck_time:.4f}s")

    total_rg, matching_rg = row_group_analysis()
    print(f"    Row groups: {matching_rg}/{total_rg} matched (zone maps)")

    print("\n  [DuckDB]  EXPLAIN ANALYZE:")
    print("-" * 70)
    plan = duck_explain()
    print(plan)
    print("-" * 70)

    # Postgres
    print("\n  [Postgres]  running...")
    pg_time, pg_result, pg_plan = run_pg()
    print(f"    Result:  {len(pg_result)} rows")
    print(f"    Time:    {pg_time:.4f}s")
    plan_node = pg_plan["Plan"]
    print(f"    Node type: {plan_node.get('Node Type', '?')}")
    if "Index Name" in plan_node.get("Plans", [{}])[0]:
        print(f"    Index:    {plan_node['Plans'][0]['Index Name']}")
    hit = plan_node.get("Shared Hit Blocks", 0)
    read = plan_node.get("Shared Read Blocks", 0)
    print(f"    Buffers:  {hit + read} blocks ({hit} hit, {read} read)")

    # Summary
    print("\n" + "=" * 70)
    print(f"  {'Engine':20} {'Time':>10} {'Rows':>10}")
    print(f"  {'─'*20} {'─'*10} {'─'*10}")
    print(f"  {'Postgres':20} {pg_time:>8.4f}s {len(pg_result):>10}")
    print(f"  {'DuckDB':20} {duck_time:>8.4f}s {len(duck_result):>10}")
    ratio = duck_time / pg_time if pg_time else 0
    print(f"\n  Postgres is {ratio:.0f}× faster for point query")


if __name__ == "__main__":
    main()
