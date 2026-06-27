"""Prove idempotency: run the pipeline N times for one date, show the target is
identical every time. This is the deliverable standard for the take-home.

Prints, after each run: row count for the date + a content checksum (md5 over the
sorted aggregate rows). If the pipeline is idempotent, every row is the same.

Usage:
    python src/prove_idempotent.py 2024-01-15
    python src/prove_idempotent.py 2024-01-15 --runs 5 --strategy upsert
    python src/prove_idempotent.py 2024-01-15 --loader naive   # watch it FAIL
"""

import argparse
import hashlib
from datetime import date

from config import connect_target
from pipeline_idempotent import run_pipeline as run_idempotent
from pipeline_naive import run_pipeline as run_naive


def snapshot(target_date: date, table: str) -> tuple[int, str]:
    """Return (row_count, checksum) for the target date's aggregate rows."""
    con = connect_target()
    rows = con.execute(
        f"""
        SELECT status, total_revenue, order_count, avg_order_value
        FROM {table} WHERE date = ?
        ORDER BY status, total_revenue, order_count
        """,
        (target_date,),
    ).fetchall()
    con.close()
    digest = hashlib.md5(repr(rows).encode()).hexdigest()[:12]
    return len(rows), digest


def main(target_date: date, runs: int, strategy: str, loader: str) -> None:
    # upsert writes to the keyed table; everything else to daily_revenue.
    table = "daily_revenue_keyed" if (loader != "naive" and strategy == "upsert") else "daily_revenue"
    print(f"Proving idempotency: {loader} loader, {runs} runs for {target_date} (target: {table})\n")
    print(f"  {'run':<5}{'rows':<8}{'checksum':<16}")
    print("  " + "-" * 27)
    checksums = []
    for i in range(1, runs + 1):
        if loader == "naive":
            run_naive(target_date)
        else:
            run_idempotent(target_date, strategy)
        n, digest = snapshot(target_date, table)
        checksums.append((n, digest))
        print(f"  {i:<5}{n:<8}{digest:<16}")

    print()
    if len(set(checksums)) == 1:
        print(f"  IDEMPOTENT: all {runs} runs produced identical state ({checksums[0][0]} rows).")
    else:
        print(f"  NOT IDEMPOTENT: {len(set(checksums))} distinct states across {runs} runs.")
        print("  Row counts grew on each run — this is the naive-pipeline bug.")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("target_date", nargs="?", default="2024-01-15")
    p.add_argument("--runs", type=int, default=3)
    p.add_argument("--strategy", choices=["delete-insert", "upsert"], default="delete-insert")
    p.add_argument("--loader", choices=["idempotent", "naive"], default="idempotent")
    args = p.parse_args()
    main(date.fromisoformat(args.target_date), args.runs, args.strategy, args.loader)
