"""Phase 4 — multi-date pipeline with a watermark, and the atomicity trap.

A pipeline_metadata ledger tracks which dates loaded successfully, so a re-run
can skip finished dates (or --force them). THE critical subtlety: the data write
and the metadata write must commit in the SAME transaction. Otherwise:

  * mark-complete AFTER commit, crash in between → date looks unloaded, re-runs.
  * mark-complete BEFORE the load commits → date looks done but has no data.

Both are silent corruption. One transaction makes "loaded" and "recorded as
loaded" the same fact. This is exactly what an orchestrator's metadata DB must do.

Usage:
    python src/pipeline_watermark.py 2024-01-10 2024-01-20      # date range
    python src/pipeline_watermark.py 2024-01-10 2024-01-20 --force
"""

import argparse
from datetime import date, datetime, timedelta

from config import connect_target
from pipeline_naive import extract, transform

TABLE = "daily_revenue"


def already_loaded(con, target_date: date) -> bool:
    row = con.execute(
        "SELECT 1 FROM pipeline_metadata WHERE table_name = ? AND date = ?",
        (TABLE, target_date),
    ).fetchone()
    return row is not None


def load_and_mark(con, rows: list[tuple], target_date: date) -> None:
    """Data write + metadata write in ONE transaction. Atomic by construction."""
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute("DELETE FROM daily_revenue WHERE date = ?", (target_date,))
        for r in rows:
            con.execute(
                """
                INSERT INTO daily_revenue
                    (date, status, total_revenue, order_count, avg_order_value, loaded_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (*r, datetime.now()),
            )
        con.execute(
            """
            INSERT OR REPLACE INTO pipeline_metadata (table_name, date, loaded_at, row_count)
            VALUES (?, ?, ?, ?)
            """,
            (TABLE, target_date, datetime.now(), len(rows)),
        )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise


def run_range(start: date, end: date, force: bool = False) -> None:
    con = connect_target()
    d = start
    while d <= end:
        if not force and already_loaded(con, d):
            print(f"  {d}  skip (watermark)")
        else:
            raw = extract(d)
            agg = transform(raw, d)
            load_and_mark(con, agg, d)
            print(f"  {d}  loaded {len(agg)} rows ({len(raw):,} orders)")
        d += timedelta(days=1)
    con.close()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("start", nargs="?", default="2024-01-10")
    p.add_argument("end", nargs="?", default="2024-01-20")
    p.add_argument("--force", action="store_true", help="reprocess even if watermarked")
    args = p.parse_args()
    run_range(date.fromisoformat(args.start), date.fromisoformat(args.end), args.force)
