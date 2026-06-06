"""Phase 3 — inject failure mid-load and recover. The point of the whole lesson.

load_with_failure() randomly raises partway through the INSERT loop, simulating a
crash. Because the DELETE + INSERTs live in one transaction, the ROLLBACK undoes
*everything* — the target is byte-for-byte what it was before the run. A retry
wrapper then re-runs until it succeeds. Run it 10×: the final state is always one
correct set of rows per (date, status), no matter how many crashes happened.

Usage:
    python src/pipeline_failure.py 2024-01-15                 # 1 attempt, may crash
    python src/pipeline_failure.py 2024-01-15 --retries 5     # retry until success
    python src/pipeline_failure.py 2024-01-15 --fail-prob 0.8
"""

import argparse
import random
from datetime import date, datetime

from config import connect_target
from pipeline_naive import extract, transform


def load_with_failure(rows: list[tuple], target_date: date,
                      fail_probability: float = 0.5) -> None:
    """Idempotent load that may crash mid-way. Transaction makes the crash safe."""
    con = connect_target()
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute("DELETE FROM daily_revenue WHERE date = ?", (target_date,))
        for i, r in enumerate(rows):
            if i > 0 and random.random() < fail_probability:
                raise ConnectionError(f"Simulated crash after {i} of {len(rows)} rows")
            con.execute(
                """
                INSERT INTO daily_revenue
                    (date, status, total_revenue, order_count, avg_order_value, loaded_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (*r, datetime.now()),
            )
        con.execute("COMMIT")
    except Exception as e:
        con.execute("ROLLBACK")
        print(f"  FAILURE: {e} — rolled back, target unchanged")
        raise
    finally:
        con.close()


def run_pipeline_with_retry(target_date: date, max_retries: int = 3,
                            fail_probability: float = 0.5) -> None:
    raw = extract(target_date)
    agg = transform(raw, target_date)
    for attempt in range(1, max_retries + 1):
        try:
            print(f"Attempt {attempt}/{max_retries} for {target_date}")
            load_with_failure(agg, target_date, fail_probability)
            print(f"  Success on attempt {attempt} ({len(agg)} rows)")
            return
        except Exception:
            if attempt == max_retries:
                print(f"  Exhausted retries for {target_date}")
                raise
            print("  Retrying...")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("target_date", nargs="?", default="2024-01-15")
    p.add_argument("--retries", type=int, default=3)
    p.add_argument("--fail-prob", type=float, default=0.5)
    args = p.parse_args()
    run_pipeline_with_retry(
        date.fromisoformat(args.target_date), args.retries, args.fail_prob
    )
