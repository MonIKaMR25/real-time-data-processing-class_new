"""Schema contract — turn the silent 3 AM schema-drift failure into a loud one.

The pipeline declares the source columns + types it expects. Before extracting,
it queries information_schema.columns and diffs. A mismatch fails fast with a
precise message instead of corrupting the load or crashing deep in transform().

Categories detected (ranked by pain in the lesson):
    missing column   — renamed or dropped upstream → hard failure
    type changed     — silent precision loss risk  → flagged
    new column       — present in source, absent from contract → warn

Usage:
    python src/schema_validate.py
    python src/schema_validate.py --simulate add      # add a column, watch it warn
    python src/schema_validate.py --simulate drop     # drop a column, watch it fail
    python src/schema_validate.py --reset             # restore the source schema
"""

import argparse
import sys

import psycopg

from config import PG_DSN

# The data contract: column name → expected SQL type (information_schema spelling).
EXPECTED_ORDERS = {
    "id": "bigint",
    "customer_id": "integer",
    "amount": "numeric",
    "status": "text",
    "created_at": "timestamp with time zone",
}


def fetch_actual(conn, table: str) -> dict[str, str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
            """,
            (table,),
        )
        return {name: dtype for name, dtype in cur.fetchall()}


def validate(expected: dict[str, str], actual: dict[str, str]) -> list[str]:
    problems: list[str] = []
    for col, exp_type in expected.items():
        if col not in actual:
            problems.append(f"FAIL  missing column '{col}' (renamed or dropped upstream)")
        elif actual[col] != exp_type:
            problems.append(
                f"FAIL  column '{col}' type {actual[col]!r}, contract expects {exp_type!r}"
            )
    for col in actual:
        if col not in expected:
            problems.append(f"WARN  new column '{col}' in source, not in contract (ignored)")
    return problems


def simulate(action: str) -> None:
    """Mutate the source schema to demonstrate detection."""
    stmts = {
        "add": "ALTER TABLE orders ADD COLUMN IF NOT EXISTS coupon_code TEXT",
        "drop": "ALTER TABLE orders DROP COLUMN IF EXISTS status",
        "rename": "ALTER TABLE orders RENAME COLUMN customer_id TO cust_id",
    }
    with psycopg.connect(PG_DSN) as conn, conn.cursor() as cur:
        cur.execute(stmts[action])
        conn.commit()
    print(f"  Source schema mutated: {action}")


def reset() -> None:
    with psycopg.connect(PG_DSN) as conn, conn.cursor() as cur:
        cur.execute("ALTER TABLE orders DROP COLUMN IF EXISTS coupon_code")
        cur.execute("""
            DO $$ BEGIN
                IF EXISTS (SELECT 1 FROM information_schema.columns
                           WHERE table_name='orders' AND column_name='cust_id') THEN
                    ALTER TABLE orders RENAME COLUMN cust_id TO customer_id;
                END IF;
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                               WHERE table_name='orders' AND column_name='status') THEN
                    ALTER TABLE orders ADD COLUMN status TEXT NOT NULL DEFAULT 'pending';
                END IF;
            END $$;
        """)
        conn.commit()
    print("  Source schema reset to the contract.")


def main() -> int:
    with psycopg.connect(PG_DSN) as conn:
        actual = fetch_actual(conn, "orders")
    problems = validate(EXPECTED_ORDERS, actual)
    if not problems:
        print("  OK  source schema matches the contract.")
        return 0
    for p in problems:
        print("  " + p)
    return 1 if any(p.startswith("FAIL") for p in problems) else 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--simulate", choices=["add", "drop", "rename"])
    parser.add_argument("--reset", action="store_true")
    args = parser.parse_args()
    if args.reset:
        reset()
    elif args.simulate:
        simulate(args.simulate)
    else:
        sys.exit(main())
