"""Slowly Changing Dimension, Type 2 — keep history when source rows mutate.

A customer moves city. The OLTP row is overwritten (history gone), but analytics
needs the address *at the time of each order*. SCD2 keeps both: expire the old
version (set valid_to + is_current=false) and insert a new open version.

The merge is a stateful comparison (source vs current dim), not an INSERT SELECT,
and it's idempotent: with no source changes a re-run is a no-op.

Usage:
    python src/scd2.py --merge                       # initial load + detect changes
    python src/scd2.py --simulate-move 500           # move 500 random customers
    python src/scd2.py --merge --effective-date 2024-03-15
    python src/scd2.py --show 42                      # show version history for id 42
"""

import argparse
import random
from datetime import date

import psycopg

from config import PG_CONN_STR, PG_DSN, connect_target

SENTINEL = "9999-12-31"
CITIES = ["NYC", "LA", "Chicago", "Houston", "Phoenix", "Seattle", "Miami", "Denver", "Austin"]
REGION = {"NYC": "East", "Miami": "East", "Chicago": "Central", "Houston": "Central",
          "Denver": "Central", "Austin": "Central", "LA": "West", "Phoenix": "West",
          "Seattle": "West"}


def simulate_move(n: int) -> None:
    """Mutate the OLTP source: move N random customers to a new city. This is the
    'history we'd lose' that SCD2 exists to capture."""
    with psycopg.connect(PG_DSN) as conn, conn.cursor() as cur:
        cur.execute("SELECT id FROM customers ORDER BY random() LIMIT %s", (n,))
        ids = [r[0] for r in cur.fetchall()]
        for cid in ids:
            city = random.choice(CITIES)
            cur.execute(
                "UPDATE customers SET city = %s, region = %s, updated_at = now() WHERE id = %s",
                (city, REGION[city], cid),
            )
        conn.commit()
    print(f"  Moved {len(ids)} customers in the OLTP source.")


def move_one(cid: int, city: str = "Austin") -> None:
    """Deterministically move ONE customer to a city the seeder never uses (Austin),
    so the next merge is GUARANTEED to detect a change. This gives the slide/demo a
    reproducible two-version history for `--show <cid>`, instead of relying on the
    random simulate-move happening to pick this customer."""
    with psycopg.connect(PG_DSN) as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE customers SET city = %s, region = %s, updated_at = now() WHERE id = %s",
            (city, REGION[city], cid),
        )
        moved = cur.rowcount
        conn.commit()
    if moved:
        print(f"  Moved customer {cid} -> {city} ({REGION[city]}) in the OLTP source.")
    else:
        print(f"  Customer {cid} not found.")


def merge(effective_date: date) -> None:
    """Idempotent SCD2 merge. Run twice with no source change → second run is a no-op."""
    con = connect_target()
    con.execute("INSTALL postgres; LOAD postgres")
    con.execute(f"ATTACH '{PG_CONN_STR}' AS pg (TYPE postgres)")

    # Stage the diff: which current versions changed, which customers are brand new.
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _changed AS
        SELECT s.id, s.name, s.city, s.region
        FROM pg.customers s
        JOIN customers_dim d ON d.customer_id = s.id AND d.is_current
        WHERE d.name <> s.name OR d.city <> s.city OR d.region <> s.region
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _new AS
        SELECT s.id, s.name, s.city, s.region, s.signup_date
        FROM pg.customers s
        LEFT JOIN customers_dim d ON d.customer_id = s.id
        WHERE d.customer_id IS NULL
    """)
    n_changed = con.execute("SELECT COUNT(*) FROM _changed").fetchone()[0]
    n_new = con.execute("SELECT COUNT(*) FROM _new").fetchone()[0]

    con.execute("BEGIN TRANSACTION")
    try:
        # 1. expire the current version of every changed customer
        con.execute(
            """
            UPDATE customers_dim
            SET valid_to = ?, is_current = false
            WHERE is_current AND customer_id IN (SELECT id FROM _changed)
            """,
            [effective_date],
        )
        # 2. insert the new open version for changed customers
        con.execute(
            f"""
            INSERT INTO customers_dim
                (customer_id, name, city, region, valid_from, valid_to, is_current)
            SELECT id, name, city, region, ?, DATE '{SENTINEL}', true FROM _changed
            """,
            [effective_date],
        )
        # 3. insert brand-new customers (valid from their signup date)
        con.execute(
            f"""
            INSERT INTO customers_dim
                (customer_id, name, city, region, valid_from, valid_to, is_current)
            SELECT id, name, city, region, signup_date, DATE '{SENTINEL}', true FROM _new
            """
        )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()

    print(f"  effective_date={effective_date}  new={n_new:,}  changed={n_changed:,}")


def show(customer_id: int) -> None:
    con = connect_target()
    rows = con.execute(
        """
        SELECT customer_id, name, city, region, valid_from, valid_to, is_current
        FROM customers_dim WHERE customer_id = ?
        ORDER BY valid_from
        """,
        (customer_id,),
    ).fetchall()
    con.close()
    print(f"  Version history for customer {customer_id}:")
    for r in rows:
        flag = "CURRENT" if r[6] else "expired"
        print(f"    {r[2]:<10} {r[4]} → {r[5]}  [{flag}]")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--merge", action="store_true")
    p.add_argument("--simulate-move", type=int, metavar="N")
    p.add_argument("--move-id", type=int, metavar="CUSTOMER_ID",
                   help="deterministically move ONE customer (reproducible demo history)")
    p.add_argument("--effective-date", default=date.today().isoformat())
    p.add_argument("--show", type=int, metavar="CUSTOMER_ID")
    args = p.parse_args()

    if args.simulate_move:
        simulate_move(args.simulate_move)
    if args.move_id is not None:
        move_one(args.move_id)
    if args.merge:
        merge(date.fromisoformat(args.effective_date))
    if args.show is not None:
        show(args.show)
