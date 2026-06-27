"""Watch the CDC stream live — print each WAL event as it arrives.

Usage:
    python src/stream_watch.py

Then in another terminal:
    psql postgresql://bench:bench@localhost:5432/bench -c \
      "INSERT INTO orders (customer_id, amount, status) VALUES (1, 9.99, 'pending');"

    psql postgresql://bench:bench@localhost:5432/bench -c \
      "UPDATE orders SET status='shipped' WHERE id = <id>;"

    psql postgresql://bench:bench@localhost:5432/bench -c \
      "DELETE FROM orders WHERE id = <id>;"
"""

import json
import time

import psycopg

from config import PG_DSN, SLOT

PEEK_SQL = """
    SELECT lsn::text, data
    FROM pg_logical_slot_peek_changes(%s, NULL, %s,
           'format-version', '2',
           'add-tables', 'public.orders')
"""


def main():
    print(f"Listening on slot '{SLOT}'... make changes to the orders table.\n", flush=True)
    with psycopg.connect(PG_DSN, autocommit=True) as pg:
        while True:
            rows = pg.execute(PEEK_SQL, (SLOT, 100)).fetchall()
            if not rows:
                time.sleep(0.5)
                continue

            last_commit_lsn = None
            for lsn, data in rows:
                ev = json.loads(data)
                action = ev["action"]
                if action == "C":
                    last_commit_lsn = lsn
                    continue
                if action == "B":
                    continue

                tbl = f'{ev["schema"]}.{ev["table"]}'
                if action == "I":
                    cols = {c["name"]: c["value"] for c in ev["columns"]}
                    print(f"INSERT  {tbl}  id={cols['id']}  ({cols['customer_id']}, {cols['amount']}, {cols['status']})")
                elif action == "U":
                    cols = {c["name"]: c["value"] for c in ev["columns"]}
                    print(f"UPDATE  {tbl}  id={cols['id']}  status={cols['status']}")
                elif action == "D":
                    ident = {c["name"]: c["value"] for c in ev["identity"]}
                    print(f"DELETE  {tbl}  id={ident['id']}  (old: {ident['status']}, {ident['amount']})")
                print(f"         lsn={lsn}\n")
                print(f"         raw: {json.dumps(ev)}\n", flush=True)

            if last_commit_lsn is not None:
                pg.execute("SELECT pg_replication_slot_advance(%s, %s::pg_lsn)", (SLOT, last_commit_lsn))


if __name__ == "__main__":
    main()
