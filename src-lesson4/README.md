# Lesson 4 — Classical batch ETL and why "just move the data" is hard

Build a batch pipeline in raw python, watch it break, then make it correct. The
same pipeline is then wrapped in **Airflow** and **Dagster** — both runnable in
docker-compose — so you can map every framework concept back to the code you
wrote by hand.

- **Source (OLTP):** Postgres with `orders` (fact) + `customers` (mutable dimension).
- **Target (analytical):** DuckDB file `data/analytics.duckdb` (`daily_revenue`, `customers_dim`, `pipeline_metadata`).
- **Transform engine:** DuckDB (the "T" with ELT performance, technically ETL).

## Quick start

```bash
# 1. Make sure OrbStack / Docker Desktop is running
# 2. Build + start postgres, the pipeline runner, Airflow, and Dagster
make up                       # == docker compose up -d --build

# 3. Seed the OLTP source (1M orders over 90 days, 50k customers)
make seed                     # == ./bench python src/seed_data.py

# 4. Hour 2 — feel the pain, then fix it
make prove-naive              # naive loader: row count GROWS each run (not idempotent)
make prove                    # idempotent loader: identical checksum every run
make failure                  # crash mid-load + retry → still exactly one correct result
make watermark                # multi-date load with an atomic watermark

# 5. Dimensions + schema drift
make scd2                     # SCD Type 2: move customers (incl. pinned #42), keep history
make schema-check             # validate source schema against the contract
make schema-drift             # rename a source column → validation FAILs loudly → reset

# 6. Hour 3 — the orchestrators (already running from `make up`)
#    Airflow  http://localhost:8080   (dev: auto-admin, no login prompt)
#    Dagster  http://localhost:3000
```

`./bench <cmd...>` is a thin wrapper over `docker compose exec runner <cmd...>`.
To run a script natively on the host instead: `uv run python src/<script>.py`.

## Layout

```
src-lesson4/
├── docker-compose.yml      # postgres + runner + airflow(+meta) + dagster
├── Dockerfile              # runner image (python 3.13 + duckdb + psycopg)
├── init.sql                # OLTP source schema (orders + customers)
├── bench                   # ./bench python src/<file>.py
├── Makefile                # make up / seed / prove / failure / scd2 / ...
├── data/                   # analytics.duckdb + staging (gitignored)
├── src/                    # raw-python pipeline (Hours 1-2)
│   ├── config.py           # connection strings + target schema
│   ├── seed_data.py
│   ├── pipeline_naive.py        # Phase 1 — not idempotent
│   ├── pipeline_idempotent.py   # Phase 2 — DELETE+INSERT / UPSERT
│   ├── pipeline_failure.py      # Phase 3 — failure injection + retry
│   ├── pipeline_watermark.py    # Phase 4 — watermark + atomic metadata
│   ├── scd2.py                  # SCD Type 2 dimension merge
│   ├── schema_validate.py       # schema contract check
│   └── prove_idempotent.py      # the take-home proof harness
├── airflow/                # Hour 3 — Airflow DAG (tasks)
│   ├── Dockerfile
│   └── dags/daily_revenue_pipeline.py
└── dagster_app/            # Hour 3 — Dagster assets
    ├── Dockerfile
    ├── assets.py
    └── definitions.py
```

## The four lessons, in code

| Problem | Where | Proof |
|---------|-------|-------|
| Idempotency | `pipeline_idempotent.py` | `make prove` vs `make prove-naive` |
| Failure recovery | `pipeline_failure.py` | `make failure` — txn rollback + retry |
| Atomic watermark | `pipeline_watermark.py` | data write + metadata write in one txn |
| Slowly Changing Dimensions | `scd2.py` | `make scd2` — version history for customer 42 |
| Schema evolution | `schema_validate.py` | `make schema-drift` — rename a column, watch it FAIL |

## Idempotency strategy

The target's idempotency comes from **DELETE + INSERT inside one transaction**
(partition replacement), with **UPSERT** (`INSERT OR REPLACE`) as the concise
alternative. The DELETE and the INSERTs must share a transaction: if a crash
happens before COMMIT, the ROLLBACK undoes the DELETE too, so the target is never
left empty. `prove_idempotent.py` runs the pipeline 3× and compares an md5
checksum of the resulting rows.

## Orchestrators

Both wrap the *same* extract → transform → load:

- **Airflow** (`airflow/dags/daily_revenue_pipeline.py`): tasks + `>>` edges,
  `retries`, `catchup`. UI at :8080 with no login (dev all-admins). Trigger a date
  with config `{"date": "2024-01-15"}`, or backfill a range:
  `docker compose exec airflow airflow backfill create --dag-id daily_revenue_pipeline --from-date 2024-01-10 --to-date 2024-01-20`.
- **Dagster** (`dagster_app/assets.py`): `daily_revenue` is *derived from*
  `raw_daily_orders` (the asset graph IS the lineage). Materialize partitions
  from the UI at `localhost:3000`.

Neither framework gives you idempotency — notice both `load` steps are the same
DELETE + INSERT you wrote by hand. They give you scheduling, retries, lineage,
and observability; correctness is still yours.

> **XCom / IO note:** the lesson warns never to push 1M rows through XCom. Both
> the Airflow and Dagster versions stage the day's orders to a Parquet file and
> pass only the *path* downstream — the "use intermediate storage" advice, made
> concrete.

## Notes & troubleshooting

- **DuckDB is single-writer.** The runner, Airflow, and Dagster share
  `data/analytics.duckdb`. Run one writer at a time, or you'll hit a lock error.
- **Airflow `catchup=False`** by default to avoid backfilling 2 years on a
  laptop. Use the `backfill` command above for a bounded window.
- **First `make up` is slow** — it builds three images and pulls Postgres ×2.
- **Reset everything:** `make clean` (drops the target) or `docker compose down -v`
  (also wipes the seeded source + Airflow metadata).
