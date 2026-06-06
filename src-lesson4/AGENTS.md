# AGENTS.md — Lesson 4 batch ETL

Context for AI coding assistants (opencode, Claude, Cascade) working in this repo.

## What this is

A teaching repo for **classical batch ETL**: idempotency, failure recovery,
Slowly Changing Dimensions, schema evolution, and orchestration (Airflow +
Dagster). The pipeline moves data from an OLTP Postgres source to an analytical
DuckDB target.

## Architecture

- **Source:** Postgres (`bench`/`bench`@`bench`), tables `orders` + `customers`, defined in `init.sql`.
- **Target:** single DuckDB file `data/analytics.duckdb`. Tables created by `src/config.py:ensure_target_schema`.
- **Runner:** `runner` container runs the raw-python pipeline in `src/`.
- **Orchestrators:** `airflow` 3.x (port 8080, dev no-login all-admins) and `dagster` (port 3000) wrap the same logic.

## Connection contract (env, with localhost defaults)

- `PG_HOST` (default `localhost`; `postgres` inside compose), `PG_PORT` (5432)
- `DUCKDB_PATH` (default `<repo>/data/analytics.duckdb`)
- `STAGING_DIR` (orchestrators only; Parquet hand-off between tasks/assets)
- DSNs are derived in `src/config.py` — import from there, don't hardcode.

## How to run

```bash
docker compose up -d                              # start Postgres + Airflow + Dagster
uv run python src/seed_data.py                    # populate the source
uv run python src/prove_idempotent.py 2024-01-15  # idempotency proof
uv run python src/<script>.py <args>              # any pipeline script (host, via uv)
# no uv installed? docker compose exec runner python src/<script>.py
```

## Hard constraints (do not break)

- **DuckDB is single-writer.** Never run two writers against `analytics.duckdb`
  concurrently. The runner, Airflow, and Dagster all target the same file.
- **Idempotency = DELETE + INSERT (or UPSERT) in ONE transaction.** The DELETE
  and the INSERTs, and any watermark write, must share a transaction. This is the
  whole point of the lesson — do not split them.
- **Never `SELECT *` in the pipeline.** Columns are listed explicitly so schema
  drift fails loudly. `schema_validate.py` enforces the contract.
- **Don't push large data through XCom / IO managers.** Stage to Parquet, pass
  the path. Both orchestrator implementations already do this.
- **Keep localhost defaults working** so scripts run both in-container and on the
  host via `uv run`.

## Style

- Match Lesson 3's conventions: `argparse` CLIs, `if __name__ == "__main__"`,
  env-overridable connection config, short module docstrings with a Usage block.
- Python 3.13 for the runner; Airflow image is python 3.12 (wheel support).
- Deps: `duckdb`, `psycopg[binary]`. Don't add heavy deps to the runner.

## Gotchas

- Airflow `catchup=False` on purpose (laptop-safe). Backfill a bounded range.
- Delete `data/analytics.duckdb` to drop the target; `docker compose down -v` also
  wipes the seeded source and Airflow metadata.
- The SCD2 merge is idempotent: a second `--merge` with no source change is a no-op.
