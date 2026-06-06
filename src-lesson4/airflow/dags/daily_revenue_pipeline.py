"""Airflow version of the Lesson 4 pipeline — the SAME extract/transform/load you
built by hand, wrapped in a DAG. Read it and map every Airflow concept back to
your manual code:

    retries=3, retry_delay         <-  your run_pipeline_with_retry() loop
    context["ds"] / dag_run.conf   <-  your target_date parameter
    extract >> transform >> load   <-  calling the three functions in sequence
    Airflow metadata DB            <-  your pipeline_metadata watermark table
    "Clear" a task in the UI       <-  re-running one failed date by hand

Note on XCom: the lesson warns never to push 1M rows through XCom (it serializes
to the metadata DB). So extract() stages rows to a Parquet file and passes only
the *path* via XCom; transform() returns the tiny aggregate. That IS the lesson's
"use intermediate storage" advice, made concrete.

Trigger a specific data date:  Trigger DAG w/ config  {"date": "2024-01-15"}
Backfill a bounded range:      airflow backfill create --dag-id daily_revenue_pipeline --from-date 2024-01-10 --to-date 2024-01-20
"""

import os
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

PG_HOST = os.environ.get("PG_HOST", "postgres")
PG_DSN = f"postgresql://bench:bench@{PG_HOST}:5432/bench"
DUCKDB_PATH = os.environ.get("DUCKDB_PATH", "/opt/airflow/data/analytics.duckdb")
STAGING_DIR = Path(os.environ.get("STAGING_DIR", "/opt/airflow/data/staging"))

default_args = {
    "owner": "data-eng",
    "retries": 3,
    "retry_delay": timedelta(seconds=10),
    "retry_exponential_backoff": True,
}


def _target_date(context) -> str:
    """Data date: explicit dag_run.conf {"date": ...} wins, else the schedule's ds."""
    conf = (context["dag_run"].conf or {}) if context.get("dag_run") else {}
    return conf.get("date") or context["ds"]


def extract(**context):
    ds = _target_date(context)
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    out = STAGING_DIR / f"orders_{ds}.parquet"
    # Stream PG → Parquet via DuckDB (intermediate storage, NOT XCom).
    con = duckdb.connect()
    con.execute("INSTALL postgres; LOAD postgres")
    con.execute(f"ATTACH '{PG_DSN}' AS pg (TYPE postgres)")
    con.execute(
        f"""
        COPY (
            SELECT id, customer_id, amount, status, created_at
            FROM pg.orders WHERE created_at::date = DATE '{ds}'
        ) TO '{out}' (FORMAT parquet)
        """
    )
    n = con.execute(f"SELECT COUNT(*) FROM '{out}'").fetchone()[0]
    context["ti"].xcom_push(key="staging_path", value=str(out))
    context["ti"].xcom_push(key="row_count", value=int(n))
    print(f"Extracted {n} rows → {out}")


def transform(**context):
    ds = _target_date(context)
    path = context["ti"].xcom_pull(key="staging_path", task_ids="extract")
    con = duckdb.connect()
    agg = con.execute(
        f"""
        SELECT DATE '{ds}' AS date, status, SUM(amount) AS total_revenue,
               COUNT(*) AS order_count, ROUND(AVG(amount), 2) AS avg_order_value
        FROM '{path}' GROUP BY status ORDER BY status
        """
    ).fetchall()
    # tiny result (one row per status) — safe to pass through XCom
    context["ti"].xcom_push(key="aggregated", value=[list(r) for r in agg])


def load(**context):
    ds = _target_date(context)
    agg = context["ti"].xcom_pull(key="aggregated", task_ids="transform")
    Path(DUCKDB_PATH).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(DUCKDB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS daily_revenue (
            date DATE, status TEXT, total_revenue DECIMAL(18,2),
            order_count BIGINT, avg_order_value DECIMAL(10,2), loaded_at TIMESTAMP)
    """)
    # Idempotency is STILL your job — Airflow did not add it. DELETE + INSERT, one txn.
    con.execute("BEGIN TRANSACTION")
    con.execute("DELETE FROM daily_revenue WHERE date = ?", (ds,))
    for r in agg:
        con.execute(
            "INSERT INTO daily_revenue VALUES (?, ?, ?, ?, ?, ?)",
            (*r, datetime.now()),
        )
    con.execute("COMMIT")
    print(f"Loaded {len(agg)} rows for {ds}")


with DAG(
    dag_id="daily_revenue_pipeline",
    schedule="@daily",
    start_date=datetime(2024, 1, 10),
    catchup=False,  # avoid backfilling 2 years on a laptop; use `backfill` for a range
    default_args=default_args,
    max_active_runs=1,  # DuckDB single-writer: serialize runs so no two loads race the file lock
    tags=["lesson4", "batch-etl"],
) as dag:
    extract_task = PythonOperator(task_id="extract", python_callable=extract)
    transform_task = PythonOperator(task_id="transform", python_callable=transform)
    load_task = PythonOperator(task_id="load", python_callable=load)

    extract_task >> transform_task >> load_task
