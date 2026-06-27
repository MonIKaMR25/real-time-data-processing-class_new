# Airflow 3.x with the two libraries our pipeline needs. Pinned to a python the
# Airflow wheels support (3.12). The DAG talks to Postgres with psycopg directly
# and writes the analytical target with DuckDB. PythonOperator now lives in the
# `standard` provider, which ships preinstalled in the official 3.x image.
FROM apache/airflow:3.2.2-python3.12

RUN pip install --no-cache-dir \
    "duckdb>=1.1.0" \
    "psycopg[binary]>=3.2.0"
