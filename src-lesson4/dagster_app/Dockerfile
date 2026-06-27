FROM python:3.12-slim

RUN pip install --no-cache-dir \
    "dagster>=1.7" \
    "dagster-webserver>=1.7" \
    "duckdb>=1.1.0" \
    "psycopg[binary]>=3.2.0"

ENV PYTHONPATH=/app \
    DAGSTER_HOME=/app/.dagster_home
RUN mkdir -p /app/.dagster_home
WORKDIR /app

# -m loads the `dagster_app` package and finds its top-level Definitions (defs).
CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000", "-m", "dagster_app"]
