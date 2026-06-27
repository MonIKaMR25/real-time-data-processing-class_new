# Live Tap Demo

A 4-component slim version of the L11 architecture. Built for the sales demo, not for production.

```
Browser → FastAPI → Redpanda → ClickHouse → FastAPI → Browser (SSE)
         POST /api/tap                       /api/stream
```

## Run it locally

From this `src/` directory:

### 1. Start infra

```bash
docker compose up -d
```

Wait ~10 seconds, then verify:

```bash
curl http://localhost:8123/ping       # ClickHouse: should print "Ok."
docker exec demo-redpanda rpk cluster health   # Redpanda: should be Healthy: true
```

Confirm ClickHouse picked up the schema:

```bash
curl 'http://localhost:8123/?query=SHOW+TABLES+FROM+demo'
# Expect: taps, taps_kafka, taps_mv
```

### 2. Install Python deps (with uv)

```bash
uv sync
```

This creates `.venv/` and installs from `pyproject.toml` / `uv.lock`.

### 3. Run the app

```bash
uv run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### 4. Open two tabs

- **Tap page** → http://localhost:8000/
- **Dashboard** → http://localhost:8000/dashboard

Spam the button. Watch the dashboard.

## Verify the pipeline manually

Tail the Kafka topic to confirm events are being produced:

```bash
docker exec demo-redpanda rpk topic consume taps --num 5
```

Query ClickHouse directly:

```bash
docker exec demo-clickhouse clickhouse-client --query \
  "SELECT count(), max(ts) FROM demo.taps"
```

## Stop / reset

```bash
docker compose down -v   # -v wipes ClickHouse data
```

## Architecture map (for the sales pitch)

| Component in this demo | Lesson it shows up in |
|---|---|
| FastAPI POST endpoint producing to Kafka | L6 (Kafka producers) |
| Redpanda topic | L6 (commit log fundamentals) |
| ClickHouse Kafka engine + materialized view | L10 (real-time OLAP) |
| FastAPI SSE → ClickHouse query | L10 (serving sub-second) |
| *Not* in the live demo, mention on slide | L1–L5, L7–L9, L11 |

## Known shortcuts taken for the demo

- No Postgres, no Debezium. Events go straight from FastAPI to Kafka.
- No Spark/Flink. ClickHouse Kafka engine is the "stream processor" — fine for aggregations, not enough for stateful joins (that's L7–L8).
- ClickHouse query inside the SSE loop is sync, run in `asyncio.to_thread`. At demo scale (1 query / 500ms) it's fine.
- No auth, no rate limiting, no observability. Add for production.

## Going remote (optional)

For a public URL during the actual session:

- **Vercel does NOT work** — Redpanda/ClickHouse are stateful long-running processes. Vercel is serverless-only.
- Recommended: Hetzner Cloud CPX21 (€7/mo, 4GB RAM) or DigitalOcean basic droplet ($6/mo).
- Provision Ubuntu, install Docker, `git clone`, `docker compose up -d`, run uvicorn behind Caddy with HTTPS.
- For the actual demo, local + tethered to your phone hotspot is often more reliable than venue wifi.
