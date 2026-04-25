"""
FastAPI app for the live tap demo.

Serves:
  GET  /                 → tap.html (the button)
  GET  /dashboard        → dashboard.html (the live view)
  POST /api/tap          → produce one event to Kafka topic `taps`
  GET  /api/stream       → Server-Sent Events stream of dashboard metrics
"""

import asyncio
import json
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

import clickhouse_connect
from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
TOPIC = "taps"

STATE: dict = {"producer": None, "ch": None}
_cached_metrics: str = "data: {}\n\n"
_metrics_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    await producer.start()
    ch = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)
    STATE["producer"] = producer
    STATE["ch"] = ch
    global _metrics_task
    _metrics_task = asyncio.create_task(_poll_metrics_loop())
    try:
        yield
    finally:
        _metrics_task.cancel()
        await producer.stop()
        ch.close()


app = FastAPI(lifespan=lifespan)

STATIC = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=STATIC), name="static")


@app.get("/")
async def root():
    return FileResponse(STATIC / "tap.html")


@app.get("/dashboard")
async def dashboard():
    return FileResponse(STATIC / "dashboard.html")


@app.post("/api/tap")
async def tap(req: Request):
    body = await req.json()
    # ClickHouse DateTime64(3) parses "YYYY-MM-DD HH:MM:SS.fff" reliably
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    payload = {
        "ts": ts,
        "session_id": str(body.get("session_id", "anon"))[:64],
        "device": req.headers.get("user-agent", "")[:64],
    }
    await STATE["producer"].send_and_wait(TOPIC, json.dumps(payload).encode("utf-8"))
    return {"ok": True}


def query_metrics() -> dict:
    ch = STATE["ch"]
    total = ch.query("SELECT count() FROM demo.taps").result_rows[0][0]

    tps_rows = ch.query(
        """
        SELECT toStartOfInterval(ts, INTERVAL 1 SECOND) AS sec, count() AS n
        FROM demo.taps
        WHERE ts > now() - INTERVAL 60 SECOND
        GROUP BY sec
        ORDER BY sec
        """
    ).result_rows

    top_rows = ch.query(
        """
        SELECT session_id, count() AS n
        FROM demo.taps
        WHERE ts > now() - INTERVAL 60 SECOND
        GROUP BY session_id
        ORDER BY n DESC
        LIMIT 10
        """
    ).result_rows

    tps_dict = {r[0].replace(tzinfo=timezone.utc).isoformat(): int(r[1]) for r in tps_rows}
    tps_list = [{"sec": k, "n": v} for k, v in sorted(tps_dict.items())]

    return {
        "total": int(total),
        "tps": tps_list,
        "now": datetime.now(timezone.utc).isoformat(),
        "top": [{"session": r[0], "n": int(r[1])} for r in top_rows],
    }


async def _poll_metrics_loop():
    """Single background task: query ClickHouse once, cache for all SSE clients."""
    global _cached_metrics
    while True:
        try:
            data = await asyncio.to_thread(query_metrics)
            _cached_metrics = f"data: {json.dumps(data)}\n\n"
        except Exception as e:
            _cached_metrics = f"data: {json.dumps({'error': str(e)})}\n\n"
        await asyncio.sleep(0.5)


async def metrics_stream():
    while True:
        yield _cached_metrics
        await asyncio.sleep(0.5)


@app.get("/api/stream")
async def stream():
    return StreamingResponse(
        metrics_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
