"""Shared config for the Lesson 6 Kafka demos.

Every script imports from here so the cluster wiring lives in one place.

Environment overrides (all optional, sane localhost defaults):
    KAFKA_BOOTSTRAP   bootstrap servers (default: localhost:19092,29092,39092)
    PG_HOST/PG_PORT   the LESSON 5 Postgres, only for experiment_second_slot.py
"""

import json
import os
import time
from pathlib import Path

# ── Kafka cluster ─────────────────────────────────────────────────────────────
# From the host we dial the EXTERNAL advertised listeners; inside the compose
# network the runner overrides this to kafka-N:9092 (see docker-compose.yml).
BOOTSTRAP = os.environ.get(
    "KAFKA_BOOTSTRAP", "localhost:19092,localhost:29092,localhost:39092"
)

TOPIC_ORDERS = "orders"            # 6 partitions, RF=3 — the workhorse topic
TOPIC_EVENTS = "order-events"      # 3 partitions — break 03 (out-of-order)
TOPIC_CDC = "cdc.public.orders"    # written by the Debezium overlay (circle-closer)
GROUP = "order-processor"          # the default consumer group

# ── Local ledger files (duplicate/loss accounting for the rebalance demo) ────
DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)


def ledger_path(group: str) -> Path:
    """One JSONL ledger per consumer group: every processed message is a line."""
    return DATA_DIR / f"ledger-{group}.jsonl"


def producer_summary_path() -> Path:
    return DATA_DIR / "produce-summary.json"


def log_processed(fh, consumer: str, partition: int, offset: int, seq: int,
                  run: int | None = None) -> None:
    """Append one processed-message record. O_APPEND keeps concurrent writers safe."""
    fh.write(json.dumps({
        "consumer": consumer, "partition": partition,
        "offset": offset, "seq": seq, "run": run, "ts": time.time(),
    }) + "\n")
    fh.flush()


# ── Lesson 5 Postgres (ONLY for experiment_second_slot.py, the L6 hook) ──────
PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_DSN = f"postgresql://bench:bench@{PG_HOST}:{PG_PORT}/bench"
