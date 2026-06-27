"""Seed the `orders-cdc` topic with order events — most on time, 5% LATE.

This is the test population for the whole lesson. Event times march forward from
the anchor (config.base_time(), today 12:00) across --span minutes. A fraction
(--late-fraction, default 5%) are stamped in the PAST relative to the marching
cursor: their created_at is earlier than the events around them in the log. They
are the mobile-client / retry / CDC-lag stragglers every real pipeline carries,
and they are what the watermark will or won't drop.

Lateness is drawn from an exponential (mean --late-mean minutes, clamped 1–45),
so it has a real tail past 10 minutes — that tail is what a 10-minute watermark
still catches. A uniform 1–10 would drop exactly nothing at a 10-min watermark.

This is a plain confluent-kafka producer — no Spark. produce() only ENQUEUES;
poll() pumps callbacks, flush() drains before exit (the L6 gotcha).

Usage:
    python src/seed_events.py                       # 10k orders, 5% late
    python src/seed_events.py --count 5000 --span 30
    python src/seed_events.py --late-fraction 0.0   # a clean, no-drops baseline
"""

import argparse
import json
import random
from collections import Counter
from datetime import datetime

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from config import BOOTSTRAP, TOPIC, base_time, iso

STATUSES = ["created", "paid", "shipped", "delivered"]

# 50 customers, zipf-ish skew (a few whales) — gives stream_to_kafka's
# per-customer window something lopsided to chew on, and makes per-partition
# event-time progress uneven (the "watermark is global" gotcha, for real).
CUSTOMERS = list(range(50))
WEIGHTS = [1 / (c + 1) for c in CUSTOMERS]


def ensure_topic(partitions: int) -> None:
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    fut = admin.create_topics([NewTopic(TOPIC, num_partitions=partitions,
                                        replication_factor=1)])
    for name, f in fut.items():
        try:
            f.result(timeout=30)
            print(f"created topic: {name} ({partitions} partitions, RF=1)")
        except Exception as e:
            if "TOPIC_ALREADY_EXISTS" in str(e):
                print(f"topic exists: {name} (appending — delete it to reset event times)")
            else:
                raise


def run(count: int, span_min: float, late_fraction: float, late_mean: float,
        partitions: int) -> None:
    ensure_topic(partitions)
    producer = Producer({"bootstrap.servers": BOOTSTRAP,
                         "acks": "all", "enable.idempotence": True})

    base = base_time()
    step_s = (span_min * 60.0) / max(1, count)   # cursor advance per event
    delivered = 0
    errors = 0
    late_count = 0
    lateness_hist = Counter()   # minutes-late bucket → n

    def on_delivery(err, _msg):
        nonlocal delivered, errors
        if err:
            errors += 1
        else:
            delivered += 1

    print(f"seeding {count:,} orders to '{TOPIC}', event time {iso(base)} "
          f"+ {span_min:g} min, {late_fraction:.0%} late (exp mean {late_mean:g}m)")

    for i in range(count):
        cursor = base.timestamp() + i * step_s
        is_late = random.random() < late_fraction
        if is_late:
            lateness_min = min(45.0, max(1.0, random.expovariate(1.0 / late_mean)))
            event_ts = cursor - lateness_min * 60.0
            late_count += 1
            lateness_hist[int(lateness_min)] += 1
        else:
            event_ts = cursor

        order = {
            "order_id": i,
            "customer_id": random.choices(CUSTOMERS, WEIGHTS)[0],
            "amount": round(10 + random.random() * 140, 2),
            "status": random.choice(STATUSES),
            "created_at": iso(datetime.fromtimestamp(event_ts)),
        }
        producer.produce(TOPIC,
                         key=str(order["customer_id"]).encode(),
                         value=json.dumps(order).encode(),
                         callback=on_delivery)
        producer.poll(0)                       # pump callbacks — not optional

    producer.flush(60)                         # drain before exit — not optional
    print(f"\ndelivered {delivered:,} ({errors} errors), of which {late_count:,} late")
    if lateness_hist:
        print("lateness (minutes → count), the watermark sweep will eat the left tail:")
        for m in sorted(lateness_hist):
            n = lateness_hist[m]
            print(f"  {m:>2}–{m+1:<2}m  {'#' * max(1, n // 3)}  {n}")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Seed orders-cdc with late stragglers")
    p.add_argument("--count", type=int, default=10_000)
    p.add_argument("--span", type=float, default=60.0, help="event-time span, minutes")
    p.add_argument("--late-fraction", type=float, default=0.05)
    p.add_argument("--late-mean", type=float, default=7.0,
                   help="mean lateness in minutes (exponential, clamped 1–45)")
    p.add_argument("--partitions", type=int, default=4)
    args = p.parse_args()
    run(args.count, args.span, args.late_fraction, args.late_mean, args.partitions)
