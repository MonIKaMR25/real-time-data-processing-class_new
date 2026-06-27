"""Toward production shape: fork ONE source into TWO queries, land both in Kafka.

    q1: revenue per 5-min window      → topic 'revenue-per-window'
    q2: orders per customer per 15-min → topic 'orders-per-customer'

Two things this teaches:

1. The Kafka sink wants a string 'value' column (and optional 'key'/'topic'), so
   we pack the result with to_json(struct(...)) — the mirror image of the
   from_json we read with.

2. ONE checkpoint directory PER query, never shared. The checkpoint stores the
   Kafka source offsets AND the window state, written atomically after each batch
   — it is __consumer_offsets from L6 plus the aggregation state. Two queries on
   one checkpoint corrupt each other.

The victory lap (the L5 crash demo, inherited): let it run, Ctrl-C it, then run it
again. It resumes from the checkpoint — same offsets, same state — and does NOT
reprocess. Verify downstream with  --readback  (a plain L6-style consumer).

Usage:
    python src/stream_to_kafka.py                 # start both queries (append → Kafka)
    python src/stream_to_kafka.py --reset         # wipe checkpoints, start clean
    python src/stream_to_kafka.py --readback       # dump revenue-per-window and exit
"""

import argparse
import json
import shutil

from config import (BOOTSTRAP, CKPT_DIR, CUSTOMERS_TOPIC, REVENUE_TOPIC, TOPIC,
                    banner, build_spark, lesson, read_orders)

REVENUE_CKPT = CKPT_DIR / "revenue"
CUSTOMERS_CKPT = CKPT_DIR / "customers"


def run(reset: bool) -> None:
    if reset:
        for d in (REVENUE_CKPT, CUSTOMERS_CKPT):
            shutil.rmtree(d, ignore_errors=True)
        print("checkpoints wiped — starting from earliest")

    from pyspark.sql.functions import col, count, struct, sum, to_json, window

    spark = build_spark("L7-stream-to-kafka")
    orders = read_orders(spark, starting="earliest", max_per_trigger=1000)

    # q1 — revenue per 5-minute window
    revenue = (orders
        .withWatermark("created_at", "10 minutes")
        .groupBy(window(col("created_at"), "5 minutes"))
        .agg(sum("amount").alias("total_revenue"), count("*").alias("order_count")))
    revenue_out = revenue.select(
        col("window.start").cast("string").alias("key"),
        to_json(struct(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "total_revenue", "order_count")).alias("value"))
    q1 = (revenue_out.writeStream.format("kafka")
          .option("kafka.bootstrap.servers", BOOTSTRAP)
          .option("topic", REVENUE_TOPIC)
          .option("checkpointLocation", str(REVENUE_CKPT))   # ← its OWN dir
          .outputMode("append").trigger(processingTime="5 seconds").start())

    # q2 — orders per customer per 15-minute window (its OWN checkpoint)
    per_customer = (orders
        .withWatermark("created_at", "10 minutes")
        .groupBy(col("customer_id"), window(col("created_at"), "15 minutes"))
        .agg(count("*").alias("order_count"), sum("amount").alias("total_revenue")))
    customers_out = per_customer.select(
        col("customer_id").cast("string").alias("key"),
        to_json(struct("customer_id",
                       col("window.start").alias("window_start"),
                       "order_count", "total_revenue")).alias("value"))
    q2 = (customers_out.writeStream.format("kafka")
          .option("kafka.bootstrap.servers", BOOTSTRAP)
          .option("topic", CUSTOMERS_TOPIC)
          .option("checkpointLocation", str(CUSTOMERS_CKPT))  # ← a DIFFERENT dir
          .outputMode("append").trigger(processingTime="5 seconds").start())

    banner("stream_to_kafka · production shape (fork → sink → checkpoint)",
           f"one source '{TOPIC}' → TWO windowed queries → TWO Kafka topics:",
           f"  q1: revenue per 5 min            → '{REVENUE_TOPIC}'   (ckpt {REVENUE_CKPT.name})",
           f"  q2: orders per customer per 15m  → '{CUSTOMERS_TOPIC}'  (ckpt {CUSTOMERS_CKPT.name})",
           "ONE checkpoint dir PER query (offsets + window state, written atomically) — never shared",
           "append mode: rows land only AFTER the watermark finalizes each window",
           "Ctrl-C, then rerun WITHOUT --reset → it resumes from the checkpoint, no reprocessing")
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        for q in (q1, q2):
            q.stop()
        lesson(
            "the two checkpoint dirs now hold each query's Kafka offsets + window state,",
            "  written atomically after every batch — rerun (no --reset) and it resumes exactly,",
            "  reprocessing nothing. That's __consumer_offsets from L6 PLUS the aggregation state.",
            "One checkpoint per query, never shared (they'd corrupt each other).",
            "This directory is the protagonist of Lesson 8 — exactly-once state that survives kill -9.")
    spark.stop()


def readback(topic: str, seconds: float) -> None:
    """Dump a result topic with a plain L6-style consumer — the downstream check."""
    import time

    from confluent_kafka import Consumer, KafkaError

    c = Consumer({"bootstrap.servers": BOOTSTRAP,
                  "group.id": f"readback-{int(time.time())}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([topic])
    print(f"reading '{topic}' (earliest):")
    n = 0
    deadline = time.time() + seconds
    while time.time() < deadline:
        msg = c.poll(1.0)
        if msg is None or msg.error():
            if msg and msg.error().code() != KafkaError._PARTITION_EOF:
                raise Exception(msg.error())
            continue
        n += 1
        print(f"  {json.loads(msg.value())}")
    c.close()
    print(f"{n} result rows in '{topic}'")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Two windowed queries → Kafka, two checkpoints")
    p.add_argument("--reset", action="store_true", help="wipe checkpoints, reprocess all")
    p.add_argument("--readback", action="store_true",
                   help="consume the revenue topic and exit (downstream verification)")
    p.add_argument("--topic", default=REVENUE_TOPIC, help="topic for --readback")
    p.add_argument("--seconds", type=float, default=8, help="--readback poll window")
    args = p.parse_args()
    if args.readback:
        readback(args.topic, args.seconds)
    else:
        run(args.reset)
