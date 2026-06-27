"""The circle-closer: consume the Lesson 5 CDC stream FROM KAFKA.

With the Debezium overlay running (see debezium/README.md), every change to the
L5 Postgres `orders` table lands in the topic cdc.public.orders. Run this script
in TWO terminals with different --group names: both get the whole stream,
independently, at their own pace — while Postgres holds exactly ONE slot.

That's the L5 wall, demolished: fan-out moved off the source and onto the log.

Usage:
    python src/consume_cdc.py --group mirror     # terminal 1
    python src/consume_cdc.py --group fraud      # terminal 2 — same stream, free
"""

import argparse
import json
import time

from confluent_kafka import Consumer, KafkaError

from config import BOOTSTRAP, TOPIC_CDC

OPS = {"c": "INSERT", "u": "UPDATE", "d": "DELETE", "r": "SNAPSHOT"}


def run(group: str) -> None:
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC_CDC])
    print(f"[{group}] consuming '{TOPIC_CDC}' — its own offsets, nobody else affected")

    counts: dict[str, int] = {}
    last_report = time.time()
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise Exception(msg.error())
            if msg.value() is None:        # tombstone (delete marker)
                continue
            payload = json.loads(msg.value()).get("payload", {})
            op = OPS.get(payload.get("op"), payload.get("op"))
            counts[op] = counts.get(op, 0) + 1
            after = payload.get("after") or payload.get("before") or {}
            print(f"[{group}] {op:<8} id={after.get('id')} status={after.get('status')}")
            consumer.commit(asynchronous=False)
            if time.time() - last_report > 10:
                print(f"[{group}] totals: {counts}")
                last_report = time.time()
    except KeyboardInterrupt:
        print(f"\n[{group}] totals: {counts}")
    finally:
        consumer.close()


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Consume the Debezium CDC topic")
    p.add_argument("--group", required=True, help="e.g. mirror / fraud / search")
    args = p.parse_args()
    run(args.group)
