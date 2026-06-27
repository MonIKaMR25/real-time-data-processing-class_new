"""Break 03: Kafka preserves PRODUCE order, not EVENT-TIME order.

Order lifecycles (created -> paid -> shipped) are produced SHUFFLED — the way
retries, mobile clients, and parallel services actually deliver them. Keyed by
order_id, so each order's events land in one partition, in arrival order...
which is faithfully NOT timestamp order.

If your logic needs event-time order, Kafka alone cannot give it to you — you
need event-time semantics and watermarks. That's Lesson 7, and this topic is
its lab rat.

Usage:
    python src/produce_out_of_order.py               # produce 4 shuffled lifecycles
    python src/produce_out_of_order.py --readback    # then read back per partition
"""

import argparse
import json
import random
import time

from confluent_kafka import Consumer, KafkaError, Producer

from config import BOOTSTRAP, TOPIC_EVENTS

LIFECYCLE = [("created", 0), ("paid", 5), ("shipped", 30)]


def produce(orders: int) -> None:
    producer = Producer({"bootstrap.servers": BOOTSTRAP,
                         "acks": "all", "enable.idempotence": True})
    base = time.gmtime()
    events = []
    for order_id in range(1, orders + 1):
        for status, offset_s in LIFECYCLE:
            events.append({
                "order_id": order_id,
                "event": status,
                "ts": time.strftime("%H:%M:", base) + f"{offset_s:02d}",
                "customer_id": 40 + order_id,
            })
    random.shuffle(events)                      # the real world, simulated

    print(f"producing {len(events)} events ({orders} lifecycles), SHUFFLED, key=order_id:")
    for e in events:
        producer.produce(TOPIC_EVENTS,
                         key=str(e["order_id"]).encode(),
                         value=json.dumps(e).encode())
        print(f"  -> order {e['order_id']}  {e['event']:<8} ts={e['ts']}")
    producer.flush(15)


def readback() -> None:
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": f"readback-{int(time.time())}",   # fresh group: from the start
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC_EVENTS])
    by_partition: dict[int, list] = {}
    deadline = time.time() + 10
    while time.time() < deadline:
        msg = consumer.poll(timeout=1.0)
        if msg is None or msg.error():
            if msg and msg.error().code() != KafkaError._PARTITION_EOF:
                raise Exception(msg.error())
            continue
        by_partition.setdefault(msg.partition(), []).append((msg.offset(), json.loads(msg.value())))
    consumer.close()

    print("\nreadback, in OFFSET order (what a consumer sees):")
    for p in sorted(by_partition):
        print(f"  partition {p}:")
        last_ts = None
        for offset, e in by_partition[p]:
            mark = "  <- timestamp disorder, preserved faithfully" \
                if last_ts is not None and e["ts"] < last_ts else ""
            last_ts = e["ts"]
            print(f"    offset {offset}  order {e['order_id']}  "
                  f"{e['event']:<8} ts={e['ts']}{mark}")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Shuffled lifecycles: arrival != event time")
    p.add_argument("--orders", type=int, default=4)
    p.add_argument("--readback", action="store_true", help="consume and annotate disorder")
    args = p.parse_args()
    if args.readback:
        readback()
    else:
        produce(args.orders)
