"""The minimal correct consumer: poll loop, manual commit AFTER processing.

The L4/L5 rule in its third costume:
    L4  write the watermark in the SAME transaction as the load
    L5  apply the event, THEN advance the slot
    L6  process the message, THEN commit the offset

auto.offset.reset=earliest only applies when the group has NO committed offsets
(first run). Afterwards the group resumes from its commits — kill this script,
restart it, and watch it pick up where it left off with zero manual bookkeeping:
__consumer_offsets is doing the L4 pipeline_metadata job.

Committing synchronously per message is correct and SLOW — fine for learning.
consume_rebalance.py batches commits the way production code does.

Usage:
    python src/consume_naive.py                  # Ctrl-C to stop
    python src/consume_naive.py --group mygroup  # fresh group = start from earliest
"""

import argparse
import json

from confluent_kafka import Consumer, KafkaError

from config import BOOTSTRAP, GROUP, TOPIC_ORDERS


def run(group: str) -> None:
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": group,
        "auto.offset.reset": "earliest",   # first run only
        "enable.auto.commit": False,       # WE own the watermark
    })

    def on_assign(c, partitions):
        committed = c.committed(partitions, timeout=10)
        resumes = "  ".join(
            f"P{tp.partition}@{tp.offset if tp.offset >= 0 else 'earliest'}"
            for tp in committed
        )
        print(f"assigned {len(partitions)} partitions, resuming: {resumes}")

    consumer.subscribe([TOPIC_ORDERS], on_assign=on_assign)
    processed = 0
    try:
        while True:
            msg = consumer.poll(timeout=1.0)   # fetch + heartbeat in one call
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise Exception(msg.error())
            order = json.loads(msg.value())
            processed += 1
            if processed <= 5 or processed % 500 == 0:
                print(f"P{msg.partition()}@{msg.offset()}  "
                      f"key={msg.key().decode() if msg.key() else None}  "
                      f"seq={order['seq']}  ({processed:,} total)")
            consumer.commit(asynchronous=False)    # process, THEN commit
    except KeyboardInterrupt:
        print(f"\nstopped after {processed:,} messages.")
    finally:
        consumer.close()


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Naive consumer: commit-per-message")
    p.add_argument("--group", default=GROUP)
    args = p.parse_args()
    run(args.group)
