"""Create the lesson's topics: orders (6 partitions) and order-events (3).

Replication factor 3 on both — with min.insync.replicas=2 set broker-side,
that's the durability triplet from the slides (the producer brings acks=all).

6 partitions on `orders` is a deliberate capacity decision: it caps the consumer
group at 6 active members. Partition count is chosen at creation and is expensive
to change later (repartitioning reshuffles keys and triggers rebalances).

Usage:
    python src/create_topics.py
    python src/create_topics.py --describe     # just show current layout
"""

import argparse
import sys
import time

from confluent_kafka.admin import AdminClient, NewTopic

from config import BOOTSTRAP, TOPIC_EVENTS, TOPIC_ORDERS

TOPICS = [
    NewTopic(TOPIC_ORDERS, num_partitions=6, replication_factor=3),
    NewTopic(TOPIC_EVENTS, num_partitions=3, replication_factor=3),
]


def describe(admin: AdminClient) -> None:
    # freshly created topics can take a beat to appear in metadata
    for _ in range(10):
        md = admin.list_topics(timeout=10)
        if all(md.topics.get(n) and not md.topics[n].error
               for n in (TOPIC_ORDERS, TOPIC_EVENTS)):
            break
        time.sleep(1)
    for name in (TOPIC_ORDERS, TOPIC_EVENTS):
        topic = md.topics.get(name)
        if topic is None:
            print(f"{name}: does not exist")
            continue
        print(f"{name}  ({len(topic.partitions)} partitions)")
        for pid, p in sorted(topic.partitions.items()):
            print(f"  Partition: {pid}   Leader: {p.leader}   "
                  f"Replicas: {','.join(map(str, p.replicas))}   "
                  f"Isr: {','.join(map(str, p.isrs))}")


def main(only_describe: bool) -> None:
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    if not only_describe:
        futures = admin.create_topics(TOPICS)
        for name, fut in futures.items():
            try:
                fut.result(timeout=30)
                print(f"created: {name}")
            except Exception as e:  # already exists is fine — idempotent setup
                if "TOPIC_ALREADY_EXISTS" in str(e):
                    print(f"exists:  {name}")
                else:
                    print(f"FAILED:  {name}: {e}", file=sys.stderr)
                    sys.exit(1)
        print()
    describe(admin)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Create/describe the lesson topics")
    p.add_argument("--describe", action="store_true", help="describe only, create nothing")
    args = p.parse_args()
    main(args.describe)
