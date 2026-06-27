"""The deliverable consumer: batch commits + rebalance callbacks done right.

The risk a rebalance adds: your work doesn't crash, it gets REASSIGNED. With
batch commits there is always in-flight work (processed but not yet committed).
When partitions are revoked, on_revoke is the last chance to commit it — skip
that, and whoever inherits the partition resumes from your LAST batch commit
and reprocesses everything since: duplicates, handed to your sink.

    on_revoke  -> commit consumer.position() for the partitions being taken
    on_assign  -> initialize per-partition state for the new arrivals

cooperative-sticky only moves the partitions that must move; the rest keep
flowing (the old eager protocol froze the whole group on every change).

Every processed message is appended to data/ledger-<group>.jsonl so
experiment_rebalance.py can count duplicates and prove no-loss across the
join -> kill -> rejoin dance.

Usage:
    python src/consume_rebalance.py --name A                      # terminal 2
    python src/consume_rebalance.py --name B                      # terminal 3
    python src/consume_rebalance.py --name A --skip-revoke-commit # the broken twin
"""

import argparse
import json
import os
import sys
import time

from confluent_kafka import Consumer, KafkaError, KafkaException

from config import BOOTSTRAP, GROUP, TOPIC_ORDERS, ledger_path, log_processed

COMMIT_EVERY = 100

# A commit that races a group change is refused by the broker. All three codes
# mean the same thing here: "the group moved on while you were committing".
COMMIT_RACES = {
    KafkaError.REBALANCE_IN_PROGRESS,   # rebalance mid-flight
    KafkaError.ILLEGAL_GENERATION,      # group generation bumped under us
    KafkaError.UNKNOWN_MEMBER_ID,       # we were evicted (e.g. poll() too slow)
}


def commit_batch(consumer: Consumer) -> bool:
    """Synchronous commit that tolerates a rebalance racing it.

    Not fatal: the next poll() runs the rebalance callbacks, and on_revoke
    commits the positions that matter. Worst case a few messages replay —
    at-least-once, the sink dedupes. Crashing here (the obvious code) would
    take the consumer down on every unlucky rebalance; this race is THE
    reason production commit code needs this guard.
    """
    try:
        consumer.commit(asynchronous=False)
        return True
    except KafkaException as e:
        if e.args[0].code() in COMMIT_RACES:
            return False
        raise


def run(name: str, group: str, skip_revoke_commit: bool, slow_ms: float = 0) -> None:
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "partition.assignment.strategy": "cooperative-sticky",
        # A SIGKILLed consumer sends no goodbye — the broker only notices when
        # heartbeats stop. The default 45s session timeout would stall the kill
        # demo for most of a minute; 6s keeps the class moving.
        "session.timeout.ms": 6000,
        "heartbeat.interval.ms": 2000,
    })

    state: dict[int, int] = {}     # partition -> messages processed by THIS consumer

    def on_assign(c, partitions):
        for tp in partitions:
            state[tp.partition] = 0
        # committed() is a broker RPC — mid-rebalance it can fail just like a
        # commit can. It's only here for the log line, so never die for it.
        try:
            committed = c.committed(partitions, timeout=10)
            resumes = "  ".join(
                f"P{tp.partition}@{tp.offset if tp.offset >= 0 else 'earliest'}"
                for tp in committed
            )
        except KafkaException:
            resumes = "(offsets unavailable mid-rebalance)"
        print(f"[{name}] ASSIGNED  +{len(partitions)}: {resumes}")

    def on_revoke(c, partitions):
        plist = ",".join(f"P{tp.partition}" for tp in partitions)
        if skip_revoke_commit:
            print(f"[{name}] REVOKED   {plist} -> commit SKIPPED (in-flight work abandoned)")
        else:
            positions = [tp for tp in c.position(partitions) if tp.offset >= 0]
            try:
                if positions:
                    c.commit(offsets=positions, asynchronous=False)
                committed = "  ".join(f"P{tp.partition}@{tp.offset}" for tp in positions)
                print(f"[{name}] REVOKED   {plist} -> committed {committed or '(nothing consumed)'}")
            except KafkaException as e:
                if e.args[0].code() not in COMMIT_RACES:
                    raise
                print(f"[{name}] REVOKED   {plist} -> commit raced the rebalance; "
                      f"in-flight work will replay (at-least-once)")
        for tp in partitions:
            state.pop(tp.partition, None)

    consumer.subscribe([TOPIC_ORDERS], on_assign=on_assign, on_revoke=on_revoke)
    ledger = open(ledger_path(group), "a")
    since_commit = 0
    total = 0
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                if since_commit and commit_batch(consumer):  # idle: flush the batch
                    since_commit = 0
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise Exception(msg.error())

            order = json.loads(msg.value())
            if slow_ms:                                # simulate expensive work
                time.sleep(slow_ms / 1000)
            log_processed(ledger, name, msg.partition(), msg.offset(), order["seq"],
                          order.get("run"))
            state[msg.partition()] = state.get(msg.partition(), 0) + 1
            total += 1
            since_commit += 1
            if since_commit >= COMMIT_EVERY and commit_batch(consumer):
                since_commit = 0                       # batch commit, the prod pattern
    except KeyboardInterrupt:
        print(f"\n[{name}] stopped after {total:,} messages.")
    finally:
        consumer.close()      # clean close commits nothing by itself — by design
        ledger.close()


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Rebalance-correct consumer (the deliverable)")
    p.add_argument("--name", default=f"c{os.getpid()}", help="label in logs and the ledger")
    p.add_argument("--group", default=GROUP)
    p.add_argument("--skip-revoke-commit", action="store_true",
                   help="demonstrate the bug: abandon in-flight work on revoke")
    p.add_argument("--slow", type=float, default=0, metavar="MS",
                   help="sleep MS per message (the lag demo)")
    args = p.parse_args()
    try:
        run(args.name, args.group, args.skip_revoke_commit, args.slow)
    except Exception as e:
        import traceback
        traceback.print_exc(file=sys.stderr)
        print(f"[{args.name}] fatal: {e}", file=sys.stderr)
        sys.exit(1)
