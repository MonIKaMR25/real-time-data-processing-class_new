"""Break 01: inject blatantly-late orders and watch the watermark eat them.

The pipeline has been running; its watermark is far past, say, 12:10. We send
--count orders stamped created_at = --at (e.g. 12:05) — a window emitted long ago,
its state already dropped. Predict the pipeline's reaction before you run it:

    console sink:               (no new output)
    [12:05,12:10) window:       unchanged — already emitted, state gone
    errors raised:              none
    log messages:               none
    numRowsDroppedByWatermark:  jumps by --count   ← the only trace

That silent drop is the third silent lie of the course (polling drift L5, the
swallowed column L5, now vanished revenue). The amount is deliberately huge so
you'd notice IF it ever landed in a total. It won't.

A plain confluent-kafka producer — no Spark. Same wiring as seed_events.py.

Usage:
    python src/inject_late.py --at "12:05" --count 50
    python src/inject_late.py --at "12:05" --count 50 --amount 999.99
"""

import argparse
import json

from confluent_kafka import Producer

from config import BOOTSTRAP, TOPIC, base_time, iso


def run(at: str, count: int, amount: float, customer: int) -> None:
    hh, mm = (int(x) for x in at.split(":"))
    when = base_time().replace(hour=hh, minute=mm, second=0, microsecond=0)

    producer = Producer({"bootstrap.servers": BOOTSTRAP,
                         "acks": "all", "enable.idempotence": True})
    delivered = 0

    def on_delivery(err, _msg):
        nonlocal delivered
        if not err:
            delivered += 1

    for i in range(count):
        order = {
            "order_id": 10_000_000 + i,           # distinct id space — clearly injected
            "customer_id": customer,
            "amount": amount,
            "status": "paid",
            "created_at": iso(when),
        }
        producer.produce(TOPIC, key=str(customer).encode(),
                         value=json.dumps(order).encode(), callback=on_delivery)
        producer.poll(0)
    producer.flush(30)

    total = count * amount
    print(f"injected {delivered} events · ${total:,.2f} of revenue · created_at={iso(when)}")
    print("watch terminal 2 (watch_progress.py): numRowsDroppedByWatermark jumps by "
          f"{count}. nothing else moves.")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Inject late events the watermark will drop")
    p.add_argument("--at", default="12:05", help="event clock time HH:MM (today's date)")
    p.add_argument("--count", type=int, default=50)
    p.add_argument("--amount", type=float, default=999.99)
    p.add_argument("--customer", type=int, default=0,
                   help="key. 0 = the whale (its partition drains LAST), so injecting "
                        "before a fresh run still drops deterministically — by the time "
                        "the reader reaches the tail, the watermark has moved on.")
    args = p.parse_args()
    run(args.at, args.count, args.amount, args.customer)
