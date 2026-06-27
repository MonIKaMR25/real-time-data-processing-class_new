"""Break 01: inject blatantly-late orders and watch the watermark eat them.

Run a pipeline first and let it drain to idle (watermark past 12:10); then inject
into the RUNNING query — it does detect new events. We send --count orders stamped
created_at = --at (e.g. 12:05), a window emitted long ago with its state already
dropped. Predict the pipeline's reaction before you run it:

    console sink:               (no new output)
    [12:05,12:10) window:       unchanged — already emitted, state gone
    errors raised:              none
    log messages:               none
    numRowsDroppedByWatermark:  rises by ONE — not --count   (see below)

THE SHARP EDGE (measured, and the real lesson): numRowsDroppedByWatermark counts
dropped *post-aggregation* rows — distinct late (group, window) keys — NOT raw
events. This job groups by window only, so all --count events share window
[--at, +5m) and pre-aggregate into ONE partial row; dropping it bumps the counter
by 1. Inject $49,999.50 into a single window and the alarm reads +1. A single hot
key can hide a fortune from your drop counter — so treat this metric as a BINARY
alarm (zero vs nonzero), not a precise loss tally.

THE PROOF the money vanished is the dollars, not the counter: the stream's
[12:05,12:10) total never includes this revenue, though these are real paid orders.
A plain batch aggregate over the same topic is ~$50k richer — the L4 batch audit
(slide 30) is what reconciles it. The amount is deliberately huge so you'd notice
IF it ever landed in a total. It won't.

Third silent lie of the course (polling drift L5, the swallowed column L5, now
vanished revenue). A plain confluent-kafka producer — no Spark; wiring as seed_events.

Usage:
    # 1) start a pipeline, let it go quiet (idle, caught up):
    #    uv run python src/stream_revenue.py --watermark 10 --mode update
    # 2) inject into the running query:
    python src/inject_late.py --at "12:05" --count 50
    python src/inject_late.py --at "12:05" --count 50 --amount 999.99
"""

import argparse
import json

from confluent_kafka import Producer

from config import BOOTSTRAP, TOPIC, banner, base_time, iso, lesson


def run(at: str, count: int, amount: float, customer: int) -> None:
    hh, mm = (int(x) for x in at.split(":"))
    when = base_time().replace(hour=hh, minute=mm, second=0, microsecond=0)

    banner("inject_late · the silent drop (Break 01)",
           f"fires {count} orders of ${amount:,.2f} each, stamped created_at={at}, into '{TOPIC}'",
           f"  — that window was emitted long ago; a running pipeline already evicted its state",
           "predict before you look: console stays silent · the window total does NOT move ·",
           "  no error · numRowsDroppedByWatermark ticks up by just 1 (one dropped group)")

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
    print(f"\ninjected {delivered} events · ${total:,.2f} · created_at={iso(when)}")
    lesson(
        f"those {count} late orders all share window [{at}, +5m) → ONE dropped group:",
        f"  numRowsDroppedByWatermark rises by 1, NOT by {count} — it counts groups, not events.",
        f"the window total never changes; the ${total:,.2f} is gone from the stream's answer,",
        "  recoverable only by the nightly BATCH audit (stream for speed, batch for truth).",
        "Defaults fail silently — and even the alarm you added can under-fire on a hot key.")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Inject late events the watermark will drop")
    p.add_argument("--at", default="12:05", help="event clock time HH:MM (today's date)")
    p.add_argument("--count", type=int, default=50)
    p.add_argument("--amount", type=float, default=999.99)
    p.add_argument("--customer", type=int, default=0,
                   help="partition key for the injected events (default 0). Only matters "
                        "for which partition they land in; the drop is driven by event "
                        "time vs the watermark, not by the key.")
    args = p.parse_args()
    run(args.at, args.count, args.amount, args.customer)
