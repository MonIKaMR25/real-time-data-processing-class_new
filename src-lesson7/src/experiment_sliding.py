"""The price of overlap: tumbling vs sliding state cost, measured.

A tumbling window puts each event in exactly ONE window. A sliding window of
size S sliding every G puts each event in S/G windows — so it remembers S/G times
as many open buckets, and emits S/G times as many rows. Overlap isn't free; you
buy smooth, frequently-refreshed dashboards with RAM.

This runs both pipelines back to back over the same seeded topic (same 10-min
watermark, update mode) and reports peak numRowsTotal — the high-water mark of
windows held in the state store at once.

    tumbling 5m        every event → 1 window
    sliding 10m / 1m   every event → 10 windows   (size/slide = 10)

Pop quiz on the slide: a 1-hour window sliding every 10s → 360 windows per event.
State and compute scale with size/slide, a capacity-planning number just like
partition count was in L6.

Usage:
    python src/experiment_sliding.py
    python src/experiment_sliding.py --seconds 25
"""

import argparse
import time

from pyspark.sql.functions import col, count, sum, window

from config import ProgressPump, build_spark, read_orders


def measure(spark, label, win_args, seconds, max_per_trigger) -> dict:
    orders = read_orders(spark, starting="earliest", max_per_trigger=max_per_trigger)
    windowed = (orders
        .withWatermark("created_at", "10 minutes")
        .groupBy(window(col("created_at"), *win_args))
        .agg(sum("amount").alias("total_revenue"), count("*").alias("order_count")))
    # noop foreach sink: we only care about the state metrics, not the rows
    q = (windowed.writeStream.outputMode("update").format("noop").queryName(label)
         .trigger(processingTime="2 seconds").start())
    pump = ProgressPump(q, echo=False, interval=0.5)
    pump.start()
    deadline = time.time() + seconds
    while time.time() < deadline and q.isActive:
        time.sleep(0.5)
    pump.stop()
    q.stop()
    print(f"  {label:<18} peak numRowsTotal = {pump.peak_state:>6,}   "
          f"(final {pump.last_state:,})")
    return {"label": label, "peak": pump.peak_state, "final": pump.last_state}


def run(seconds: int, max_per_trigger: int) -> None:
    spark = build_spark("L7-sliding-cost")
    print("\nmeasuring state cost (peak windows held at once):\n")
    t = measure(spark, "tumbling 5m", ("5 minutes",), seconds, max_per_trigger)
    s = measure(spark, "sliding 10m/1m", ("10 minutes", "1 minute"), seconds, max_per_trigger)
    spark.stop()

    ratio = (s["peak"] / t["peak"]) if t["peak"] else float("nan")
    print("\n  ┌─ the bill ─────────────────────────────────────────────")
    print(f"  │ tumbling 5m     peak state {t['peak']:>6,}")
    print(f"  │ sliding 10m/1m  peak state {s['peak']:>6,}   ≈ {ratio:.1f}× more")
    print("  │ each event lands in size/slide = 10 windows → ~10× state & output")
    print("  └────────────────────────────────────────────────────────")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Tumbling vs sliding window state cost")
    p.add_argument("--seconds", type=int, default=20, help="run time per window type")
    p.add_argument("--max-per-trigger", type=int, default=1000)
    args = p.parse_args()
    run(args.seconds, args.max_per_trigger)
