"""Break 03: what really happens when you forget the watermark.

The slide's story: delete withWatermark, keep append mode, and the pipeline runs
green forever emitting nothing while state grows to OOM. Half of that is true —
and the half that isn't is the more useful lesson, so this script shows BOTH.

PART A — append + no watermark.
    Spark refuses at analysis time: "Append output mode not supported when there
    are streaming aggregations ... without watermark." Append only ever emits
    FINAL windows, and with no watermark no window is provably final, so Spark
    fails fast rather than guaranteeing eternal silence. That guard is a feature.

PART B — update + no watermark  (the real unbounded-state failure).
    This one runs. It emits updated windows every batch, looks healthy... and
    numRowsTotal climbs forever because no window is ever finalized and dropped.
    The L5 abandoned slot reincarnated: unbounded retention, nobody watching, OOM
    weeks later at 3 AM. A watermark isn't an optimization — for a windowed
    aggregate in production it's a requirement.

Usage:
    python src/experiment_no_watermark.py            # show A (refusal) then B (growth)
    python src/experiment_no_watermark.py --append-only
    python src/experiment_no_watermark.py --mode complete   # also grows forever
"""

import argparse
import time

from pyspark.sql.functions import col, count, sum, window

from config import ProgressPump, build_spark, read_orders


def windowed_no_watermark(spark, max_per_trigger):
    orders = read_orders(spark, starting="earliest", max_per_trigger=max_per_trigger)
    # NOTE: no .withWatermark(...) — that omission is the whole experiment.
    return (orders
        .groupBy(window(col("created_at"), "5 minutes"))
        .agg(sum("amount").alias("total_revenue"), count("*").alias("order_count")))


def part_a_append_refused(spark, max_per_trigger) -> None:
    print("\n── PART A · append + NO watermark ─────────────────────────────")
    w = windowed_no_watermark(spark, max_per_trigger)
    try:
        q = (w.writeStream.outputMode("append").format("console").start())
        q.awaitTermination(8)
        q.stop()
        print("  (no AnalysisException — append started; see notes for the version diff)")
    except Exception as e:
        msg = str(e).split("\n")[0]
        print(f"  Spark REFUSED to start the query:\n    {msg}")
        print("  → append needs a watermark to know a window is final. Fail-fast = feature.")


def part_b_update_grows(spark, mode: str, max_per_trigger: int, batches: int) -> None:
    print(f"\n── PART B · {mode} + NO watermark · watch numRowsTotal only climb ──")
    w = windowed_no_watermark(spark, max_per_trigger)
    q = (w.writeStream.outputMode(mode).format("console")
         .option("truncate", "false").option("numRows", 5)
         .trigger(processingTime="2 seconds").start())
    pump = ProgressPump(q, echo=True, interval=1.0)
    pump.start()
    # let it run a while so the climb is undeniable, then stop (we won't wait for OOM)
    deadline = time.time() + batches
    while time.time() < deadline and q.isActive:
        time.sleep(1)
    pump.stop()
    q.stop()
    print("\n  numRowsTotal never fell. No window was ever finalized. This is the OOM path.")


def run(append_only: bool, mode: str, max_per_trigger: int, seconds: int) -> None:
    spark = build_spark("L7-no-watermark")
    part_a_append_refused(spark, max_per_trigger)
    if not append_only:
        part_b_update_grows(spark, mode, max_per_trigger, seconds)
    spark.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="No-watermark: refusal (append) + OOM (update)")
    p.add_argument("--append-only", action="store_true", help="just show Part A")
    p.add_argument("--mode", choices=["update", "complete"], default="update",
                   help="Part B output mode; both grow state unboundedly")
    p.add_argument("--max-per-trigger", type=int, default=1000)
    p.add_argument("--seconds", type=int, default=30, help="how long to let Part B grow")
    args = p.parse_args()
    run(args.append_only, args.mode, args.max_per_trigger, args.seconds)
