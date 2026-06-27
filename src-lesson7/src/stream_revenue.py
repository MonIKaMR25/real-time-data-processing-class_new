"""The lesson's core pipeline: revenue per time-window over the orders-cdc stream.

Three independent dials, and confusing them is the classic streaming bug:
    --watermark   how long to WAIT for stragglers (and how much state to keep)
    --window      the QUESTION ("revenue per 5 minutes") — the grain, not the speed
    --trigger     how OFTEN to look — latency only, never correctness
    --mode        update = upserts (dev/dashboards) · append = final rows only

Run it three times with --watermark 1, 10, 30 against the seeded stream and watch
the dropped-events counter move: that sweep is the completeness/latency tradeoff,
measured on your own data, and it's the table your take-home README must contain.

No fixed checkpoint here on purpose: each run starts from earliest and reprocesses
the whole topic, so the watermark sweep is apples-to-apples. (stream_to_kafka.py
is where checkpoints, and crash-resume, come in.)

Usage:
    python src/stream_revenue.py                       # 5-min window, 10-min wm, update
    python src/stream_revenue.py --watermark 1         # aggressive drops
    python src/stream_revenue.py --mode append         # only final, immutable rows
    python src/stream_revenue.py --trigger 30          # same totals, fewer/bigger batches
"""

import argparse

from pyspark.sql.functions import avg, col, count, sum, window

from config import ProgressPump, build_spark, read_orders


def run(watermark_min: int, window_min: int, trigger_s: int, mode: str,
        max_per_trigger: int) -> None:
    spark = build_spark("L7-stream-revenue")
    orders = read_orders(spark, starting="earliest", max_per_trigger=max_per_trigger)

    windowed = (orders
        .withWatermark("created_at", f"{watermark_min} minutes")        # the promise
        .groupBy(window(col("created_at"), f"{window_min} minutes"))    # the question
        .agg(sum("amount").alias("total_revenue"),
             count("*").alias("order_count"),
             avg("amount").alias("avg_order_value")))

    out = windowed.select(
        col("window.start").alias("win_start"),
        col("window.end").alias("win_end"),
        col("order_count"),
        col("total_revenue"),
        col("avg_order_value"))

    print(f"\nwindow={window_min}m · watermark={watermark_min}m · "
          f"trigger={trigger_s}s · mode={mode} · maxOffsetsPerTrigger={max_per_trigger}\n")

    query = (out.writeStream
        .outputMode(mode)                                    # ← update/append/complete
        .format("console").option("truncate", "false").option("numRows", 50)
        .trigger(processingTime=f"{trigger_s} seconds")
        .start())

    pump = ProgressPump(query, echo=True)
    pump.start()
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        pump.stop()
        query.stop()
        print(f"\nstopped. cumulative numRowsDroppedByWatermark = "
              f"{pump.dropped_total:,}  (watermark={watermark_min}m)")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Windowed revenue over orders-cdc")
    p.add_argument("--watermark", type=int, default=10, help="allowed lateness, minutes")
    p.add_argument("--window", type=int, default=5, help="tumbling window size, minutes")
    p.add_argument("--trigger", type=int, default=5, help="trigger interval, seconds")
    p.add_argument("--mode", choices=["update", "append", "complete"], default="update")
    p.add_argument("--max-per-trigger", type=int, default=300,
                   help="rows/batch. SMALL batches = a sharp watermark dial: the "
                        "watermark only updates BETWEEN batches, so a wide batch "
                        "inflates effective lateness. Set huge to swallow the topic "
                        "in one batch and watch drops vanish entirely.")
    args = p.parse_args()
    run(args.watermark, args.window, args.trigger, args.mode, args.max_per_trigger)
