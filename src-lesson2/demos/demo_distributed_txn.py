"""Demo — Measure the cost of distributed transactions (2PC) and contention.

Forces range splits on the accounts table so transfers between distant keys
actually cross Raft groups and require 2PC. Four scenarios:

  1. Local, low contention   — both accounts on same range, wide key space
  2. Local, high contention  — same range, narrow key space (retries)
  3. Cross-range, low contention — accounts on different ranges, wide keys
  4. Cross-range, high contention — different ranges, narrow keys (retries + 2PC)

Usage:
    python demos/demo_distributed_txn.py [--transfers 2000] [--connections 10]
"""

import argparse
import asyncio
import random
import time

import asyncpg

DSN = "postgresql://root@localhost:26257/bench?sslmode=disable"

# Accounts 1-500 → range A,  501-1000 → range B (split at 501)
RANGE_A = (1, 500)
RANGE_B = (501, 1000)


def percentile(latencies: list[float], p: float) -> float:
    if not latencies:
        return 0.0
    k = (len(latencies) - 1) * (p / 100)
    f = int(k)
    c = f + 1 if f + 1 < len(latencies) else f
    return latencies[f] + (k - f) * (latencies[c] - latencies[f])


async def transfer_with_retry(conn, from_id: int, to_id: int, amount: float,
                               stats: dict, latencies: list):
    """Execute a debit-credit transfer with CockroachDB retry logic."""
    t0 = time.monotonic()
    max_retries = 10
    for attempt in range(max_retries):
        try:
            async with conn.transaction():
                await conn.execute(
                    "UPDATE accounts SET balance = balance - $1 WHERE id = $2",
                    amount, from_id,
                )
                await conn.execute(
                    "UPDATE accounts SET balance = balance + $1 WHERE id = $2",
                    amount, to_id,
                )
            latencies.append(time.monotonic() - t0)
            return  # success
        except asyncpg.SerializationError:
            stats["retries"] += 1
            await asyncio.sleep(0.001 * (2 ** attempt))
    stats["failures"] += 1


async def bench_transfers(
    connections: int,
    transfers: int,
    pick_accounts,
    label: str,
) -> dict:
    """Run debit-credit transfers and measure TPS, retries, and latency."""
    pool = await asyncpg.create_pool(DSN, min_size=connections, max_size=connections)
    per_worker = transfers // connections
    remainder = transfers % connections

    stats = {"done": 0, "retries": 0, "failures": 0}
    latencies: list[float] = []
    running = True

    async def worker(n: int):
        async with pool.acquire() as conn:
            for _ in range(n):
                from_id, to_id = pick_accounts()
                await transfer_with_retry(
                    conn, from_id, to_id,
                    round(random.uniform(0.01, 10.0), 2),
                    stats, latencies,
                )
                stats["done"] += 1

    async def progress():
        last_done = 0
        last_time = time.monotonic()
        while running:
            await asyncio.sleep(2.0)
            if not running:
                break
            now = time.monotonic()
            delta = stats["done"] - last_done
            dt = now - last_time
            tps = delta / dt if dt > 0 else 0
            pct = stats["done"] / transfers * 100 if transfers > 0 else 0
            print(f"      {stats['done']:>6,}/{transfers:,}  ({pct:4.0f}%)  ~{tps:,.0f} TPS  retries: {stats['retries']:,}", flush=True)
            last_done = stats["done"]
            last_time = now

    t0 = time.monotonic()
    progress_task = asyncio.create_task(progress())
    await asyncio.gather(*[
        worker(per_worker + (1 if i < remainder else 0))
        for i in range(connections)
    ])
    elapsed = time.monotonic() - t0
    running = False
    progress_task.cancel()
    await asyncio.gather(progress_task, return_exceptions=True)
    await pool.close()

    latencies.sort()
    tps = stats["done"] / elapsed
    retry_rate = stats["retries"] / max(1, stats["done"] + stats["retries"]) * 100

    return {
        "label": label,
        "tps": tps,
        "elapsed": elapsed,
        "done": stats["done"],
        "retries": stats["retries"],
        "failures": stats["failures"],
        "retry_rate": retry_rate,
        "p50": percentile(latencies, 50) * 1000,
        "p95": percentile(latencies, 95) * 1000,
        "p99": percentile(latencies, 99) * 1000,
    }


def pick_local_wide():
    """Both accounts in range A (1-500) — same range, low contention."""
    a = random.randint(*RANGE_A)
    b = random.randint(*RANGE_A)
    while b == a:
        b = random.randint(*RANGE_A)
    return a, b


def pick_local_narrow():
    """Both accounts in a tiny slice (1-5) — same range, high contention."""
    a = random.randint(1, 5)
    b = random.randint(1, 5)
    while b == a:
        b = random.randint(1, 5)
    return a, b


def pick_cross_wide():
    """One account from range A, one from range B — crosses ranges."""
    return random.randint(*RANGE_A), random.randint(*RANGE_B)


def pick_cross_narrow():
    """Narrow keys but across ranges — 2PC + contention."""
    return random.randint(498, 500), random.randint(501, 503)


async def run(transfers: int, connections: int) -> None:
    print("Demo — Distributed transactions: 2PC overhead + contention")
    print("=" * 70)

    # ── Force range splits so cross-range actually means something ───
    conn = await asyncpg.connect(DSN)
    print("  Forcing range split at key 501...")
    try:
        await conn.execute("ALTER TABLE accounts SPLIT AT VALUES (501)")
    except asyncpg.UniqueViolationError:
        pass  # already split
    await asyncio.sleep(1)  # let split propagate

    range_count = await conn.fetchval(
        "SELECT count(*) FROM [SHOW RANGES FROM TABLE accounts]"
    )

    # Reset balances for clean accounting
    await conn.execute("UPDATE accounts SET balance = 10000.00")
    await conn.close()

    print(f"  accounts table: {range_count} range(s)")
    print(f"  Transfers: {transfers:,}  |  Connections: {connections}")
    print(f"  Range A: keys 1-500  |  Range B: keys 501-1000\n")

    scenarios = [
        ("Local, low contention", pick_local_wide, transfers,
         "Both accounts in range A (1-500). No 2PC, minimal retries."),
        ("Local, high contention", pick_local_narrow, transfers // 4,
         "Both accounts 1-5, same range. Serializable retries pile up."),
        ("Cross-range, low cont.", pick_cross_wide, transfers,
         "One from range A, one from B. 2PC required, but keys spread out."),
        ("Cross-range, high cont.", pick_cross_narrow, transfers // 4,
         "Keys 498-500 vs 501-503. 2PC + contention = worst case."),
    ]

    results = []
    for label, picker, n, desc in scenarios:
        print(f"  {label}")
        print(f"  → {desc}")
        r = await bench_transfers(connections, n, picker, label)
        results.append(r)
        print(f"    {r['tps']:,.0f} TPS  |  retries: {r['retries']:,} ({r['retry_rate']:.1f}%)"
              f"  |  p50={r['p50']:.1f}ms  p95={r['p95']:.1f}ms  p99={r['p99']:.1f}ms\n")

    # Summary table
    print("=" * 70)
    print(f"  {'Scenario':<24} {'TPS':>7} {'Retries':>8} {'Retry%':>7}"
          f" {'p50ms':>7} {'p95ms':>7} {'p99ms':>7}")
    print("  " + "-" * 66)
    for r in results:
        print(f"  {r['label']:<24} {r['tps']:>7,.0f} {r['retries']:>8,}"
              f" {r['retry_rate']:>6.1f}% {r['p50']:>7.1f} {r['p95']:>7.1f} {r['p99']:>7.1f}")
    print("=" * 70)

    # 2PC overhead: compare local-wide vs cross-wide (same contention level)
    local_tps = results[0]["tps"]
    cross_tps = results[2]["tps"]
    if local_tps > 0:
        overhead = (1 - cross_tps / local_tps) * 100
        print(f"\n  2PC overhead (low contention): ~{overhead:.0f}% TPS reduction")
        print(f"    Local:       {local_tps:,.0f} TPS  (p99 = {results[0]['p99']:.1f}ms)")
        print(f"    Cross-range: {cross_tps:,.0f} TPS  (p99 = {results[2]['p99']:.1f}ms)")

    # Contention cost: compare local-wide vs local-narrow
    contention_tps = results[1]["tps"]
    if local_tps > 0:
        contention_cost = (1 - contention_tps / local_tps) * 100
        print(f"\n  Contention cost (same range): ~{contention_cost:.0f}% TPS reduction")
        print(f"    Wide keys:   {local_tps:,.0f} TPS  (retries: {results[0]['retry_rate']:.1f}%)")
        print(f"    Hot keys:    {contention_tps:,.0f} TPS  (retries: {results[1]['retry_rate']:.1f}%)")

    print("\n  Key takeaway: 2PC and contention are independent costs.")
    print("  Both are real. Combined, they compound.\n")

    # Verify balances are consistent
    conn = await asyncpg.connect(DSN)
    total = await conn.fetchval("SELECT sum(balance) FROM accounts")
    expected = await conn.fetchval("SELECT count(*) * 10000.00 FROM accounts")
    await conn.close()
    if total == expected:
        print("  ✓ Balance check passed — total money is conserved.")
    else:
        print(f"  ⚠ Balance mismatch: {total} vs expected {expected}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Demo — distributed transaction overhead and contention"
    )
    parser.add_argument("--transfers", "-t", type=int, default=2000)
    parser.add_argument("--connections", "-c", type=int, default=10)
    args = parser.parse_args()
    asyncio.run(run(args.transfers, args.connections))
