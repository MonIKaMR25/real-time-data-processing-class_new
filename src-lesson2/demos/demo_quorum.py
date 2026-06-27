"""Demo — Progressive node failure: quorum mechanics in action.

Shows the full lifecycle:
  Phase 1: 3 nodes, steady state           → writes flow
  Phase 2: Kill 1 node (2/3 alive)         → quorum holds, writes continue
  Phase 3: Kill 2nd node (1/3 alive)       → quorum lost, writes REFUSED (CP!)
  Phase 4: Restart 1 node (2/3 alive)      → quorum restored, writes resume
  Phase 5: Restart last node (3/3 alive)   → full cluster, smooth sailing

This is the CP guarantee in action: the cluster refuses to write rather
than risk inconsistency when it can't get majority agreement.

Usage:
    python demos/demo_quorum.py [--connections 20] [--phase-duration 8]
"""

import argparse
import asyncio
import subprocess
import time

import asyncpg

DSN = "postgresql://root@localhost:26257/bench?sslmode=disable"
INSERT_SQL = "INSERT INTO orders (customer_id, amount) VALUES ($1, $2)"

NODE_1 = "lesson2-crdb-1"  # gateway — never kill this one (our connection target)
NODE_2 = "lesson2-crdb-2"
NODE_3 = "lesson2-crdb-3"

BOLD = "\033[1m"
DIM = "\033[2m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
RED = "\033[31m"
CYAN = "\033[36m"
RESET = "\033[0m"


def docker_kill(container: str) -> bool:
    r = subprocess.run(["docker", "kill", container], capture_output=True, text=True)
    return r.returncode == 0


def docker_start(container: str) -> bool:
    r = subprocess.run(["docker", "start", container], capture_output=True, text=True)
    return r.returncode == 0


async def run(connections: int, phase_duration: float) -> None:
    import random

    print(f"{BOLD}Demo — Quorum: the CP guarantee in action{RESET}")
    print("=" * 66)

    pool = await asyncpg.create_pool(DSN, min_size=connections, max_size=connections)

    # Clean slate
    async with pool.acquire() as conn:
        await conn.execute("TRUNCATE orders")

    # Shared state
    stats = {"total": 0, "errors": 0, "phase": "3/3 nodes", "error_delta": 0}
    running = True
    phase_events: list[tuple[float, str]] = []
    t0 = time.monotonic()

    async def worker():
        while running:
            try:
                async with pool.acquire() as conn:
                    # 3s timeout so no-quorum writes error visibly
                    # instead of hanging 30+s on CockroachDB's internal timeout
                    await conn.execute("SET statement_timeout = '3s'")
                    while running:
                        await conn.execute(
                            INSERT_SQL,
                            random.randint(1, 10_000),
                            round(random.uniform(1, 500), 2),
                        )
                        stats["total"] += 1
            except (
                asyncpg.ConnectionDoesNotExistError,
                asyncpg.InterfaceError,
                asyncpg.InternalServerError,
                asyncpg.PostgresError,
                OSError,
            ):
                stats["errors"] += 1
                if running:
                    await asyncio.sleep(0.2)

    async def reporter():
        last_total = 0
        last_errors = 0
        last_time = t0

        while running:
            await asyncio.sleep(1.0)
            now = time.monotonic()
            elapsed = now - t0
            interval_rows = stats["total"] - last_total
            interval_errs = stats["errors"] - last_errors
            interval_secs = now - last_time
            tps = interval_rows / interval_secs if interval_secs > 0 else 0

            err_str = f"{interval_errs:>3}" if interval_errs == 0 else f"{RED}{interval_errs:>3}{RESET}"
            tps_color = GREEN if tps > 100 else (RED if tps == 0 else YELLOW)

            # Show phase event markers
            marker = ""
            for evt_time, evt_msg in phase_events:
                if abs(now - evt_time) < 1.5:
                    marker = f"  ← {evt_msg}"

            print(f"    [{elapsed:5.1f}s]  {tps_color}{tps:>8,.0f} TPS{RESET}  |  errs/s: {err_str}  |  {stats['phase']}{marker}")

            last_total = stats["total"]
            last_errors = stats["errors"]
            last_time = now

    # Start workers + reporter
    tasks = [asyncio.create_task(worker()) for _ in range(connections)]
    reporter_task = asyncio.create_task(reporter())

    # ── Phase 1: Steady state (3/3) ──────────────────────────
    print(f"\n  {CYAN}Phase 1:{RESET} Steady state — {BOLD}3/3 nodes{RESET} ({phase_duration:.0f}s)")
    print(f"  {DIM}All nodes alive. Raft quorum = 2. Writes flow normally.{RESET}\n")
    await asyncio.sleep(phase_duration)

    # ── Phase 2: Kill node 3 (2/3) ───────────────────────────
    print(f"\n  {YELLOW}💥 Killing {NODE_3}...{RESET}")
    docker_kill(NODE_3)
    stats["phase"] = f"{YELLOW}2/3 nodes{RESET}"
    phase_events.append((time.monotonic(), "NODE 3 KILLED"))
    print(f"  {GREEN}✓ Cluster has 2/3 nodes — quorum holds. Writes should continue.{RESET}\n")
    await asyncio.sleep(phase_duration)

    # ── Phase 3: Kill node 2 (1/3) — QUORUM LOST ────────────
    errors_before = stats["errors"]
    print(f"\n  {RED}💥💥 Killing {NODE_2}... QUORUM LOST!{RESET}")
    docker_kill(NODE_2)
    stats["phase"] = f"{RED}1/3 nodes — NO QUORUM{RESET}"
    phase_events.append((time.monotonic(), "QUORUM LOST"))
    print(f"  {RED}✗ Cluster has 1/3 nodes — quorum requires 2. Writes will be REFUSED.{RESET}")
    print(f"  {DIM}  This is CP: consistency over availability. No split-brain risk.{RESET}\n")
    await asyncio.sleep(phase_duration)
    errors_during_no_quorum = stats["errors"] - errors_before

    # ── Phase 4: Restart node 2 (2/3) — QUORUM RESTORED ─────
    print(f"\n  {GREEN}🔄 Restarting {NODE_2}...{RESET}")
    docker_start(NODE_2)
    stats["phase"] = f"{GREEN}2/3 nodes — quorum restored{RESET}"
    phase_events.append((time.monotonic(), "QUORUM RESTORED"))
    print(f"  {GREEN}✓ Cluster has 2/3 nodes — quorum restored. Writes should resume.{RESET}\n")
    await asyncio.sleep(phase_duration)

    # ── Phase 5: Restart node 3 (3/3) — FULL CLUSTER ────────
    print(f"\n  {GREEN}🔄 Restarting {NODE_3}...{RESET}")
    docker_start(NODE_3)
    stats["phase"] = f"{GREEN}3/3 nodes — full cluster{RESET}"
    phase_events.append((time.monotonic(), "FULL CLUSTER"))
    print(f"  {GREEN}✓ All 3 nodes alive. Raft log replay will catch up the restarted nodes.{RESET}\n")
    await asyncio.sleep(phase_duration)

    # ── Stop ─────────────────────────────────────────────────
    running = False
    await asyncio.gather(*tasks, return_exceptions=True)
    reporter_task.cancel()
    await asyncio.gather(reporter_task, return_exceptions=True)

    # Count DB rows
    conn = await asyncpg.connect(DSN)
    db_count = await conn.fetchval("SELECT count(*) FROM orders")
    await conn.close()

    # Summary
    print("\n" + "=" * 66)
    print(f"  {BOLD}Summary{RESET}")
    print(f"    Client-side inserts:      {stats['total']:>10,}")
    print(f"    Database row count:       {db_count:>10,}")
    print(f"    Total errors:             {stats['errors']:>10,}")
    print(f"    Errors during no-quorum:  {errors_during_no_quorum:>10,}")
    print()
    print(f"  {BOLD}What you saw:{RESET}")
    print(f"    {GREEN}3/3 nodes:{RESET}  writes flow normally")
    print(f"    {YELLOW}2/3 nodes:{RESET}  quorum holds → brief stall, then recovery")
    print(f"    {RED}1/3 nodes:{RESET}  quorum lost → writes REFUSED (CP guarantee)")
    print(f"    {GREEN}2/3 nodes:{RESET}  quorum restored → writes resume immediately")
    print(f"    {GREEN}3/3 nodes:{RESET}  full cluster → smooth sailing")
    print("=" * 66)

    await pool.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Demo — progressive quorum failure")
    parser.add_argument("--connections", "-c", type=int, default=20)
    parser.add_argument("--phase-duration", "-d", type=float, default=8,
                        help="Seconds per phase (default: 8)")
    args = parser.parse_args()
    asyncio.run(run(args.connections, args.phase_duration))
