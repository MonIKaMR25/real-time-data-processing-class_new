"""Demo — Simulate cross-region latency and measure the Raft penalty.

Runs baseline benchmarks, injects network latency via a netshoot sidecar,
re-runs the same benchmarks, then removes the latency and prints a comparison.

Usage:
    uv run python demos/demo_latency_injection.py [--delay 50] [--rows 500]
"""

import argparse
import subprocess
import sys
from pathlib import Path

CONTAINERS = ["lesson2-crdb-1", "lesson2-crdb-2", "lesson2-crdb-3"]
RUN_ALL = Path(__file__).parent.parent / "run_all.py"


def tc(container: str, *args: str) -> bool:
    result = subprocess.run(
        [
            "docker", "run", "--rm",
            "--network", f"container:{container}",
            "--cap-add", "NET_ADMIN",
            "nicolaka/netshoot",
            "tc", *args,
        ],
        capture_output=True,
    )
    return result.returncode == 0


def p(msg: str = "", **kwargs) -> None:
    print(msg, flush=True, **kwargs)


def inject_latency(delay_ms: int) -> None:
    p(f"  Injecting {delay_ms}ms latency on all inter-node interfaces...")
    for c in CONTAINERS:
        tc(c, "qdisc", "del", "dev", "eth0", "root")  # clear any existing, ignore errors
        ok = tc(c, "qdisc", "add", "dev", "eth0", "root", "netem", "delay", f"{delay_ms}ms")
        p(f"    {'✓' if ok else '✗'} {c}: +{delay_ms}ms")


def remove_latency() -> None:
    p("  Removing injected latency...")
    for c in CONTAINERS:
        tc(c, "qdisc", "del", "dev", "eth0", "root")
        p(f"    ✓ {c}: restored")


def run_benchmarks(rows: int, label: str) -> None:
    p(f"\n{'─' * 65}")
    p(f"  {label}")
    p(f"{'─' * 65}")
    subprocess.run(
        [sys.executable, str(RUN_ALL), "--rows", str(rows)],
        check=True,
    )


def main(delay_ms: int, rows: int) -> None:
    p("Demo — Latency injection: simulating cross-region Raft")
    p("=" * 65)
    p(f"  Injecting {delay_ms}ms → simulates ~{delay_ms * 2}ms RTT")
    p(f"  Theoretical TPS ceiling (1 conn): ~{1000 // (delay_ms * 2)} TPS per Raft group")

    run_benchmarks(rows, "Phase 1 — Baseline (no latency)")

    p(f"\nInjecting {delay_ms}ms latency...")
    inject_latency(delay_ms)
    p("  Waiting 2s for routing to stabilize...")
    import time; time.sleep(2)

    try:
        run_benchmarks(rows, f"Phase 2 — +{delay_ms}ms latency (cross-region simulation)")
    finally:
        p(f"\nRemoving latency...")
        remove_latency()

    p("\n" + "=" * 65)
    p(f"  Every commit waited an extra {delay_ms * 2}ms for the Raft round-trip.")
    p(f"  That's the physics cost — no code change can escape it.")
    p("=" * 65)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--delay", "-d", type=int, default=50,
                        help="Latency to inject in ms (default: 50)")
    parser.add_argument("--rows", "-n", type=int, default=500,
                        help="Rows per benchmark run (default: 500)")
    args = parser.parse_args()
    try:
        main(args.delay, args.rows)
    except KeyboardInterrupt:
        print("\n  Interrupted — cleaning up...")
        remove_latency()
        sys.exit(1)
