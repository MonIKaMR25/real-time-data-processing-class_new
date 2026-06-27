"""Break 01, automated: join -> kill -> count the duplicates. Twice.

Scenario (producer runs continuously throughout):
    1. consumer A starts, owns all 6 partitions
    2. consumer B joins            -> rebalance one (A's on_revoke fires)
    3. consumer B is SIGKILLed     -> rebalance two (A inherits B's partitions)
    4. producer stops, A drains, A stops

Run once with correct on_revoke commits, once with --skip-revoke-commit, and
compare the duplicate counts in the ledgers. Nothing is ever LOST either way —
at-least-once holds. The difference is how much REPLAYED work your sink eats,
and whether your sink being idempotent (L4, L5) saves you from your own bug.

Usage:
    python src/experiment_rebalance.py                       # correct callbacks
    python src/experiment_rebalance.py --skip-revoke-commit  # the broken twin
"""

import argparse
import json
import signal
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path

from config import ledger_path, producer_summary_path

SRC = Path(__file__).parent
PY = sys.executable


def spawn(*args: str) -> subprocess.Popen:
    return subprocess.Popen([PY, *args], stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True)


def run_lines(group: str, run: int) -> list[dict]:
    """Ledger records for THIS producer run only — the topic accumulates history
    across runs and seq restarts at 0, so unfiltered counting invents both
    duplicates (seq collisions across runs) and losses (old seqs never replayed)."""
    lines = [json.loads(l) for l in ledger_path(group).read_text().splitlines()]
    return [r for r in lines if r.get("run") == run]


def analyze(group: str) -> None:
    summary = json.loads(producer_summary_path().read_text())
    lines = run_lines(group, summary["run"])

    seq_counts = Counter(r["seq"] for r in lines)
    dup_events = sum(n - 1 for n in seq_counts.values() if n > 1)
    lost = set(range(summary["produced"])) - set(seq_counts)
    by_consumer = Counter(r["consumer"] for r in lines)

    print(f"\n{'-' * 56}\nledger analysis  (group={group}, run={summary['run']})")
    print(f"  produced            {summary['produced']:>7,}")
    print(f"  processed (ledger)  {len(lines):>7,}  "
          f"({', '.join(f'{c}: {n:,}' for c, n in sorted(by_consumer.items()))})")
    print(f"  duplicates          {dup_events:>7,}")
    print(f"  lost                {len(lost):>7,}  <- must be 0: at-least-once")


def run(skip: bool) -> None:
    group = f"rebalance-{'broken' if skip else 'correct'}-{int(time.time())}"
    flag = ["--skip-revoke-commit"] if skip else []
    print(f"group={group}  on_revoke commit: {'SKIPPED' if skip else 'on'}\n")

    producer = spawn(SRC / "produce_orders.py", "--rate", "400")
    consumer_a = spawn(SRC / "consume_rebalance.py", "--name", "A", "--group", group, *flag)
    time.sleep(8)

    print(">> consumer B joins (rebalance one: A revokes half its partitions)")
    consumer_b = spawn(SRC / "consume_rebalance.py", "--name", "B", "--group", group, *flag)
    time.sleep(8)

    print(">> consumer B is SIGKILLed (rebalance two: A inherits the partitions back)")
    consumer_b.kill()                           # crash, not clean shutdown — no commit
    time.sleep(10)                              # session timeout + A re-consumes

    producer.send_signal(signal.SIGINT)         # flush + write produce-summary.json
    producer.wait(timeout=30)

    # Drain: wait until A's ledger covers every seq of THIS run (or time out).
    # A fixed sleep here is a race — A re-reads accumulated topic history first.
    summary = json.loads(producer_summary_path().read_text())
    target = set(range(summary["produced"]))
    for _ in range(60):
        seen = {r["seq"] for r in run_lines(group, summary["run"])}
        if target <= seen:
            break
        time.sleep(1)
    consumer_a.send_signal(signal.SIGINT)
    consumer_a.wait(timeout=30)

    print("\nconsumer A output:")
    a_log = consumer_a.stdout.read()
    (Path(ledger_path(group)).parent / "consumer-A.log").write_text(a_log)
    for line in a_log.splitlines():
        if any(k in line for k in ("ASSIGNED", "REVOKED", "stopped", "fatal", "Error", "Traceback")):
            print(f"  {line}")

    analyze(group)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Automated rebalance experiment")
    p.add_argument("--skip-revoke-commit", action="store_true")
    args = p.parse_args()
    run(args.skip_revoke_commit)
