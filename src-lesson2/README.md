# Lesson 2 — What happens when you distribute OLTP?

Last week: 3.5k → 357k rows/sec on a single Postgres node.
This week: same workload, 3 nodes, CockroachDB. Faster? Safer? At what cost?

## Quick start

```bash
# 1. Start the 3-node CockroachDB cluster
docker compose up -d

# Wait for crdb-init to finish (creates the schema)
docker compose logs crdb-init --follow

# 2. Install Python dependencies
uv sync

# 3. Run the benchmark suite (same scenarios as Lesson 1)
uv run python run_all.py --rows 50000

# 4. Open the Admin UI
open http://localhost:8080
```

## Demos

### Benchmark comparison (`run_all.py`)
Same workload as Lesson 1: naive, async, COPY, parallel COPY, hot row.
No `synchronous_commit = off` — CockroachDB won't let you disable durability.

```bash
uv run python run_all.py --rows 50000
```

### Kill a node (`demos/demo_kill_node.py`)
Insert under load, kill a node mid-flight, measure TPS dip and data loss.
Compare with Lesson 1's `demo_sync_loss.py` where Postgres *lost* data.

```bash
uv run python demos/demo_kill_node.py --connections 50 --kill-after 10 --observe 30
```

### Distributed transactions (`demos/demo_distributed_txn.py`)
Debit-credit transfers: same-range vs cross-range vs hot accounts.
Measures 2PC overhead and serializable retry rates.

```bash
uv run python demos/demo_distributed_txn.py --transfers 5000 --connections 10
```

### Latency injection (`demos/demo_latency_injection.sh`)
Simulate cross-region deployment by adding network latency between nodes.

```bash
# Add 50ms latency (simulates US-East ↔ US-West)
chmod +x demos/demo_latency_injection.sh
./demos/demo_latency_injection.sh add 50

# Re-run benchmarks — watch TPS crater
uv run python run_all.py --rows 10000

# Remove latency
./demos/demo_latency_injection.sh remove
```

### Diagnostic queries (`instrument.sql`)
CockroachDB equivalents of Lesson 1's instrumentation:

```bash
cockroach sql --insecure --host=localhost:26257 -d bench < instrument.sql
```

## Architecture

```
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│   crdb-1    │  │   crdb-2    │  │   crdb-3    │
│  :26257     │  │  :26258     │  │  :26259     │
│  :8080 (UI) │  │  :8081      │  │  :8082      │
└──────┬──────┘  └──────┬──────┘  └──────┬──────┘
       │                │                │
       └────── Raft consensus ───────────┘

Each node: 2 CPUs, 4 GB RAM (6 CPUs / 12 GB total)
Lesson 1 Postgres: 2 CPUs, 4 GB RAM
```

Data is split into **ranges** (~512 MB each). Each range is a Raft group
replicated across all 3 nodes. One node holds the **lease** (acts as leader)
for each range.

## The question

> When should you reach for a distributed database instead of scaling a single node?

The answer is **not** "when you need more throughput." Your own numbers will prove it.
