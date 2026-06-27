-- ============================================================
-- Instrumentation queries — CockroachDB (run in cockroach sql)
-- Connect: cockroach sql --insecure --host=localhost:26257 -d bench
-- ============================================================

-- ─── Range distribution ─────────────────────────────────────
-- How is the orders table split across nodes?
SHOW RANGES FROM TABLE orders;

-- ─── Lease holders ──────────────────────────────────────────
-- Which node owns the lease for each range? (bottleneck if uneven)
SELECT range_id, lease_holder, replicas
FROM [SHOW RANGES FROM TABLE orders];

-- ─── Range distribution for accounts ────────────────────────
SHOW RANGES FROM TABLE accounts;

-- ─── Node status ────────────────────────────────────────────
-- Are all nodes alive?
SELECT node_id, address, is_available, is_live
FROM crdb_internal.gossip_liveness;

-- ─── Active sessions ────────────────────────────────────────
-- Equivalent of pg_stat_activity
SELECT node_id, session_id, status, left(last_active_query, 60) AS query
FROM crdb_internal.cluster_sessions
ORDER BY node_id, status;

-- ─── Statement statistics ───────────────────────────────────
-- Equivalent of pg_stat_statements
SELECT key, count, service_lat_avg, service_lat_p99
FROM crdb_internal.node_statement_statistics
ORDER BY count DESC
LIMIT 10;

-- ─── Transaction contention ─────────────────────────────────
-- See which transactions are retrying (serializable conflicts)
SELECT * FROM crdb_internal.cluster_contention_events
ORDER BY count DESC
LIMIT 10;

-- ─── Raft leadership ────────────────────────────────────────
-- Per-store Raft stats: useful after killing a node
SELECT store_id, range_count, lease_count, quiescent_count
FROM crdb_internal.kv_store_status;

-- ─── Cluster settings (relevant ones) ───────────────────────
SHOW CLUSTER SETTING kv.range_merge.queue_enabled;
SHOW CLUSTER SETTING kv.range_split.by_load_enabled;
SHOW CLUSTER SETTING server.time_until_store_dead;

-- ─── Table sizes ────────────────────────────────────────────
SELECT table_name,
       range_count,
       approximate_disk_size,
       live_bytes,
       total_bytes
FROM [SHOW RANGES FROM DATABASE bench]
WHERE table_name IN ('orders', 'accounts');

-- ─── Verify account balances are conserved ──────────────────
-- Should equal count(*) * 10000.00 after any number of transfers
SELECT sum(balance) AS total_balance,
       count(*) AS accounts,
       count(*) * 10000.00 AS expected
FROM accounts;
