#!/usr/bin/env bash
# Register the Debezium Postgres connector with Kafka Connect.
# snapshot.mode=no_data: we skip the initial snapshot on purpose — the L5
# source has 1M+ rows and this demo is about the STREAM reaching two groups.
set -euo pipefail
cd "$(dirname "$0")"

echo "waiting for Kafka Connect on :8083..."
until curl -sf http://localhost:8083/connectors >/dev/null; do sleep 2; done

curl -s -X POST -H "Content-Type: application/json" \
  --data @register-orders-connector.json \
  http://localhost:8083/connectors | python3 -m json.tool || true

echo
echo "status:"
sleep 3
curl -s http://localhost:8083/connectors/orders-connector/status | python3 -m json.tool

cat <<'EOF'

Next:
  uv run python src/consume_cdc.py --group mirror    # terminal 1
  uv run python src/consume_cdc.py --group fraud     # terminal 2
  # then UPDATE/DELETE rows in the L5 postgres and watch BOTH groups see it.

Teardown (drops the slot on the L5 source — every slot needs an owner!):
  curl -s -X DELETE http://localhost:8083/connectors/orders-connector
  docker exec lesson5-postgres psql -U bench -d bench \
    -c "SELECT pg_drop_replication_slot('debezium_l6_slot');" || true
EOF
