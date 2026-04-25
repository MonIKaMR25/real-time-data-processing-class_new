CREATE DATABASE IF NOT EXISTS demo;

-- Persistent storage (created first so ALTER works on existing deploys)
CREATE TABLE IF NOT EXISTS demo.taps (
    ts          DateTime64(3),
    session_id  String,
    device      String,
    client_ts   Int64 DEFAULT 0
) ENGINE = MergeTree()
ORDER BY ts;

-- Migration: add client_ts if table already existed without it
ALTER TABLE demo.taps ADD COLUMN IF NOT EXISTS client_ts Int64 DEFAULT 0;

-- Drop Kafka engine + MV to recreate with updated schema
DROP TABLE IF EXISTS demo.taps_mv;
DROP TABLE IF EXISTS demo.taps_kafka;

-- Kafka engine table: streams from Redpanda topic `taps`
CREATE TABLE demo.taps_kafka (
    ts          DateTime64(3),
    session_id  String,
    device      String,
    client_ts   Int64
) ENGINE = Kafka
SETTINGS
    kafka_broker_list      = 'redpanda:9092',
    kafka_topic_list       = 'taps',
    kafka_group_name       = 'clickhouse-taps-consumer',
    kafka_format           = 'JSONEachRow',
    kafka_num_consumers    = 1,
    kafka_max_block_size   = 1024,
    kafka_flush_interval_ms = 500;

-- Materialized view: bridge from Kafka stream into the persistent table
CREATE MATERIALIZED VIEW demo.taps_mv TO demo.taps AS
SELECT ts, session_id, device, client_ts FROM demo.taps_kafka;
