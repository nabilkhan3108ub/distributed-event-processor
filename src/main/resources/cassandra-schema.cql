-- ============================================================================
-- EventFlow Cassandra Schema
-- ============================================================================
-- This file is for REFERENCE and manual setup. The application auto-creates
-- these tables via CassandraConfig.java on startup.
--
-- INTERVIEW TALKING POINT: Every table here is designed QUERY-FIRST.
-- In Cassandra, you don't normalize like SQL. You ask "what queries will I run?"
-- and design tables to answer each query with a SINGLE partition lookup.
-- This means data is duplicated across tables — that's intentional and expected.
-- ============================================================================

-- Create the keyspace (like a database in MySQL)
-- SimpleStrategy: fine for single datacenter. Production would use NetworkTopologyStrategy.
-- Replication factor 3: every piece of data is stored on 3 different nodes.
-- If any 1 node dies, 2 copies still exist. This is how Cassandra achieves fault tolerance.
CREATE KEYSPACE IF NOT EXISTS eventflow
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 3
};

USE eventflow;

-- ============================================================================
-- TABLE 1: events_by_user
-- ============================================================================
-- PURPOSE: Answer the query "Give me all events for user X in time range Y to Z"
--
-- PARTITION KEY: (user_id, time_bucket)
--   - user_id: all events for one user land on the same Cassandra node = fast
--   - time_bucket: prevents "unbounded partition growth". Without it, a user
--     with 10 million events would have a single massive partition (bad).
--     By bucketing into daily chunks (e.g., "2026-05-24"), each partition
--     stays manageable. The app generates the bucket from the timestamp.
--
-- CLUSTERING COLUMN: timestamp DESC
--   - Within each partition, events are physically sorted newest-first on disk.
--   - "Give me the last 50 events" = read 50 sequential bytes. No sorting needed.
--
-- COMPACTION: TimeWindowCompactionStrategy
--   - Optimized for time-series data where you mostly write recent data and
--     query recent data. Old data compacts efficiently into large files.
--   - compaction_window_size = 1 day: data within each day is compacted together.
--
-- INTERVIEW QUESTION: "Why not just use user_id as the partition key?"
-- ANSWER: Unbounded partition growth. A power user with millions of events
-- would create a partition too large for a single node to handle efficiently.
-- Time bucketing bounds the maximum partition size.
-- ============================================================================
CREATE TABLE IF NOT EXISTS events_by_user (
    user_id       TEXT,
    time_bucket   TEXT,
    timestamp     TIMESTAMP,
    event_id      UUID,
    event_type    TEXT,
    source        TEXT,
    payload       TEXT,
    processed_at  TIMESTAMP,
    PRIMARY KEY ((user_id, time_bucket), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC)
  AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_size': '1',
    'compaction_window_unit': 'DAYS'
  }
  AND default_time_to_live = 2592000;  -- 30 days auto-expiry (cleanup!)

-- ============================================================================
-- TABLE 2: aggregations
-- ============================================================================
-- PURPOSE: Answer "How many events of type X happened per hour/day?"
--
-- This uses Cassandra COUNTER columns — a special column type that supports
-- atomic increment/decrement operations. Instead of read-modify-write
-- (which has race conditions), you just say "add 1" and Cassandra handles it.
--
-- PARTITION KEY: (event_type, time_bucket)
--   - Groups all counts for one event type within one time window
--   - Query: "How many CLICK events on 2026-05-24?" = single partition read
--
-- WHY COUNTERS MATTER (interview): In a distributed system, two nodes might
-- simultaneously try to increment a count. With a normal column, both read
-- the value as 5, add 1, and write 6 — losing one increment. Counter columns
-- use Cassandra's internal merge mechanism to correctly produce 7.
-- ============================================================================
CREATE TABLE IF NOT EXISTS aggregations (
    event_type  TEXT,
    time_bucket TEXT,
    event_count COUNTER,
    PRIMARY KEY ((event_type, time_bucket))
);

-- ============================================================================
-- TABLE 3: anomalies
-- ============================================================================
-- PURPOSE: Answer "Show me detected anomalies in the last N hours"
--
-- PARTITION KEY: (user_id, hour_bucket)
--   - Anomalies are bucketed by hour (finer than daily) because you want
--     to query recent anomalies frequently for alerting
--
-- CLUSTERING COLUMN: detected_at DESC
--   - Newest anomalies first within each bucket
--
-- INTERVIEW TALKING POINT: This table demonstrates the "denormalization"
-- principle. The anomaly data could theoretically be derived from the
-- events_by_user table, but storing it separately means the anomaly query
-- doesn't need to scan and recompute — it's pre-materialized.
-- This is the CQRS pattern: separate your read models from your write models.
-- ============================================================================
CREATE TABLE IF NOT EXISTS anomalies (
    user_id     TEXT,
    hour_bucket TEXT,
    detected_at TIMESTAMP,
    event_type  TEXT,
    severity    TEXT,
    z_score     DOUBLE,
    message     TEXT,
    PRIMARY KEY ((user_id, hour_bucket), detected_at)
) WITH CLUSTERING ORDER BY (detected_at DESC)
  AND default_time_to_live = 604800;  -- 7 days auto-expiry

-- ============================================================================
-- QUERY EXAMPLES (for your understanding)
-- ============================================================================

-- Get last 50 events for user "alice" today:
-- SELECT * FROM events_by_user
--   WHERE user_id = 'alice' AND time_bucket = '2026-05-24'
--   LIMIT 50;

-- Get click event counts for today:
-- SELECT * FROM aggregations
--   WHERE event_type = 'CLICK' AND time_bucket = '2026-05-24';

-- Get anomalies for user "bob" in the last hour:
-- SELECT * FROM anomalies
--   WHERE user_id = 'bob' AND hour_bucket = '2026-05-24-15';

-- ============================================================================
-- ANTI-PATTERNS TO KNOW FOR INTERVIEWS
-- ============================================================================
-- 1. NEVER do SELECT * FROM events_by_user (no WHERE clause)
--    → This is a full table scan across ALL nodes = terrible performance
--
-- 2. NEVER filter on non-partition-key columns without ALLOW FILTERING
--    → "Get all events of type CLICK" requires scanning every partition
--    → Instead, create a separate table with event_type as partition key
--
-- 3. NEVER use secondary indexes for high-cardinality columns
--    → They create hidden tables on every node = scattered reads
--    → Denormalize into a new table instead
-- ============================================================================
