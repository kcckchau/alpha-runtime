-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Hypertables are created AFTER SQLAlchemy creates the base tables.
-- This script runs after the DB is initialized; it converts the regular
-- tables into TimescaleDB hypertables for time-series optimization.
--
-- Note: init_db() in session.py creates the tables using SQLAlchemy ORM.
-- Run this script manually (or via a migration) after first startup.

-- Convert candles to hypertable (partitioned by timestamp, 1-day chunks)
SELECT create_hypertable(
    'candles', 'timestamp',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Convert indicators to hypertable
SELECT create_hypertable(
    'indicators', 'timestamp',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Convert market_context to hypertable
SELECT create_hypertable(
    'market_context', 'timestamp',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Convert fills to hypertable
SELECT create_hypertable(
    'fills', 'timestamp',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Compression policy for candles older than 7 days
-- SELECT add_compression_policy('candles', INTERVAL '7 days');

-- Retention policy (optional - uncomment and adjust as needed)
-- SELECT add_retention_policy('candles', INTERVAL '365 days');
