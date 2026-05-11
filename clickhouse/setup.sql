-- Create database
CREATE DATABASE IF NOT EXISTS cdc_db;

-- Kafka Engine table (raw consumer)
CREATE TABLE IF NOT EXISTS cdc_db.kafka_orders_raw
(
    id          Int64,
    customer_id Int32,
    amount      Float64,
    status      String,
    created_at  String,
    updated_at  String,
    __op        String,
    __table     String,
    __source_ts_ms Int64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list    = 'your-kafka-vm-ip:29092',
    kafka_topic_list     = 'pgcdc.public.orders',
    kafka_group_name     = 'ch_orders_consumer',
    kafka_format         = 'JSONEachRow',
    kafka_num_consumers  = 1,
    kafka_skip_broken_messages = 10;

-- Final storage table
CREATE TABLE IF NOT EXISTS cdc_db.orders_final
(
    id          Int64,
    customer_id Int32,
    amount      Float64,
    status      String,
    created_at  DateTime64(6, 'UTC'),
    updated_at  DateTime64(6, 'UTC'),
    cdc_op      String,
    cdc_ts      DateTime64(3, 'UTC'),
    _is_deleted UInt8 DEFAULT 0
)
ENGINE = ReplacingMergeTree(cdc_ts)
ORDER BY id;

-- Materialized View — pipes Kafka → final table
CREATE MATERIALIZED VIEW IF NOT EXISTS cdc_db.mv_orders
TO cdc_db.orders_final
AS SELECT
    id,
    customer_id,
    amount,
    status,
    parseDateTime64BestEffort(created_at, 6, 'UTC') AS created_at,
    parseDateTime64BestEffort(updated_at, 6, 'UTC') AS updated_at,
    __op          AS cdc_op,
    toDateTime64(__source_ts_ms / 1000, 3, 'UTC') AS cdc_ts,
    if(__op = 'd', 1, 0) AS _is_deleted
FROM cdc_db.kafka_orders_raw;

-- Query deduplicated current state
-- SELECT * FROM cdc_db.orders_final FINAL WHERE _is_deleted = 0;
