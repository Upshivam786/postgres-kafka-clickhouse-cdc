-- Enable logical replication (run as superuser)
ALTER SYSTEM SET wal_level = logical;
ALTER SYSTEM SET max_replication_slots = 10;
ALTER SYSTEM SET max_wal_senders = 10;

-- Create Debezium replication user
CREATE USER debezium WITH REPLICATION LOGIN PASSWORD 'your_strong_password';
GRANT CONNECT ON DATABASE your_db TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium;

-- Sample table for testing
CREATE TABLE IF NOT EXISTS public.orders (
    id          BIGSERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    amount      NUMERIC(10, 2) NOT NULL,
    status      VARCHAR(50) DEFAULT 'pending',
    created_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

ALTER TABLE public.orders REPLICA IDENTITY FULL;

-- Heartbeat table (prevents WAL bloat)
CREATE TABLE IF NOT EXISTS public.debezium_heartbeat (
    id           INTEGER PRIMARY KEY,
    heartbeat_ts TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
GRANT SELECT, INSERT, UPDATE ON public.debezium_heartbeat TO debezium;

-- Publication
CREATE PUBLICATION dbz_publication FOR ALL TABLES;
