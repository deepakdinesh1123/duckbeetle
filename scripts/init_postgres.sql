-- Initialize databases and rich production-like seed data for DuckBeetle

CREATE DATABASE ducklake_catalog;
GRANT ALL PRIVILEGES ON DATABASE ducklake_catalog TO "testUser";

\c testDB;

CREATE TABLE IF NOT EXISTS entries (
    id BIGINT PRIMARY KEY,
    amount REAL,
    user_id VARCHAR(32),
    entry_date DATE,
    timestamp TIMESTAMP,
    transaction_type VARCHAR(16),
    status VARCHAR(16)
);

-- Fast bulk generation using generate_series (250,000 rows across 500 users over a 1-year timeline)
INSERT INTO entries (id, amount, user_id, entry_date, timestamp, transaction_type, status)
SELECT 
    i AS id,
    ROUND((RANDOM() * 10000 + 10)::numeric, 2)::real AS amount,
    'user_' || (1 + (i % 500)) AS user_id,
    (CURRENT_DATE - ((i % 365) || ' days')::interval)::date AS entry_date,
    NOW() - ((i % 365 * 86400 + (i % 86400)) || ' seconds')::interval AS timestamp,
    CASE WHEN i % 3 = 0 THEN 'CREDIT' ELSE 'DEBIT' END AS transaction_type,
    CASE WHEN i % 10 = 0 THEN 'PENDING' ELSE 'COMPLETED' END AS status
FROM generate_series(1, 250000) AS s(i)
ON CONFLICT (id) DO NOTHING;

CREATE INDEX IF NOT EXISTS idx_entries_user_id ON entries(user_id);
CREATE INDEX IF NOT EXISTS idx_entries_entry_date ON entries(entry_date);
CREATE INDEX IF NOT EXISTS idx_entries_status ON entries(status);
