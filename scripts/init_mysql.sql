-- Initialize and seed rich production-like data for MySQL
USE mydb;

CREATE TABLE IF NOT EXISTS entries (
    id BIGINT PRIMARY KEY,
    amount REAL,
    user_id VARCHAR(32),
    entry_date DATE,
    timestamp TIMESTAMP,
    transaction_type VARCHAR(16),
    status VARCHAR(16)
);

-- Fast cross-join generator to insert 100,000 realistic rows
INSERT IGNORE INTO entries (id, amount, user_id, entry_date, timestamp, transaction_type, status)
SELECT 
    (t5.d * 10000 + t4.d * 1000 + t3.d * 100 + t2.d * 10 + t1.d + 1) AS id,
    ROUND(10 + (RAND() * 10000), 2) AS amount,
    CONCAT('user_', 1 + ((t5.d * 10000 + t4.d * 1000 + t3.d * 100 + t2.d * 10 + t1.d + 1) % 500)) AS user_id,
    DATE_SUB(CURRENT_DATE(), INTERVAL ((t5.d * 10000 + t4.d * 1000 + t3.d * 100 + t2.d * 10 + t1.d + 1) % 365) DAY) AS entry_date,
    DATE_SUB(NOW(), INTERVAL ((t5.d * 10000 + t4.d * 1000 + t3.d * 100 + t2.d * 10 + t1.d + 1) % 86400) SECOND) AS timestamp,
    IF((t1.d % 3) = 0, 'CREDIT', 'DEBIT') AS transaction_type,
    IF((t1.d % 10) = 0, 'PENDING', 'COMPLETED') AS status
FROM 
    (SELECT 0 d UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) t1,
    (SELECT 0 d UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) t2,
    (SELECT 0 d UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) t3,
    (SELECT 0 d UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) t4,
    (SELECT 0 d UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) t5;

CREATE INDEX idx_entries_user_id ON entries(user_id);
CREATE INDEX idx_entries_entry_date ON entries(entry_date);
CREATE INDEX idx_entries_status ON entries(status);
