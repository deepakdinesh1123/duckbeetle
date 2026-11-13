-- name: get_user_profit_summary
-- db: pg_orders
SELECT 
    user_id, 
    entry_date, 
    SUM(amount) AS total_amount, 
    COUNT(*) AS total_transactions 
FROM entries 
WHERE user_id = ? 
GROUP BY user_id, entry_date 
ORDER BY entry_date DESC;

-- name: get_user_completed_credits
-- db: pg_orders
SELECT * 
FROM entries 
WHERE user_id = ? AND status = 'COMPLETED' AND transaction_type = 'CREDIT';

-- name: get_date_range_report
-- db: pg_orders
SELECT 
    entry_date, 
    transaction_type, 
    COUNT(*) AS tx_count, 
    SUM(amount) AS total_volume, 
    AVG(amount) AS avg_ticket_size 
FROM entries 
WHERE entry_date >= ? AND entry_date <= ? 
GROUP BY entry_date, transaction_type 
ORDER BY entry_date DESC;

-- name: pg_full_yearly_rollup
-- db: pg_orders
SELECT 
    transaction_type,
    status,
    COUNT(*) AS total_transactions,
    ROUND(SUM(amount)::numeric, 2) AS total_amount,
    ROUND(AVG(amount)::numeric, 2) AS avg_amount,
    ROUND(MIN(amount)::numeric, 2) AS min_amount,
    ROUND(MAX(amount)::numeric, 2) AS max_amount
FROM entries
GROUP BY transaction_type, status
ORDER BY total_amount DESC;

-- name: pg_high_value_transactions
-- db: pg_orders
SELECT 
    id, 
    user_id, 
    amount, 
    entry_date, 
    transaction_type, 
    status
FROM entries
WHERE amount >= ?
ORDER BY amount DESC
LIMIT 5000;

-- name: pg_daily_status_breakdown
-- db: pg_orders
SELECT 
    entry_date, 
    status, 
    COUNT(*) AS tx_count, 
    SUM(amount) AS volume 
FROM entries 
WHERE entry_date >= ?
GROUP BY entry_date, status 
ORDER BY entry_date DESC, status;

-- name: get_mysql_user_transactions
-- db: mysql_orders
SELECT 
    user_id, 
    transaction_type, 
    SUM(amount) AS total_amount, 
    COUNT(*) AS tx_count 
FROM entries 
WHERE user_id = ? 
GROUP BY user_id, transaction_type;

-- name: mysql_full_status_rollup
-- db: mysql_orders
SELECT 
    transaction_type,
    status,
    COUNT(*) AS total_transactions,
    ROUND(SUM(amount), 2) AS total_amount,
    ROUND(AVG(amount), 2) AS avg_amount
FROM entries
GROUP BY transaction_type, status
ORDER BY total_amount DESC;

-- name: mysql_high_value_filter
-- db: mysql_orders
SELECT 
    id,
    user_id,
    amount,
    entry_date,
    transaction_type,
    status
FROM entries
WHERE amount >= ?
ORDER BY amount DESC
LIMIT 5000;
