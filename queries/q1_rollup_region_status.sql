-- Dashboard rollup: delivery volume, bytes and worst latency per region and status class, over every row.
SELECT region, status_class, COUNT(*) AS deliveries, SUM(payload_bytes) AS bytes, MAX(latency_ms) AS worst_ms
FROM {table}
GROUP BY region, status_class
ORDER BY region, status_class
LIMIT 100
