-- Support lookup: deliveries and worst latency for one receiver endpoint, which is not a star-tree dimension.
SELECT COUNT(*) AS deliveries, MAX(latency_ms) AS worst_ms
FROM {table}
WHERE endpoint_id = 'ep-00042'
