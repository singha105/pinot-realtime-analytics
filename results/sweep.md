Each row: 50, 100, 200 QPS for 20 s after a warm-up, open loop, latency measured client-side from the scheduled send time. Errors include requests dropped at the in-flight cap.

**q1_rollup_region_status**: Dashboard rollup: delivery volume, bytes and worst latency per region and status class, over every row. Same answer on every table: yes.

```sql
SELECT region, status_class, COUNT(*) AS deliveries, SUM(payload_bytes) AS bytes, MAX(latency_ms) AS worst_ms FROM <table> GROUP BY region, status_class ORDER BY region, status_class LIMIT 100
```

| QPS | Table | p50 ms | p95 ms | p99 ms | Broker time p50 ms | Docs scanned | Filter entries scanned | Errors |
|---:|---|---:|---:|---:|---:|---:|---:|---:|
| 50 | deliveries_noidx | 14.81 | 33.94 | 125.88 | 13 | 5,000,000 | 0 | 0 |
| 50 | deliveries_startree | 3.68 | 7.10 | 10.76 | 3 | 1,968 | 0 | 0 |
| 100 | deliveries_noidx | 125.26 | 441.22 | 493.29 | 117 | 5,000,000 | 0 | 0 |
| 100 | deliveries_startree | 2.85 | 5.99 | 27.03 | 2 | 1,968 | 0 | 0 |
| 200 | deliveries_noidx | 626.41 | 752.27 | 861.81 | 621 | 5,000,000 | 0 | 1964 |
| 200 | deliveries_startree | 2.33 | 5.22 | 13.89 | 2 | 1,968 | 0 | 0 |

