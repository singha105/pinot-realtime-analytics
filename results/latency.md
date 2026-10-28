Each row: 20 QPS for 30 s after a warm-up, open loop, latency measured client-side from the scheduled send time. Errors include requests dropped at the in-flight cap.

**q1_rollup_region_status**: Dashboard rollup: delivery volume, bytes and worst latency per region and status class, over every row. Same answer on every table: yes.

```sql
SELECT region, status_class, COUNT(*) AS deliveries, SUM(payload_bytes) AS bytes, MAX(latency_ms) AS worst_ms FROM <table> GROUP BY region, status_class ORDER BY region, status_class LIMIT 100
```

| Table | p50 ms | p95 ms | p99 ms | Broker time p50 ms | Docs scanned | Filter entries scanned | Errors |
|---|---:|---:|---:|---:|---:|---:|---:|
| deliveries_noidx | 16.74 | 26.18 | 40.45 | 15 | 5,000,000 | 0 | 0 |
| deliveries_inverted | 16.17 | 25.86 | 46.92 | 15 | 5,000,000 | 0 | 0 |
| deliveries_startree | 7.16 | 12.71 | 17.09 | 5 | 1,968 | 0 | 0 |

**q2_failing_tenants**: On-call view: the ten tenants with the most 5xx failures on invoice.paid webhooks. Same answer on every table: yes.

```sql
SELECT tenant_id, COUNT(*) AS failures, AVG(latency_ms) AS avg_ms FROM <table> WHERE event_type = 'invoice.paid' AND status_class = '5xx' GROUP BY tenant_id ORDER BY COUNT(*) DESC, tenant_id LIMIT 10
```

| Table | p50 ms | p95 ms | p99 ms | Broker time p50 ms | Docs scanned | Filter entries scanned | Errors |
|---|---:|---:|---:|---:|---:|---:|---:|
| deliveries_noidx | 12.36 | 17.40 | 49.72 | 11 | 66,227 | 5,433,911 | 0 |
| deliveries_inverted | 7.62 | 11.82 | 14.09 | 6 | 66,227 | 0 | 0 |
| deliveries_startree | 7.62 | 12.80 | 17.14 | 6 | 3,883 | 0 | 0 |

**q3_endpoint_lookup**: Support lookup: deliveries and worst latency for one receiver endpoint, which is not a star-tree dimension. Same answer on every table: yes.

```sql
SELECT COUNT(*) AS deliveries, MAX(latency_ms) AS worst_ms FROM <table> WHERE endpoint_id = 'ep-00042'
```

| Table | p50 ms | p95 ms | p99 ms | Broker time p50 ms | Docs scanned | Filter entries scanned | Errors |
|---|---:|---:|---:|---:|---:|---:|---:|
| deliveries_noidx | 6.54 | 11.60 | 41.05 | 5 | 19,920 | 1,410,263 | 0 |
| deliveries_inverted | 6.40 | 11.41 | 18.10 | 4 | 19,920 | 0 | 0 |
| deliveries_startree | 6.73 | 10.26 | 12.79 | 5 | 19,920 | 1,410,263 | 0 |

