Each cell: 20 QPS for 10 s, open loop, latency measured client-side from the scheduled send time.

**q1_rollup_region_status**: Dashboard rollup: delivery volume, bytes and worst latency per region and status class, over every row. Same answer on every table: yes.

```sql
SELECT region, status_class, COUNT(*) AS deliveries, SUM(payload_bytes) AS bytes, MAX(latency_ms) AS worst_ms FROM <table> GROUP BY region, status_class ORDER BY region, status_class LIMIT 100
```

| Table | p50 ms | p95 ms | p99 ms | Broker time p50 ms | Docs scanned | Filter entries scanned | Errors |
|---|---:|---:|---:|---:|---:|---:|---:|
| deliveries_noidx | 17.15 | 21.99 | 44.40 | 8 | 1,000,000 | 0 | 0 |
| deliveries_inverted | 18.15 | 24.58 | 28.43 | 8 | 1,000,000 | 0 | 0 |
| deliveries_startree | 13.37 | 20.33 | 23.83 | 5 | 65,444 | 0 | 0 |

**q2_failing_tenants**: On-call view: the ten tenants with the most 5xx failures on invoice.paid webhooks. Same answer on every table: yes.

```sql
SELECT tenant_id, COUNT(*) AS failures, AVG(latency_ms) AS avg_ms FROM <table> WHERE event_type = 'invoice.paid' AND status_class = '5xx' GROUP BY tenant_id ORDER BY COUNT(*) DESC, tenant_id LIMIT 10
```

| Table | p50 ms | p95 ms | p99 ms | Broker time p50 ms | Docs scanned | Filter entries scanned | Errors |
|---|---:|---:|---:|---:|---:|---:|---:|
| deliveries_noidx | 14.84 | 19.91 | 22.23 | 7 | 13,301 | 1,086,753 | 0 |
| deliveries_inverted | 12.88 | 20.27 | 25.17 | 4 | 13,301 | 0 | 0 |
| deliveries_startree | 13.94 | 21.03 | 45.85 | 5 | 830 | 31,084 | 0 |

**q3_endpoint_lookup**: Support lookup: deliveries and worst latency for one receiver endpoint, which is not a star-tree dimension. Same answer on every table: yes.

```sql
SELECT COUNT(*) AS deliveries, MAX(latency_ms) AS worst_ms FROM <table> WHERE endpoint_id = 'ep-00042'
```

| Table | p50 ms | p95 ms | p99 ms | Broker time p50 ms | Docs scanned | Filter entries scanned | Errors |
|---|---:|---:|---:|---:|---:|---:|---:|
| deliveries_noidx | 13.85 | 20.73 | 27.14 | 4 | 4,022 | 281,268 | 0 |
| deliveries_inverted | 11.94 | 18.32 | 19.77 | 4 | 4,022 | 0 | 0 |
| deliveries_startree | 12.65 | 18.52 | 21.44 | 4 | 4,022 | 281,268 | 0 |

