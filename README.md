# pinot-realtime-analytics

<!-- results:start -->
Run on 2026-10-28 15:48 UTC at commit `unknown`.
Host: Apple M3, 8 cores, 8 GB RAM, macOS 26.6.2. Docker VM memory: 3.9 GiB. Images: apache/kafka-native:4.2.2, apachepinot/pinot:1.5.0-21-ms-openjdk.
Client JVM: java version "22.0.1" 2024-04-16.

### Ingestion

| Events | Kafka partitions | Producer target rate | Producer achieved rate | Rows queryable in all 3 tables after last send |
|---:|---:|---:|---:|---:|
| 1,000,000 | 4 | 50,000/s | 49,936/s | 0.4 s |

### Query latency by index

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

### Freshness

10 probe events, one at a time. Kafka ack p50 4.9 ms. Broker polled every 5 ms; timeouts: 0.

| Table | Produce-to-queryable p50 ms | p95 ms | max ms |
|---|---:|---:|---:|
| deliveries_noidx | 16.2 | 587.5 | 587.5 |
| deliveries_inverted | 22.0 | 598.7 | 598.7 |
| deliveries_startree | 28.4 | 607.5 | 607.5 |
<!-- results:end -->
