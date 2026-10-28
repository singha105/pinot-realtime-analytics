# pinot-realtime-analytics

[![ci](https://github.com/singha105/pinot-realtime-analytics/actions/workflows/ci.yml/badge.svg)](https://github.com/singha105/pinot-realtime-analytics/actions/workflows/ci.yml)

Real-time analytics on webhook delivery events with Apache Kafka and Apache Pinot, run on one laptop.

A Java 17 producer streams synthetic webhook-delivery events into a 4-partition Kafka topic. Three
Pinot real-time tables consume that same topic. They hold identical rows and differ only in indexing:
no extra index, inverted indexes, or a star-tree index. A Java load generator sends the same SQL to
each table at a fixed rate and records p50, p95 and p99 latency. A probe measures how long a new
event takes to become queryable.

The question it answers: for a given query shape, what does each index buy, and where does a
table run out of headroom?

```
 DeliveryProducer (Java 17)                  LoadGenerator (Java 17, open loop, N QPS)
   keyed by tenant, lz4                      FreshnessProbe (produce -> first query hit)
          |                                              |
          v                                              v
 Kafka topic webhook-deliveries         Pinot broker :18000  (POST /query/sql)
   4 partitions, KRaft, 1 broker                         |
          |                     +------------------------+------------------------+
          |                     |                        |                        |
          +-------------> deliveries_noidx      deliveries_inverted      deliveries_startree
             each table     forward index only   + inverted indexes on    + star-tree on region,
             consumes all                        5 dimension columns      status, event type, tenant
             4 partitions
```

## Run it

You need Docker with about 4 GB of memory for the VM, JDK 17 or newer, and about 3 GB of disk for
the two images. On a Mac with OrbStack, run `orb start` first.

```bash
make demo
```

`make demo` builds the Java tools with the Maven wrapper, starts a fresh Kafka and Pinot stack, and
runs [`scripts/bench.sh`](scripts/bench.sh). On the machine below it takes about 12 minutes. The
script writes `results/` and replaces the results section of this README, so every number below comes
from that script. Override the defaults with environment variables, for example
`ROWS=1000000 QPS=50 make demo`.

| Command | What it does |
|---|---|
| `make demo` | build, reset the stack, run the whole benchmark |
| `make test` | unit tests only (no Docker needed) |
| `make up` / `make down` | start or remove the stack |
| `java -jar target/pinot-rt.jar` | list the subcommands: `topic`, `produce`, `await`, `commit`, `loadgen`, `freshness` |

Pinot's controller UI is at http://localhost:19000 while the stack is up. The host ports are
19000 and 18000 rather than Pinot's usual 9000 and 8000, because Homebrew's php-fpm and other dev
tools often hold `127.0.0.1:9000`. Java connects over IPv4, so it reached php-fpm instead of Pinot,
while curl went over IPv6 and worked.

## Results

<!-- results:start -->
Run on 2026-10-28 16:15 UTC at commit `08bf4a5`.
Host: Apple M3, 8 cores, 8 GB RAM, macOS 26.6.2. Docker VM memory: 3.9 GiB. Images: apache/kafka-native:4.2.2, apachepinot/pinot:1.5.0-21-ms-openjdk.
Client JVM: java version "22.0.1" 2024-04-16.

### Ingestion

| Events | Kafka partitions | Producer target rate | Producer achieved rate | Rows queryable in all 3 tables after last send |
|---:|---:|---:|---:|---:|
| 5,000,000 | 4 | 50,000/s | 49,982/s | 0.5 s |

### Query latency by index

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

### Capacity: rollup query as offered load rises

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

### Freshness

20 probe events sent one at a time, after one uncounted warm-up probe. Kafka ack p50 4.6 ms. All tables queried in parallel every 5 ms until the event shows up; timeouts: 0.

| Table | Produce-to-queryable p50 ms | p95 ms | max ms |
|---|---:|---:|---:|
| deliveries_noidx | 16.1 | 37.6 | 46.9 |
| deliveries_inverted | 16.5 | 25.1 | 54.3 |
| deliveries_startree | 16.2 | 25.1 | 37.6 |
<!-- results:end -->

## Reading the results

The numbers quoted here are from the committed run above.

**Only the star-tree helps the rollup.** Q1 groups every row by two low-cardinality columns with no
filter, so an inverted index has nothing to do: the inverted table scans all 5,000,000 rows, just
like the plain one. The star-tree answers from 1,968 pre-aggregated records and cut p99 from 40.45 ms
to 17.09 ms at 20 QPS.

**At low load, capacity matters more than latency.** At 20 QPS every query pays a few milliseconds of
fixed cost: HTTP, scatter-gather over the table's 86 segments, and waking idle threads. The gap
shows up when load rises. On the rollup query, the scan table's p99 reached 125.88 ms at 50 QPS and
493.29 ms at 100 QPS. At 200 QPS it dropped 1,964 of 4,000 requests at the 64-in-flight cap, and the
ones it served took 626 ms at the median. The star-tree table stayed under 28 ms p99 at every level
and dropped nothing. Its median even fell as load rose, probably because its worker threads never
went idle. I have not profiled that.

**Inverted indexes win on selective filters.** Q2 filters on event type and status class. The plain
table evaluated 5,433,911 filter entries per query and the inverted table evaluated none, because it
intersects bitmaps instead. That took p99 from 49.72 ms to 14.09 ms. The star-tree covers both
filter columns too, so it landed close to the inverted table.

**A star-tree only covers its own dimensions.** `endpoint_id` is not in the star-tree, so for Q3 the
star-tree table falls back to the same scan as the plain table (1,410,263 filter entries), while the
inverted table reads one bitmap. With only 19,920 matching rows, all three p50s land within half a
millisecond of each other at 20 QPS. The fixed per-query cost dominates the scan.

**Fresh data is queryable in milliseconds.** A new event showed up in all three tables 16 ms after
the send started at the median, including the 4.6 ms Kafka ack. The worst of the 60 readings was
54.3 ms. During the bulk load, every table had all 5 million rows within 0.5 s of the last send,
which is the resolution of that check. The producer held its 50,000 events/s target. This was not a
maximum-throughput test.

## How it works

**Events.** [`EventGenerator`](src/main/java/io/github/singha105/pinotrt/EventGenerator.java) is
seeded, so every run produces the same dimensions and metrics in the same order. 200 tenants follow
a Zipf distribution, so a few tenants send most of the traffic. Each tenant owns 25 endpoints and has
a home region. Every 50th endpoint is flaky and returns 5xx about 30% of the time. Latency is
log-normal around a per-region median, and timeouts are recorded as 10 s. Records are keyed by
tenant, so one tenant's deliveries stay in order in one partition. That also makes the partitions
uneven in size, as real tenant-keyed topics often are.

**Three tables, one topic.** Each Pinot table runs its own consumers over all four partitions, so the
tables hold the same rows and only the indexes differ. Before measuring anything, the load generator
runs every query against every table and fails the run if any answer differs. An index has to be
faster and give the same answer.

**Star-tree tuning.** The star-tree pre-aggregates `COUNT(*)`, `SUM(payload_bytes)`,
`MAX(latency_ms)` and `AVG(latency_ms)` over the split order region, status class, event type,
tenant. The first version used Pinot's default `maxLeafRecords` of 10,000. On segments this small
the tree never split below region, so the rollup query still scanned tens of thousands of
pre-aggregated records. Lowering it to 100 let queries use the star nodes for the dimensions they
don't group by. Pinot builds a star-tree when a segment is sealed, and by default it does not rebuild
one on reload (`enableDynamicStarTreeCreation` is off). The config change only took effect on
freshly built segments.

**Sealing segments.** A consuming (still-open) real-time segment has no star-tree. The benchmark
stops the producer, waits until all three tables have every row, then force-commits the consuming
segments through the controller. Every query is then served from sealed segments, and the
comparison measures the indexes rather than the mix of open and sealed segments.

**Measuring latency.** [`LoadGenerator`](src/main/java/io/github/singha105/pinotrt/LoadGenerator.java)
is open loop. Request *i* is due at `start + i / QPS` and its latency is measured from that due time,
so a slow broker shows up in the tail instead of quietly lowering the offered load (coordinated
omission). Requests beyond 64 in flight are dropped and counted as errors. One macOS detail
mattered: `LockSupport.parkNanos` for 50 ms overslept by about 6 ms at the median because the OS
coalesces timers. Since latency is measured from the due time, that oversleep was being reported as
query latency. [`RatePacer`](src/main/java/io/github/singha105/pinotrt/RatePacer.java) now parks for
half the remaining wait at a time and spins for the last millisecond.

**Freshness.** [`FreshnessProbe`](src/main/java/io/github/singha105/pinotrt/FreshnessProbe.java)
sends one event, waits for the Kafka ack, then queries all three tables in parallel every 5 ms until
each can see the event's `delivery_id`. The first probe warms up the connections and is not counted.

## Caveats

- One laptop: Kafka, Pinot, the producer and the load generator all share the same 8 CPU cores, and
  the Docker VM has about 3.9 GiB of memory. On separate machines the numbers would differ.
- The data is synthetic. The distributions are reasonable guesses, not measurements of real webhook
  traffic.
- Pinot runs as `QuickStart -type EMPTY`: ZooKeeper, controller, broker, server and minion in one JVM
  with a 2 GB max heap and one replica. It is a lean dev cluster, not a production topology.
- Segments are small. The table configs set `realtime.segment.flush.threshold.rows` to 250,000, and
  Pinot splits that across the four partitions the single server consumes. Each sealed segment
  holds 62,500 rows, so 5M rows become 86 segments per table. That is plenty to compare indexes,
  but it also adds per-segment overhead to every query. Production segments are usually hundreds
  of MB.
- Query latencies are measured at the client and include HTTP through OrbStack's port forwarding.
  Broker time (`timeUsedMs`) is reported next to them and has 1 ms resolution.
- Queries run after ingestion stops. Latency while the server is also consuming and building
  segments would be higher.
- Freshness readings include one query round trip and up to one 5 ms poll interval, so they overstate
  the true delay slightly.
- Numbers move from run to run. Run `make demo` yourself rather than trusting mine.

## Layout

```
src/main/java/.../pinotrt/   producer, load generator, freshness probe, Pinot HTTP client
src/test/java/.../pinotrt/   unit tests (JUnit 5), run in CI on Java 17 and 21
pinot/                       schema template and the three real-time table configs
queries/                     the benchmark SQL; {table} is replaced per table
scripts/                     bench.sh, create-tables.sh, wait-healthy.sh
results/                     raw CSV and markdown from the last committed run
```

## License

MIT, see [LICENSE](LICENSE).
