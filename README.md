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
runs [`scripts/bench.sh`](scripts/bench.sh). On the machine below it takes about 10 minutes. The
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
<!-- results:end -->

## Reading the results

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
- Segments flush at 250,000 rows per partition, which is small. It gives 5M rows enough sealed
  segments to compare indexes. Production segments are usually hundreds of MB.
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
