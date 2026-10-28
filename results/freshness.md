20 probe events sent one at a time, after one uncounted warm-up probe. Kafka ack p50 4.6 ms. All tables queried in parallel every 5 ms until the event shows up; timeouts: 0.

| Table | Produce-to-queryable p50 ms | p95 ms | max ms |
|---|---:|---:|---:|
| deliveries_noidx | 16.1 | 37.6 | 46.9 |
| deliveries_inverted | 16.5 | 25.1 | 54.3 |
| deliveries_startree | 16.2 | 25.1 | 37.6 |
