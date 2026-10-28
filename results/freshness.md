10 probe events, one at a time. Kafka ack p50 4.9 ms. Broker polled every 5 ms; timeouts: 0.

| Table | Produce-to-queryable p50 ms | p95 ms | max ms |
|---|---:|---:|---:|
| deliveries_noidx | 16.2 | 587.5 | 587.5 |
| deliveries_inverted | 22.0 | 598.7 | 598.7 |
| deliveries_startree | 28.4 | 607.5 | 607.5 |
