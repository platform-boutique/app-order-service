# Operations

## Health

- `grpc.health.v1.Health/Check` on the gRPC port (`8085`), for the empty service name and for `hipstershop.orders.OrderService`.
- `NOT_SERVING` from start until the migrations are done, and again whenever the Postgres ping fails (checked every 5 s). While not ready the handlers answer `UNAVAILABLE`.
- Kafka is never part of the health: a broker that is down or slow changes nothing about `SERVING`.
- Container probe: `/app/order-service healthcheck` calls the local health service (`127.0.0.1:$PORT`) and exits 0 only on `SERVING` (Docker `HEALTHCHECK`, interval 5 s, start period 10 s).

```bash
grpcurl -plaintext localhost:8085 grpc.health.v1.Health/Check
```

## Shutdown

SIGTERM or SIGINT:

1. health goes `NOT_SERVING` so probes fail first;
2. `GracefulStop` with a 10 s cap, then `Stop`;
3. the Kafka consumer finishes and commits the record it is handling, handles no further record, leaves the group (at most 8 s);
4. the pool is closed;
5. telemetry and profiles are flushed and the metrics server is shut down, together capped at 4 s (`obs.DefaultShutdownTimeout`), exit 0.

Measured earlier by the author: `docker stop` exit code 0 (image smoke run), plain-binary SIGTERM about 0.1 s with the consumer. Not re-measured with a blackholed OTLP endpoint.

## Scaling and replicas

- Postgres is the only state. Several replicas are possible in principle: migrations take goose's advisory lock, `CreateOrder` / `ConfirmOrder` / `ClaimOrders` are safe under concurrency (tests cover racing calls), Kafka consumers share the group `order-service`.
- The computed status uses the clock of the replica that answers: skewed clocks give slightly different stages.
- Running more than one replica was not tested.
- Resource use in the local stack: `mem_limit: 128m` for the service (workspace `local/compose.yaml`).

## Failure behaviour

| Failure | Behaviour |
|---|---|
| Postgres down at start | server listens, health `NOT_SERVING`, migration retried every 2 s |
| Postgres goes away while running | health `NOT_SERVING` within about 5 s, handlers answer `UNAVAILABLE`, `SERVING` again after it returns |
| Kafka down | consumer logs a warning every few seconds; gRPC and health unaffected |
| Poison record with a permanent database error | retried forever by design; later records wait; the warn log shows the attempt count |
| Handler panic | turned into `INTERNAL` by a recovery interceptor; the process keeps running |
| Collector unreachable | telemetry export fails in the background; start-up does not depend on it |

## Runbook

| Symptom | Check |
|---|---|
| Health stays `NOT_SERVING` | logs for `database not ready (attempt N)`: wrong `DATABASE_URL`, network, credentials |
| Order stays `PENDING` | did checkout reach `ConfirmOrder`? with Kafka on, look for `order_service_kafka_events_skipped_total{reason}` and the consumer lag gauge |
| Status looks wrong | the offsets (`ORDER_*_AFTER`) printed at start-up in the log line `status stages after confirmation` |
| Process exits at start | `DATABASE_URL is not set`, or an invalid `ORDER_*_AFTER` combination (must satisfy `0 < processing < shipped < delivered`) |

There is no dashboard or alert for this service yet.
