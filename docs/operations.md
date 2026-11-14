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
2. `GracefulStop` and the Kafka consumer (finish and commit the record it is handling, handle no further record, leave the group) run in parallel and get 5 s together; what is still running after that is cut off (`Stop`; the consumer's last record is then redelivered and recognised as a duplicate);
3. the pool is closed, waiting at most 1 s for connections that are still checked out;
4. telemetry and profiles are flushed and the metrics server is shut down, capped at 3 s, exit 0.

The budgets add up to 9 s inside the 10 s of CODE-04 (`cmd/server/shutdown.go`); a watchdog started at the signal ends the process with exit 1 after 10 s if anything else hangs. Unit-tested with a fake clock (a gRPC call, a consumer and a pool that never finish are each abandoned at their budget, the watchdog fires at 10 s). Earlier, by the author: `docker stop` exit code 0 (image smoke run), plain-binary SIGTERM about 0.1 s with the consumer. Not re-measured with a blackholed OTLP endpoint or a stuck consumer.

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
| Topic missing | the consumer creates `[KAFKA_TOPIC_PREFIX]orders.placed` with the broker's default partitions and replication on its first poll (after the database is ready); if the broker refuses (ACL) it logs a warning and assumes the topic exists |
| Database slow or hanging | every call is cut off after `REQUEST_TIMEOUT` (5 s) and answered `DEADLINE_EXCEEDED` |
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
