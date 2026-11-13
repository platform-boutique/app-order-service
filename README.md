# order-service

## Purpose and place in the shop

A gRPC service that stores the orders of the Everly shop and reports their tracking status. Checkout writes an order as `PENDING` before it charges the card, then marks it `PLACED` (or `FAILED`). After that the status shown to clients advances with the age of the order (`PLACED` -> `PROCESSING` -> `SHIPPED` -> `DELIVERED`): it is **computed on every read**, nothing is stored for it and no timer runs. Orders belong to a session id and, once the buyer is signed in, also to a user (`ClaimOrders` hands the orders of an anonymous session to a user after login). The service owns its own Postgres database and creates the schema with embedded migrations at start-up. With `KAFKA_BROKERS` set it also consumes the topic `orders.placed` and confirms a still-`PENDING` order from the event (self-healing when checkout's `ConfirmOrder` call was lost).

Context diagram and the main flow as sequence diagram: [`docs/architecture.md`](docs/architecture.md).

| | |
|---|---|
| Language / runtime | Go 1.26 (`go.mod`: `go 1.26.0`, `toolchain go1.26.10`), static binary, `CGO_ENABLED=0` |
| Image | built locally only: `platform-demo/order-service:local` (`Dockerfile`, user `10001:10001`) |
| Ports | gRPC `8085` (plaintext), HTTP `9090` (`/metrics`, pprof only with profiling) |
| Health | `grpc.health.v1.Health/Check` on `8085`, `NOT_SERVING` until the migrations are done and whenever the database does not answer a ping (every 5 s) |
| State | Postgres (`orders`, `order_items`) via `DATABASE_URL` |
| Depends on | its own Postgres (required); Kafka broker (optional, `KAFKA_BROKERS`); OTLP collector and Pyroscope (optional) |
| Called by | checkout-service (`CreateOrder`, `ConfirmOrder`, `FailOrder`), frontend (`ListOrders`, `GetOrder`, `ClaimOrders`); see `local/compose.yaml` in the workspace |
| Consumes | topic `orders.placed` (published by checkout-service) |
| gRPC reflection | enabled |

## Status

No CI pipeline, GitHub repository, Helm chart or Backstage registration exists yet; the service is built and run locally only. Status badges are added when the repo has a pipeline.

## Quick start

In the platform-demo workspace (this repo is built from `apps/order-service` by `local/compose.yaml`; the compose commands were not run for this README):

```bash
local/up.sh                      # builds all images and starts the shop, http://localhost:8080
SKIP_BUILD=1 local/up.sh         # reuse images
local/rebuild.sh order-service   # rebuild and recreate only this service
```

Run the tests (these were run on 2026-11-13, see Develop):

```bash
GOTOOLCHAIN=auto go vet ./... && GOTOOLCHAIN=auto go test -race -count=1 ./...
```

## API

Package `hipstershop.orders`, service `OrderService` ([`proto/orders.proto`](proto/orders.proto)). Full request/response rules, limits, error mapping and `grpcurl` examples: [`docs/api.md`](docs/api.md).

| Method | Behaviour |
|---|---|
| `CreateOrder` | Stores the order as `PENDING`. Idempotent by `order_id` (same content returns the stored order, other content under the same id is `ALREADY_EXISTS`). `total` must equal `sum(quantity x unit_price) + shipping_cost`, one currency, otherwise `INVALID_ARGUMENT` |
| `ConfirmOrder` | `PENDING` -> `PLACED` with tracking and transaction id. Idempotent with the same ids |
| `FailOrder` | `PENDING` -> `FAILED` with a reason. A placed order is `FAILED_PRECONDITION` |
| `GetOrder` | Returns the order; an optional `session_id` / `user_id` guard must match, otherwise `NOT_FOUND` (same as unknown id) |
| `ListOrders` | Exactly one of `session_id` / `user_id`, paged, newest first, optional `placed_only` |
| `ClaimOrders` | Gives the unowned orders of a `session_id` to a `user_id`; returns how many changed. Idempotent |

Status schedule for an order confirmed at `T` (offsets from the configuration table):

| Condition | Shown status |
|---|---|
| stored `PENDING` / `FAILED` | `PENDING` / `FAILED`, never advances |
| `now < T + ORDER_PROCESSING_AFTER` | `PLACED` |
| `T + ORDER_PROCESSING_AFTER <= now < T + ORDER_SHIPPED_AFTER` | `PROCESSING` |
| `T + ORDER_SHIPPED_AFTER <= now < T + ORDER_DELIVERED_AFTER` | `SHIPPED` |
| `T + ORDER_DELIVERED_AFTER <= now` | `DELIVERED` |

The schedule is the pure function `Stages.Schedule(order, now)`; the service takes the clock as a parameter.

Events: with `KAFKA_BROKERS` set the service consumes `orders.placed` (key = order id, JSON, schema version 1; the contract is owned by checkout). Behaviour per case and the metrics: [`docs/architecture.md`](docs/architecture.md#kafka-consumer).

## Configuration

Every variable read by the code (`grep -rn 'Getenv\|getenv'` over `cmd/` and `internal/`, checked 2026-11-13). All configuration is by environment; there is no config file.

| Variable | Default | Required | Meaning |
|---|---|---|---|
| `DATABASE_URL` | none | **yes** | Postgres URL, e.g. `postgres://order:order@order-db:5432/orders?sslmode=disable`. The process logs an error and exits 1 if it is unset. A 5 s connect timeout is added unless the URL sets one |
| `PORT` | `8085` | no | gRPC listen port. The `healthcheck` subcommand reads it too |
| `METRICS_PORT` | `9090` | no | HTTP port for `/metrics` (and pprof when profiling is on) |
| `ORDER_PROCESSING_AFTER` | `1m` | no | Offset after confirmation at which `PLACED` becomes `PROCESSING` (Go duration, e.g. `90s`) |
| `ORDER_SHIPPED_AFTER` | `3m` | no | Offset at which it becomes `SHIPPED` |
| `ORDER_DELIVERED_AFTER` | `6m` | no | Offset at which it becomes `DELIVERED`. Must satisfy `0 < processing < shipped < delivered`, otherwise the process exits at start-up |
| `KAFKA_BROKERS` | empty (consumer off) | no | Comma-separated bootstrap servers, e.g. `kafka:9092`. Empty: no consumer, no Kafka metrics registered |
| `KAFKA_CONSUMER_GROUP` | `order-service` | no | Consumer group of the `orders.placed` consumer. The topic name is a constant (no variable) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | unset (telemetry off) | no | OTLP/**gRPC** endpoint URL with scheme for traces, metrics and logs |
| `OTEL_SDK_DISABLED` | unset | no | `true` forces telemetry off even with an endpoint |
| `OTEL_SERVICE_NAME` | `order-service` | no | `service.name` resource attribute |
| `OTEL_METRIC_EXPORT_INTERVAL` | `15000` | no | Metric push interval: a number of milliseconds, or a Go duration (`15s`). Invalid values fall back to 15 s |
| `PYROSCOPE_ADDR` | unset (profiling off) | no | Pyroscope server URL. Profiling (push and pprof endpoints) is on only if this is set |
| `ENABLE_PROFILING` | unset | no | `0` forces profiling off even with `PYROSCOPE_ADDR` |
| `ENABLE_TRACING` | unset | no | Legacy: `0` forces telemetry off |
| `COLLECTOR_SERVICE_ADDR` | unset | no | **Legacy, to be removed** (owner decision 2026-11-13): `host:port` of a collector; turns telemetry on without `OTEL_EXPORTER_OTLP_ENDPOINT` |
| `HOSTNAME` | OS hostname | no | Used as `service.instance.id` (the pod name in Kubernetes) |
| `OTEL_RESOURCE_ATTRIBUTES` | unset | no | Read by the OTel SDK resource detector (`resource.WithFromEnv`), not by this code directly |

The OTLP exporters of the OpenTelemetry Go SDK also read their own standard `OTEL_EXPORTER_OTLP_*` variables (headers, TLS, timeout); they are not read by this service's code and were not tested. `OTEL_TRACES_SAMPLER` is **not** honoured: the sampler is fixed to always-on (`otlp.go`). `LOG_LEVEL` does not exist: the log level is logrus' default (info).

Start-up: the gRPC server listens at once (health `NOT_SERVING`); a background loop runs the goose migrations (under goose's Postgres advisory lock, so replicas starting together apply each migration once), retrying every 2 s until the database is reachable. There is no seed data.

## Observability

What the service emits today (code read 2026-11-13; the compose observability profile was not run for this README):

| Signal | Today | Not there yet (standards section 6) |
|---|---|---|
| Traces | OTLP/gRPC, only when `OTEL_EXPORTER_OTLP_ENDPOINT` is set. A server span per inbound gRPC call (`otelgrpc`), W3C `traceparent` + `baggage` propagation, sampler always-on | spans for Postgres calls and for the Kafka consumer, trace context from Kafka headers |
| Metrics | Prometheus on `:9090/metrics`: `grpc_server_*` (go-grpc-prometheus with handling-time histogram), Go runtime and process metrics, `order_service_kafka_*` (below). The same registry is pushed over OTLP when the endpoint is set | `build_info`, DB pool stats, business counters |
| Logs | JSON on stdout, one object per line: `timestamp` (RFC3339Nano), `severity`, `message` plus fields (`order_id`, `topic`, `partition`, `offset` for the consumer). Mirrored over OTLP when the endpoint is set. Card data does not exist here; the Kafka payload (email, address) is never logged | `trace_id` / `span_id` on the stdout line, `service`, `version`, `LOG_LEVEL` |
| Profiles | Pyroscope push (CPU, alloc objects/space, inuse objects/space) and pprof on the metrics port, only with `PYROSCOPE_ADDR` | goroutine / mutex / block profiles, tags `version`, `environment` |

Resource attributes today: `service.name`, `service.instance.id` and the SDK's telemetry attributes (plus whatever `OTEL_RESOURCE_ATTRIBUTES` adds). `service.version`, `service.namespace` and `deployment.environment` are not set.

Kafka consumer metrics:

| Metric | Meaning |
|---|---|
| `order_service_kafka_events_processed_total{result}` | `confirmed` or `already_placed` |
| `order_service_kafka_events_skipped_total{reason}` | `malformed`, `unsupported_schema`, `unknown_order`, `failed_order`, `conflict`, `invalid` |
| `order_service_kafka_events_failed_total` | handling attempts that failed with a transient error |
| `order_service_kafka_events_retried_total` | retries started after a backoff |
| `order_service_kafka_offset_commits_total{result}` | `ok` / `error` |
| `order_service_kafka_consumer_lag{partition}` | high watermark minus the next offset, as of the last handled record of the partition |

Example queries (not run against a live backend): `sum by (grpc_method, grpc_code) (rate(grpc_server_handled_total{grpc_service="hipstershop.orders.OrderService"}[5m]))`; Loki `{service_name="order-service"} | json | severity="error"`.

## Data

Postgres, database owned by this service. `orders` (one row per order, stored state `PENDING | PLACED | FAILED`, money as `units` + `nanos`, `fingerprint` = hash of the request for the idempotency check) and `order_items` (one row per line, ordered by `position`). Check constraints repeat the validation limits; `PLACED` requires a confirmation time and both references. Two indexes serve the two `ListOrders` shapes (by session, by user).

Concurrency: `CreateOrder` is `INSERT ... ON CONFLICT DO NOTHING` in one transaction; `ConfirmOrder` / `FailOrder` run under `SELECT ... FOR UPDATE` (a racing confirm and fail: exactly one wins); `ClaimOrders` is one `UPDATE ... WHERE session_id = $1 AND user_id = ''` and returns its row count.

Migrations: [goose](https://github.com/pressly/goose) SQL files in `internal/orders/migrations/`, embedded in the binary and applied by the service at start-up, forward-only (`00001_create_orders.sql`). No seed data. Demo database in compose: `postgres:17-alpine`, user/password `order` (demo values only, they live in `local/compose.yaml`).

## Develop

Requirements: Go 1.26 (`GOTOOLCHAIN=auto` fetches it), Docker for integration tests, [`grpcurl`](https://github.com/fullstorydev/grpcurl) for manual calls. Layout: `cmd/server/` (entry point, telemetry, health, Kafka start), `internal/orders/` (service, validation, store, migrations), `internal/events/` (Kafka consumer), `proto/`, `tests/integration/`.

| Step | Command | Result on 2026-11-13 |
|---|---|---|
| Vet + unit tests | `go vet ./... && go test -race -count=1 ./...` | run, passed |
| Format | `gofmt -l .` | run, no files listed |
| Lint | `golangci-lint run ./...` (config `.golangci.yaml`, v2 default linters) | run with golangci-lint v2.11.4, 0 issues; also with `--build-tags integration`, 0 issues. CI pins v2.14.0: not run |
| Integration (Postgres) | see below | not run today (earlier runs by the author are recorded in `docs/development.md`) |
| Integration (Kafka) | see below | not run today |
| Vulnerability / security scan | `govulncheck ./...`, `gosec -exclude-generated ./...` | not run today |
| Image | `docker build -t platform-demo/order-service:local .` | not run today |
| Docs | `mkdocs build --strict` (needs `mkdocs` + `mkdocs-techdocs-core`) | see `docs/development.md` |

Integration tests (build tag `integration`, each test creates and drops its own database; Kafka tests skip without `TEST_KAFKA_BROKERS`):

```bash
docker run -d --rm --name order-it-pg --memory 256m -e POSTGRES_USER=order -e POSTGRES_PASSWORD=order \
  -p 127.0.0.1:55433:5432 postgres:17-alpine
TEST_DATABASE_URL='postgres://order:order@127.0.0.1:55433/postgres?sslmode=disable' \
  go test -race -tags integration ./...
docker rm -f order-it-pg
```

The Kafka variant (a throwaway single-node `apache/kafka:4.3.2`) is in [`docs/development.md`](docs/development.md).

Regenerate the Go code after editing the proto: `protoc -I proto --go_out=proto --go_opt=paths=source_relative --go-grpc_out=proto --go-grpc_opt=paths=source_relative proto/orders.proto` (protoc 3.21.12, protoc-gen-go 1.36.11, protoc-gen-go-grpc 1.6.1; not run today). Proto changes are additive only, server first.

## Operate

- **Health:** `grpc.health.v1` on the service port, empty service name and `hipstershop.orders.OrderService`. `NOT_SERVING` until the migrations are done, and again whenever the Postgres ping fails (checked every 5 s); handlers answer `UNAVAILABLE` meanwhile. Kafka is never part of the health. The image `HEALTHCHECK` runs `/app/order-service healthcheck` (exit 0 only on `SERVING`).
- **Shutdown:** on SIGTERM/SIGINT health goes `NOT_SERVING`, `GracefulStop` with a 10 s cap (then `Stop`), the metrics server stops (5 s), the Kafka consumer finishes and commits its current record (at most 8 s), telemetry is flushed (5 s), the pool is closed, exit 0.
- **Scaling:** stateless except for Postgres, so several replicas are possible: migrations are serialised by the advisory lock, Kafka consumers share the group `order-service`. Computed status uses the clock of the replica that answers (see limits). Multi-replica operation was not run.
- **Image:** Alpine runtime, user `10001:10001`, `EXPOSE 8085 9090`, about 12 MB when last built.
- Runbook entries and failure behaviour: [`docs/operations.md`](docs/operations.md).

## Known limits

- No authentication or rate limiting: anyone who can reach the port can read and create orders, and claim the unowned orders of any session id they know; `session_id` is a guard against mixing orders up, not a credential. Product ids are not checked against the catalog.
- The computed status uses the clock of the replica that serves the call; replicas with skewed clocks can show a stage a moment early or late.
- `zip_code` is an `int32` to stay compatible with `hipstershop.Address`: postal codes with letters or leading zeros do not fit.
- Offset paging (page limit 1,000,000); `ListOrders` reads the total and the page in separate statements.
- No spans for database calls; no `trace_id` in logs; no version/commit exposed (see Observability).
- Kafka: PLAINTEXT without authentication only; a permanently failing record is retried forever by design (later records wait behind it).
- Not run: the image with the Kafka consumer built into it, arm64 build, more than one replica, a multi-broker cluster. The legacy `COLLECTOR_SERVICE_ADDR` is still read (to be removed).

## License

To be decided by the owner. No `LICENSE` file exists in this repository yet.
