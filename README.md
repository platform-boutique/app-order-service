# order-service

## Purpose and place in the shop

A gRPC service that stores the orders of the Everly shop and reports their tracking status. Checkout writes an order as `PENDING` before it charges the card, then marks it `PLACED` (or `FAILED`). After that the status shown to clients advances with the age of the order (`PLACED` -> `PROCESSING` -> `SHIPPED` -> `DELIVERED`): it is **computed on every read**, nothing is stored for it and no timer runs. Orders belong to a session id and, once the buyer is signed in, also to a user (`ClaimOrders` hands the orders of an anonymous session to a user after login). The service owns its own Postgres database and creates the schema with embedded migrations at start-up. With `KAFKA_BROKERS` set it also consumes the topic `orders.placed` and confirms a still-`PENDING` order from the event (self-healing when checkout's `ConfirmOrder` call was lost).

Context diagram and the main flow as sequence diagram: [`docs/architecture.md`](docs/architecture.md).

| | |
|---|---|
| Language / runtime | Go 1.26 (`go.mod`: `go 1.26.11`), static binary, `CGO_ENABLED=0` |
| Image | built locally only: `platform-demo/order-service:local` (`Dockerfile`, user `10001:10001`) |
| Ports | gRPC `8085` (plaintext), HTTP `9090` (`/metrics`; pprof only on a separate `PPROF_ADDR`) |
| Health | `grpc.health.v1.Health/Check` on `8085`, `NOT_SERVING` until the migrations are done and whenever the database does not answer a ping (every 5 s) |
| State | Postgres (`orders`, `order_items`) via `DATABASE_URL` |
| Depends on | its own Postgres (required); Kafka broker (optional, `KAFKA_BROKERS`); OTLP collector and Pyroscope (optional) |
| Called by | checkout-service (`CreateOrder`, `ConfirmOrder`, `FailOrder`), frontend (`ListOrders`, `GetOrder`, `ClaimOrders`); see `local/compose.yaml` in the workspace |
| Consumes | topic `orders.placed` (published by checkout-service) |
| gRPC reflection | enabled |

## Status

The repository has the shared CI pipeline (`ci / gate`, platform-demo-workflows v3.8.0) and merges to `main` publish nothing (`start-publish: false`, no `publish.yaml`). No Helm chart exists and the service is not deployed in any environment yet; it runs in the local shop only, built from this repository by `local/compose.yaml`.

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
| `ORDER_PROCESSING_AFTER` | `1m` | no | Offset after confirmation at which `PLACED` becomes `PROCESSING` (Go duration, e.g. `90s`) |
| `ORDER_SHIPPED_AFTER` | `3m` | no | Offset at which it becomes `SHIPPED` |
| `ORDER_DELIVERED_AFTER` | `6m` | no | Offset at which it becomes `DELIVERED`. Must satisfy `0 < processing < shipped < delivered`, otherwise the process exits at start-up |
| `KAFKA_BROKERS` | empty (consumer off) | no | Comma-separated bootstrap servers, e.g. `kafka:9092`. Empty: no consumer, no Kafka metrics registered |
| `KAFKA_CONSUMER_GROUP` | `order-service` | no | Consumer group of the `orders.placed` consumer (never prefixed) |
| `KAFKA_TOPIC_PREFIX` | empty | no | Put in front of the topic name, `dev.` gives `dev.orders.placed`, so several environments can share one cluster. Letters, digits, `.`, `_`, `-` only; anything else stops the start with an error. Checkout must publish to the same prefixed name. The consumer creates the topic when it is missing (broker default partitions and replication) |
| `REQUEST_TIMEOUT` | `5s` | no | Server-side bound of every unary call (Go duration, at least `100ms`; `0` switches the bound off). A call without a deadline gets this one, a longer caller deadline is shortened to it; the context reaches every database query. A value that does not parse keeps `5s` with a warning |
| `HOSTNAME` | OS hostname | no | Used as `service.instance.id` (the pod name in Kubernetes) |
| `METRICS_PORT` | `9090` | no | Port of the Prometheus `/metrics` endpoint (scraped by Alloy). A value that is not a port falls back to `9090` with a warning |
| `PPROF_ADDR` | unset (no pprof) | no | `host:port` of a separate pprof listener, e.g. `127.0.0.1:6060`. Off unless set; never expose it. pprof is never served on the metrics port |
| `LOG_LEVEL` | `info` | no | `debug`, `info`, `warn` or `error`; anything else falls back to `info` with a warning |
| `SERVICE_VERSION` | build info, else `dev` | no | `service.version`, the `version` log field and the `build_info` label. Fallbacks in order: ldflags `-X <module>/internal/obs.Version=...` (and `.Commit=`, `.BuildTime=`), the VCS revision Go embeds in the binary (12 characters, `-dirty` if modified), `dev` |
| `DEPLOYMENT_ENVIRONMENT` | `local` | no | `deployment.environment` on every signal and the Pyroscope tag `environment` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | unset (telemetry off) | no | OTLP/**gRPC** endpoint for **traces, metrics and logs**, e.g. `http://alloy:4317`. Telemetry is on only if this is a valid absolute http(s) URL; empty or malformed means off (one warning, never a crash). The legacy `COLLECTOR_SERVICE_ADDR` and `ENABLE_TRACING` are no longer read |
| `OTEL_SDK_DISABLED` | unset | no | `true` (trimmed, case-insensitive) forces everything off: OTLP export and profiling, even if an endpoint or `PYROSCOPE_ADDR` is set |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | `grpc` | no | Documented knob; only gRPC is implemented, the value is not read |
| `OTEL_TRACES_SAMPLER`, `OTEL_TRACES_SAMPLER_ARG` | SDK default (`parentbased_always_on`) | no | Standard OpenTelemetry sampler variables, read by the SDK (tested for `always_on` and `always_off`) |
| `OTEL_RESOURCE_ATTRIBUTES` | unset | no | Extra resource attributes (`k=v,...`); applied after the defaults, so they can override them |
| `OTEL_SERVICE_NAME` | `order-service` | no | `service.name`; keep the default, dashboards depend on it |
| `OTEL_METRIC_EXPORT_INTERVAL` | `15000` | no | Milliseconds between metric pushes (also accepts durations such as `15s`) |
| `PYROSCOPE_ADDR` | unset (profiling off) | no | Pyroscope URL (SDK push protocol, not OTLP). The only switch: no default address, `ENABLE_PROFILING=1` alone starts nothing |
| `ENABLE_PROFILING` | unset | no | `0` forces profiling off even with `PYROSCOPE_ADDR`; any other value changes nothing |

The OTLP exporters of the OpenTelemetry Go SDK also read their own standard `OTEL_EXPORTER_OTLP_*` variables (headers, TLS, timeout); they are not read by this service's code and were not tested. The whole contract is in `internal/obs` (`env_test.go` is the table-driven test of the variables above).

Start-up: the gRPC server listens at once (health `NOT_SERVING`); a background loop runs the goose migrations (under goose's Postgres advisory lock, so replicas starting together apply each migration once), retrying every 2 s until the database is reachable. There is no seed data.

## Observability

| Signal | Implementation | Destination |
|---|---|---|
| **Traces** | OpenTelemetry (`otelgrpc` handlers), OTLP/gRPC exporter (batch processor, queue bounded at 2048), W3C TraceContext + Baggage propagated to every downstream call. **Kafka consumer span:** each `orders.placed` record is handled inside a CONSUMER span `orders.placed process` (`messaging.system=kafka`, `messaging.destination.name`, `messaging.operation=process`, `messaging.kafka.consumer.group=order-service`, partition, offset) that is a child of the producer span named by the record's `traceparent` header; a record with missing or malformed headers starts a new root trace and is handled normally (unit-tested; the end-to-end trace through Kafka is not run here). Sampler from `OTEL_TRACES_SAMPLER` (SDK default `parentbased_always_on`) | Alloy `:4317` -> Tempo |
| **Metrics** | Prometheus default registry: `grpc_server_*` (go-grpc-prometheus with handling-time histogram), Go runtime and process metrics, `order_service_kafka_*` (below). Served as Prometheus text on `:9090/metrics` (`METRICS_PORT`) and, because the same registry is bridged into OpenTelemetry, also pushed over OTLP every 15 s (metric names identical). Plus `build_info{service,version,commit} 1`, `order_service_orders_total{event}` (committed state changes: `created`, `placed`, `failed`; replays are not counted) | Alloy scrapes `:9090` -> Mimir |
| **Logs** | `logrus`, one JSON object per line on stdout, plus an OTLP/gRPC hook (`otellogrus`); handlers (`internal/orders`) and the Kafka consumer log with `log.WithContext(ctx)`, so those lines carry `trace_id`/`span_id` while a span is recording (the span only exists when telemetry is on: without an OTLP endpoint the SDK is not installed and no ids are written). Card data does not exist here; the order e-mail, address and session id are never logged | stdout -> Alloy -> Loki; OTLP -> Alloy -> Loki |
| **Profiles** | Pyroscope Go SDK (CPU, alloc/in-use objects and space, goroutines, mutex, block), only with `PYROSCOPE_ADDR`; tags `service_name`, `version`, `environment` | Pyroscope |

**Telemetry contract** (the code is `internal/obs`, the same package in every Go service): telemetry is on only if `OTEL_EXPORTER_OTLP_ENDPOINT` is a valid absolute http(s) URL; `OTEL_SDK_DISABLED=true` forces everything off (profiling too); profiling only with `PYROSCOPE_ADDR`. Start never waits for the collector (the exporters connect in the background) and on SIGTERM the flush is cut off after 4 s, so a blackholed collector cannot hold the process (unit-tested with a listener that never answers: start and shutdown both return within the bound).

**Resource attributes** on every signal: `service.name` (`OTEL_SERVICE_NAME`, default `order-service`), `service.version` (`SERVICE_VERSION`, else build info, else `dev`), `service.namespace=everly`, `deployment.environment` (`DEPLOYMENT_ENVIRONMENT`, default `local`), `service.instance.id` (hostname). `OTEL_RESOURCE_ATTRIBUTES` is applied last.

**Log shape** (one shape for all services): `timestamp` (RFC 3339, UTC, milliseconds), `severity` (`debug`, `info`, `warning`, `error`), `message`, `service`, `version`, `trace_id` and `span_id` (hex, only while a valid span is active, never empty or all zeros), `request_id` (when the caller sent one), `error` (string, on errors), plus the call's own fields (for example `order_id`). Level from `LOG_LEVEL`. `trace_id`/`span_id` come from the context of the log call: handlers and clients log with `log.WithContext(ctx)`, the formatter reads the span from it. Calls without a context (startup, background work) have no ids. No card data, tokens or e-mail addresses are logged.

**Startup and version**: the first line is `message: "starting"` with `version`, `commit` and `environment`; `version`/`commit` come from `SERVICE_VERSION`, ldflags `-X <module>/internal/obs.Version=` / `.Commit=`, or the VCS data Go embeds (no Dockerfile change was made: without any of them a container image reports `version: "dev"`, `commit: "unknown"`). Panics are logged as one JSON line (`message: "panic"`, `error`) before the process dies; a startup error is one JSON error line, exit 1.

**pprof** is off. `PPROF_ADDR=127.0.0.1:6060` starts a separate listener; it is never on the metrics port.

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

Requirements: Go 1.26 (`GOTOOLCHAIN=auto` fetches it), Docker for integration tests, [`grpcurl`](https://github.com/fullstorydev/grpcurl) for manual calls. Layout: `cmd/server/` (entry point, health, Kafka start), `internal/obs/` (telemetry, logs, build_info, metrics server), `internal/orders/` (service, validation, store, migrations), `internal/events/` (Kafka consumer), `proto/`, `tests/integration/`.

| Step | Command | Result on 2026-11-13 (rows marked 2026-11-21 were run again on `origin/main`) |
|---|---|---|
| Vet + unit tests | `go vet ./... && go test -race -count=1 ./...` | run, passed; run again 2026-11-21: passed |
| Format | `gofmt -l .` | run, no files listed; run again 2026-11-22: none |
| Lint | `golangci-lint run ./...` (config `.golangci.yaml`, v2 default linters) | run 2026-11-22 with golangci-lint v2.14.0 (the version CI pins), 0 issues; also with `--build-tags integration`, 0 issues |
| Integration (Postgres) | see below | run on 2026-11-22 with `-race` against a throwaway `postgres:17-alpine` together with the Kafka tests below: 23 tests passed, none skipped (earlier runs by the author are recorded in `docs/development.md`) |
| Integration (Kafka) | see below | run on 2026-11-22 against a throwaway single-node `apache/kafka:4.3.2` with `TEST_KAFKA_CONTAINER` set: all Kafka tests passed, including the broker restart and the broker unavailable at start |
| Vulnerability / security scan | `govulncheck ./...`, `gosec -exclude-generated ./...` | run 2026-11-22: govulncheck `No vulnerabilities found`; gosec 0 issues (29 files) |
| Image | `docker build -t platform-demo/order-service:local .` | run 2026-11-22 (linux/amd64, 51 MB, runs as 10001:10001, exec-form healthcheck) |
| Docs | `mkdocs build --strict` (mkdocs 1.6.1 + mkdocs-techdocs-core 1.7.1, Python 3.12 venv made with uv) and the link check `python3 -I tools/check-md-links.py .` from `platform-demo-workflows` (relative links in `README.md` and `docs/`) | run, passed; run again 2026-11-21, both passed |
| Image scan | `trivy image --severity CRITICAL,HIGH --exit-code 1 platform-demo/order-service:local` | run 2026-11-22: CRITICAL and HIGH 0 |
| Secret scan | `pre-commit run --all-files` (`.pre-commit-config.yaml`, gitleaks) or `gitleaks git --redact` | run 2026-11-22 with the gitleaks binary of the pre-commit cache (`gitleaks git --redact`, full history): no leaks found |
| Workflow lint | `actionlint` | run 2026-11-21 (actionlint 1.7.12), no findings |

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
- **Shutdown:** on SIGTERM/SIGINT health goes `NOT_SERVING`; `GracefulStop` and the Kafka consumer (finish and commit the current record, leave the group) run in parallel for at most 5 s (then `Stop`); the pool is closed (at most 1 s); telemetry and profiles are flushed and the metrics server stops (at most 3 s, so a blackholed collector cannot hold the process); exit 0. At most 9 s in total inside the 10 s of CODE-04, and a watchdog ends the process with exit 1 at 10 s if something else hangs.
- **Scaling:** stateless except for Postgres, so several replicas are possible: migrations are serialised by the advisory lock, Kafka consumers share the group `order-service`. Computed status uses the clock of the replica that answers (see limits). Multi-replica operation was not run.
- **Image:** Alpine runtime, user `10001:10001`, `EXPOSE 8085 9090`, about 12 MB when last built.
- Runbook entries and failure behaviour: [`docs/operations.md`](docs/operations.md).

## Known limits

- No authentication or rate limiting: anyone who can reach the port can read and create orders, and claim the unowned orders of any session id they know; `session_id` is a guard against mixing orders up, not a credential. Product ids are not checked against the catalog.
- The computed status uses the clock of the replica that serves the call; replicas with skewed clocks can show a stage a moment early or late.
- `zip_code` is an `int32` to stay compatible with `hipstershop.Address`: postal codes with letters or leading zeros do not fit.
- Offset paging (page limit 1,000,000); `ListOrders` reads the total and the page in separate statements.
- Kafka: PLAINTEXT without authentication only; a permanently failing record is retried forever by design (later records wait behind it).
- Not run: arm64 build (amd64 only by decision), more than one replica, a multi-broker cluster, a live collector and Pyroscope. The image with the Kafka consumer was run in the compose stack on 2026-11-22 (smoke 87/87 three times; checkout events confirmed orders through Kafka, consumer lag 0).

## License

Apache License 2.0, see [LICENSE](LICENSE).
