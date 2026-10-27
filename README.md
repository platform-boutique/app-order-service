# order-service

A gRPC service that stores the shop's orders and reports their tracking status. Checkout writes an order as `PENDING` before it charges and ships, then marks it `PLACED` (or `FAILED`). After that the status shown to clients advances with the age of the order (`PLACED` -> `PROCESSING` -> `SHIPPED` -> `DELIVERED`): it is **computed on every read**, nothing is stored for it and no timer or cron runs. It owns its own Postgres database and creates the schema with embedded migrations at start-up. Orders belong to a session id (accounts come later; `user_id` is already stored and can be queried).

## At a glance

| | |
|---|---|
| **Language / runtime** | Go 1.26 (static binary, `CGO_ENABLED=0`) |
| **Image** | built locally only: `platform-demo/order-service:local` |
| **Protocol / port** | gRPC (HTTP/2, plaintext) on `8085` |
| **Metrics** | Prometheus `/metrics` on `9090`; pushed over OTLP/gRPC only when an endpoint is set |
| **Health** | Standard `grpc.health.v1.Health/Check` on `8085`: `NOT_SERVING` until the migrations are done and whenever the database does not answer a ping (checked every 5 s) |
| **State** | Postgres (`orders`, `order_items`), via `DATABASE_URL` |
| **Depends on** | Its own Postgres (optional: OTLP collector, Pyroscope) |
| **Called by** | nobody yet (checkout and frontend: separate tickets) |
| **gRPC reflection** | Enabled: `grpcurl` works without `-proto` |

## API

Package `hipstershop.orders`, service `OrderService` ([`proto/orders.proto`](proto/orders.proto)). The package is a sub-package of `hipstershop` on purpose: `Money` and `Address` are re-declared with exactly the fields and numbers of `hipstershop.Money` / `hipstershop.Address`, and a client that also compiles `demo.proto` would otherwise get two messages with the same full name.

| Method | Request | Behaviour |
|---|---|---|
| `CreateOrder` | `order_id`, `session_id`, optional `user_id`, `email`, `shipping_address`, `items[]` (`product_id`, `name`, `quantity`, `unit_price`, `picture`), optional `shipping_cost`, `total` | Stores the order as `PENDING`. Idempotent by `order_id`: the same content again returns the stored order, other content under the same id is `ALREADY_EXISTS`. `total` must equal `sum(quantity x unit_price) + shipping_cost` exactly and all amounts must share one currency, otherwise `INVALID_ARGUMENT` |
| `ConfirmOrder` | `order_id`, `shipping_tracking_id`, `payment_transaction_id` | `PENDING` -> `PLACED`, sets the confirmation time. Idempotent with the same two ids; other ids on a placed order or a failed order: `FAILED_PRECONDITION` |
| `FailOrder` | `order_id`, `reason` | `PENDING` -> `FAILED`. An already failed order is returned unchanged (the first reason stays); a placed order is `FAILED_PRECONDITION` |
| `GetOrder` | `order_id`, optional `session_id`, optional `user_id` | Returns the order. If a guard is set it must match the order, otherwise `NOT_FOUND` (the same answer as for an unknown id, so ids cannot be probed) |
| `ListOrders` | exactly one of `session_id` / `user_id`, `page`, `page_size`, `placed_only` | Newest first (`created_at DESC`, id as tie-break). `page` is 1-based, `0` means 1; `page_size` `0` means 10, above 50 is capped. `total` counts all matches. `placed_only` leaves out `PENDING` and `FAILED` orders. Both or neither owner: `INVALID_ARGUMENT` |

All five return the `Order` message (`ListOrders`: `{ orders[], total }`). `Order` carries every field of the request plus:

- `status`: `ORDER_STATUS_PENDING | PLACED | PROCESSING | SHIPPED | DELIVERED | FAILED`, computed for the moment of the call.
- `failure_reason`, `shipping_tracking_id`, `payment_transaction_id`.
- `created_at`, and for the tracker `placed_at`, `processing_at`, `shipped_at`, `delivered_at`: the moments those stages are reached (`placed_at` plus the configured offsets), also in the future for a stage not reached yet. All four are unset for `PENDING` and `FAILED`. `failed_at` is set for `FAILED`.
- `evaluated_at`: the server clock the status was computed with. Compare the stage timestamps with this one, not with the browser clock.

Limits: `order_id` 1-64 characters `[A-Za-z0-9._-]` starting with a letter or digit; `session_id` 1-128; `user_id` 0-128; `email` 3-254, plain address; 1-100 items; `product_id` 1-128, `name` 1-200, `quantity` 1-1000, `picture` 0-500; `street_address` 1-200, `city` 1-100, `state` 0-100, `country` 1-100, `zip_code` >= 0; tracking and transaction ids 1-128; reason 1-500. Text is trimmed, lengths count characters, invalid UTF-8 and NUL are rejected. Money: three upper-case letters, no negative values, `nanos` 0..999,999,999, unit prices and total above zero, units at most 1,000,000,000 (total: 200,000,000,000,000). Item name, unit price and picture are snapshots: the order keeps them if the catalog changes.

Errors: `INVALID_ARGUMENT`, `NOT_FOUND`, `ALREADY_EXISTS`, `FAILED_PRECONDITION` as above; `UNAVAILABLE` while the database is not ready or reachable; `INTERNAL` (without SQL details, which are logged) for other storage errors. A recovery interceptor turns a handler panic into `INTERNAL`. Product ids are not checked against the catalog.

Examples (run against the container, see below; note that `grpcurl` prints int64 as strings):

```bash
grpcurl -plaintext -d '{"order_id":"o-1","session_id":"sess-1","email":"ada@example.com",
  "shipping_address":{"street_address":"1 Main St","city":"London","country":"UK","zip_code":1234},
  "items":[{"product_id":"blue-frock-162","name":"Blue Frock","quantity":2,"unit_price":{"currency_code":"USD","units":19,"nanos":990000000}}],
  "shipping_cost":{"currency_code":"USD","units":4,"nanos":750000000},
  "total":{"currency_code":"USD","units":44,"nanos":730000000}}' localhost:8085 hipstershop.orders.OrderService/CreateOrder
grpcurl -plaintext -d '{"order_id":"o-1","shipping_tracking_id":"TRK-1","payment_transaction_id":"TXN-1"}' localhost:8085 hipstershop.orders.OrderService/ConfirmOrder
grpcurl -plaintext -d '{"order_id":"o-1","session_id":"sess-1"}' localhost:8085 hipstershop.orders.OrderService/GetOrder
grpcurl -plaintext -d '{"session_id":"sess-1","placed_only":true}' localhost:8085 hipstershop.orders.OrderService/ListOrders
```

## Status schedule

For an order confirmed at time `T` (`placed_at`) and the current time `now`:

| Condition | Shown status |
|---|---|
| stored `PENDING` | `PENDING`, never advances |
| stored `FAILED` | `FAILED`, never advances |
| `now < T + ORDER_PROCESSING_AFTER` | `PLACED` |
| `T + ORDER_PROCESSING_AFTER <= now < T + ORDER_SHIPPED_AFTER` | `PROCESSING` |
| `T + ORDER_SHIPPED_AFTER <= now < T + ORDER_DELIVERED_AFTER` | `SHIPPED` |
| `T + ORDER_DELIVERED_AFTER <= now` | `DELIVERED` |

The offsets are all measured from the confirmation (defaults 1m, 3m, 6m). In code the schedule is the pure function `Stages.Schedule(order, now)` and the service takes the clock as a parameter, so the tests run it with a fake clock.

## Configuration

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | **required** | Postgres URL, e.g. `postgres://order:order@order-db:5432/orders?sslmode=disable`. The process exits if it is unset. A 5 s connect timeout is added unless the URL sets one |
| `PORT` | `8085` | gRPC listen port |
| `METRICS_PORT` | `9090` | Prometheus (and, with profiling, pprof) HTTP port |
| `ORDER_PROCESSING_AFTER` | `1m` | Offset after confirmation at which `PLACED` becomes `PROCESSING` (Go duration: `90s`, `2m`) |
| `ORDER_SHIPPED_AFTER` | `3m` | Offset at which it becomes `SHIPPED` |
| `ORDER_DELIVERED_AFTER` | `6m` | Offset at which it becomes `DELIVERED`. Must satisfy `0 < processing < shipped < delivered`, otherwise the process exits at start-up |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | unset (telemetry off) | OTLP/**gRPC** endpoint for traces, metrics and logs (scheme required). Legacy fallback: `COLLECTOR_SERVICE_ADDR` (`host:port`). Telemetry is on only if one of them is set and non-empty |
| `OTEL_SDK_DISABLED` | — | `true` forces telemetry off even with an endpoint (the legacy `ENABLE_TRACING=0` does the same) |
| `OTEL_METRIC_EXPORT_INTERVAL` | `15000` | Milliseconds between metric pushes |
| `OTEL_SERVICE_NAME` | `order-service` | `service.name` |
| `PYROSCOPE_ADDR` | unset (profiling off) | Profiling (Pyroscope push **and** the pprof endpoints on `METRICS_PORT`) is on only if this is set and `ENABLE_PROFILING` is not `0` |
| `ENABLE_PROFILING` | — | `0` forces profiling off |

Start-up: the gRPC server listens at once (health `NOT_SERVING`); a background loop runs the goose migrations (under goose's Postgres session advisory lock, so replicas starting together apply each migration once), retrying every 2 s until the database is reachable. There is no seed data. SIGTERM/SIGINT: health goes `NOT_SERVING`, `GracefulStop` (10 s cap), then the pool is closed and the process exits 0.

## Storage

`orders` (one row per order, stored state `PENDING | PLACED | FAILED`, money as `units` + `nanos`, `fingerprint` = hash of the request for the idempotency check) and `order_items` (one row per line, ordered by `position`). Check constraints repeat the validation limits, and `PLACED` requires a confirmation time and both references. Indexes serve the two `ListOrders` shapes (by session, by user). `CreateOrder` is `INSERT ... ON CONFLICT DO NOTHING` in one transaction, `ConfirmOrder`/`FailOrder` run under `SELECT ... FOR UPDATE`, so concurrent calls on one order serialise (a racing confirm and fail: exactly one wins).

## Container image

Multi-stage [`Dockerfile`](Dockerfile) (same as review-service): `golang:1.26-alpine` builder (`--platform=$BUILDPLATFORM`, `GOARCH=$TARGETARCH`), `alpine:3.24` runtime with the single binary at `/app/order-service`.

- **User:** `10001:10001` (numeric, non-root).
- **HEALTHCHECK:** `/app/order-service healthcheck` calls the gRPC health service on `127.0.0.1:$PORT` and exits 0 only on `SERVING`.
- **EXPOSE:** `8085`, `9090`. Built once locally on amd64 (`docker build -t platform-demo/order-service:local .`, about 12 MB); arm64 was not built.

## Used by (local shop)

- **checkout-service** (branch `feat/orders-integration`, env `ORDER_SERVICE_ADDR=order-service:8085`) calls `CreateOrder` before it charges the card, `ConfirmOrder` after shipping and `FailOrder` on any error. With the variable unset checkout does not store orders.
- **frontend** (env `ORDER_SERVICE_ADDR`, default `order-service:8085`) calls `ListOrders` (`session_id`, `placed_only`) for `/orders` and `GetOrder` (with the session as guard) for `/orders/{id}`, and draws the tracker from the status and the stage timestamps compared with `evaluated_at`.
- Orders are keyed by the browser session today. Accounts (a user service) are the next slice: `user_id` is already stored and `ListOrders` accepts it, but nothing sets it yet and anonymous orders are not adopted by a user after login (see the plan).

The local stack (`local/compose.yaml` in the workspace, not in this repository) runs `order-db` (postgres:17-alpine) and `order-service` on port 8085. Run on the live stack: an order placed in the browser is stored as `PLACED` with its tracking id and shows up in the frontend's history and tracking page (first stage seen), declined cards are stored as `FAILED` with the reason. The progression through all four stages is covered by this service's tests (injectable clock) and the frontend's tests with a fake client; the default 1/3/6 minute offsets were not waited through on the live stack.

## Not deployed anywhere yet

No Helm chart, pipeline, Backstage entry or GitHub repository exists; the service is only built and run locally.

## Running locally

Requirements: Go 1.26 (`GOTOOLCHAIN=auto` fetches it), Docker, [`grpcurl`](https://github.com/fullstorydev/grpcurl).

What was actually run:

```bash
# unit tests (validation, status schedule with a fake clock, state transitions, handlers, config) + vet
go vet ./... && go test -race ./...

# integration tests against a throwaway Postgres (each test creates and drops its own database)
docker run -d --rm --name order-it-pg --memory 256m -e POSTGRES_USER=order -e POSTGRES_PASSWORD=order \
  -p 127.0.0.1:55433:5432 postgres:17-alpine
TEST_DATABASE_URL='postgres://order:order@127.0.0.1:55433/postgres?sslmode=disable' \
  go test -race -tags integration ./...

# container smoke run: image + a Postgres with its own database and user on one docker network
docker build -t platform-demo/order-service:local .
docker network create order-smoke
docker run -d --name order-smoke-db --network order-smoke --network-alias order-db -e POSTGRES_DB=orders -e POSTGRES_USER=order -e POSTGRES_PASSWORD=order postgres:17-alpine
docker run -d --name order-smoke-svc --network order-smoke -p 127.0.0.1:58085:8085 \
  -e DATABASE_URL='postgres://order:order@order-db:5432/orders?sslmode=disable' \
  -e ORDER_PROCESSING_AFTER=5s -e ORDER_SHIPPED_AFTER=10s -e ORDER_DELIVERED_AFTER=15s platform-demo/order-service:local
```

The smoke run (with the 5/10/15 s offsets) checked: health `SERVING` and Docker health `healthy`; create, repeated create, confirm and the status going `PLACED` -> `PROCESSING` -> `SHIPPED` -> `DELIVERED` while polling `GetOrder`; `NOT_FOUND` for a foreign session; `INVALID_ARGUMENT` for a wrong total and for list with both owners; `ALREADY_EXISTS` for other content under the same id; `FAILED_PRECONDITION` for failing a placed order; `ListOrders` with `placed_only`; health `NOT_SERVING` + `UNAVAILABLE` after disconnecting the database from the network and `SERVING` again after reconnecting it; the order still there after restarting the service container; user `10001`; exit code 0 on `docker stop`.
`golangci-lint run` (default linters, also with `-tags integration`) and `gosec -exclude-generated ./...` report no issues. `govulncheck` was tried but crashed on the toolchain ("package without types"), so no vulnerability scan was done.

Regenerate the Go code after editing the proto: `protoc -I proto --go_out=proto --go_opt=paths=source_relative --go-grpc_out=proto --go-grpc_opt=paths=source_relative proto/orders.proto` (protoc 3.21.12, protoc-gen-go 1.36.11, protoc-gen-go-grpc 1.6.1).

## Project structure

```
├── cmd/server/
│   ├── main.go            # entrypoint: telemetry, profiling, gRPC server, DB watcher, graceful shutdown
│   ├── config.go          # ORDER_*_AFTER stage offsets
│   ├── otlp.go            # OTLP traces/metrics/logs (same as review-service)
│   └── healthcheck.go     # `order-service healthcheck` for the container HEALTHCHECK
├── internal/orders/
│   ├── service.go         # gRPC handlers, error mapping, computed status in the response
│   ├── validate.go        # request limits, money arithmetic, paging
│   ├── order.go           # stored states, Confirm/Fail transitions, status Schedule
│   ├── store.go           # Postgres store (pgx/v5)
│   ├── migrate.go         # goose migrations, Setup
│   └── migrations/        # embedded SQL migrations
├── proto/                 # orders.proto + generated Go
├── tests/integration/     # tests against a real Postgres (build tag `integration`)
└── Dockerfile
```

## Known limitations

- No authentication or rate limiting: anyone who can reach the port can read and create orders; `session_id` is a guard against mixing orders up, not a credential. Product ids are not checked against the catalog.
- The computed status uses the clock of the replica that serves the call; replicas with skewed clocks can show a stage a moment early or late (a clock behind the confirmation counts as age zero).
- `zip_code` is an `int32` to stay compatible with `hipstershop.Address`, so postal codes with letters or leading zeros do not fit.
- Offset paging (page limit 1,000,000); `ListOrders` reads the total and the page in separate statements.
- Database calls do not produce trace spans (no pgx tracer).
- The smoke-run used the final code of this tree; arm64 was not built.
