# Develop and test

Requirements: Go 1.26 (`GOTOOLCHAIN=auto` fetches the toolchain from `go.mod`), Docker for integration tests, `grpcurl` for manual calls, `golangci-lint` v2.

Commands run on 2026-11-13 for the standards change (host: Go 1.22 with `GOTOOLCHAIN=auto` selecting 1.26.10):

| Command | Result |
|---|---|
| `go vet ./... && go test -race -count=1 ./...` | passed (3 packages with tests) |
| `golangci-lint run ./...` and `golangci-lint run --build-tags integration ./...` (golangci-lint v2.11.4, `.golangci.yaml`) | 0 issues. The shared pipeline pins v2.14.0: not run |
| `gofmt -l .` | no files listed |
| `mkdocs build --strict` (mkdocs 1.6.1, mkdocs-techdocs-core 1.7.1, Python 3.12 venv made with uv) | passed |

## What the earlier author ran (recorded, not re-run for this page)

Commands and results below come from the README before the standards change; they were not repeated on 2026-11-13.

Requirements: Go 1.26 (`GOTOOLCHAIN=auto` fetches it), Docker, [`grpcurl`](https://github.com/fullstorydev/grpcurl).

What was actually run:

```bash
# unit tests (validation, status schedule with a fake clock, state transitions, handlers incl. ClaimOrders, config) + vet
go vet ./... && go test -race ./...

# integration tests against a throwaway Postgres (each test creates and drops its own database)
docker run -d --rm --name order-it-pg --memory 256m -e POSTGRES_USER=order -e POSTGRES_PASSWORD=order \
  -p 127.0.0.1:55433:5432 postgres:17-alpine
TEST_DATABASE_URL='postgres://order:order@127.0.0.1:55433/postgres?sslmode=disable' \
  go test -race -tags integration ./...

# Kafka consumer integration tests: a throwaway single-node broker (~40 s to start) plus the Postgres above
docker run -d --rm --name order-it-kafka --hostname order-it-kafka --memory 768m -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:19092 -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 -e KAFKA_NUM_PARTITIONS=3 -e KAFKA_HEAP_OPTS="-Xms256m -Xmx384m" \
  -p 127.0.0.1:19092:9092 apache/kafka:4.3.2
TEST_DATABASE_URL='postgres://order:order@127.0.0.1:55433/postgres?sslmode=disable' \
TEST_KAFKA_BROKERS=127.0.0.1:19092 TEST_KAFKA_CONTAINER=order-it-kafka \
  go test -race -count=1 -tags integration ./tests/integration/   # Kafka tests skip without TEST_KAFKA_BROKERS

# container smoke run: image + a Postgres with its own database and user on one docker network
docker build -t platform-demo/order-service:local .
docker network create order-smoke
docker run -d --name order-smoke-db --network order-smoke --network-alias order-db -e POSTGRES_DB=orders -e POSTGRES_USER=order -e POSTGRES_PASSWORD=order postgres:17-alpine
docker run -d --name order-smoke-svc --network order-smoke -p 127.0.0.1:58085:8085 \
  -e DATABASE_URL='postgres://order:order@order-db:5432/orders?sslmode=disable' \
  -e ORDER_PROCESSING_AFTER=5s -e ORDER_SHIPPED_AFTER=10s -e ORDER_DELIVERED_AFTER=15s platform-demo/order-service:local
```

The Kafka consumer was also run as a plain binary (not the image) against that broker and Postgres: with `KAFKA_BROKERS` set an order created over gRPC and a published `order.placed` ended `PLACED` with the event's tracking id, the five metrics above appeared on `:9090/metrics`, and SIGTERM stopped the process in about 0.1 s with the log line `stopped`; with `KAFKA_BROKERS` empty no Kafka metric exists and the log says the consumer is off; with `KAFKA_BROKERS=127.0.0.1:1` (nothing there) health stayed `SERVING`, the client logged one warning every few seconds and SIGTERM exited 0.

The smoke run of the image (with the 5/10/15 s offsets) checked: health `SERVING` and Docker health `healthy`; create, repeated create, confirm and the status going `PLACED` -> `PROCESSING` -> `SHIPPED` -> `DELIVERED` while polling `GetOrder`; `NOT_FOUND` for a foreign session; `INVALID_ARGUMENT` for a wrong total and for list with both owners; `ALREADY_EXISTS` for other content under the same id; `FAILED_PRECONDITION` for failing a placed order; `ListOrders` with `placed_only`; health `NOT_SERVING` + `UNAVAILABLE` after disconnecting the database from the network and `SERVING` again after reconnecting it; the order still there after restarting the service container; user `10001`; exit code 0 on `docker stop`.
`golangci-lint run` (default linters, also with `-tags integration`) and `gosec -exclude-generated ./...` report no issues. `govulncheck` was tried but crashed on the toolchain ("package without types"), so no vulnerability scan was done.

Regenerate the Go code after editing the proto: `protoc -I proto --go_out=proto --go_opt=paths=source_relative --go-grpc_out=proto --go-grpc_opt=paths=source_relative proto/orders.proto` (protoc 3.21.12, protoc-gen-go 1.36.11, protoc-gen-go-grpc 1.6.1).

## Kafka consumer: what was run and what was not (earlier author)

What was run and what was not:

- Run (commands above): unit tests for event parsing (valid, unknown fields, schema version 0/2/missing/string, malformed, truncated), `ConfirmFromEvent` against the in-memory store (all cases in the table), the handler's outcome mapping and log content, and the consumer loop with a fake record source and fake sleep (batch commit, skips, retry then success in order without skipping, shutdown during a retry, record in flight at shutdown, poll and commit failures). Integration tests (build tag `integration`) against a throwaway single-node `apache/kafka:4.3.2` and `postgres:17-alpine`: `PENDING -> PLACED`, the topic created explicitly after the consumer started (every other test creates its topic with 3 partitions and waits for the leaders before producing; the test producer retries unknown topics without limit), with a one-time wait at the start until the fresh broker can serve consumer groups (its first group request creates and loads `__consumer_offsets`, which took 10 to 60 s on a loaded host and made the first test slow), duplicate / conflicting / FAILED / unknown / malformed / schema-2 records in one partition, restart of the consumer resuming after the committed offset, a transient failure retried and not skipped, the broker unreachable at start and then reachable (through a late-started TCP forwarder), and a `docker restart` of the broker while the consumer runs.
- Not run: the container image with the consumer, the compose stack end to end with checkout publishing, more than one consumer instance in the same group (rebalancing between two replicas), a multi-broker cluster, TLS/SASL, a poison record that fails with a permanent database error (it would be retried forever by design; the warn log shows the attempt count).

## Build the docs

```bash
uv venv docs-venv && uv pip install --python docs-venv/bin/python 'mkdocs==1.6.1' 'mkdocs-techdocs-core==1.7.1'
docs-venv/bin/mkdocs build --strict
```

Mermaid diagrams are plain fenced code blocks for `mkdocs`; rendering them in Backstage needs the TechDocs Mermaid add-on (not verified here).
