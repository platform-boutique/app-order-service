# Configuration

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
