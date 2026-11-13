# Observability

What the service emits today (code read 2026-11-13; the compose observability profile was not run for this README):

| Signal | Today | Not there yet (standards section 6) |
|---|---|---|
| Traces | OTLP/gRPC, only when `OTEL_EXPORTER_OTLP_ENDPOINT` is set. A server span per inbound gRPC call (`otelgrpc`), W3C `traceparent` + `baggage` propagation, sampler always-on. **Kafka consumer span (step 3, unit-tested only):** each `orders.placed` record is handled inside a CONSUMER span `orders.placed process` (`messaging.system=kafka`, `messaging.destination.name`, `messaging.operation=process`, `messaging.kafka.consumer.group=order-service`, partition, offset) that is a child of the producer span named by the record's `traceparent` header; a record with missing or malformed headers starts a new root trace (no link: there is nothing to link to) and is handled normally. Not run against Kafka or the stack | spans for Postgres calls |
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

## Switches

Telemetry is off unless `OTEL_EXPORTER_OTLP_ENDPOINT` is set; `OTEL_SDK_DISABLED=true` forces it off; profiling needs `PYROSCOPE_ADDR`. The legacy `COLLECTOR_SERVICE_ADDR` still turns telemetry on in the current code and is to be removed. Details: [configuration](configuration.md).


## Observability step 3 status (branch `feat/observability-step3`)

Done: the Kafka consumer span above, request-scoped logging in the consumer path (`WithContext`, `error` field), and the shared `internal/obs` package (copied from checkout-service, unit-tested, **not wired into `cmd/server` yet**). Not done: the env contract rewrite (`COLLECTOR_SERVICE_ADDR` and `ENABLE_TRACING` are still read, `otlp.go` is unchanged), JSON log shape with `trace_id`/`span_id`/`service`/`version`, `LOG_LEVEL`, `build_info`, `PPROF_ADDR`, resource attributes. The tables above describe the running binary, not `internal/obs`.
