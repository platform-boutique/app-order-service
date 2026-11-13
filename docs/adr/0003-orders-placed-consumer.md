# 3. A Kafka consumer confirms orders from `orders.placed`

Status: accepted (reconstructed from the code and the earlier README).

## Context

Checkout calls `ConfirmOrder` after shipping and also publishes `orders.placed`. If the `ConfirmOrder` call is lost, the order would stay `PENDING` forever although the customer was charged.

## Decision

With `KAFKA_BROKERS` set, order-service consumes `orders.placed` (consumer group `order-service`) and confirms the order through the same code path as the RPC (`ConfirmFromEvent`). Delivery is at-least-once, handling is idempotent, offsets are committed by hand after a batch. Transient errors are retried with backoff and never skipped; unusable records are logged and skipped. Kafka is optional and never part of the health.

## Consequences

- The topic contract (key = order id, JSON, schema version 1) is owned by checkout.
- A record that fails permanently with a database error blocks the partition by design.
- PLAINTEXT without authentication only; TLS/SASL are not wired.
- Trace context from Kafka headers is not extracted yet.
