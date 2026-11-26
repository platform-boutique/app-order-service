# order-service

A gRPC service that stores the orders of the Everly shop and reports their tracking status.

- Checkout writes an order as `PENDING` before it charges the card, then marks it `PLACED` (or `FAILED`).
- After that the status shown to clients advances with the age of the order (`PLACED`, `PROCESSING`, `SHIPPED`, `DELIVERED`). It is computed on every read; nothing is stored for it and no timer runs.
- Orders belong to a session id and, once the buyer is signed in, also to a user (`ClaimOrders`).
- With `KAFKA_BROKERS` set the service consumes `orders.placed` and confirms a still-`PENDING` order from the event.

| | |
|---|---|
| Runtime | Go 1.26, static binary, image `platform-demo/order-service:local` (built locally only) |
| Ports | gRPC `8085`, HTTP `9090` (`/metrics`; pprof only on `PPROF_ADDR`) |
| State | its own Postgres (`orders`, `order_items`) |
| Called by | checkout-service, frontend |
| Status | shared CI pipeline with Publish; Helm chart in platform-demo-helm; not deployed in any environment yet |

Pages: [architecture](architecture.md), [API](api.md), [configuration](configuration.md), [observability](observability.md), [operations](operations.md), [development](development.md), [decisions](adr/0001-one-postgres-per-service.md).
