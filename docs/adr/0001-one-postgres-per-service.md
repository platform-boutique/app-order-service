# 1. Orders live in a Postgres owned by this service

Status: accepted (reconstructed from the code and the earlier README; no dated decision record existed).

## Context

Orders are written by checkout before the card is charged, must survive restarts, and are queried by session and by user. The shop's other data services (product catalog, reviews, users, inventory) each keep their own database.

## Decision

order-service owns one Postgres database (`orders`, `order_items`). No other service reads it. The schema is created by embedded goose migrations that the service runs at start-up, forward-only, under goose's advisory lock so replicas starting together apply each migration once. There is no seed data.

## Consequences

- Other services need the gRPC API; there are no cross-database joins.
- The service is not ready (`NOT_SERVING`) until the database is migrated and reachable, and follows the database ping afterwards.
- Check constraints in the schema repeat the validation limits as a second line of defence.
- A shared instance with a database per service is possible (CI-03 / RDS later); this is not decided here.
