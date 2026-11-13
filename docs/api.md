# API

Package `hipstershop.orders`, service `OrderService` ([`proto/orders.proto`](https://github.com/platform-boutique/app-order-service/blob/main/proto/orders.proto)). The package is a sub-package of `hipstershop` on purpose: `Money` and `Address` are re-declared with exactly the fields and numbers of `hipstershop.Money` / `hipstershop.Address`, and a client that also compiles `demo.proto` would otherwise get two messages with the same full name.

| Method | Request | Behaviour |
|---|---|---|
| `CreateOrder` | `order_id`, `session_id`, optional `user_id`, `email`, `shipping_address`, `items[]` (`product_id`, `name`, `quantity`, `unit_price`, `picture`), optional `shipping_cost`, `total` | Stores the order as `PENDING`. Idempotent by `order_id`: the same content again returns the stored order, other content under the same id is `ALREADY_EXISTS`. The returned `Order` has `created = true` only on the call that inserted the row and `created = false` (present) on the replay (decided by the `INSERT ... ON CONFLICT DO NOTHING` row count, so of N parallel identical calls exactly one gets `true`); the field is `optional bool`, so a client can tell false from unset. `total` must equal `sum(quantity x unit_price) + shipping_cost` exactly and all amounts must share one currency, otherwise `INVALID_ARGUMENT` |
| `ConfirmOrder` | `order_id`, `shipping_tracking_id`, `payment_transaction_id` | `PENDING` -> `PLACED`, sets the confirmation time. Idempotent with the same two ids; other ids on a placed order or a failed order: `FAILED_PRECONDITION` |
| `FailOrder` | `order_id`, `reason` | `PENDING` -> `FAILED`. An already failed order is returned unchanged (the first reason stays); a placed order is `FAILED_PRECONDITION` |
| `GetOrder` | `order_id`, optional `session_id`, optional `user_id` | Returns the order. If a guard is set it must match the order, otherwise `NOT_FOUND` (the same answer as for an unknown id, so ids cannot be probed) |
| `ListOrders` | exactly one of `session_id` / `user_id`, `page`, `page_size`, `placed_only` | Newest first (`created_at DESC`, id as tie-break). `page` is 1-based, `0` means 1; `page_size` `0` means 10, above 50 is capped. `total` counts all matches. `placed_only` leaves out `PENDING` and `FAILED` orders. Both or neither owner: `INVALID_ARGUMENT` |
| `ClaimOrders` | `session_id` (1-128), `user_id` (1-128) | Gives the orders of an anonymous session to a user: every order of this `session_id` whose `user_id` is empty gets `user_id` set, whatever its state (`PENDING` and `FAILED` too, it is only an owner field). Orders that already have a user are never touched, so a second user on the same browser cannot take them. Idempotent: repeating returns `claimed = 0`. Returns `{ claimed }`, the number of orders changed by this call. Empty or too long id: `INVALID_ARGUMENT` |

The first five return the `Order` message (`ListOrders`: `{ orders[], total }`; `ClaimOrders` is described in its row). `Order` carries every field of the request plus:

- `status`: `ORDER_STATUS_PENDING | PLACED | PROCESSING | SHIPPED | DELIVERED | FAILED`, computed for the moment of the call.
- `failure_reason`, `shipping_tracking_id`, `payment_transaction_id`.
- `created_at`, and for the tracker `placed_at`, `processing_at`, `shipped_at`, `delivered_at`: the moments those stages are reached (`placed_at` plus the configured offsets), also in the future for a stage not reached yet. All four are unset for `PENDING` and `FAILED`. `failed_at` is set for `FAILED`.
- `evaluated_at`: the server clock the status was computed with. Compare the stage timestamps with this one, not with the browser clock.
- `created` (field 20, `optional bool`): set only on `CreateOrder` responses, `true` for the call that inserted the order and `false` (explicitly present) on the idempotent replay; unset on every other RPC (`ConfirmOrder`, `FailOrder`, `GetOrder`, `ListOrders`). Checkout uses it to detect that another replica or an earlier attempt already created the order. It is `optional` so that a client can tell "false" from "unset" (an older order-service never sends it: unknown). On the wire it is a normal bool.

Limits: `order_id` 1-64 characters `[A-Za-z0-9._-]` starting with a letter or digit; `session_id` 1-128; `user_id` 0-128 (1-128 in `ClaimOrders`); `email` 3-254, plain address; 1-100 items; `product_id` 1-128, `name` 1-200, `quantity` 1-1000, `picture` 0-500; `street_address` 1-200, `city` 1-100, `state` 0-100, `country` 1-100, `zip_code` >= 0; tracking and transaction ids 1-128; reason 1-500. Text is trimmed, lengths count characters, invalid UTF-8 and NUL are rejected. Money: three upper-case letters, no negative values, `nanos` 0..999,999,999, unit prices and total above zero, units at most 1,000,000,000 (total: 200,000,000,000,000). Item name, unit price and picture are snapshots: the order keeps them if the catalog changes.

Errors: `INVALID_ARGUMENT`, `NOT_FOUND`, `ALREADY_EXISTS`, `FAILED_PRECONDITION` as above; `UNAVAILABLE` while the database is not ready or reachable; `INTERNAL` (without SQL details, which are logged) for other storage errors. A recovery interceptor turns a handler panic into `INTERNAL`. Product ids are not checked against the catalog.

Examples (note that `grpcurl` prints int64 as strings):

```bash
grpcurl -plaintext -d '{"order_id":"o-1","session_id":"sess-1","email":"ada@example.com",
  "shipping_address":{"street_address":"1 Main St","city":"London","country":"UK","zip_code":1234},
  "items":[{"product_id":"blue-frock-162","name":"Blue Frock","quantity":2,"unit_price":{"currency_code":"USD","units":19,"nanos":990000000}}],
  "shipping_cost":{"currency_code":"USD","units":4,"nanos":750000000},
  "total":{"currency_code":"USD","units":44,"nanos":730000000}}' localhost:8085 hipstershop.orders.OrderService/CreateOrder
grpcurl -plaintext -d '{"order_id":"o-1","shipping_tracking_id":"TRK-1","payment_transaction_id":"TXN-1"}' localhost:8085 hipstershop.orders.OrderService/ConfirmOrder
grpcurl -plaintext -d '{"order_id":"o-1","session_id":"sess-1"}' localhost:8085 hipstershop.orders.OrderService/GetOrder
grpcurl -plaintext -d '{"session_id":"sess-1","placed_only":true}' localhost:8085 hipstershop.orders.OrderService/ListOrders
grpcurl -plaintext -d '{"session_id":"sess-1","user_id":"user-1"}' localhost:8085 hipstershop.orders.OrderService/ClaimOrders
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
