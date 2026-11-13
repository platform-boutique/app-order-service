# 2. Tracking status is computed on read

Status: accepted (reconstructed from the code and the earlier README).

## Context

The shop shows an order moving through `PROCESSING`, `SHIPPED` and `DELIVERED` although no warehouse system exists.

## Decision

Only `PENDING`, `PLACED` and `FAILED` are stored. The later stages are a pure function of the confirmation time, the configured offsets and the clock (`Stages.Schedule(order, now)`), evaluated on every read. The response carries `evaluated_at` so clients compare stage times with the server clock.

## Consequences

- No timer, cron or write path for status changes; nothing to recover after a restart.
- Tests run with a fake clock.
- Replicas with skewed clocks can show a stage a moment early or late.
- A real fulfilment system would replace this by stored transitions (not planned).
