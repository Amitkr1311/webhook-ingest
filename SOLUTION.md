# Solution

## What was broken and why

Webhook ingestion used a read-then-write duplicate check. `event_id` had only a
non-unique index, so concurrent redeliveries could all observe no event and
then independently insert an event and increment the account totals. Recording
work inherited the HTTP request context, which is cancelled once the handler
responds; its later database update failed and the error was discarded. The
recording goroutines were also not tracked during shutdown, so deferred database
closure could terminate accepted work. Finally, the in-memory stats cache wrote
its map and counters without taking its mutex.

## Deduplication strategy

PostgreSQL is the durable source of truth. A unique constraint on
`events.event_id` is paired with `INSERT ... ON CONFLICT DO NOTHING` inside one
transaction containing the call upsert and durable statistics update. Only the
transaction that inserts the event applies those side effects; concurrent
duplicates wait for the unique constraint and then become no-ops. This is more
reliable than an application-level check (which races) or Redis alone (which is
not the durable system of record and complicates failure recovery).

## At 10,000 webhooks/sec

Keep the database constraint, but batch or partition event writes, tune the
pool and indexes, and make recording work a durable queue/outbox consumed by
separate workers. The stats endpoint should use Redis or a materialized,
asynchronously updated aggregate rather than a per-process cache. Operationally,
add retry/dead-letter handling and metrics for duplicate rate, queue depth, and
recording failures.
