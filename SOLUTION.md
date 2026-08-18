# Solution

## What was broken and why

The webhook ingestion path had several correctness issues that were not covered by the existing test suite.

- Ingestion used a read-then-write duplicate check, while `events.event_id` had only a non-unique index. Concurrent redeliveries could therefore all observe that an event did not exist and independently insert the event and increment the account totals.
- Recording processing inherited the HTTP request context. That context is cancelled after the handler returns, causing the later database update to fail. The resulting error was discarded, so recordings were not marked as processed and the failure was not visible in the logs.
- Recording goroutines were not tracked during shutdown. The database could therefore be closed while accepted background work was still running, causing in-flight recording processing to be lost during deployment.
- The in-memory statistics cache updated its map and counters without consistently holding its mutex, creating a concurrency race.

## Deduplication strategy

PostgreSQL is the durable source of truth for idempotency. A unique constraint on `events.event_id` is paired with `INSERT ... ON CONFLICT DO NOTHING` inside a transaction containing the event insertion and durable statistics update.

Only the transaction that successfully inserts a new event applies the associated side effects. Concurrent redeliveries are rejected by the database constraint and become no-ops, preventing duplicate records and double-counting.

This approach was preferred over an application-level read-then-write check because that pattern is vulnerable to concurrent races. Redis was also considered, but using it as the primary deduplication mechanism would introduce additional consistency and failure-recovery concerns while PostgreSQL already provides the required durable guarantee.

## At 10,000 webhooks/sec

I would retain PostgreSQL-backed idempotency while separating ingestion from asynchronous processing using a durable queue or outbox consumed by independently scalable workers. PostgreSQL connection pools, indexes, and write throughput would need to be tuned, with batching or partitioning evaluated based on the workload.

The per-process in-memory statistics cache would also need to be replaced with a shared or durable aggregation mechanism, such as Redis or an asynchronously maintained aggregate. Operationally, I would add backpressure, retries, dead-letter handling, and metrics for duplicate events, queue depth, processing latency, and recording failures.
