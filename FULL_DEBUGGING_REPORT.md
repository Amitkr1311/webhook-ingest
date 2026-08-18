# Full debugging report

## Baseline and architecture

The clean Git worktree was on `main` with `origin` set to
`https://github.com/Amitkr1311/webhook-ingest.git`. Go was `go1.26.5`; Docker
Desktop and Compose were available. The first full test invocation exceeded the
tool's 60-second command window during initial compilation, while targeted
original-code tests ran successfully. Docker was healthy after Compose startup
and `/healthz` returned `ok`.

The flow is `POST /webhooks/calls` -> HTTP validation -> `ingest.Service` ->
PostgreSQL event/call/account statistics -> optional asynchronous recording
update. Redis is connected but unused. PostgreSQL has `events`, `calls`, and
`account_stats`; `calls.call_id` is primary-keyed, while the original
`events.event_id` had only a non-unique index. The server stops HTTP through
`http.Server.Shutdown` and originally then returned immediately.

During initial investigation the existing local PostgreSQL volume contained an
untracked unique `events_event_id_key` constraint. I reset only this project's
local Compose volume and recreated it from the checked-in migration before
proving the original idempotency defect.

## Bug 1: duplicate webhook processing

- **Symptom / root cause:** Concurrent duplicate deliveries raced between
  `EventExists` and `InsertEvent`; neither schema nor code provided an atomic
  claim. Each winner then incremented `account_stats`.
- **Reproduction:** `TestConcurrentDuplicateWebhookIsProcessedOnce` uses a
  temporary per-test insert trigger to hold simultaneous inserts open. Against
  original schema/code it failed with **10 event rows** and aggregate
  `{CallCount:10 TotalDurationSec:1000}` instead of one 100-second call.
- **Fix:** migration `002_events_event_id_unique.sql` adds a unique constraint.
  `Store.InsertEventAndProcess` uses `ON CONFLICT DO NOTHING` and performs the
  event insert, call upsert, and aggregate increment in one transaction. The
  cache updates only when that transaction inserted the event.
- **Why it works:** the unique constraint serializes the claim; transaction
  rollback prevents an inserted event without its durable side effects.
- **Commits / push:** failing test `7ca183b`; fix `e237592`; both pushed.

## Bug 2: recording processing and errors

- **Symptom / root cause:** The goroutine passed `r.Context()` into
  `processRecording`. Once the handler returned, that request context was
  cancelled; `MarkRecordingProcessed` failed after the simulated work. The
  error block was an empty TODO.
- **Reproduction:** `TestRecordingProcessingOutlivesWebhookRequest` posts a
  recording event and polls the durable call row for up to one second. Before
  the fix it failed: `recording was never marked processed after the webhook
  request completed`.
- **Fix:** recording work uses an independent background context and logs any
  failure with `event_id`, `call_id`, and `err`.
- **Commits / push:** failing test `7e8292a`; fix `0b1284b`; both pushed.

## Bug 3: lost in-flight work at shutdown

- **Symptom / root cause:** `http.Server.Shutdown` waits for active handlers,
  not detached recording goroutines. `main` then returned, closing Postgres
  while accepted work was sleeping.
- **Reproduction:** `TestShutdownWaitsForRecordingProcessing` accepts a
  recording event, emulates dependency closure, and checks from an independent
  pool. Original code failed: `recording work was lost when the service
  dependency closed`.
- **Fix:** `Service` tracks recording goroutines with a `sync.WaitGroup` and
  exposes `Shutdown(context.Context)`. After stopping HTTP acceptance, main
  drains this work within the existing 10-second deadline before deferred
  dependency close.
- **Commits / push:** failing test `2119830`; fix `9ddd000`; both pushed.

## Bug 4: concurrent in-memory statistics race

- **Symptom / root cause:** `Cache.Record` mutated its map and `AccountStats`
  without locking, despite `Cache.Get` using an RW mutex.
- **Reproduction:** `TestCacheRecordIsSafeForConcurrentDeliveries` starts 100
  concurrent writes for one account. `go test -race` reported map/counter data
  races and observed 58 calls / 46 seconds instead of 100.
- **Fix:** `Record` now holds the existing mutex while finding/creating and
  updating the account aggregate.
- **Commits / push:** failing test `f8d7adb`; fix `e05b372`; both pushed.

## Validation and manual checks

Final commands succeeded:

```
go test ./...
go test -race ./...
docker compose up -d --build
docker compose ps
curl localhost:8080/healthz
```

All Go packages passed under normal and race-enabled tests. The rebuilt stack
was healthy and health returned `ok`. Two manual identical deliveries returned
accepted; PostgreSQL showed one event row, `call_count=1`, and
`total_duration_sec=123`. A manual recording delivery produced
`recording_processed=true`. The deterministic regression tests cover concurrent
duplicates, request completion, and shutdown lifecycle. No standalone manual
SIGTERM test was needed in addition to the deterministic shutdown test.

## Remaining limitations and scale

The process-local cache starts empty after restart and is not hydrated from
Postgres. Recording work is still in-process rather than a durable queue, so a
forced kill or shutdown timeout can still interrupt it (the normal graceful
path drains it). At 10k/sec, retain the database idempotency constraint but use
a durable outbox/queue for recordings, batched or partitioned event storage,
and a shared cache/materialized aggregate.

## Interview discussion prompts

Be ready to explain atomic uniqueness versus check-then-insert, transaction
boundaries, `ON CONFLICT DO NOTHING`, why Redis was not used for correctness,
at-least-once delivery, cache-versus-durable aggregates, Go context ownership,
WaitGroup shutdown ordering, race-detector evidence, and the durable-outbox
path for high-volume recording work.
