# Pipeline Observability Engine (local, Go)

A small, dependency-free local state engine that ingests pipeline **runtime
events** and **metrics**, merges them into consistent run/pipeline state, and
drives a de-duplicated alert lifecycle. It is explicitly built for the hard
cases:

- out-of-order and late arrivals inside a defined window,
- duplicate reports (same id takes effect exactly once),
- restart recovery (confirmed state is never lost),
- concurrent writers and readers (consistent snapshots, no double counting),
- bounded query time as history grows,
- configuration changes that never silently rewrite old conclusions.

Only the Go standard library is used.

## Layout

| Package   | Responsibility |
|-----------|----------------|
| `model`   | Core value types: events, metrics, results, run state, alerts. |
| `clock`   | Injectable clock (`SystemClock`, `FakeClock`) for deterministic tests. |
| `config`  | Immutable, versioned rule sets (`Config.Version` monotonic). |
| `store`   | Durability: append-only fsync'ed WAL + atomic snapshot, plus `MemStore`. |
| `alert`   | Rule evaluation and alert lifecycle (fire / escalate / recover / close). |
| `engine`  | Concurrency-safe facade: validation, windows, dedup, merging, recovery. |
| `cmd/poe` | Deterministic end-to-end smoke program. |

## Data model

Events (`model.Event`), idempotency by caller-provided `ID` **per pipeline**:

| Event | Meaning |
|-------|---------|
| `run_started` | opens a run (required before any other event for that run) |
| `run_succeeded` | terminal success |
| `run_failed` | terminal failure (adds 1 to pipeline failures) |
| `retry_scheduled` | increments the run's retry count and pipeline retries |
| `retry_succeeded` | terminal success reached via retry |

Metrics (`model.Metric`, idempotent the same way):

| Name | Effect |
|------|--------|
| `latency_ms` | keeps the latest-by-event-time latency (must be > 0) |
| `records` | throughput points summed over the throughput window |
| `failures` | non-negative value added to cumulative failures |
| `data_arrived` | advances freshness watermark |

All timestamps are **event time** in unix milliseconds. A monotonic
**watermark** per pipeline tracks the highest accepted event time.

## Time windows and rejection reasons

Validation order on every ingest:

1. **`invalid_data`** — missing pipeline/run/id, unknown type/metric name,
   non-positive timestamp, negative metric value, zero latency.
2. **`window_expired`** — `now - eventTime > LateWindowMS` (late arrival).
3. **`future_skew`** — `eventTime - now > FutureSkewMS` (clock skew guard).
4. **`duplicate`** — the same `ID` was already accepted for that pipeline
   (even if the payload differs).
5. **`state_conflict`** — e.g. event for an unknown run without
   `run_started`, a second `run_started`, or any lifecycle event after the
   run already reached a terminal state.

Rejected input is **never persisted and never mutates state**, so it cannot
contaminate established conclusions. Within the lateness window, out-of-order
events are merged purely by event time, giving the same result regardless of
arrival order. Terminal runs are retained for `max(RetentionMS,
LateWindowMS)` and then pruned lazily; active runs are never pruned.

## Alert rules

One alert identity = `pipeline|rule` fingerprint. While a rule keeps
breaching it stays a single incident (de-duplication); after recovery a new
breach starts a new incident (`IncidentSeq` increments). History of closed
incidents is retained (bounded, newest 50 per rule) and **keeps the config
version it was produced under forever**.

| Rule | Fire condition | Severity |
|------|----------------|----------|
| `freshness` | `now - watermark >= FreshnessStaleMS` | critical |
| `latency` | latest latency `>= WarnAt` / `>= CriticalAt` | warn / critical |
| `throughput` | records/sec over `ThroughputWindowMS` `<= WarnAt` / `<= CriticalAt` | warn / critical |
| `failures` | cumulative failures `>= WarnAt` / `>= CriticalAt` | warn / critical |
| `retries` | cumulative retries `>= WarnAt` / `>= CriticalAt` | warn / critical |
| `sla` | an active run's wall duration `>= SLABreachMS` | critical |

Lifecycle and config semantics:

- **fire** records `FiredAt`, current value, threshold and `ConfigVersion`;
- **severity change** (warn<->critical) writes a durable update to the same
  incident; plain value drift at the same severity is not persisted;
- **recovery** sets `RecoveredAt` and archives the incident;
- **disabling a rule** in a newer config closes the firing incident with
  message `rule disabled by config vN`;
- open alerts are evaluated immediately on `UpdateConfig`, but closed
  history is immutable;
- `UpdateConfig` rejects versions older than the active one.

## Durability and recovery semantics

- Every accepted fact (event/metric) and every alert lifecycle change is a
  frame in an append-only WAL (`wal.log`) and fsync'ed before the API returns.
- A global monotonic sequence orders facts and alert changes in one stream.
- `Snapshot()` atomically writes `snapshot.dat` (tmp+rename+fsync) and
  compacts WAL records already covered by the snapshot (newer records are
  rewritten, not lost).
- On open, the engine loads the newest snapshot, replays the WAL tail, and
  reconciles alerts. Confirmed runs, counters, dedup sets, watermarks and
  alert conclusions survive; duplicates are still rejected after restart and
  the sequence continues strictly forward.
- A torn final frame (crash mid-write) is ignored during replay rather than
  corrupting recovery.
- Rejected input never reaches disk, so it can never resurrect.

Query consistency: all reads take a read lock and return value copies; all
ingest/evaluation happens under the write lock. Readers therefore always
observe one coherent state, never a half-merged one.

## Configuration

`config.Default()` (version 1):

| Field | Default | Meaning |
|-------|---------|---------|
| `LateWindowMS` | 300000 | max event-time lag accepted |
| `FutureSkewMS` | 30000 | max event-time lead accepted |
| `RetentionMS` | 3600000 | terminal-run retention for late/duplicate handling |
| `FreshnessStaleMS` | 120000 | stale-watermark alert threshold |
| `LatencyMS` | warn 1000 / crit 5000 | latency thresholds |
| `ThroughputMinRPS` | warn 1 / crit 0.1 | minimum records/sec |
| `ThroughputWindowMS` | 60000 | throughput window, also bounds stored points |
| `FailuresTotal` | warn 1 / crit 3 | cumulative failures |
| `RetriesTotal` | warn 3 / crit 5 | cumulative retries |
| `SLABreachMS` | 30000 | active-run SLA budget |

Zero threshold/disabled rule means the rule is off. Build new settings with
`cfg.WithVersion(n)`; versions must never decrease.

## Local verification

No external services or dependencies are required.

```sh
# build everything
go build ./...

# full test suite (covers out-of-order, duplicates, late, invalid input,
# state conflicts, config changes, restart recovery, concurrent R/W)
go test ./... -v

# race detector (needs cgo; standard on local Go toolchains)
CGO_ENABLED=1 go test -race -count=1 ./...

# deterministic end-to-end smoke script
go run ./cmd/poe
```

Key tests log the input event/metric, the decision (accepted/rejected), the
machine-readable reason, and the resulting state, so failure output
distinguishes `window_expired` vs `duplicate` vs `invalid_data` vs
`state_conflict`. See `engine/*_test.go` and `store/store_test.go`.
