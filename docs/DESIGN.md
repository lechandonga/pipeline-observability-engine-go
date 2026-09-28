# Design notes: consistency under disorder, duplicates, restart and concurrency

This document states the invariants the implementation relies on and where
they are enforced.

## 1. Facts vs conclusions

- A **fact** is an accepted `Event` or `Metric` (caller-provided `ID`).
- A **conclusion** is a derived `Alert` (fire / severity change / recovery).
- Facts are the source of truth and are replayed first; conclusions are
  persisted too so restart cannot drop an in-flight incident, but after
  replay the manager also reconciles facts under the active config to
  self-heal a conclusion whose final durable write was lost in a crash.

## 2. Validation order is part of the API contract

For each ingest call, checks short-circuit in this order:

```
invalid_data -> window_expired -> future_skew -> duplicate -> state_conflict
```

This matters when several conditions hold at once (e.g. an invalid event
that is also late): callers can rely on the precedence, and tests assert on
the exact machine-readable `RejectReason`. Rejections have no side effects
and are never written to the WAL.

## 3. Idempotency

- Dedup key is `(pipeline, ID)` for both events and metrics.
- The dedup set is inserted **only after** the fact has been fsync'ed to the
  WAL, so a failed write never "spends" the ID.
- The set is itself persisted (snapshot + WAL replay), therefore resends
  after restart are still `duplicate`.
- Resending an ID with a modified payload is still a duplicate; the first
  accepted payload is the one that counts.

## 4. Time

- Event time is authoritative for merging; wall clock (`clock.Clock`) is
  only used for the lateness window, freshness and SLA. The clock is
  injectable, so tests are fully deterministic.
- Lateness window: `now - eventTime <= LateWindowMS`. Future skew guard:
  `eventTime - now <= FutureSkewMS`. Boundaries are inclusive.
- Within the window, arrival order cannot change the merged result (see
  `TestMergeIsOrderInvariantWithinWindow`,
  `TestOutOfOrderLatencyDeterministic`). Out-of-window input is rejected and
  cannot change established terminal state.
- Per-pipeline watermark = max accepted event time.

## 5. Run state machine

```
(absent) --run_started--> active
active --run_succeeded------> succeeded (terminal)
active --run_failed---------> failed    (terminal)
active --retry_scheduled----> active (retries++)
active --retry_succeeded----> succeeded (terminal)
```

- A non-start event for an unknown run is `state_conflict`.
- Any lifecycle event after the terminal state is `state_conflict`; the
  terminal `EndedAt` and status are immutable.
- Durability-before-mutation: the WAL append happens before run/pipeline
  state is changed, and transitions were validated before that, so a write
  failure leaves the engine exactly as it was (no partial run).

## 6. Alerts

- Identity `pipeline|rule`; repeated breach while firing produces zero new
  incidents.
- Every incident numbers its episodes (`IncidentSeq`, 1-based per
  fingerprint) so "fire again after recovery" is distinguishable from the
  old incident while the closed record stays in history.
- Only lifecycle changes (fire, warn<->critical, recover, disable-close) are
  durable; same-severity value refresh is an in-memory update only.
- Every stored alert records `ConfigVersion`. Historical (recovered) alerts
  are never rewritten by later config; an open alert keeps the version it
  fired under until it recovers.
- Config versions are non-decreasing; `UpdateConfig` with an older version
  is an error.

## 7. Concurrency

- One `sync.RWMutex` guards all engine state; queries take `RLock`, ingests
  and config changes take the full lock. Queries return value copies.
- The alert manager has its own lock and is always invoked while the engine
  lock is held; locks are never taken in the opposite order (manager ->
  engine), so there is no lock-ordering deadlock.
- The global durable sequence is assigned under the write lock; all
  goroutines observe a strictly increasing, gap-free-where-durable order.
- Bounded work inside the critical section: throughput points are pruned to
  the window on each write; terminal runs are pruned past retention lazily;
  alert history is capped per fingerprint. No unbounded scans sit on the
  query path (`TestQueryTimeBoundedByRetention` asserts sub-millisecond
  queries after thousands of historical facts).

## 8. Persistence format and failure modes

- `wal.log` frames: `u32 length | i64 sequence | u8 kind | JSON payload`,
  fsync per append. Kinds: event(1), metric(2), alert(3).
- `snapshot.dat`: `i64 sequence | JSON snapshotState`, written via
  `snapshot.tmp` + `fsync` + `rename` (+ dir fsync). SaveSnapshot then
  rewrites the WAL keeping only records newer than the snapshot sequence.
- Replay ignores a torn trailing frame (partial write of a crash) and
  refuses structurally corrupt (absurd length) frames loudly.
- Close does not write ahead; durability is per-ingest fsync, so hard
  crashes lose at most un-acknowledged input, never confirmed state.

## 9. Known scope limits

- The dedup set grows with the number of accepted unique IDs; it is required
  for exactly-once semantics and is persisted. Retention prunes runs and
  windowed metric points, not the dedup set. At very large cardinalities,
  sharding/rotating data dirs would be the next step.
- SLA covers long-running active runs (and recovers when the run ends).
  SLA for already-finished runs is observable via run `EndedAt-StartedAt`
  but is not a persistent alert type.
