# Venue Adapter Abstraction Design v1 (core-runtime)

---

## Purpose and scope

This document defines an implementation-facing design note for a future
Venue Adapter Abstraction in `core-runtime` using split capability protocols.

This is a docs-only slice:

- it does not implement adapter APIs or protocols;
- it does not modify production code or tests;
- it does not change runtime behavior;
- it does not implement canonical `FillEvent` ingress;
- it does not expand canonical account feedback beyond current `OrderExecutionFeedbackEvent`;
- it does not change snapshot ingestion behavior;
- it does not change reducers or event taxonomy;
- it does not implement `ProcessingContext`;
- it does not implement replay/storage/EventStream persistence;
- it does not rename packages or directories.

`VADN-01` - `core` remains venue-agnostic and must continue to consume canonical
Event Stream input and explicit configuration through existing boundaries.

`VADN-02` - Adapters expose source capabilities; runtime owns orchestration and
maps capability outputs into canonical `EventStreamEntry` or compatibility paths.

`VADN-03` - This note follows the split capability direction from Phase 7A and
does not introduce implementation API changes in this slice.

---

## Contract references

This note is implementation-facing and must stay consistent with:

- `core/docs/venue-adapter-capability-model-v1.md`
- `core/docs/semantic-core-upgrade-milestone-closure-v1.md`
- `core/docs/runtime-execution-feedback-contract-v1.md`
- `core/docs/runtime-adapter-execution-feedback-source-contract-v1.md`
- `core/docs/post-submission-lifecycle-compatibility-map-v1.md`
- `core/docs/event-stream-cursor-characterization-v1.md`

Current runtime anchors:

- `core-runtime/core_runtime/backtest/adapters/venue.py`
- `core-runtime/core_runtime/backtest/adapters/execution.py`
- `core-runtime/core_runtime/backtest/engine/strategy_runner.py`
- `core-runtime/core_runtime/backtest/engine/event_stream_cursor.py`

---

## Proposed split capability protocols (future, non-implemented)

`VADN-04` - Future abstraction should be split by source responsibility and
authority class rather than one monolithic adapter interface.

Conceptual capability names for future implementation planning:

- `VenueEventWaiter` (or `WakeupSource`)
- `VenueClock` (or runtime clock boundary view)
- `MarketInputSource`
- `OrderSubmissionGateway`
- `OrderSnapshotSource`
- `AccountSnapshotSource`
- `ExecutionFeedbackRecordSource`

`VADN-05` - Names above are conceptual and documentation-facing in this slice;
they do not define production protocol signatures yet.

---

## Capability classification matrix

| capability | responsibility | authority classification | current hftbacktest mapping | future live venue possibility | guardrails / non-goals |
| --- | --- | --- | --- | --- | --- |
| `VenueEventWaiter` / `WakeupSource` | wakeup signaling and wait control for runtime loop progression | runtime/internal only | mapped by `wait_next(...)` wrapper calling `wait_next_feed(...)` | may support richer wakeup sources while preserving runner loop ownership | wakeup signaling is not canonical Event authority; no branch ordering changes in this slice |
| `VenueClock` (runtime clock boundary view) | provide adopted venue-local timestamp axis used by runtime timestamp update | runtime/internal only | mapped by `current_timestamp_ns()` wrapper | may expose richer venue receipt/event-time metadata while runtime keeps canonical ordering by `ProcessingPosition` | clock/timestamp must not be treated as `ProcessingOrder` authority |
| `MarketInputSource` | provide market snapshots/deltas for canonical market mapping | canonical event capable | `read_market_snapshot()` mapped to canonical `MarketEvent` in runner | live adapters may map native book/trade feeds into canonical market events under runtime mapping | no hidden mutable snapshot promotion to canonical semantics outside boundary mapping |
| `OrderSubmissionGateway` | submit/modify/cancel outbound intents and expose dispatch result boundary | canonical event capable (submission boundary), plus runtime/internal transport | `apply_intents(...)`; successful `new` dispatch leads to canonical `OrderSubmittedEvent` | live adapters may provide richer dispatch metadata while preserving current canonical submission boundary semantics | no post-submission execution authority from synchronous return codes |
| `OrderSnapshotSource` | provide raw order snapshots for runtime-local bookkeeping only | runtime/internal only | `read_orders_snapshot()` -> runtime-side bookkeeping (no Core snapshot reducer input) | may remain runtime sidecar where canonical execution feedback is unavailable | no snapshot row payload promotion into Core |
| `AccountSnapshotSource` | provide account snapshots for canonical execution feedback mapping | canonical feedback input + runtime/internal support | `state_values` -> canonical `OrderExecutionFeedbackEvent` | live adapters may offer richer account views without changing Core boundaries | no implicit expansion beyond current canonical feedback event schema |
| `ExecutionFeedbackRecordSource` | provide authoritative execution-feedback records for future canonical `FillEvent` mapping | optional future capability (canonical only after REFC/RAEFSC gates) | unsupported/ineligible today for hftbacktest integration | live adapters may satisfy this with native execution reports and deterministic source sequencing | no `FillEvent` ingress implementation here; no synthetic required-field authority |

---

## hftbacktest capability map (current snapshot)

`VADN-06` - Current hftbacktest integration under this model:

- `MarketInputSource`: supported; canonical `MarketEvent` mapping path exists.
- `OrderSubmissionGateway`: supported for successful `new` dispatch boundary via
  canonical `OrderSubmittedEvent` path.
- `OrderSnapshotSource`: supported; remains runtime-internal bookkeeping only.
- `AccountSnapshotSource`: supported for canonical account-level feedback mapping.
- `VenueEventWaiter` + `VenueClock`: supported through existing wrappers.
- `ExecutionFeedbackRecordSource`: unsupported/ineligible today.

`VADN-07` - Runtime keeps post-submission snapshot handling local; Core receives
only canonical account-level execution feedback.

---

## hftbacktest internals that remain internal

`VADN-08` - The following are adapter/runtime internals and must not be treated
as canonical source semantics:

- rc wakeup codes and branch signaling (`rc == 1/2/3`);
- hftbacktest order/depth object schemas;
- numeric enum mapping (time-in-force/order type) in execution adapter;
- string-to-`int64` order id adaptation at adapter boundary;
- recorder plumbing (`record(...)` wrapper behavior).

---

## Future live venue expansion (non-implemented)

`VADN-09` - A future live adapter may satisfy additional capabilities without
changing `core` semantics:

- native execution reports exposed as `ExecutionFeedbackRecordSource`;
- source-authoritative `liquidity_flag` values;
- stable canonical correlation to `instrument + client_order_id`;
- deterministic strictly monotone non-timestamp `source_sequence`.

`VADN-10` - Runner remains owner of global merge into `EventStreamEntry` with
`ProcessingPosition` ordering authority across canonical categories.

`VADN-11` - `core` remains unchanged and venue-agnostic under this expansion.

---

## Boundary rules

`VADN-12` - `core` must not import runtime adapter classes.

`VADN-13` - `core` must not know hftbacktest-specific APIs or structures.

`VADN-14` - Runtime owns adapter orchestration and capability composition.

`VADN-15` - Runtime owns mapping from adapter capability outputs to canonical
`EventStreamEntry` or compatibility ingestion paths.

`VADN-16` - Adapter capabilities must not mutate `StrategyState` directly.

`VADN-17` - Adapter capabilities must not call `process_event_entry` directly.

`VADN-18` - Adapters expose source capabilities only; semantic authority is
decided at runtime boundary mapping under existing contracts.

---

## Explicit non-goals for this slice

`VADN-19` - No adapter protocol implementation.

`VADN-20` - No runtime branch ordering or wakeup behavior changes.

`VADN-21` - No canonical `FillEvent` ingress implementation.

`VADN-22` - No snapshot row canonicalization into Core event inputs.

`VADN-23` - No canonical feedback schema expansion in this slice.

`VADN-24` - No snapshot lifecycle rewrite.

`VADN-25` - No reducer or event taxonomy changes.

`VADN-26` - No `ProcessingContext` implementation.

`VADN-27` - No replay/storage/EventStream persistence implementation.

`VADN-28` - No package or directory rename.

---

## Future implementation prerequisites (before protocol introduction)

`VADN-29` - Define a protocol-by-protocol migration strategy (incremental,
behavior-preserving) before introducing concrete protocol interfaces.

`VADN-30` - Prepare characterization-first test plan for current behavior and
ordering invariants before abstraction refactors.

`VADN-31` - Require hftbacktest parity tests proving no behavior drift across:

- wakeup semantics;
- market mapping path;
- snapshot compatibility path;
- submission boundary behavior;
- canonical cursor/position sequencing invariants.

`VADN-32` - Require explicit no-behavior-change proof for each migration step.

`VADN-33` - Keep `ExecutionFeedbackRecordSource` gated by REFC/RAEFSC contracts;
no implementation planning should bypass those gate clauses.

---

