# Temporal evidence and current implementation mapping

The foundation preserves clocks as separate fields. It does not manufacture a
clock when the current runtime does not record one.

| Evidence field | Current source | Status |
| --- | --- | --- |
| provider/event timestamp | `CanonicalEvent.source_timestamp` | available |
| provider sequence/order | `CanonicalEvent.sequence` and `ingest_order` | available |
| local receive wall clock | `CanonicalEvent.received_timestamp` | available |
| local monotonic receive clock | provider normalizers record `_received_monotonic_ns` in the canonical payload | available for newly normalized provider events |
| normalization timestamp | `CanonicalEvent.normalized_timestamp` | available |
| canonical acceptance timestamp | pipeline records `_canonical_acceptance_timestamp` at first pipeline admission | available for newly processed events |
| processing timestamp | `EventTiming.processing_timestamp` | available |
| processing completion timestamp | `EventTiming.processing_completed_timestamp` | available in timing telemetry |
| emission timestamp | pipeline annotates `NEW_QUALIFYING_SWEEP` transition audits | available for newly emitted detections |
| source, symbol, sequence | canonical event fields | available |
| precision/timezone conversion | event `timestamp_conversion` payload and `EventTiming` | available where provider adapter supplies it |
| duplicate/out-of-order status | pipeline metrics and sweep reasons | aggregate/status evidence; not a complete per-receipt clock |
| durable receipt identity | canonical `event_id` and persisted event receipt | available |

## Option `t0` lifecycle

The current `SweepEngine` emits `first_constituent_timestamp` from the first
event-time constituent and `last_constituent_timestamp` from the latest
constituent currently in the cluster. The compatible `map_existing_t0` function
binds the first to `t0_onset`. It binds `close_timestamp` to
`t0_cluster_complete` only when the audit is `final`; an active cluster is still
growing and therefore has no completed-cluster clock yet. `last_constituent_timestamp`
remains evidence about the episode, not a substitute for closure.

`qualification_state` and transition names such as
`NEW_QUALIFYING_SWEEP` prove a qualification state transition. The runtime now
records the event-time `qualification_timestamp` on that transition when the
existing rule first becomes true. Because the rule is evaluated on every
constituent, qualification may precede later cluster constituents and closure.
The pipeline records `emission_timestamp` on the transition when the detection
is emitted. Historical audits without these fields remain explicit gaps.

The `T0Lifecycle` validation rules require:

```text
t0_onset <= t0_qualify <= t0_emit
t0_onset <= t0_cluster_complete
```

only for clocks that are actually present. `economic_detection_clock` uses
`t0_emit`, then `t0_qualify`; it refuses to substitute `t0_onset`. This keeps a
scientific onset clock separate from an actionable detection clock.
