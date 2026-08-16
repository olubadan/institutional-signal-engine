# Temporal evidence and current implementation mapping

The foundation preserves clocks as separate fields. It does not manufacture a
clock when the current runtime does not record one.

Clock domains are explicit: provider event timestamps belong to the provider
event clock; receive, admission, processing, and emission wall timestamps belong
to the local wall clock; monotonic ingress and normalization values belong to
the local monotonic clock. Provider-to-local numeric age is an estimate with
clock-offset uncertainty, not a causal ordering proof. The temporal model
rejects cross-domain duration calculations unless synchronization is explicitly
provided. Existing `event_age_at_receipt_ms` is therefore a local-wall age
estimate for observability, not a synchronized provider-to-local duration.

| Evidence field | Current source | Status |
| --- | --- | --- |
| provider/event timestamp | `CanonicalEvent.source_timestamp` | available |
| provider sequence/order | `CanonicalEvent.sequence` and `ingest_order` | available |
| local receive wall clock | `CanonicalEvent.received_timestamp` | available |
| local monotonic receive clock | socket-loop ingress records `_received_monotonic_ns` immediately after frame receipt | available for newly received provider frames |
| normalization monotonic clock | normalizers record `_normalized_monotonic_ns` after message parsing | available for newly normalized events |
| normalization timestamp | `CanonicalEvent.normalized_timestamp` | available |
| pipeline admission timestamp | pipeline records `_pipeline_admission_timestamp` at first pipeline admission | available for newly processed events |
| durable journal acceptance timestamp | append-only `canonical_event_receipts` records local wall time after canonical insert/flush; in-memory repository records its admission receipt | available for new journal writes; historical receipts may be absent |
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
