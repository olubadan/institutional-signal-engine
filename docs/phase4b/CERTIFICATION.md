# Phase 4B causal-journal certification

Run:

```text
uv run python -m institutional_signal_engine.phase4b_certify \
  --output /tmp/phase4b-certification/CERTIFICATE.json \
  --journal-output /tmp/phase4b-certification/JOURNAL.json
```

The runtime evidence authority is the sealed journal produced by one invocation
of the production `OrchestrationShell` with deterministic injected ports. The
certificate function accepts exactly one `VerifiedJournal`. It reconstructs and
re-verifies that value before projecting membership, subscription command and
acknowledgement correlation, events, independent clock reevaluations, recovery,
lifecycle, persistence, replay, disabled trading, and zero orders.

The journal recalculates payload and complete-record digests, verifies its
previous-record chain, run and sequence consistency, causal-parent existence and
order, terminal seal, and lifecycle bounds. Semantic validation checks the
meaning of discovery, enrichment, epoch, command, acknowledgement, activation,
event, clock, restoration, stop, drain, finalization, persistence, and replay
relationships. Failures have stable production codes.

The persisted artifact is read back through the journal repository, deserialized
into a new object, verified, and compared canonically. The replayed verified
journal must produce exactly the same certificate.

Git head, clean-worktree status, GitHub CI, and pull-request state are external
build-envelope evidence and are deliberately absent from the runtime
certificate. No committed synthetic example certificate is retained. This
hermetic command runs no live provider, keeps trading disabled, and constructs
and submits no orders.
