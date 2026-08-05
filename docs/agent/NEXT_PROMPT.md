# Proposed Next Codex Prompt

```text
Continue the Phase 3 implementation verification for `olubadan/institutional-signal-engine`.

Read `AGENTS.md`, `docs/agent/CURRENT_STATE.md`,
`docs/agent/JOURNAL_INDEX.md`, the completed SESSION-0009 record, and the full
draft PR #3 diff. Compare them with live GitHub and Vast state before changing
anything.

SESSION-0011 added quote conflation, bounded classification windows,
material-change evaluation, asynchronous persistence with backpressure,
consumed-quote replay, historical indicator bootstrap/wiring, idempotent
session-boundary handling, and the owner-authorized fixed-window sweep
cluster/session gate with correction and freshness handling. Verify the
pushed head on the VM and in GitHub Actions, then run a bounded smoke only if
the regular session is open; do not claim a live qualifying sweep unless the
feed naturally produces one. Treat ThetaData discovery as
fixture-verified but not live-verified while HTTP 500 remains awaiting
support.
Do not infer or purchase a subscription. Keep `TRADING_ENABLED=false`; do not
construct or submit orders, expose ports, reveal credentials, merge PR #3, or
begin execution work.
```
