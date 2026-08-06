# Proposed Next Codex Prompt

```text
Continue Phase 4 live multi-symbol universe validation for
`olubadan/institutional-signal-engine`.

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
PR #3 is merged into main. Continue on `feat/phase-4-live-universe` and draft
PR #4. SESSION-0016 added two-stage coarse shortlist and Alpaca snapshot
enrichment. The owner then authorized observation subscriptions without OI:
use `phase4-sweep-observation-without-oi-v1`; missing OI must remain null for
the signal ratio and block complete S/F/R/E. The single bounded ThetaData OI
snapshot diagnostic returned HTTP 500 while MDDS was CONNECTED; do not retry
it today. Run the regular-session observation with
`--skip-oi-diagnostic`. Contract/expiration/strike discovery remains
cross-validation-only and must not be retried repeatedly. Keep
`TRADING_ENABLED=false`; do not construct or submit orders, expose ports,
reveal credentials, merge PR #4, or begin Phase 5 work.
```
