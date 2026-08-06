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
PR #4. The final regular-session catalog run received and mapped 11,956 Alpaca
contracts but selected none because dated open interest or average options
volume evidence was unavailable; do not fabricate either value. Add a typed
liquidity-evidence source or obtain owner-approved equivalent evidence before
retrying the bounded pilot. ThetaData REST discovery remains unavailable,
cross-validation-only, and must not be retried repeatedly. Keep
`TRADING_ENABLED=false`; do not construct or submit orders, expose ports,
reveal credentials, merge PR #4, or begin Phase 5 work.
```
