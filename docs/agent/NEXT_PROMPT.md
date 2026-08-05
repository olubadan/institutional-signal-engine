# Proposed Next Codex Prompt

```text
Continue the Phase 3 implementation verification for `olubadan/institutional-signal-engine`.

Read `AGENTS.md`, `docs/agent/CURRENT_STATE.md`,
`docs/agent/JOURNAL_INDEX.md`, the completed SESSION-0009 record, and the full
draft PR #3 diff. Compare them with live GitHub and Vast state before changing
anything.

SESSION-0010 added stateful Decimal indicator primitives, continuous processing,
typed PostgreSQL event replay, correlated ThetaData acknowledgements, dynamic
universe selection ports, separate candidate counters, and hermetic CI. Verify
the pushed head on the VM and in GitHub Actions, then run a bounded smoke only
if the regular session is open.
Do not infer or purchase a subscription. Keep `TRADING_ENABLED=false`; do not
construct or submit orders, expose ports, reveal credentials, merge PR #3, or
begin execution work.
```
