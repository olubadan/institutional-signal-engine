# Proposed Next Codex Prompt

```text
Continue the Phase 3 provider checkpoint for `olubadan/institutional-signal-engine`.

Read `AGENTS.md`, `docs/agent/CURRENT_STATE.md`,
`docs/agent/JOURNAL_INDEX.md`, the completed SESSION-0009 record, and the full
draft PR #3 diff. Compare them with live GitHub and Vast state before changing
anything.

The final SESSION-0009 smoke authenticated Alpaca and received 7,500 events.
Theta Terminal authenticated and acknowledged the stream request, but produced
zero events because the account reported Options Standard while the requested
bulk option-trade stream requires Options Pro. Contract discovery also returned
HTTP 500 during the regular session.

Ask the owner to choose one path: confirm/enable Options Pro for the existing
bulk adapter, or authorize a focused follow-up that implements exact-contract
Options Standard subscriptions after ThetaData contract discovery is healthy.
Do not infer or purchase a subscription. Keep `TRADING_ENABLED=false`; do not
construct or submit orders, expose ports, reveal credentials, merge PR #3, or
begin execution work.
```
