# Proposed Next Codex Prompt

```text
Continue SESSION-0009 after I replace or correct the Alpaca key pair directly in
`/etc/institutional-signal-engine/runtime.env` and confirm completion.

Read AGENTS.md, CURRENT_STATE.md, JOURNAL_INDEX.md, and the active SESSION-0009
record first. Reconcile live PR #3 and the Vast checkout. Verify the Alpaca fields
by presence only, preserve root:root mode 0600 and TRADING_ENABLED=false, then
authenticate Alpaca with sanitized reporting. If successful, rerun the bounded
signal-only smoke and record event counts, latency, stale events, rejection
reasons, and ranked-signal counts. Finish the journal and update draft PR #3.

Do not construct or submit orders, enable trading, expose internal ports,
provision infrastructure, reveal secrets, or merge PR #3.
```
