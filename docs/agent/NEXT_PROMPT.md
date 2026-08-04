# Proposed Next Codex Prompt

```text
Continue SESSION-0009 after I explicitly authorize secure restoration of the six
missing internal PostgreSQL/Redis variables from the still-running containers
into `/etc/institutional-signal-engine/runtime.env`.

Read AGENTS.md, CURRENT_STATE.md, JOURNAL_INDEX.md, and the active SESSION-0009
record first. Reconcile live PR #3 and the Vast checkout. Restore only
POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, REDIS_PASSWORD, DATABASE_URL, and
REDIS_URL without printing or logging values. Preserve provider fields,
root:root mode 0600, and TRADING_ENABLED=false. Restart and verify dependencies,
authenticate Alpaca and ThetaData with sanitized reporting, run the bounded
signal-only smoke, finish the journal, and update draft PR #3.

Do not construct or submit orders, enable trading, expose internal ports,
provision infrastructure, reveal secrets, or merge PR #3.
```
