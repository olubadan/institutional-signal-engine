# Proposed Next Codex Prompt

Independently validate the complete live observational-session path on draft
PR #6 from the pushed exact head. Do not contact providers, access production
credentials, merge, begin Phase 5, enable trading, or construct or submit
orders. Keep PR #5 unchanged at `4a3afe8dd70fecc1b629ff7ef5614a652a572529`.

Reconcile local and GitHub state, then inspect the new `live_session.py`,
`live_session_harness.py`, validation rules resource, tests, and run card.
Confirm that production requires a fresh non-aliased output directory, durable
repository and journal configuration, protected authority key, exact commit,
disabled trading, and zero orders before provider connection. Verify one unified
provider composition carries command transmission, receive-loop acknowledgements,
market events, generation-aware subscription state, reevaluation, disconnect,
reconnect, restoration, 16:00 ET intake stop, drain, finalization, sealed journal,
durable persistence, replay, and manifest-last publication.

Independently attempt missing/rejected/malformed/unmatched/duplicate
acknowledgements, event-before-activation, removal with pending traffic,
disconnect during transition, provider termination, incomplete restoration,
missing durable storage, output collisions/aliases, partial writes, journal
corruption, coherent rehash without trusted authority, mixed/cross-session or
cross-version components, copied receipts, altered final state, trading/order
activity, and certification before finalization. Confirm incomplete evidence
cannot produce a valid manifest or certificate and that the live certificate is
distinct from the accepted hermetic certificate and says the deterministic
scenario did not run.

Run CLI-help/runbook drift checks, Ruff format/lint, strict mypy, lockfile,
complete hermetic tests, focused live-session tests, structural/Bash/ShellCheck/
secret checks, and two accelerated harness executions with byte and semantic
equality comparison. Confirm exact-head CI, PR #6 open/draft/mergeable/unmerged,
PR #5 unchanged, trading disabled, orders `0/0`, no provider access, and no
Phase 5 work.

Do not claim live readiness merely because the hermetic proof passes. Report:

`LIVE PATH BUILD COMPLETE — AWAITING INDEPENDENT VALIDATION`
