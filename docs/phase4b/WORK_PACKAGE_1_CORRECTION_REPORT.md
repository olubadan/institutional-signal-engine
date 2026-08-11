# Final causal-journal correction handoff

The recovery branch now has one production and deterministic orchestration path.
Its driver independently selects the next provider event, scheduled reevaluation,
disconnect/reconnect input, or intake-stop boundary. Deterministic time no longer
advances as a side effect of market-event delivery.

Every material action is appended at execution time to one immutable,
chain-committed journal. The terminal seal commits to run ID, count, terminal
sequence, and terminal digest. Structural and semantic production verification
must succeed before the one-input acceptance projection can run.

The canonical journal is persisted through a repository boundary, read back,
deserialized, recalculated, and compared. A separately reconstructed sealed
journal produces an exactly equal runtime certificate. Twenty-five focused tests
include the positive path and every required genuine journal corruption class;
no test edits a certificate or assigns a verdict.

Runtime artifacts:

- `/tmp/phase4b-certification/JOURNAL.json`
- `/tmp/phase4b-certification/CERTIFICATE.json`

Exact Git head, clean-worktree status, CI, and PR state are reported separately
as build-envelope evidence. Providers are not run; trading is disabled; orders
remain `0/0`.
