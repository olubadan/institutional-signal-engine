# Proposed Next Codex Prompt

```text
Phase 4 is merged at b62d017dc572fd4dfbef99afccfc3f01b4399840. SESSION-0024
repairs the Phase 4B ignition harness on feat/phase-4b-impact-shadow: it uses
the typed database/authentication paths, an async persistence drain probe, and
the live discovery/enrichment/coverage planner to produce a protected prepared
plan. Verify exact-head CI, deploy the exact head, run ignition, and launch a
partial observation only if ignition passes and regular-session time remains.
The first recovery attempt failed closed before WebSocket connection because a
second discovery/enrichment pass diverged from the ignition plan. Correct the
handoff so the live runner consumes the exact ignition plan or receives a
complete persisted input snapshot; then run exact-head CI and repeat the
pre-open observation. Do not retry ThetaData REST/OI endpoints, construct
orders, merge PR #5, or begin Phase 5.

The published draft is PR #5 at the current branch tip with exact-head CI
passing. Monday runbook: docs/phase4b/MONDAY_RUNBOOK.md.
```
