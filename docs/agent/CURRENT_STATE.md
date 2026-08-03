# Current State

- **Project mission:** Build a production-oriented, deterministic, real-time
  institutional signal engine using Alpaca equity data and paper execution,
  a replaceable external options-data provider, synchronized normalized events,
  formal S/F/R/E gates, deterministic ranking, traceable persistence,
  observability, replay, and reproducible Vast.ai operation.
- **Current phase:** Phase 1 — Engineering Baseline (in progress)
- **Completed phases:** Phase 0 — Ground the Current State
- **Active branch:** `chore/engineering-baseline-bootstrap`
- **Latest verified commit:** `cbec064121faeb82245004e69f316179380a8e62`
  (Phase 1 branch point; journal and baseline changes are not yet committed)
- **Open pull request:** None
- **Current architecture:** Not implemented; baseline boundaries are pending in
  this session.
- **Vast environment state:** Not provisioned. Local macOS host has the Vast CLI;
  Vast authentication and available offers have not been inspected.
- **Provider-integration state:** Not implemented. No credentials have been
  requested or stored. The options provider has not been supplied.
- **Test state:** No project tests existed at Phase 0; baseline checks are pending.
- **Approved decisions:** Phase 1 is approved; use branch
  `chore/engineering-baseline-bootstrap`; maintain the permanent append-only Codex
  journal; GitHub remains authoritative; paper trading only; no merges or paid
  Vast provisioning without explicit approval.
- **Pending approvals:** Review of the Phase 1 draft PR; any paid Vast rental;
  resolution of unavailable private-repository branch protection.
- **Known blockers:** The repository does not contain the formal mathematical
  signal specification required for Phase 5. GitHub reports branch protection and
  rulesets are unavailable on the current plan while the repository is private.
- **Next action:** Complete Phase 1 baseline, validate it, open a draft PR, finish
  `SESSION-0002`, and stop for review.
