# Current State

- **Project mission:** Build a production-oriented, deterministic, real-time
  institutional signal engine using Alpaca equity data and paper execution,
  a replaceable external options-data provider, synchronized normalized events,
  formal S/F/R/E gates, deterministic ranking, traceable persistence,
  observability, replay, and reproducible Vast.ai operation.
- **Current phase:** Phase 1 — Engineering Baseline (complete; awaiting owner review)
- **Completed phases:** Phase 0 — Ground the Current State; Phase 1 — Engineering
  Baseline
- **Active branch:** `chore/engineering-baseline-bootstrap`
- **Latest implementation commit:** `abc14ea5aaeed89b2987788989343de6f5ae5781`.
  The authoritative branch head after session closure is the commit containing
  this snapshot; a commit cannot embed its own SHA, so resolve it from GitHub or
  `git rev-parse HEAD`.
- **Open pull request:** Draft PR
  [#1](https://github.com/olubadan/institutional-signal-engine/pull/1), clean and
  open against `main`; CI quality check passed.
- **Current architecture:** No application behavior is implemented. Approved
  ports-and-adapters boundaries cover configuration, provider adapters, universe,
  normalization, synchronization, signal state, S/F/R/E gates, ranking, paper
  execution, positions, persistence, observability, and replay.
- **Vast environment state:** Not provisioned. Local macOS host has the Vast CLI;
  Vast authentication and available offers have not been inspected.
- **Provider-integration state:** Not implemented. No credentials have been
  requested or stored. The options provider has not been supplied.
- **Test state:** Local frozen sync, lock validation, Ruff format/lint, strict mypy,
  pytest (1 passed, 100% measured coverage), package build, CI YAML parsing, diff
  whitespace, ignore behavior, and credential-pattern checks passed. GitHub Actions
  run 30838674051 passed all quality steps on Ubuntu 24.04 with no annotation.
- **Approved decisions:** Phase 1 is approved; use branch
  `chore/engineering-baseline-bootstrap`; maintain the permanent append-only Codex
  journal; GitHub remains authoritative; paper trading only; no merges or paid
  Vast provisioning without explicit approval.
- **Pending approvals:** Owner review and explicit merge authorization for draft PR
  #1; authorization to begin Phase 2; any paid Vast rental; resolution of
  unavailable private-repository branch protection.
- **Known blockers:** The repository does not contain the formal mathematical
  signal specification required for Phase 5. GitHub reports branch protection and
  rulesets are unavailable on the current plan while the repository is private.
- **Next action:** Owner reviews draft PR #1. If acceptable, the owner explicitly
  authorizes its merge and Phase 2 read-only Vast capability/offer inspection. No
  instance may be rented without a subsequent explicit selection or approval.
