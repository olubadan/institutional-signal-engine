# Current State

- **Project mission:** Build a production-oriented, deterministic, real-time
  institutional signal engine using Alpaca equity data and paper execution,
  a replaceable external options-data provider, synchronized normalized events,
  formal S/F/R/E gates, deterministic ranking, traceable persistence,
  observability, replay, and reproducible Vast.ai operation.
- **Current phase:** Phase 3 — signal-only vertical slice in progress on
  `feat/signal-only-vertical-slice`
- **Completed phases:** Phase 0 — Ground the Current State; Phase 1 — canonical
  documentation baseline; Phase 2 — reproducible Vast environment verified with
  documented Ubuntu 22.04/finite-duration exception and merged in PR #2
- **Active branch:** `feat/signal-only-vertical-slice`
- **Latest authoritative main commit:** `a3b34c5166040797123e943c23f0428344198ac3`.
  Phase 2 implementation is verified through
  `1b2920ce7a6a305c9b9cfa4f395b4f06862d1a68`; the active branch head after
  session closure is the journal-close commit containing this snapshot.
- **Pull-request state:** PRs #1 and #2 are merged into `main`; Phase 3 PR
  [#3](https://github.com/olubadan/institutional-signal-engine/pull/3) is open,
  draft, cleanly mergeable, and unmerged at
  The branch is receiving the SESSION-0010 implementation correction pass.
- **Current architecture:** Phase 3 now includes typed stateful indicator
  primitives, fail-closed missing-data reasons, continuous event processing,
  correlated Standard exact-contract requests, dynamic-universe selection ports,
  typed PostgreSQL event replay, separate candidate counters, and hermetic CI.
  Execution remains absent.
- **Vast environment state:** Owner-created instance `46725769` is running and
  SSH-accessible. Runtime inspection measured Ubuntu 22.04, 19 visible vCPU,
  approximately 49.3 GiB RAM, and 130 GB allocated disk. Docker/Compose, Python
  3.12, `uv`, Git, GitHub CLI, PostgreSQL/Redis clients, build tools, protected
  configuration, local persistent dependency services, restart recovery,
  connectivity, and SSH reconnection are verified. Trading is disabled. Contract
  end is `20260901T103519Z`. Historical evidence: Vast CLI authentication succeeded
  without exposing account data. The owner-selected offer `40176329` / machine
  `138965` returned no exact marketplace record in two pre-charge queries at
  `20260803T183129Z` and `20260803T183142Z`, including one without default
  availability filters, and remained absent in the owner-authorized retry at
  `20260803T183559Z`. Newly selected offer `39005761` also returned no record in
  standard and no-default exact-ID searches at `20260803T184616Z` and
  `20260803T184627Z`. No rental, charge, provisioning, or substitution occurred.
- **Provider-integration state:** Fixture-testable Alpaca and ThetaData v3
  adapters implemented. Runtime loading is data-only; required provider fields are present in the protected
  runtime file; values were never displayed. Alpaca feed-name parsing is fixed and
  regression-tested. The official Theta Terminal runtime is active with both
  ports loopback-only. Options Standard exact-contract trade streaming is now
  implemented for the supplied AAPL contract; the subscription acknowledgement
  was verified with request ID 1. The 2026-08-05 regular-session smoke received
  80 option trades and 45,184 Alpaca events, with one synchronized input.
- **Test state:** Phase 1 structural checks remain passed. Phase 2 `make setup`
  passed twice; `make lint`, `make typecheck`, `make build`, and `make test` passed
  on Vast. Verification covered resources, tool versions, root-only environment
  files, trading-disabled state, branch, systemd/Docker enablement, container
  health, local connectivity, filesystem/PostgreSQL/Redis restart persistence,
  SSH key-only policy, reconnection, outbound HTTPS, and clean checkout. Phase 3
  checks pass: `uv sync`, Ruff, mypy, 22 pytest tests split across provider,
  reconnection, signal, persistence, and replay groups. ThetaData service/runtime
  startup and loopback binding passed. The six internal runtime variables were
  restored under explicit authority; dependency connectivity and filesystem,
  PostgreSQL, and Redis persistence survived restart. Regular-session live smoke
  passed with zero orders constructed or submitted.
- **Approved decisions:** Phase 1 is canonical documentation only; use branch
  `chore/engineering-baseline-bootstrap`; the 14-part owner model is authoritative;
  maintain the append-only Codex journal; GitHub remains authoritative; paper
  trading only; no merge or paid Vast provisioning without explicit approval;
  use owner-created instance `46725769` with the ADR-0001 Ubuntu 22.04 and finite-
  duration exception.
- **Pending approvals:** Next regular-session exact-contract smoke; private-
  repository branch protection remains pending.
- **Known blockers:** ThetaData MDDS reports CONNECTED and the supplied Standard
  exact-contract stream acknowledges successfully, but the documented AAPL
  contract-list, expirations, and strikes helpers return Jetty HTTP 500. The
  supplied contract is used for validation; dynamic live discovery remains
  unverified. The formal model does not define the eligible-trade set,
  ask-side classification, or five-day resistance/high formula; implementation
  fails closed with explicit reasons at those missing inputs. GitHub branch
  protection/rulesets remain unavailable on the current private-repository plan.
- **Next action:** Run VM/database/CI verification on the pushed SESSION-0010
  head, update PR #3 with sanitized evidence, and keep it draft and unmerged.
