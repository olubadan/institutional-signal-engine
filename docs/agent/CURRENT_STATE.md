# Current State

- **Project mission:** Build a production-oriented, deterministic, real-time
  institutional signal engine using Alpaca equity data and paper execution,
  a replaceable external options-data provider, synchronized normalized events,
  formal S/F/R/E gates, deterministic ranking, traceable persistence,
  observability, replay, and reproducible Vast.ai operation.
- **Current phase:** Phase 2 — Vast Development Environment completed on the
  feature branch; draft PR #2 awaits owner review and merge approval
- **Completed phases:** Phase 0 — Ground the Current State; Phase 1 — canonical
  documentation baseline; Phase 2 — reproducible Vast environment verified with
  documented Ubuntu 22.04/finite-duration exception
- **Active branch:** `chore/vast-environment-bootstrap`
- **Latest authoritative main commit:** `40f466155236f53cf76d19048507ae4bb1ae50ac`.
  Phase 2 implementation is verified through
  `1b2920ce7a6a305c9b9cfa4f395b4f06862d1a68`; the active branch head after
  session closure is the journal-close commit containing this snapshot.
- **Pull-request state:** PR
  [#1](https://github.com/olubadan/institutional-signal-engine/pull/1) merged into
  `main` with a normal merge commit. Phase 2 PR
  [#2](https://github.com/olubadan/institutional-signal-engine/pull/2) is open,
  draft, cleanly mergeable, and unmerged.
- **Current architecture:** Documentation-only reference architecture. Approved
  boundaries cover configuration, provider adapters, universe, normalization,
  synchronization, signal state, S/F/R/E gates, ranking, paper execution,
  positions, persistence, observability, and replay. No application exists.
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
- **Provider-integration state:** Not implemented. No credentials have been
  requested or stored. The options provider has not been supplied.
- **Test state:** Phase 1 structural checks remain passed. Phase 2 `make setup`
  passed twice; `make lint`, `make typecheck`, `make build`, and `make test` passed
  on Vast. Verification covered resources, tool versions, root-only environment
  files, trading-disabled state, branch, systemd/Docker enablement, container
  health, local connectivity, filesystem/PostgreSQL/Redis restart persistence,
  SSH key-only policy, reconnection, outbound HTTPS, and clean checkout.
- **Approved decisions:** Phase 1 is canonical documentation only; use branch
  `chore/engineering-baseline-bootstrap`; the 14-part owner model is authoritative;
  maintain the append-only Codex journal; GitHub remains authoritative; paper
  trading only; no merge or paid Vast provisioning without explicit approval;
  use owner-created instance `46725769` with the ADR-0001 Ubuntu 22.04 and finite-
  duration exception.
- **Pending approvals:** Owner review and merge approval for draft PR #2;
  provider credentials remain deferred to their integration phase. Private-
  repository branch protection remains pending.
- **Known blockers:** Options-provider details are required for Phase 4. GitHub
  branch protection/rulesets remain unavailable on the current private-repository
  plan.
- **Next action:** Review draft PR #2 and its Phase 2 acceptance evidence. Do not
  merge without explicit owner approval; after merge, begin Phase 3 architecture.
