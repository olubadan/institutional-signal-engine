# Current State

- **Project mission:** Build a production-oriented, deterministic, real-time
  institutional signal engine using Alpaca equity data and paper execution,
  a replaceable external options-data provider, synchronized normalized events,
  formal S/F/R/E gates, deterministic ranking, traceable persistence,
  observability, replay, and reproducible Vast.ai operation.
- **Current phase:** Phase 2 — Vast Development Environment (SESSION-0008 taking
  ownership of owner-created instance `46725769`; SSH inspection pending)
- **Completed phases:** Phase 0 — Ground the Current State; Phase 1 — canonical
  documentation baseline
- **Active branch:** `chore/vast-environment-bootstrap`
- **Latest authoritative main commit:** `40f466155236f53cf76d19048507ae4bb1ae50ac`.
  The Phase 2 candidate-report commit is
  `08469581ff64d3ff13e3005be5166b8d697fafe7`; the active branch head after
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
- **Vast environment state:** Prior Codex rental attempts created no instance. The
  owner now reports instance `46725769` running Ubuntu 22.04 with 20 vCPU, 64 GB
  RAM, 130 GB disk, `$0.108/hour`, and a 28-day maximum; independent inspection
  and bootstrap are in progress. Historical evidence: Vast CLI authentication succeeded
  without exposing account data. The owner-selected offer `40176329` / machine
  `138965` returned no exact marketplace record in two pre-charge queries at
  `20260803T183129Z` and `20260803T183142Z`, including one without default
  availability filters, and remained absent in the owner-authorized retry at
  `20260803T183559Z`. Newly selected offer `39005761` also returned no record in
  standard and no-default exact-ID searches at `20260803T184616Z` and
  `20260803T184627Z`. No rental, charge, provisioning, or substitution occurred.
- **Provider-integration state:** Not implemented. No credentials have been
  requested or stored. The options provider has not been supplied.
- **Test state:** Phase 1 structural checks passed: canonical tracked paths,
  prohibited-artifact absence, formal sections 1–14, exact owner-source comparison
  after allowed layout normalization, required-equation tokens, local Markdown
  links, credential-value scan, and diff whitespace. Python tests and CI are
  intentionally absent from the documentation-only phase.
- **Approved decisions:** Phase 1 is canonical documentation only; use branch
  `chore/engineering-baseline-bootstrap`; the 14-part owner model is authoritative;
  maintain the append-only Codex journal; GitHub remains authoritative; paper
  trading only; no merge or paid Vast provisioning without explicit approval.
- **Pending approvals:** None for bootstrap of existing instance `46725769`.
  Private-repository branch protection remains pending.
- **Known blockers:** Options-provider details are required for Phase 4. GitHub
  branch protection/rulesets remain unavailable on the current private-repository
  plan.
- **Next action:** Inspect existing instance `46725769`, implement reproducible
  Phase 2 bootstrap, verify persistence and health, and update PR #2.
