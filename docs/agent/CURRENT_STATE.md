# Current State

- **Project mission:** Build a production-oriented, deterministic, real-time
  institutional signal engine using Alpaca equity data and paper execution,
  a replaceable external options-data provider, synchronized normalized events,
  formal S/F/R/E gates, deterministic ranking, traceable persistence,
  observability, replay, and reproducible Vast.ai operation.
- **Current phase:** Phase 2 — Vast Development Environment (selected offer became
  unavailable at pre-charge revalidation; no instance was created)
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
- **Vast environment state:** Not provisioned. Vast CLI authentication succeeded
  without exposing account data. The owner-selected offer `40176329` / machine
  `138965` returned no exact marketplace record in two pre-charge queries at
  `20260803T183129Z` and `20260803T183142Z`, including one without default
  availability filters. No rental, charge, provisioning, or substitution occurred.
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
- **Pending approvals:** A new live candidate inspection and subsequent explicit
  owner selection/price authorization are required before any Vast charge.
  Resolution of unavailable private-repository branch protection remains pending.
- **Known blockers:** Options-provider details are required for Phase 4. GitHub
  branch protection/rulesets remain unavailable on the current private-repository
  plan.
- **Next action:** With owner authorization, refresh the Vast shortlist read-only
  and stop for a new explicit machine selection. Do not reuse or substitute the
  unavailable offer.
