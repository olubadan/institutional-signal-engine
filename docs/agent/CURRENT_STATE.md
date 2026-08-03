# Current State

- **Project mission:** Build a production-oriented, deterministic, real-time
  institutional signal engine using Alpaca equity data and paper execution,
  a replaceable external options-data provider, synchronized normalized events,
  formal S/F/R/E gates, deterministic ranking, traceable persistence,
  observability, replay, and reproducible Vast.ai operation.
- **Current phase:** Phase 1 — Engineering Baseline (corrected; awaiting owner
  review)
- **Completed phases:** Phase 0 — Ground the Current State; Phase 1 — canonical
  documentation baseline
- **Active branch:** `chore/engineering-baseline-bootstrap`
- **Latest substantive commit:** `6dbc9e5ade051be485fd2f63d60f6f27d2661810`.
  The authoritative branch head after session closure is the commit containing
  this snapshot; resolve it from GitHub or `git rev-parse HEAD`.
- **Open pull request:** Draft PR
  [#1](https://github.com/olubadan/institutional-signal-engine/pull/1), clean and
  open against `main`; CI quality check passed.
- **Current architecture:** Documentation-only reference architecture. Approved
  boundaries cover configuration, provider adapters, universe, normalization,
  synchronization, signal state, S/F/R/E gates, ranking, paper execution,
  positions, persistence, observability, and replay. No application exists.
- **Vast environment state:** Not provisioned. Local macOS host has the Vast CLI;
  Vast authentication and available offers have not been inspected.
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
- **Pending approvals:** Owner review and explicit merge authorization for draft PR
  #1; authorization to begin Phase 2; any paid Vast rental; resolution of
  unavailable private-repository branch protection.
- **Known blockers:** Options-provider details are required for Phase 4. GitHub
  branch protection/rulesets remain unavailable on the current private-repository
  plan.
- **Next action:** Owner reviews corrected draft PR #1. If acceptable, the owner
  explicitly authorizes merge in a new instruction. Do not begin Phase 2 or rent a
  Vast instance without separate approval.
