# Project Charter

## Mission

Build an end-to-end, production-oriented Real-Time Institutional Signal Engine
using GitHub as the source of truth and a persistent Vast.ai instance as the
development and runtime environment.

## Target outcome

The completed system will:

1. receive real-time equity market data from Alpaca;
2. receive real-time options data through a replaceable provider adapter;
3. normalize and synchronize both streams;
4. compute the authoritative formal signal model;
5. identify valid, fresh, executable institutional signals;
6. rank candidates deterministically;
7. support Alpaca paper trading only;
8. record inputs, signals, decisions, orders, fills, health, and performance;
9. run reproducibly on a persistent Vast.ai environment; and
10. be developed, tested, reviewed, and released through GitHub.

## Scope and exclusions

The project includes engineering documentation, environment reproducibility,
provider-neutral adapters, deterministic signal policy, paper execution,
persistence, observability, replay, backtesting, and verification.

The following are excluded unless separately and explicitly approved:

- live trading or any live-broker endpoint;
- paid Vast.ai rental before candidate presentation and owner selection;
- undocumented changes to the owner-supplied mathematical model;
- credentials in Git, durable logs, issues, pull requests, or documentation; and
- merging a pull request without owner approval.

Phase 1 is documentation-only. Application code, package manifests, tests, CI
workflows, provider integration, and infrastructure provisioning belong to later
approved implementation phases.

## Engineering governance

- GitHub is authoritative; never commit directly to the default branch.
- Use focused branches, intentional commits, and one draft PR per major phase.
- Keep requirements, design decisions, acceptance evidence, and traceability
  current in the same PR as the work they govern.
- Preserve deterministic behavior: identical ordered inputs and configuration must
  produce identical decisions.
- Use UTC for system time and retain source, receipt, normalization, and decision
  timestamps with provenance.
- Keep providers behind explicit ports so vendors can be replaced without changing
  signal policy.
- Use exact decimal or integer representations for money, prices, quantities, and
  thresholds under a documented precision policy.
- Trading defaults disabled, fails closed, and may target only verified Alpaca
  paper endpoints.
- Never rewrite or remove unrelated owner work.

## Definition of done

A deliverable is complete only when its requirements and boundaries are documented,
relevant acceptance evidence passes, traceability is updated, secrets are absent,
operational effects are recorded, the Codex journal is current, and the draft PR
remains available for owner review.

## Authority hierarchy

1. The owner's latest explicit instruction governs scope.
2. The authoritative formal model in `03_Formal_Mathematical_Model.md` governs
   signal mathematics.
3. This canonical documentation set governs engineering intent and acceptance.
4. `AGENTS.md` and the append-only journal govern Codex operation.
