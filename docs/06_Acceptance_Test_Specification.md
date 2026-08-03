# Acceptance Test Specification

Acceptance requires durable behavioral evidence. Compilation or document presence
alone is insufficient for implementation phases.

| Test ID | Phase | Acceptance specification |
| --- | ---: | --- |
| AT-000 | 0 | GitHub access, default branch, branches, PRs, files, workflows, instructions, documentation, environment, and gaps are inventoried without mutation. |
| AT-100 | 1 | Exactly the canonical `00`–`08` documents plus `docs/environment/` and `docs/adr/` exist; links resolve. |
| AT-101 | 1 | The formal-model document contains numbered sections 1–14 in order and the owner-supplied equations/constants for S, F, R, E, candidate selection, execution, returns, break-even, and invariant. |
| AT-102 | 1 | The PR contains no Python source, tests, Python package/lock/version files, or CI workflow. |
| AT-103 | 1 | `AGENTS.md`, PR template, secret exclusions, and concise append-only journal remain present; the PR is draft and unmerged. |
| AT-200 | 2 | Before rental, qualified candidates and prices are presented and owner approval is recorded. The selected persistent Ubuntu 24.04 host meets approved CPU, memory, disk, network, and tool requirements. |
| AT-201 | 2 | Version-controlled setup reproduces the required toolchain and repository verification from a clean Vast environment. |
| AT-300 | 3 | Typed provider, persistence, execution, observability, clock, and replay ports enforce the reference dependency direction; trading defaults disabled. |
| AT-400 | 4 | Provider contract tests cover normal messages, malformed data, timestamps, symbols, rate limits, heartbeat, reconnect, backoff, staleness, duplication, ordering, and provenance. |
| AT-401 | 4 | Authenticated Alpaca paper data and approved options feeds operate without credential disclosure; a sanitized event stream replays deterministically. |
| AT-500 | 5 | Unit tests cover every formal equation, threshold equality boundary, gate transition, episode/freshness transition, ratio boundary, capacity boundary, and invariant. |
| AT-501 | 5 | Ranking tests cover every lexicographic key and full tie; empty `C(t)` yields the unique deterministic no-action state. |
| AT-502 | 5 | Identical ordered replay plus identical versions yields identical ordered decisions. |
| AT-600 | 6 | Endpoint guards reject live endpoints; disabled mode, kill switch, entry cap, take-profit, sizing, capacity, horizon, freshness-loss, and order lifecycle pass paper acceptance tests. |
| AT-700 | 7 | A trace query connects each decision to canonical inputs, configuration, thresholds, engine version, order lifecycle, and outcome. |
| AT-701 | 7 | Health, feed, latency, staleness, gate reasons, orders, fills, exposure, errors, and daily performance are observable without secrets. |
| AT-800 | 8 | Unit, integration, provider contract, replay, failure, reconnection, paper, and invariant suites pass in CI and on Vast using reproducible instructions. |

## Phase 1 verification method

Phase 1 is verified by repository-tree assertions, Markdown structural checks,
formal-section/order checks, prohibited-file scans, secret-value scans, diff
whitespace checks, link/path checks, Git status, branch/commit confirmation, and
live draft-PR confirmation. It intentionally has no Python test suite or CI
workflow.
