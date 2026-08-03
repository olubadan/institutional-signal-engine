# Phase Acceptance Criteria

Acceptance requires durable evidence; code compilation alone is insufficient.

## Phase 0 — Ground current state

- GitHub access, default branch, branches, PRs, files, workflows, instructions, and
  documentation are inventoried.
- Existing work and material gaps are identified without repository changes.

Status: satisfied by `SESSION-0001`.

## Phase 1 — Engineering baseline

- Work is isolated on `chore/engineering-baseline-bootstrap`.
- Repository instructions, development conventions, architecture boundaries,
  phase criteria, requirements traceability, and secret policy are documented.
- Python 3.12 packaging and a committed `uv` lockfile are reproducible.
- Formatting, linting, strict typing, tests, and CI use the same commands.
- The append-only Codex journal contains recovered Phase 0 and current Phase 1
  records without secrets.
- One draft PR contains acceptance evidence and is not merged.

## Phase 2 — Vast development environment

- Paid candidates are presented and explicit approval is recorded before rental.
- The selected persistent Ubuntu 24.04 host meets the approved CPU, memory, disk,
  network, and tool requirements.
- Setup is reproducible from version-controlled instructions or automation.
- A clean verification proves required tools and repository checks work on Vast.

## Phase 3 — Application architecture

- Planned boundaries are represented by typed ports and isolated adapters.
- Configuration is validated, redacted, and versioned; trading defaults disabled.
- Deterministic orchestration, replay seams, persistence seams, and test seams are
  demonstrated without real credentials.

## Phase 4 — Data integration

- Alpaca paper data and the approved options provider authenticate without secret
  disclosure.
- Streaming clients demonstrate reconnect, bounded backoff, heartbeat, staleness,
  rate-limit, timestamp, normalization, and provenance behavior.
- Sanitized canonical events can be recorded and replayed deterministically.

## Phase 5 — Formal signal engine

- Every approved equation, boundary, gate transition, invariant, and tie-break has
  tests linked to its requirement.
- Threshold and engine versions are recorded with every decision.
- Identical replay inputs produce byte-stable or semantically identical ordered
  decisions, including the no-candidate state.

## Phase 6 — Paper execution

- Only the authenticated Alpaca paper endpoint is usable.
- Kill switch, disabled mode, entry protection, sizing, capacity, take-profit,
  horizon, and freshness-loss behavior have acceptance tests.
- Intended orders through realized returns are traceable without credentials.

## Phase 7 — Persistence and observability

- PostgreSQL lineage connects every decision to inputs, configuration, and engine
  version; Redis use is justified and non-authoritative.
- Health, feed status, latency, staleness, gates, execution, exposure, errors, and
  daily performance are observable with structured, secret-free output.

## Phase 8 — Verification and release readiness

- Unit, integration, contract, replay, failure, reconnection, paper acceptance,
  and invariant suites pass in CI and on Vast.
- Local and Vast instructions reproduce the verified result.
- Authenticated paper connectivity evidence exists; no live deployment occurs.
