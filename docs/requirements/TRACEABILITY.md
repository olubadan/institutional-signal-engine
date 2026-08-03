# Requirements Traceability

Status values are `Not started`, `In progress`, `Blocked`, and `Verified`.
Evidence must link to tests, documentation, run artifacts, or PR acceptance output.

| ID | Requirement | Phase | Status | Planned verification / current evidence |
| --- | --- | ---: | --- | --- |
| REQ-GOV-001 | GitHub is authoritative; focused branches and unmerged draft PRs govern work | 1 | Verified | Draft PR #1 from `chore/engineering-baseline-bootstrap`; CI run 30838674051 passed |
| REQ-GOV-002 | Permanent append-only, redacted Codex journal records every session | 1 | Verified | `docs/agent/`; SESSION-0001 recovery and SESSION-0002 completion records |
| REQ-SEC-001 | Credentials never enter Git, durable chat output, or logs | 1–8 | In progress | `.gitignore`, `.env.example`, secret policy, diff review |
| REQ-ENV-001 | Persistent Vast host meets approved Ubuntu, CPU, RAM, disk, network, and tool targets | 2 | Not started | Provisioning evidence after explicit approval |
| REQ-DATA-001 | Receive resilient real-time equity market data from Alpaca | 4 | Not started | Adapter contract, integration, reconnect tests |
| REQ-DATA-002 | Receive resilient real-time options data through a replaceable provider adapter | 4 | Blocked | Provider details required; contract and integration tests planned |
| REQ-DATA-003 | Normalize and synchronize streams with provenance and event timestamps | 4 | Not started | Schema, ordering/lateness tests, deterministic replay |
| REQ-SIG-001 | Compute the authoritative liquidity, options-flow, equity, market, and sector indicators | 5 | Blocked | Formal mathematical specification absent |
| REQ-SIG-002 | Evaluate S, F, R, and E gates with deterministic reason codes | 5 | Blocked | Formal mathematical specification absent |
| REQ-SIG-003 | Build C(t), rank lexicographically, fire valid candidates, and record deterministic no-candidate state | 5 | Blocked | Formal mathematical specification and tie-break definition absent |
| REQ-SIG-004 | Version thresholds and preserve identical behavior for identical inputs | 3, 5 | Not started | Configuration schema, replay and invariant tests |
| REQ-EXE-001 | Execute only through Alpaca paper trading with trading disabled by default | 6 | Not started | Endpoint guard and acceptance tests |
| REQ-EXE-002 | Enforce entry protection, exits, sizing, capacity, and kill switch | 6 | Not started | Policy unit and paper acceptance tests |
| REQ-AUD-001 | Record signals, decisions, orders, fills, health, costs, exposure, and performance | 7 | Not started | PostgreSQL integration and lineage tests |
| REQ-AUD-002 | Trace every decision to canonical inputs, configuration version, and engine version | 7 | Not started | Persistence constraints and trace query test |
| REQ-OBS-001 | Expose structured health, feed, latency, staleness, gate, order, fill, exposure, error, and performance telemetry | 7 | Not started | Metrics/log schema and integration tests |
| REQ-VER-001 | Unit, integration, contract, replay, failure, reconnect, paper, and invariant tests run in CI and on Vast | 8 | Not started | CI and Vast test artifacts |

Update this table in the same PR as the implementation and evidence it tracks.
