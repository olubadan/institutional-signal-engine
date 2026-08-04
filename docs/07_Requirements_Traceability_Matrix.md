# Requirements Traceability Matrix

Status values are `Documented`, `Not started`, `Blocked`, and `Verified`.

| Requirement | Source | Design authority | Acceptance evidence | Phase | Status |
| --- | --- | --- | --- | ---: | --- |
| REQ-GOV-001 — GitHub source of truth, focused branches, draft PRs, explicit merge approval | Charter | `00_Project_Charter.md`, `AGENTS.md` | AT-000, AT-103, PR #1 | 0–1 | Verified |
| REQ-GOV-002 — Append-only redacted Codex journal | Owner journal instruction | `AGENTS.md`, `docs/agent/` | AT-103, SESSION-0001–0003 | 1 | Verified |
| REQ-DOC-001 — Canonical Phase 1 documentation scaffold | Owner reconciliation instruction | Canonical `docs/00`–`08`, `environment/`, `adr/` | AT-100 | 1 | Documented |
| REQ-SEC-001 — No secrets in Git or durable output | Charter, SRS | `01_System_Requirements_Specification.md`, `.gitignore` | AT-103 and secret scan | 1–8 | Documented |
| REQ-ENV-001 — Approved persistent Vast environment target | SRS | `docs/environment/VAST_SETUP.md`, `docs/infrastructure/VAST_INSTANCE.md`, ADR-0001 | AT-200 | 2 | Blocked: owner-approved Ubuntu 22.04/finite-duration exception does not fully meet the Ubuntu 24.04 persistent target |
| REQ-ENV-002 — Reproducible Vast toolchain, dependency services, and recovery checks | Mission, SRS | `Makefile`, `infra/vast/`, `docs/environment/VAST_SETUP.md` | AT-201, SESSION-0008/0009 | 2–3 | Verified: protected restoration preserved provider fields; dependency connectivity and filesystem/PostgreSQL/Redis restart persistence passed |
| REQ-PORT-001 — Replaceable provider boundaries | Mission, SRS | `02_Reference_Architecture.md`, `src/.../ports.py` | AT-300 | 3 | Verified: typed ports and inward dependency direction |
| REQ-DATA-001 — Resilient Alpaca equity stream | SRS FR-DATA-001/004 | `src/.../providers/alpaca.py` | AT-400, AT-401 | 3–4 | Blocked: contract/feed-name/handshake tests pass, but live credentials were rejected by Alpaca |
| REQ-DATA-002 — Resilient replaceable options stream | SRS FR-DATA-002/004 | `src/.../providers/thetadata.py`, `infra/vast/*thetadata*` | AT-400, AT-401 | 3–4 | Partially verified: official Terminal authenticated, active, and loopback-only; socket connected but emitted zero events in the bounded smoke |
| REQ-DATA-003 — Canonical synchronized events with provenance | SRS FR-DATA-003/004 | `src/.../schemas.py`, `synchronization.py` | AT-400, AT-401 | 3 | Verified with typed schemas and deterministic staleness/order handling |
| REQ-SIG-001 — Authoritative indicators and S/F/R/E | Owner formal model | `03_Formal_Mathematical_Model.md`, `src/.../signals.py` | AT-101, AT-500 | 3, 5 | Verified for implemented signal-only path |
| REQ-SIG-002 — Candidate set, ranking, fire, and no-action decision | Owner formal model | `03_Formal_Mathematical_Model.md`, `src/.../signals.py` | AT-101, AT-501, AT-502 | 3, 5 | Verified with deterministic ranking and explicit rejection reasons |
| REQ-EXE-001 — Paper-only protected execution and exits | Owner formal model, SRS | `03_Formal_Mathematical_Model.md` §9; execution/position ports | AT-600 | 6 | Documented |
| REQ-PERF-001 — Trade/day returns, Little's Law, break-even | Owner formal model | `03_Formal_Mathematical_Model.md` §§10–13 | AT-101, AT-500 | 5–7 | Documented |
| REQ-AUD-001 — Durable decision and execution lineage | SRS FR-AUD-001/002 | `src/.../persistence.py`, `replay.py` | AT-700 | 3, 7 | Verified for normalized inputs, decisions, reasons, versions, and replay; query export follows |
| REQ-OBS-001 — Operational and performance telemetry | SRS FR-OBS-001 | `src/.../observability.py` | AT-701 | 3, 7 | Verified for secret-free loopback health/status; feed metrics follow |
| REQ-VER-001 — Full reproducible verification | Mission, SRS | WBS and roadmap | AT-800 | 8 | Not started |

Update this matrix in the same PR as the requirement, design, implementation, or
evidence it tracks.
