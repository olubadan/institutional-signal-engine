# Requirements Traceability Matrix

Status values are `Documented`, `Not started`, `Blocked`, and `Verified`.

| Requirement | Source | Design authority | Acceptance evidence | Phase | Status |
| --- | --- | --- | --- | ---: | --- |
| REQ-GOV-001 — GitHub source of truth, focused branches, draft PRs, explicit merge approval | Charter | `00_Project_Charter.md`, `AGENTS.md` | AT-000, AT-103, PR #1 | 0–1 | Verified |
| REQ-GOV-002 — Append-only redacted Codex journal | Owner journal instruction | `AGENTS.md`, `docs/agent/` | AT-103, SESSION-0001–0003 | 1 | Verified |
| REQ-DOC-001 — Canonical Phase 1 documentation scaffold | Owner reconciliation instruction | Canonical `docs/00`–`08`, `environment/`, `adr/` | AT-100 | 1 | Documented |
| REQ-SEC-001 — No secrets in Git or durable output | Charter, SRS | `01_System_Requirements_Specification.md`, `.gitignore` | AT-103 and secret scan | 1–8 | Documented |
| REQ-ENV-001 — Approved persistent Vast environment target | SRS | `docs/environment/README.md` | AT-200, AT-201 | 2 | Not started |
| REQ-PORT-001 — Replaceable provider boundaries | Mission, SRS | `02_Reference_Architecture.md` | AT-300 | 3 | Not started |
| REQ-DATA-001 — Resilient Alpaca equity stream | SRS FR-DATA-001/004 | Equity adapter boundary | AT-400, AT-401 | 4 | Not started |
| REQ-DATA-002 — Resilient replaceable options stream | SRS FR-DATA-002/004 | Options adapter boundary | AT-400, AT-401 | 4 | Blocked: provider details required |
| REQ-DATA-003 — Canonical synchronized events with provenance | SRS FR-DATA-003/004 | Normalization and synchronization boundaries | AT-400, AT-401 | 4 | Not started |
| REQ-SIG-001 — Authoritative indicators and S/F/R/E | Owner formal model | `03_Formal_Mathematical_Model.md` §§1–6 | AT-101, AT-500 | 5 | Documented |
| REQ-SIG-002 — Candidate set, ranking, fire, and no-action decision | Owner formal model | `03_Formal_Mathematical_Model.md` §§7–8, 14 | AT-101, AT-501, AT-502 | 5 | Documented |
| REQ-EXE-001 — Paper-only protected execution and exits | Owner formal model, SRS | `03_Formal_Mathematical_Model.md` §9; execution/position ports | AT-600 | 6 | Documented |
| REQ-PERF-001 — Trade/day returns, Little's Law, break-even | Owner formal model | `03_Formal_Mathematical_Model.md` §§10–13 | AT-101, AT-500 | 5–7 | Documented |
| REQ-AUD-001 — Durable decision and execution lineage | SRS FR-AUD-001/002 | Persistence boundary | AT-700 | 7 | Not started |
| REQ-OBS-001 — Operational and performance telemetry | SRS FR-OBS-001 | Observability boundary | AT-701 | 7 | Not started |
| REQ-VER-001 — Full reproducible verification | Mission, SRS | WBS and roadmap | AT-800 | 8 | Not started |

Update this matrix in the same PR as the requirement, design, implementation, or
evidence it tracks.
