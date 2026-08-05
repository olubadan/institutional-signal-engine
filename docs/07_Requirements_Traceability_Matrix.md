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
| REQ-DATA-001 — Resilient Alpaca equity stream | SRS FR-DATA-001/004 | `src/.../providers/alpaca.py`, `indicators.py`, `pipeline.py` | AT-400, AT-401, SESSION-0010 | 3–4 | Partially verified: authenticated sequencing and stateful Decimal calculation primitives are tested; live historical-baseline integration remains unverified |
| REQ-DATA-002 — Resilient replaceable options stream | SRS FR-DATA-002/004 | `src/.../providers/thetadata.py`, `infra/vast/*thetadata*` | AT-400, AT-401, SESSION-0009 | 3–4 | Verified: Options Standard exact-contract trade payload implemented and acknowledgement verified; discovery helpers remain HTTP 500 |
| REQ-DATA-003 — Canonical synchronized events with provenance | SRS FR-DATA-003/004 | `src/.../schemas.py`, `synchronization.py`, `quote_book.py` | AT-400, AT-401 | 3 | Verified with typed schemas, O(1) latest-value quote slots, bounded event-time classification windows, deterministic staleness/order handling, and consumed-quote provenance |
| REQ-SIG-001 — Authoritative indicators and S/F/R/E | Owner formal model plus owner SESSION-0010 definitions | `03_Formal_Mathematical_Model.md`, `indicators.py`, `thetadata_conditions.py`, `signals.py` | AT-101, AT-500, SESSION-0010 | 3, 5 | Verified for the three owner-specified definitions, Decimal boundaries, cached resistance ladder, and fail-closed unknown conditions; live discovery remains blocked |
| REQ-SIG-002 — Candidate set, ranking, fire, and no-action decision | Owner formal model | `03_Formal_Mathematical_Model.md`, `src/.../signals.py` | AT-101, AT-501, AT-502 | 3, 5 | Verified with deterministic ranking, explicit rejection reasons, and four persisted candidate counters |
| REQ-EXE-001 — Paper-only protected execution and exits | Owner formal model, SRS | `03_Formal_Mathematical_Model.md` §9; execution/position ports | AT-600 | 6 | Documented |
| REQ-PERF-001 — Trade/day returns, Little's Law, break-even | Owner formal model | `03_Formal_Mathematical_Model.md` §§10–13 | AT-101, AT-500 | 5–7 | Documented |
| REQ-AUD-001 — Durable decision and execution lineage | SRS FR-AUD-001/002 | `persistence.py`, `pipeline.py`, `replay.py`, `persistence_async.py` | AT-700 | 3, 7 | Verified for throughput run `b9e0f47a-a16d-422f-b389-0386d66634ef`: 115 trade events, 91 consumed quotes, 5 decisions, and field-by-field replay equality; trigger reasons, synchronized-state identities, run counters, and quote roles persist. |
| REQ-OBS-001 — Operational and performance telemetry | SRS FR-OBS-001 | `src/.../observability.py`, `pipeline.py`, `live_smoke.py`, `persistence_async.py` | AT-701 | 3, 7 | Verified for secret-free loopback health/status, provider/event-kind age/queue/processing/total timing distributions, quote conflation counters, evaluation skips/triggers, queue depth, database-write latency, and backpressure counters |
| REQ-VER-001 — Full reproducible verification | Mission, SRS | WBS and roadmap, `.github/workflows/ci.yml` | AT-800 | 8 | In progress: hermetic CI workflow added; hosted run and live VM evidence pending |

Update this matrix in the same PR as the requirement, design, implementation, or
evidence it tracks.
