# Engineering Work Breakdown Structure

| WBS | Phase | Deliverables | Exit condition |
| --- | --- | --- | --- |
| 0 | Ground current state | Repository, environment, branch, PR, workflow, and gap inventory | Read-only findings recorded |
| 1 | Engineering baseline | Canonical documentation, repository instructions, journal, PR template, secret exclusions | Documentation-only draft PR reviewed; no implementation artifacts |
| 2 | Vast development environment | Approved instance selection, reproducible setup, environment guide, tool verification | Clean Ubuntu 24.04 environment reproduces checks; rental was explicitly approved |
| 3 | Application architecture | Configuration boundary, typed ports, canonical event schemas, orchestration seams, disabled execution mode | Boundary and architecture acceptance tests pass without credentials |
| 4 | Data integration | Alpaca paper-data adapter, options-provider adapter, normalization, synchronization, resiliency, replay format | Authenticated feeds and deterministic sanitized replay verified |
| 5 | Formal signal engine | Indicators, S/F/R/E, candidate set, ranking, fire/no-action decisions, versioned thresholds | Equation, boundary, invariant, and replay tests pass |
| 6 | Paper execution | Entry protection, sizing, capacity, exits, kill switch, paper order lifecycle | Authenticated paper-only acceptance tests pass |
| 7 | Persistence and observability | PostgreSQL lineage, justified transient Redis, logs, metrics, health, exposure, daily summaries | Decision trace and operational telemetry verified |
| 8 | Verification and release readiness | Unit, integration, contract, replay, failure, reconnection, paper, invariant, CI, and Vast evidence | All required suites pass; no live deployment |

## Work-package rules

- Each phase uses a focused feature branch and draft PR.
- Each implementation work package updates its canonical requirement, architecture,
  acceptance, and traceability documentation.
- Credentials and paid provisioning require the specific approval gates defined in
  the charter and roadmap.
- Later-phase work cannot silently alter the formal model; owner-approved changes
  must update the authoritative source and traceability explicitly.
