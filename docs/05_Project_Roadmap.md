# Project Roadmap

## Phase sequence

```text
Phase 0 inventory
  -> Phase 1 canonical documentation
  -> Phase 2 approved Vast environment
  -> Phase 3 application architecture
  -> Phase 4 provider integration
  -> Phase 5 formal signal engine
  -> Phase 6 paper execution
  -> Phase 7 persistence and observability
  -> Phase 8 verification and release readiness
```

## Approval gates

| Gate | Required decision | Stop condition |
| --- | --- | --- |
| G1 | Owner accepts the Phase 1 foundational documentation PR | Do not merge or begin foundational implementation without approval |
| G2 | Owner selects a priced Vast candidate | Do not create a charge or provision an instance |
| G3 | Owner supplies provider details and credentials through the approved secret channel | Do not attempt authenticated provider integration |
| G4 | Owner confirms the formal-model source and any later amendments | Do not infer or alter equations |
| G5 | Owner explicitly enables Alpaca paper trading after disabled-mode verification | Do not submit paper orders |
| G6 | Owner separately approves any release or merge | Do not merge or deploy |

## Dependencies and current constraints

- Phase 2 depends on Phase 1 acceptance and a separate paid-instance selection.
- Phase 4 options integration depends on provider URL, subscription details, and
  credentials supplied securely.
- Phase 5 implementation depends on the authoritative model now preserved in
  `03_Formal_Mathematical_Model.md` and on explicit definitions for any operational
  ambiguity discovered during test design.
- Live trading is not a roadmap phase.
