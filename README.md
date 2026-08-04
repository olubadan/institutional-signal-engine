# Institutional Signal Engine

Documentation baseline for a deterministic, event-driven system that will
synchronize equity and options market data, evaluate institutional signal gates,
rank candidates, and support Alpaca paper execution.

> [!IMPORTANT]
> Phase 1 produced the documentation baseline. Phase 2 adds only the verified Vast
> development/runtime environment; no market-data, signal, execution, or live-
> trading implementation is present. Live trading is outside the authorized scope.

## Phase 2 environment

The reproducible Vast bootstrap and verification entrypoints are documented in
[`docs/environment/VAST_SETUP.md`](docs/environment/VAST_SETUP.md). On the approved
host, run `make setup`, `make lint`, `make typecheck`, `make build`, and `make test`.

## Canonical documentation

- [Project Charter](docs/00_Project_Charter.md)
- [System Requirements Specification](docs/01_System_Requirements_Specification.md)
- [Reference Architecture](docs/02_Reference_Architecture.md)
- [Formal Mathematical Model](docs/03_Formal_Mathematical_Model.md)
- [Engineering WBS](docs/04_Engineering_WBS.md)
- [Project Roadmap](docs/05_Project_Roadmap.md)
- [Acceptance Test Specification](docs/06_Acceptance_Test_Specification.md)
- [Requirements Traceability Matrix](docs/07_Requirements_Traceability_Matrix.md)
- [Glossary](docs/08_Glossary.md)
- [Environment documentation](docs/environment/README.md)
- [Architecture decision records](docs/adr/README.md)
- [Codex operating journal](docs/agent/README.md)

GitHub is the source of truth. Work is performed on focused feature branches and
submitted through draft pull requests; merges require explicit owner approval.
