# Institutional Signal Engine

Deterministic, signal-only event-driven system that synchronizes equity and
options market data, evaluates institutional signal gates, and records decisions.

> [!IMPORTANT]
> Phase 3 is signal observation only. Trading remains disabled and this release
> contains no order-construction or order-submission route.

## Phase 3 signal-only slice

The Python 3.12/uv package provides typed canonical events, replaceable Alpaca
and ThetaData adapters, deterministic synchronization, S/F/R/E gates, ranking,
PostgreSQL audit storage, replay, structured logging, and a loopback status
endpoint. Provider credentials are supplied only through the protected Vast
runtime file.

Run the local fixture suite with `make lint-python`, `make typecheck-python`, and
`make test-python`.

## Vast environment

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
