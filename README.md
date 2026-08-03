# Institutional Signal Engine

Deterministic, event-driven infrastructure for synchronizing equity and options
market data, evaluating institutional signal gates, ranking candidates, and
supporting Alpaca paper execution.

> [!IMPORTANT]
> This repository is in its engineering-baseline phase. It does not yet contain a
> working signal engine or trading integration. Trading must remain disabled by
> default, and live trading is outside the authorized scope.

## Development baseline

- Python 3.12
- [`uv`](https://docs.astral.sh/uv/) for environments and dependency locking
- Ruff for linting and formatting
- mypy for strict static analysis
- pytest for automated verification
- GitHub Actions for pull-request checks

```bash
uv sync --all-groups --frozen
uv run ruff format --check .
uv run ruff check .
uv run mypy src tests
uv run pytest
```

Copy `.env.example` to a protected local `.env` only when a later phase requires
runtime configuration. Never commit the resulting file or disclose its values.

## Project documentation

- [Repository operating instructions](AGENTS.md)
- [Contribution conventions](CONTRIBUTING.md)
- [Architecture boundaries](docs/architecture/BOUNDARIES.md)
- [Phase acceptance criteria](docs/requirements/ACCEPTANCE_CRITERIA.md)
- [Requirements traceability](docs/requirements/TRACEABILITY.md)
- [Secret-handling policy](docs/security/SECRET_HANDLING.md)
- [Codex operating journal](docs/agent/README.md)

GitHub is the source of truth. Work is performed on focused feature branches and
submitted through draft pull requests; merges require explicit owner approval.
