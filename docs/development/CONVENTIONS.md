# Development Conventions

## Python and dependencies

- Target Python 3.12 and declare compatibility explicitly.
- Manage and lock dependencies with `uv`; commit `uv.lock`.
- Prefer the standard library until a dependency has a clear operational benefit.
- Pin tool compatibility ranges and update them deliberately.

## Design

- Domain types and decisions are immutable where practical.
- Use explicit, typed interfaces between components.
- Keep I/O at the edges and inject clocks, identifiers, and provider clients.
- Represent time as timezone-aware UTC. Never use naive datetimes.
- Store exact source, receipt, normalization, and decision timestamps.
- Avoid floating-point arithmetic for prices, quantities, money, and thresholds;
  use integer units or `Decimal` under an explicit precision policy.
- Stable ordering must be fully specified, including the final tie-break key.
- Version normalized event schemas, configuration, thresholds, and engine logic.

## Reliability and safety

- Treat missing, stale, malformed, duplicated, and out-of-order data explicitly.
- External calls require bounded timeouts, retry classification, backoff, and
  structured errors. Never retry non-idempotent order submission blindly.
- Execution must fail closed. Trading is disabled unless explicitly enabled for an
  authenticated Alpaca paper endpoint.
- Logs are structured and must not contain secrets.

## Testing

- Unit-test equations, boundaries, invariants, state transitions, and tie-breaks.
- Contract-test each provider adapter against recorded, sanitized fixtures.
- Use deterministic clocks and event IDs in replay tests.
- Add reconnection and failure tests for streams and external services.
- A compilation or import check alone is not acceptance evidence.

## Naming and quality

- Use domain terms from the approved formal specification.
- Ruff formatting and linting, strict mypy, and pytest are mandatory.
- Public modules and non-obvious decisions require concise documentation.
