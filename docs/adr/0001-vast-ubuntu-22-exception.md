# ADR-0001: Use the owner-provisioned Ubuntu 22.04 Vast instance for Phase 2

- **Status:** Accepted for Phase 2 development only
- **Date:** 2026-08-03
- **Decision owner:** Repository owner

## Context

The baseline target specifies Ubuntu 24.04 and long-lived persistent capacity. The
owner supplied an already-running Vast instance using Ubuntu 22.04 with a maximum
duration of 28 days and explicitly directed Phase 2 to advance on that machine.
Provisioning another instance is prohibited.

## Decision

Use instance `46725769` for Phase 2 development and runtime-environment validation.
The bootstrap supports Ubuntu 22.04 and 24.04, installs Python 3.12 independently
through pinned `uv`, and keeps trading disabled. Treat the 28-day limit as an
operational migration deadline, not as durable production hosting.

## Consequences

- AT-200 and REQ-ENV-001 are only partially satisfied because Ubuntu 24.04 and
  indefinite persistence are not demonstrated.
- The environment remains reproducible on a later Ubuntu 24.04 replacement.
- State must be backed up or migrated before the instance duration expires.
- No application or trading behavior is authorized by this decision.
