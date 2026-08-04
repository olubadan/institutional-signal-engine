# Vast Development Environment Setup

## Scope

This guide bootstraps the Phase 2 development host only. It does not install or
start a trading application. `TRADING_ENABLED=false` is enforced during setup.

The current owner-provisioned host is Ubuntu 22.04. The original Ubuntu 24.04
target remains the desired replacement baseline; see
[`ADR-0001`](../adr/0001-vast-ubuntu-22-exception.md).

## Prerequisites

- Root SSH access using an approved key. Never place a private key in this
  repository or a command argument.
- The repository cloned at `/opt/institutional-signal-engine` on branch
  `chore/vast-environment-bootstrap`.
- Outbound HTTPS access for Ubuntu packages, the pinned `uv` installer, and
  container images.

## Documented commands

Run from `/opt/institutional-signal-engine`:

```bash
make setup
make lint
make typecheck
make build
make test
```

`make setup` is idempotent. It installs the required clients and build tools,
installs `uv` 0.11.28 and a uv-managed Python 3.12, hardens SSH to key-only root
access, generates local PostgreSQL/Redis credentials without printing them, and
enables the dependency service.

The remaining targets provide the Phase 2 quality gates:

- `lint`: ShellCheck and Bash syntax validation.
- `typecheck`: Docker Compose schema/interpolation and systemd unit validation.
- `build`: pull the digest-pinned PostgreSQL 16 and Redis 7 images.
- `test`: verify resources, tools, protected environment files, trading-disabled
  state, repository branch, service health, connectivity, and persistence across
  a dependency-service restart.

## Configuration and secrets

The committed `infra/vast/runtime.env.example` contains variable names only. Setup
installs it as root-owned mode `0600` at
`/etc/institutional-signal-engine/runtime.env.example` and creates the uncommitted
runtime file `/etc/institutional-signal-engine/runtime.env`, also root-owned mode
`0600`.

PostgreSQL and Redis credentials are generated locally. Provider variables remain
empty until their integration phase. Never print or commit the runtime file.

The owner will eventually populate these still-empty variables directly in the
protected runtime file, not in chat or Git:

- `ALPACA_API_KEY_ID`
- `ALPACA_API_SECRET_KEY`
- `OPTIONS_API_KEY`
- `OPTIONS_API_BASE_URL`

They are not required for Phase 2 and were not requested or configured. The
bootstrap supplies the non-secret Alpaca paper URL/feed defaults and keeps
`TRADING_ENABLED=false`.

## Persistent services

`institutional-signal-dependencies.service` is enabled under systemd and manages
local-only PostgreSQL and Redis containers. Both bind only to `127.0.0.1`, use
restart policy `unless-stopped`, and persist data below
`/var/lib/institutional-signal-engine/`.

Useful secret-safe checks:

```bash
systemctl is-enabled institutional-signal-dependencies.service
systemctl is-active institutional-signal-dependencies.service
docker inspect --format '{{.State.Health.Status}}' institutional-signal-postgres
docker inspect --format '{{.State.Health.Status}}' institutional-signal-redis
```

The verification suite restarts the dependency unit and proves that a filesystem
sentinel below `/var/lib/institutional-signal-engine`, a PostgreSQL row, and a Redis
AOF-backed key survive. A full VM reboot is not
required for this check; enablement plus service restart verifies restart recovery
without risking the active SSH path.

## Reconnection and recovery

After an SSH disconnect, reconnect using the owner-managed SSH configuration,
then run:

```bash
cd /opt/institutional-signal-engine
git status --short --branch
sudo bash infra/vast/verify.sh
```

The current host has a 28-day maximum duration. Before expiry, migrate the Git
checkout and `/var/lib/institutional-signal-engine` data to an approved replacement
or backup. GitHub remains the source of truth for versioned work.
