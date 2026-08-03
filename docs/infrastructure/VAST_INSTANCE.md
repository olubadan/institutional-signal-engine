# Vast Instance Record

## Active Phase 2 instance

- **Status:** Running; owner-created and SSH-accessible
- **Vast instance ID:** `46725769`
- **Machine ID:** `25189`
- **Provisioned:** `20260803T185217Z`
- **Vast contract end:** `20260901T103519Z`
- **Location:** Quebec, Canada
- **Operating system:** Ubuntu 22.04
- **Image:** `docker.io/vastai/kvm:cuda-12.9.1-auto`
- **Rental type:** On-demand
- **Disk allocation:** `130 GB`
- **Vast effective CPU allocation:** `20 vCPU`
- **Runtime-visible CPU allocation:** `19 vCPU`
- **Runtime-visible RAM:** `51,676,916 KiB` (approximately `49.3 GiB`)
- **Vast host RAM field:** `128,722 MB`; the owner dashboard reported `64 GB`
- **Measured offer networking:** `1619.6 Mbps` upload / `2110.0 Mbps` download
- **Static IP:** No; direct SSH is available
- **Base compute price:** `$0.0866666667/hour`
- **Storage component:** `$0.0216666667/hour`
- **Total price:** `$0.1083333333/hour`
- **Public host:** `[REDACTED_PUBLIC_HOST]`
- **SSH port:** `[REDACTED_SSH_PORT]`
- **Trading state:** Disabled

The public connection string is intentionally excluded from Git. Instance and
machine IDs are retained for Vast lifecycle administration; no key, token, or
credential is present here.

## Verified initial state

SSH succeeded using the existing key configuration. PID 1 is systemd, the root
filesystem is ext4, and `/opt` is writable. Docker Engine 29.7.1, Docker Compose
5.4.0, Git, curl, jq, Make, and GCC were already installed. Python was initially
3.10.12; Python 3.12, `uv`, GitHub CLI, PostgreSQL client, and Redis client required
bootstrap installation.

The runtime allocation differs from summary metadata. Reproducible verification
uses resources visible inside the VM, while the Vast/API and owner-reported fields
are retained above for auditability.

## Constraints

- Ubuntu 22.04 is an owner-approved Phase 2 exception to the Ubuntu 24.04 target.
- The contract has a finite end date; migrate or back up state before expiry.
- The host does not have a static IP. Never commit its public connection string.
- This instance is for development and paper-only future work. Live trading is
  prohibited.

See [`VAST_SETUP.md`](../environment/VAST_SETUP.md) and
[`ADR-0001`](../adr/0001-vast-ubuntu-22-exception.md).

## Prior selection attempts

Earlier owner-selected offers `40176329` and `39005761` disappeared before their
mandatory pre-charge checks. Codex created no rental from either offer. Their
timestamped evidence remains in the append-only agent journal and candidate log.
