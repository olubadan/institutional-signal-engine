# Environment Documentation

Phase 2 reproducible setup instructions are in [`VAST_SETUP.md`](VAST_SETUP.md).
The owner-created active instance and sanitized verification evidence are recorded
in [`VAST_INSTANCE.md`](../infrastructure/VAST_INSTANCE.md).

The target is persistent Ubuntu 24.04 with 4 vCPU and 16 GB RAM minimum (8 vCPU
and 32 GB preferred), at least 100 GB persistent storage, reliable networking,
Docker Engine, Docker Compose, Python 3.12, `uv`, Git, GitHub CLI, PostgreSQL
client, Redis client, build tools, and required system libraries.

Before any charge, current candidates, specifications, reliability indicators, and
prices must be presented to the owner for explicit selection or approval.

The active Phase 2 host is an explicitly approved Ubuntu 22.04 exception with a
finite contract end. The bootstrap remains compatible with Ubuntu 24.04 so the
original target can be restored on a replacement host.
