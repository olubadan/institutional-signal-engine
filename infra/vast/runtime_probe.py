#!/usr/bin/env python3
"""Verify protected runtime data without shell parsing or credential arguments."""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

RUNTIME = Path("/etc/institutional-signal-engine/runtime.env")
STATE = Path("/var/lib/institutional-signal-engine")


def run(command: list[str], environment: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, env=environment, check=False, capture_output=True, text=True)


def fail(message: str) -> int:
    print(f"[verify] FAIL: {message}", file=sys.stderr)
    return 1


def read_env_file(path: Path) -> dict[str, str]:
    """Read runtime assignments as data; this verifier has no third-party imports."""
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, value = line.split("=", 1)
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        values[name.strip()] = value
    return values


def healthy(container: str, environment: dict[str, str]) -> bool:
    for _ in range(60):
        if (
            run(
                ["docker", "inspect", "--format", "{{.State.Health.Status}}", container],
                environment,
            ).stdout.strip()
            == "healthy"
        ):
            return True
        time.sleep(2)
    return False


def main() -> int:
    values = read_env_file(RUNTIME)
    if values.get("TRADING_ENABLED") != "false":
        return fail("trading is not disabled")
    print("[verify] PASS: trading disabled")
    environment = os.environ.copy()
    environment.update(
        {
            "PGUSER": values.get("POSTGRES_USER", ""),
            "PGDATABASE": values.get("POSTGRES_DB", ""),
            "PGPASSWORD": values.get("POSTGRES_PASSWORD", ""),
            "REDISCLI_AUTH": values.get("REDIS_PASSWORD", ""),
        }
    )
    if run(["psql", "-h", "127.0.0.1", "-Atqc", "SELECT 1"], environment).stdout.strip() != "1":
        return fail("PostgreSQL connectivity failed")
    if run(["redis-cli", "-h", "127.0.0.1", "ping"], environment).stdout.strip() != "PONG":
        return fail("Redis connectivity failed")
    print("[verify] PASS: local dependency connectivity")
    if len(sys.argv) > 1 and sys.argv[1] == "--persistence":
        STATE.mkdir(mode=0o750, parents=True, exist_ok=True)
        (STATE / "persistence-sentinel").write_text("phase3-persistence-ok\n", encoding="utf-8")
        sql = "CREATE TABLE IF NOT EXISTS phase2_persistence (id integer PRIMARY KEY, value text NOT NULL); INSERT INTO phase2_persistence (id, value) VALUES (1, 'verified') ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value;"
        if (
            run(
                ["psql", "-h", "127.0.0.1", "-v", "ON_ERROR_STOP=1", "-c", sql], environment
            ).returncode
            != 0
        ):
            return fail("PostgreSQL persistence write failed")
        if (
            run(
                ["redis-cli", "-h", "127.0.0.1", "SET", "phase2:persistence", "verified"],
                environment,
            ).returncode
            != 0
        ):
            return fail("Redis persistence write failed")
        if (
            run(
                ["systemctl", "restart", "institutional-signal-dependencies.service"], environment
            ).returncode
            != 0
        ):
            return fail("dependency service restart failed")
        if not healthy("institutional-signal-postgres", environment) or not healthy(
            "institutional-signal-redis", environment
        ):
            return fail("dependency unhealthy after restart")
        if (STATE / "persistence-sentinel").read_text(
            encoding="utf-8"
        ).strip() != "phase3-persistence-ok":
            return fail("filesystem sentinel did not persist")
        if (
            run(
                [
                    "psql",
                    "-h",
                    "127.0.0.1",
                    "-Atqc",
                    "SELECT value FROM phase2_persistence WHERE id = 1",
                ],
                environment,
            ).stdout.strip()
            != "verified"
        ):
            return fail("PostgreSQL data did not persist")
        if (
            run(
                ["redis-cli", "-h", "127.0.0.1", "GET", "phase2:persistence"], environment
            ).stdout.strip()
            != "verified"
        ):
            return fail("Redis data did not persist")
        print("[verify] PASS: filesystem, PostgreSQL and Redis state survived service restart")
    return 0


raise SystemExit(main())
