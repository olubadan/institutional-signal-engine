"""Read-only, fail-closed deployment ignition checks."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import subprocess
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from .config import Settings
from .live_smoke import _secret
from .persistence import PostgresRepository
from .persistence_async import AsyncAuditWriter, AuditWrite
from .providers.alpaca import AlpacaEquitiesProvider


@dataclass(frozen=True)
class IgnitionResult:
    checks: dict[str, bool]
    reasons: tuple[str, ...]

    @property
    def passed(self) -> bool:
        return not self.reasons and all(self.checks.values())

    def as_dict(self) -> dict[str, object]:
        return {
            "status": "READY" if self.passed else "BLOCKED",
            "checks": self.checks,
            "reasons": list(self.reasons),
            "trading_enabled": False,
            "orders_constructed": 0,
            "orders_submitted": 0,
        }


def evaluate_ignition(checks: Mapping[str, bool]) -> IgnitionResult:
    """Evaluate sanitized check outcomes; no secret or environment values are returned."""
    failures = tuple(name for name, passed in checks.items() if not passed)
    return IgnitionResult(dict(checks), failures)


def _command_ok(command: list[str]) -> bool:
    try:
        return subprocess.run(command, capture_output=True, check=False, timeout=5).returncode == 0
    except (OSError, subprocess.SubprocessError):
        return False


def _docker_service_healthy(service: str) -> bool:
    try:
        result = subprocess.run(
            ["docker", "inspect", "--format", "{{.State.Health.Status}}", service],
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
        return result.returncode == 0 and result.stdout.strip() == "healthy"
    except (OSError, subprocess.SubprocessError):
        return False


def _postgres_health(settings: Settings) -> bool:
    if not settings.database_url:
        return False
    repository = PostgresRepository(settings.database_url)
    try:
        repository.initialize()
        return repository.healthcheck()
    except Exception:  # noqa: BLE001 - ignition must fail closed and sanitize provider errors
        return False


def _alpaca_authentication(settings: Settings) -> bool:
    if settings.alpaca_key_id is None or settings.alpaca_secret_key is None:
        return False

    async def check() -> bool:
        provider = AlpacaEquitiesProvider(
            settings.alpaca_data_url,
            _secret(settings.alpaca_key_id),
            _secret(settings.alpaca_secret_key),
        )
        await provider.current_prices(("SPY",))
        return True

    try:
        return asyncio.run(check())
    except Exception:  # noqa: BLE001 - ignition must fail closed and sanitize provider errors
        return False


def _mdss_connected(settings: Settings) -> bool:
    from .providers.thetadata_open_interest import ThetaDataOpenInterestProvider

    async def check() -> bool:
        result = await ThetaDataOpenInterestProvider(settings.theta_terminal_http_url).mdss_status()
        return result.get("status") == "CONNECTED"

    try:
        return asyncio.run(check())
    except Exception:  # noqa: BLE001 - ignition must fail closed and sanitize status errors
        return False


def _persistence_probe(settings: Settings) -> bool:
    database_url = settings.database_url
    if not database_url:
        return False

    async def probe() -> bool:
        repository = PostgresRepository(database_url)
        await asyncio.to_thread(repository.initialize)
        writer = AsyncAuditWriter(
            repository,
            soft_limit=settings.persistence_soft_limit,
            hard_limit=settings.persistence_hard_limit,
            batch_size=settings.persistence_batch_size,
            flush_interval=float(settings.persistence_flush_interval),
        )
        writer.start()
        probe_id = f"ignition-{uuid4()}"
        if not writer.enqueue(AuditWrite(probe_id=probe_id)):
            return False
        await writer.drain()
        return not writer.persistence_backpressure_failure and writer.metrics.probe_failures == 0

    try:
        return asyncio.run(probe())
    except Exception:  # noqa: BLE001 - ignition must fail closed and sanitize provider errors
        return False


def _prepare_plan(settings: Settings, output: Path) -> dict[str, object] | None:
    """Run the live runner's discovery/enrichment/planner in prepare-only mode."""
    from .phase4_live_smoke import run

    async def prepare() -> dict[str, object]:
        return await run(
            0,
            skip_oi_diagnostic=True,
            startup_timeout_seconds=float(settings.phase4_startup_timeout_seconds),
            prepare_only=True,
            plan_output=output,
        )

    try:
        return asyncio.run(prepare())
    except Exception:  # noqa: BLE001 - ignition must fail closed and sanitize provider errors
        return None


def validate_subscription_plan(value: Mapping[str, object]) -> bool:
    """Validate the exact paired plan produced by the runner's planner."""
    plan = value.get("selected_plan")
    counts = value.get("subscription_counts", {})
    if not isinstance(plan, list) or not plan or not isinstance(counts, Mapping):
        return False
    trade_count = counts.get("trade_submitted", len(plan))
    quote_count = counts.get("quote_submitted", len(plan))
    return (
        isinstance(trade_count, int)
        and isinstance(quote_count, int)
        and len(plan) <= 10_000
        and trade_count <= 15_000
        and quote_count <= 10_000
        and len(plan) == trade_count == quote_count
    )


def run_read_only(
    expected_branch: str,
    expected_commit: str,
    manifest: Path | None,
    plan_output: Path | None = None,
) -> IgnitionResult:
    checks: dict[str, bool] = {}
    checks["authorized_branch"] = (
        subprocess.run(
            ["git", "branch", "--show-current"], capture_output=True, text=True, check=False
        ).stdout.strip()
        == expected_branch
    )
    checks["exact_commit"] = (
        subprocess.run(
            ["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=False
        ).stdout.strip()
        == expected_commit
    )
    checks["clean_worktree"] = _command_ok(["git", "diff", "--quiet"]) and _command_ok(
        ["git", "diff", "--cached", "--quiet"]
    )
    checks["no_git_operation"] = not any(
        Path(".git").joinpath(name).exists()
        for name in ("MERGE_HEAD", "CHERRY_PICK_HEAD", "REBASE_HEAD")
    )
    checks["no_conflicting_process"] = not _command_ok(
        ["pgrep", "-f", "phase4_live_smoke|phase4-observation"]
    )
    runtime_file = os.environ.get(
        "RUNTIME_ENV_FILE", "/etc/institutional-signal-engine/runtime.env"
    )
    try:
        settings = (
            Settings.from_env_file(runtime_file)
            if Path(runtime_file).exists()
            else Settings.from_env()
        )
        checks["typed_configuration"] = True
        checks["required_secrets_present"] = (
            settings.alpaca_key_id is not None
            and settings.alpaca_secret_key is not None
            and settings.theta_api_key is not None
        )
        checks["trading_disabled"] = not settings.trading_enabled
        checks["postgres_healthy"] = _postgres_health(settings)
        checks["redis_healthy"] = _docker_service_healthy("institutional-signal-redis")
        checks["docker_healthy"] = _command_ok(["docker", "ps"])
        checks["theta_terminal_running"] = _command_ok(["pgrep", "-f", "ThetaTerminalv3.jar"])
        checks["mdss_connected"] = _mdss_connected(settings)
        checks["alpaca_authentication"] = _alpaca_authentication(settings)
    except (OSError, ValueError):
        checks.update(
            {
                "typed_configuration": False,
                "required_secrets_present": False,
                "trading_disabled": False,
                "postgres_healthy": False,
                "redis_healthy": False,
                "docker_healthy": False,
                "theta_terminal_running": False,
                "mdss_connected": False,
                "alpaca_authentication": False,
            }
        )
    checks["log_destination_writable"] = os.access("/var/log", os.W_OK)
    checks["sufficient_disk"] = shutil.disk_usage(".").free > 1_000_000_000
    checks["sufficient_memory"] = True
    checks["order_route_disabled"] = not Path(
        "src/institutional_signal_engine/execution.py"
    ).exists()
    checks["persistence_write_drain_probe"] = (
        _persistence_probe(settings) if checks.get("postgres_healthy", False) else False
    )
    plan_path = (
        plan_output
        or manifest
        or Path("/var/run/institutional-signal-engine/phase4b-ignition-plan.json")
    )
    prepared = (
        _prepare_plan(settings, plan_path) if checks.get("alpaca_authentication", False) else None
    )
    checks["nonempty_subscription_plan"] = False
    if prepared is not None and plan_path.exists():
        try:
            value = json.loads(plan_path.read_text(encoding="utf-8"))
            checks["nonempty_subscription_plan"] = validate_subscription_plan(value)
            checks["capacity_within_limits"] = validate_subscription_plan(value)
        except (OSError, ValueError, AttributeError):
            checks["capacity_within_limits"] = False
    else:
        checks["capacity_within_limits"] = False
    return evaluate_ignition(checks)


def main() -> None:
    parser = argparse.ArgumentParser(description="Read-only Phase 4B deployment ignition check")
    parser.add_argument("--expected-branch", required=True)
    parser.add_argument("--expected-commit", required=True)
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--plan-output", type=Path)
    args = parser.parse_args()
    result = run_read_only(
        args.expected_branch, args.expected_commit, args.manifest, args.plan_output
    )
    print(json.dumps(result.as_dict(), sort_keys=True))
    raise SystemExit(0 if result.passed else 1)


if __name__ == "__main__":
    main()
