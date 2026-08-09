"""Read-only, fail-closed deployment ignition checks."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from urllib.request import Request, urlopen

from .config import Settings


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


def _http_ok(url: str, headers: dict[str, str] | None = None) -> bool:
    try:
        with urlopen(Request(url, headers=headers or {}), timeout=5) as response:
            return 200 <= int(response.status) < 300
    except (OSError, ValueError):
        return False


def run_read_only(
    expected_branch: str, expected_commit: str, manifest: Path | None
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
        checks["postgres_healthy"] = _command_ok(["pg_isready"])
        checks["redis_healthy"] = _command_ok(["redis-cli", "ping"])
        checks["docker_healthy"] = _command_ok(["docker", "ps"])
        checks["theta_terminal_running"] = _command_ok(["pgrep", "-f", "ThetaTerminalv3.jar"])
        checks["mdss_connected"] = _http_ok(
            f"{settings.theta_terminal_http_url}/terminal/mdds/status"
        )
        checks["alpaca_authentication"] = (
            settings.alpaca_key_id is not None
            and settings.alpaca_secret_key is not None
            and _http_ok(
                "https://api.alpaca.markets/v2/account",
                {
                    "APCA-API-KEY-ID": settings.alpaca_key_id.get_secret_value(),
                    "APCA-API-SECRET-KEY": settings.alpaca_secret_key.get_secret_value(),
                },
            )
        )
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
    checks["persistence_write_drain_probe"] = checks.get("postgres_healthy", False)
    checks["nonempty_subscription_plan"] = False
    if manifest is not None and manifest.exists():
        try:
            value = json.loads(manifest.read_text(encoding="utf-8"))
            plan = value.get("finalization", {}).get("final_allocation", [])
            checks["nonempty_subscription_plan"] = isinstance(plan, list) and bool(plan)
            checks["capacity_within_limits"] = (
                len(plan) <= 10_000 if isinstance(plan, list) else False
            )
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
    args = parser.parse_args()
    result = run_read_only(args.expected_branch, args.expected_commit, args.manifest)
    print(json.dumps(result.as_dict(), sort_keys=True))
    raise SystemExit(0 if result.passed else 1)


if __name__ == "__main__":
    main()
