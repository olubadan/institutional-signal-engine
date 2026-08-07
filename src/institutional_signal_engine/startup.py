"""Sanitized, flushed startup progress and bounded startup operations."""

import asyncio
import json
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from time import monotonic

_SENSITIVE_FIELD_MARKERS = ("secret", "token", "password", "credential", "header", "api_key")

StageCallback = Callable[[str, dict[str, object]], None]
STARTUP_STAGES: tuple[str, ...] = (
    "configuration_loaded",
    "database_connected",
    "providers_authenticated",
    "universe_discovery_started",
    "universe_discovery_completed",
    "enrichment_started",
    "enrichment_completed",
    "selection_completed",
    "subscription_planning_completed",
    "websocket_connected",
    "subscriptions_acknowledged",
    "observation_started",
    "observation_completed",
    "persistence_drained",
    "report_emitted",
)


class StartupTimeout(RuntimeError):
    def __init__(self, stage: str, elapsed_seconds: float, completed: int, remaining: int):
        super().__init__(stage)
        self.stage = stage
        self.elapsed_seconds = elapsed_seconds
        self.completed = completed
        self.remaining = remaining

    def report(self) -> dict[str, object]:
        return {
            "status": "startup_timeout",
            "stage": self.stage,
            "elapsed_seconds": self.elapsed_seconds,
            "completed_items": self.completed,
            "remaining_items": self.remaining,
            "error_category": "startup_timeout",
            "trading_enabled": False,
            "orders_constructed": 0,
            "orders_submitted": 0,
        }


class StageRecorder:
    def __init__(self, callback: StageCallback | None = None) -> None:
        self.callback = callback
        self.started = monotonic()

    @property
    def elapsed_seconds(self) -> float:
        return round(monotonic() - self.started, 3)

    def emit(self, stage: str, **fields: object) -> None:
        safe_fields = {
            key: "[REDACTED]"
            if any(marker in key.lower() for marker in _SENSITIVE_FIELD_MARKERS)
            else value
            for key, value in fields.items()
        }
        record: dict[str, object] = {
            "record_type": "phase4_stage",
            "stage": stage,
            "timestamp_utc": datetime.now(UTC).isoformat(),
            "elapsed_seconds": self.elapsed_seconds,
            **safe_fields,
        }
        if self.callback is not None:
            self.callback(stage, record)
        else:
            print(json.dumps(record, sort_keys=True), flush=True)


async def bounded_startup[T](
    awaitable: Awaitable[T],
    recorder: StageRecorder,
    stage: str,
    timeout_seconds: float,
    completed: int = 0,
    remaining: int = 0,
) -> T:
    try:
        return await asyncio.wait_for(awaitable, timeout=timeout_seconds)
    except TimeoutError as exc:
        elapsed = round(monotonic() - recorder.started, 3)
        recorder.emit(
            "startup_timeout",
            failed_stage=stage,
            elapsed_seconds=elapsed,
            completed_items=completed,
            remaining_items=remaining,
            error_category="startup_timeout",
        )
        raise StartupTimeout(stage, elapsed, completed, remaining) from exc
