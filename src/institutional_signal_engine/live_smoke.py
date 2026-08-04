"""Bounded live signal-only smoke test. This module has no order capability."""

import argparse
import asyncio
import json
import os
from collections import Counter
from datetime import UTC, datetime
from statistics import median

from pydantic import SecretStr

from .config import Settings
from .providers.alpaca import AlpacaEquitiesProvider
from .providers.common import ProviderError
from .providers.thetadata import ThetaDataOptionsProvider
from .schemas import CanonicalEvent, EventKind
from .signals import decide
from .synchronization import Synchronizer


def _secret(value: SecretStr | None) -> str:
    if value is None:
        raise RuntimeError("provider credential is not configured")
    return value.get_secret_value()


async def _collect(
    provider: object, symbols: list[str], seconds: float
) -> tuple[list[CanonicalEvent], str]:
    events: list[CanonicalEvent] = []
    try:
        async with asyncio.timeout(seconds):
            async for event in provider.events(symbols):  # type: ignore[attr-defined]
                events.append(event)
    except TimeoutError:
        return events, "healthy" if events else "connected_no_events"
    except ProviderError as exc:
        return events, f"failed:{exc.category}"
    except (OSError, RuntimeError, ValueError) as exc:
        return events, f"failed:{type(exc).__name__}"
    return events, "healthy" if events else "connected_no_events"


def _as_market_input(event: CanonicalEvent, kind: EventKind) -> CanonicalEvent:
    return event.model_copy(update={"kind": kind, "symbol": "AAPL"})


async def run(seconds: float) -> dict[str, object]:
    runtime_file = os.environ.get(
        "RUNTIME_ENV_FILE", "/etc/institutional-signal-engine/runtime.env"
    )
    settings = (
        Settings.from_env_file(runtime_file)
        if os.path.exists(runtime_file)
        else Settings.from_env()
    )
    if settings.trading_enabled:
        raise RuntimeError("signal-only smoke refuses trading-enabled configuration")

    alpaca = AlpacaEquitiesProvider(
        settings.alpaca_data_url,
        _secret(settings.alpaca_key_id),
        _secret(settings.alpaca_secret_key),
    )
    theta = ThetaDataOptionsProvider(
        settings.theta_events_url,
        _secret(settings.theta_api_key),
    )
    (equities, alpaca_health), (options, theta_health) = await asyncio.gather(
        _collect(alpaca, ["AAPL", "SPY", "XLK"], seconds),
        _collect(theta, ["AAPL"], seconds),
    )

    now = datetime.now(UTC)
    all_events = equities + options
    latencies = [
        max(0.0, (event.received_timestamp - event.source_timestamp).total_seconds() * 1000)
        for event in all_events
    ]
    stale = sum((now - event.normalized_timestamp).total_seconds() > 30 for event in all_events)
    synchronizer = Synchronizer()
    for event in equities:
        if event.symbol == "AAPL":
            synchronizer.add(event)
        elif event.symbol == "SPY":
            synchronizer.add(_as_market_input(event, EventKind.MARKET_INDEX))
        elif event.symbol == "XLK":
            synchronizer.add(_as_market_input(event, EventKind.SECTOR_INDEX))
    for event in options:
        synchronizer.add(event)
    snapshot = synchronizer.snapshot("AAPL", now)
    decision = decide([snapshot], settings) if snapshot is not None else None
    reasons = (
        list(decision.rejection_reasons)
        if decision is not None
        else ["AAPL:insufficient_synchronized_inputs"]
    )
    return {
        "trading_enabled": False,
        "provider_authentication": {
            "alpaca": "success" if alpaca.authenticated else "failed",
            "thetadata": "success" if theta.connected else "failed",
        },
        "subscription_acknowledgement": {
            "thetadata": "success" if theta.subscription_acknowledged else "not_observed"
        },
        "provider_stream_status": {"thetadata": theta.stream_status},
        "feed_health": {"alpaca": alpaca_health, "thetadata": theta_health},
        "received_event_counts": dict(Counter(event.source for event in all_events)),
        "latency_ms": {
            "median": round(median(latencies), 3) if latencies else None,
            "maximum": round(max(latencies), 3) if latencies else None,
        },
        "stale_events": stale,
        "rejection_reasons": reasons,
        "ranked_signal_count": len(decision.candidates) if decision is not None else 0,
        "executable_signal_count": int(bool(decision and decision.fire)),
        "orders_constructed": 0,
        "orders_submitted": 0,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seconds", type=float, default=20.0)
    arguments = parser.parse_args()
    print(json.dumps(asyncio.run(run(arguments.seconds)), sort_keys=True))


if __name__ == "__main__":
    main()
