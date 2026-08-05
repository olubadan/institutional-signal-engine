"""Bounded live signal-only smoke test. This module has no order capability."""

import argparse
import asyncio
import json
import os
from collections import Counter
from statistics import median

from pydantic import SecretStr

from .config import Settings
from .persistence import InMemoryRepository, PostgresRepository
from .pipeline import SignalPipeline
from .providers.alpaca import AlpacaEquitiesProvider
from .providers.common import ProviderError
from .providers.thetadata import SMOKE_AAPL_CONTRACT, ThetaDataOptionsProvider
from .schemas import CanonicalEvent, EventKind


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
        contracts=(SMOKE_AAPL_CONTRACT,),
    )
    (equities, alpaca_health), (options, theta_health) = await asyncio.gather(
        _collect(alpaca, ["AAPL", "SPY", "XLK"], seconds),
        _collect(theta, ["AAPL"], seconds),
    )

    repository = (
        PostgresRepository(settings.database_url) if settings.database_url else InMemoryRepository()
    )
    if isinstance(repository, PostgresRepository):
        repository.initialize()
    pipeline = SignalPipeline(settings, repository=repository)
    for event in sorted(equities + options, key=lambda value: value.received_timestamp):
        if event.symbol == "SPY":
            event = _as_market_input(event, EventKind.MARKET_INDEX)
        elif event.symbol == "XLK":
            event = _as_market_input(event, EventKind.SECTOR_INDEX)
        pipeline.process(event)
    decision = pipeline.decisions[-1] if pipeline.decisions else None
    all_events = equities + options
    latencies = pipeline.metrics.provider_transport_latency_ms
    synchronized_input_count = len(pipeline.decisions)
    reasons = (
        list(decision.rejection_reasons)
        if decision is not None
        else ["AAPL:insufficient_synchronized_inputs"]
    )
    return {
        "trading_enabled": False,
        "contracts_subscribed": [
            {
                "root": contract.root,
                "expiration": contract.expiration,
                "strike": contract.strike,
                "right": contract.right,
            }
            for contract in theta.contracts
        ],
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
        "alpaca_event_count": len(equities),
        "theta_option_event_count": len(options),
        "synchronized_input_count": synchronized_input_count,
        "latency_ms": {
            "median": round(median(latencies), 3) if latencies else None,
            "maximum": round(max(latencies), 3) if latencies else None,
        },
        "stale_events": pipeline.metrics.stale_events,
        "late_events": pipeline.metrics.late_events,
        "out_of_order_events": pipeline.metrics.out_of_order_events,
        "duplicate_events": pipeline.metrics.duplicate_events,
        "processing_latency_ms": {
            "median": round(median(pipeline.metrics.processing_latency_ms), 3)
            if pipeline.metrics.processing_latency_ms
            else None,
            "maximum": round(max(pipeline.metrics.processing_latency_ms), 3)
            if pipeline.metrics.processing_latency_ms
            else None,
        },
        "rejection_reasons": reasons,
        "candidates_evaluated": decision.counters.candidates_evaluated if decision else 0,
        "candidates_passing_S": decision.counters.candidates_passing_S if decision else 0,
        "candidates_passing_S_and_F_and_R": decision.counters.candidates_passing_S_and_F_and_R
        if decision
        else 0,
        "executable_candidates": decision.counters.executable_candidates if decision else 0,
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
