"""Bounded live signal-only smoke test. This module has no order capability."""

import argparse
import asyncio
import json
import os
from collections import Counter
from collections.abc import Callable, Iterable

from pydantic import SecretStr

from .config import Settings
from .persistence import InMemoryRepository, PostgresRepository
from .persistence_async import AsyncAuditWriter
from .pipeline import EventTiming, SignalPipeline
from .providers.alpaca import AlpacaEquitiesProvider
from .providers.common import ProviderError
from .providers.thetadata import SMOKE_AAPL_CONTRACT, ThetaDataOptionsProvider
from .schemas import CanonicalEvent, EventKind


def _secret(value: SecretStr | None) -> str:
    if value is None:
        raise RuntimeError("provider credential is not configured")
    return value.get_secret_value()


async def _collect(
    provider: object, symbols: list[str], seconds: float, on_event: Callable[[CanonicalEvent], None]
) -> tuple[list[CanonicalEvent], str]:
    events: list[CanonicalEvent] = []
    try:
        async with asyncio.timeout(seconds):
            async for event in provider.events(symbols):  # type: ignore[attr-defined]
                events.append(event)
                on_event(event)
    except TimeoutError:
        return events, "healthy" if events else "connected_no_events"
    except ProviderError as exc:
        return events, f"failed:{exc.category}"
    except (OSError, RuntimeError, ValueError) as exc:
        return events, f"failed:{type(exc).__name__}"
    return events, "healthy" if events else "connected_no_events"


def _as_market_input(event: CanonicalEvent, kind: EventKind) -> CanonicalEvent:
    return event.model_copy(update={"kind": kind, "symbol": "AAPL"})


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * percentile
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def _distribution(timings: Iterable[EventTiming]) -> dict[str, object]:
    values = list(timings)
    ages = [timing.event_age_at_receipt_ms for timing in values]
    processing = [timing.processing_duration_ms for timing in values]
    queue_wait = [timing.internal_queue_wait_ms or 0.0 for timing in values]
    total_age = [timing.total_age_at_completion_ms or 0.0 for timing in values]

    def stats(sample: list[float]) -> dict[str, object]:
        return {
            "sample_count": len(sample),
            "minimum": min(sample) if sample else None,
            "p50": _percentile(sample, 0.50),
            "p95": _percentile(sample, 0.95),
            "p99": _percentile(sample, 0.99),
            "maximum": max(sample) if sample else None,
        }

    return {
        "event_age_at_receipt_ms": stats(ages),
        "processing_duration_ms": stats(processing),
        "internal_queue_wait_ms": stats(queue_wait),
        "total_age_at_completion_ms": stats(total_age),
        "timestamp_conversions": {
            "precision_converted": sum(t.precision_conversion_required for t in values),
            "timezone_converted": sum(t.timezone_conversion_required for t in values),
        },
    }


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
    repository = (
        PostgresRepository(settings.database_url) if settings.database_url else InMemoryRepository()
    )
    if isinstance(repository, PostgresRepository):
        repository.initialize()
    writer = AsyncAuditWriter(
        repository,
        soft_limit=settings.persistence_soft_limit,
        hard_limit=settings.persistence_hard_limit,
        batch_size=settings.persistence_batch_size,
        flush_interval=float(settings.persistence_flush_interval),
    )
    writer.start()
    pipeline = SignalPipeline(settings, repository=repository, writer=writer)

    def process(event: CanonicalEvent) -> None:
        if event.symbol == "SPY":
            event = _as_market_input(event, EventKind.MARKET_INDEX)
        elif event.symbol == "XLK":
            event = _as_market_input(event, EventKind.SECTOR_INDEX)
        pipeline.process(event)
        if pipeline.incomplete_run:
            raise RuntimeError("persistence_backpressure_failure")

    (equities, alpaca_health), (options, theta_health) = await asyncio.gather(
        _collect(alpaca, ["AAPL", "SPY", "XLK"], seconds, process),
        _collect(theta, ["AAPL"], seconds, process),
    )
    await writer.close()
    decision = pipeline.decisions[-1] if pipeline.decisions else None
    all_events = equities + options
    timings = pipeline.metrics.timings or []
    timing_groups = {
        "alpaca_trades": [
            timing
            for timing in timings
            if timing.provider == "alpaca" and timing.event_kind == "trade"
        ],
        "alpaca_quotes": [
            timing
            for timing in timings
            if timing.provider == "alpaca" and timing.event_kind == "quote"
        ],
        "thetadata_option_trades": [
            timing
            for timing in timings
            if timing.provider == "thetadata" and timing.event_kind == "trade"
        ],
        "thetadata_option_quotes": [
            timing
            for timing in timings
            if timing.provider == "thetadata" and timing.event_kind == "quote"
        ],
    }
    synchronized_input_count = len(pipeline.decisions)
    reasons = (
        list(decision.rejection_reasons)
        if decision is not None
        else ["AAPL:insufficient_synchronized_inputs"]
    )
    return {
        "run_id": str(pipeline.run_id),
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
        "received_event_counts_by_kind": dict(
            Counter(
                f"{event.source}:{event.payload.get('provider_event_kind', event.kind.value)}"
                for event in all_events
            )
        ),
        "alpaca_event_count": len(equities),
        "theta_option_event_count": len(options),
        "trades_received": pipeline.metrics.trades_received,
        "trades_processed": pipeline.metrics.trades_processed,
        "quotes_received": pipeline.quote_book.metrics.quotes_received,
        "current_state_overwrites": pipeline.quote_book.metrics.current_state_overwrites,
        "quotes_consumed": pipeline.quote_book.metrics.quotes_consumed,
        "quotes_pending_at_shutdown": pipeline.quote_book.quotes_pending_at_shutdown,
        "evaluations_triggered": pipeline.metrics.evaluations_triggered,
        "evaluations_skipped": pipeline.metrics.evaluations_skipped,
        "evaluation_skip_reasons": pipeline.metrics.skip_reasons or {},
        "synchronized_input_count": synchronized_input_count,
        "timing_distributions": {
            name: _distribution(group) for name, group in timing_groups.items()
        },
        "stale_events": pipeline.metrics.stale_events,
        "late_events": pipeline.metrics.late_events,
        "out_of_order_events": pipeline.metrics.out_of_order_events,
        "duplicate_events": pipeline.metrics.duplicate_events,
        "unknown_condition_events": pipeline.metrics.unknown_condition_events,
        "persistence_queue": {
            "depth_sample_count": len(writer.metrics.queue_depth_samples),
            "depth_p50": _percentile([float(v) for v in writer.metrics.queue_depth_samples], 0.50),
            "depth_p95": _percentile([float(v) for v in writer.metrics.queue_depth_samples], 0.95),
            "depth_maximum": max(writer.metrics.queue_depth_samples, default=0),
            "batch_sizes": writer.metrics.batch_sizes,
            "database_write_latency_ms": _distribution(
                [
                    EventTiming(
                        "persistence",
                        "write",
                        pipeline.now(),
                        pipeline.now(),
                        pipeline.now(),
                        0,
                        value,
                        False,
                        False,
                    )
                    for value in writer.metrics.database_write_latency_ms
                ]
            )["processing_duration_ms"],
            "soft_limit_crossings": writer.metrics.soft_limit_crossings,
            "hard_limit_failures": writer.metrics.hard_limit_failures,
            "time_above_soft_limit_seconds": writer.metrics.time_above_soft_limit,
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
