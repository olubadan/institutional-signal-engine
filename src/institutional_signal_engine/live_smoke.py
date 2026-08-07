"""Bounded live signal-only smoke test. This module has no order capability."""

import argparse
import asyncio
import json
import os
from collections import Counter
from collections.abc import Callable, Iterable
from datetime import datetime
from zoneinfo import ZoneInfo

from pydantic import SecretStr

from .config import Settings
from .indicators import IndicatorCalculator
from .persistence import InMemoryRepository, PostgresRepository
from .persistence_async import AsyncAuditWriter
from .pipeline import EventTiming, SignalPipeline
from .providers.alpaca import AlpacaEquitiesProvider
from .providers.common import ProviderError
from .providers.thetadata import SMOKE_AAPL_CONTRACT, ThetaContract, ThetaDataOptionsProvider
from .schemas import CanonicalEvent, EventKind
from .startup import StageCallback, StageRecord, StageRecorder, StartupTimeout, bounded_startup


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
    return event.model_copy(update={"kind": kind})


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


async def run(
    seconds: float,
    symbols: Iterable[str] = ("AAPL",),
    contracts: Iterable[ThetaContract] = (SMOKE_AAPL_CONTRACT,),
    request_types: Iterable[str] = ("TRADE",),
    contract_metadata: dict[ThetaContract, dict[str, object]] | None = None,
    diagnostic_membership: dict[str, set[ThetaContract]] | None = None,
    startup_timeout_seconds: float = 120.0,
    stage_callback: StageCallback | None = None,
) -> dict[str, object]:
    recorder = StageRecorder(stage_callback)

    def startup_remaining() -> float:
        return max(0.001, startup_timeout_seconds - recorder.elapsed_seconds)

    pilot_symbols = tuple(sorted({symbol.upper() for symbol in symbols}))
    requested_contracts = tuple(contracts)
    requested_types = tuple(request_types)
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
    recorder.emit("configuration_loaded")

    alpaca = AlpacaEquitiesProvider(
        settings.alpaca_data_url,
        _secret(settings.alpaca_key_id),
        _secret(settings.alpaca_secret_key),
    )
    sector_by_symbol = {symbol: "XLK" for symbol in pilot_symbols}
    historical_symbols = (*pilot_symbols, "SPY", *sorted(set(sector_by_symbol.values())))
    historical = await bounded_startup(
        alpaca.historical_bootstrap(
            historical_symbols, datetime.now(ZoneInfo("America/New_York")).date()
        ),
        recorder,
        "alpaca_historical_bootstrap",
        startup_remaining(),
    )

    def provider_stage(stage: StageRecord) -> None:
        recorder.emit_record(stage)

    theta = ThetaDataOptionsProvider(
        settings.theta_events_url,
        _secret(settings.theta_api_key),
        contracts=requested_contracts,
        request_types=requested_types,
        diagnostic_membership=diagnostic_membership,
        stage_callback=provider_stage,
    )
    repository = (
        PostgresRepository(settings.database_url) if settings.database_url else InMemoryRepository()
    )
    if isinstance(repository, PostgresRepository):
        await bounded_startup(
            asyncio.to_thread(repository.initialize),
            recorder,
            "database_connected",
            startup_remaining(),
        )
    recorder.emit("database_connected")
    writer = AsyncAuditWriter(
        repository,
        soft_limit=settings.persistence_soft_limit,
        hard_limit=settings.persistence_hard_limit,
        batch_size=settings.persistence_batch_size,
        flush_interval=float(settings.persistence_flush_interval),
    )
    writer.start()
    pipeline = SignalPipeline(
        settings,
        repository=repository,
        writer=writer,
        indicator_calculator=IndicatorCalculator(historical),
        symbols=pilot_symbols,
        sector_by_symbol=sector_by_symbol,
    )

    def process(event: CanonicalEvent) -> None:
        if event.kind == EventKind.OPTIONS and contract_metadata:
            contract = event.payload.get("contract")
            if isinstance(contract, dict):
                key = ThetaContract(
                    str(contract.get("root", "")).upper(),
                    int(contract.get("expiration", 0)),
                    int(contract.get("strike", 0)),
                    str(contract.get("right", "")),
                )
                metadata = contract_metadata.get(key)
                if metadata is not None:
                    indicator_provenance = {
                        name: str(value)
                        for name, value in metadata.items()
                        if name
                        in {
                            "oi_date_source",
                            "open_interest_verified_as_of",
                            "evidence_quality",
                            "symbol_liquidity_evidence_source",
                            "symbol_liquidity_verified",
                            "policy_version",
                        }
                    }
                    event = event.model_copy(update={"payload": {**event.payload, **metadata}})
                    event = event.model_copy(
                        update={
                            "payload": {
                                **event.payload,
                                "indicator_provenance": indicator_provenance,
                            }
                        }
                    )
        if event.symbol == "SPY":
            event = _as_market_input(event, EventKind.MARKET_INDEX)
        elif event.symbol == "XLK":
            event = _as_market_input(event, EventKind.SECTOR_INDEX)
        pipeline.process(event)
        if pipeline.incomplete_run:
            raise RuntimeError("persistence_backpressure_failure")

    async def timer() -> None:
        while True:
            await asyncio.sleep(0.25)
            pipeline.tick()

    timer_task = asyncio.create_task(timer())
    theta_acknowledged = asyncio.Event()

    def mark_provider_stage(stage: dict[str, object]) -> None:
        recorder.emit_record(stage)
        if stage.get("stage") == "subscriptions_acknowledged":
            theta_acknowledged.set()

    theta.stage_callback = mark_provider_stage
    option_task = asyncio.create_task(
        _collect(
            theta,
            sorted({contract.root for contract in theta.contracts}),
            seconds + startup_timeout_seconds,
            process,
        )
    )
    equity_task: asyncio.Task[tuple[list[CanonicalEvent], str]] | None = None
    try:
        try:
            await asyncio.wait_for(theta_acknowledged.wait(), startup_remaining())
        except TimeoutError as exc:
            recorder.emit(
                "startup_timeout",
                failed_stage="subscriptions_acknowledged",
                completed_items=0,
                remaining_items=len(theta.contracts) * len(requested_types),
                error_category="startup_timeout",
            )
            await bounded_startup(
                writer.close(), recorder, "persistence_drained", startup_timeout_seconds
            )
            recorder.emit("persistence_drained")
            raise StartupTimeout(
                "subscriptions_acknowledged",
                recorder.elapsed_seconds,
                0,
                len(theta.contracts),
            ) from exc
        recorder.emit("observation_started", observation_seconds=seconds)
        equity_task = asyncio.create_task(
            _collect(alpaca, [*pilot_symbols, "SPY", "XLK"], seconds, process)
        )
        equities, alpaca_health = await equity_task
        if not option_task.done():
            option_task.cancel()
        option_result = await asyncio.gather(option_task, return_exceptions=True)
        options_result = option_result[0]
        if isinstance(options_result, BaseException):
            options: list[CanonicalEvent] = []
            theta_health = f"failed:{type(options_result).__name__}"
        else:
            options, theta_health = options_result
    finally:
        tasks = (equity_task, option_task)
        for task in tasks:
            if task is None:
                continue
            if not task.done():
                task.cancel()
        await asyncio.gather(*(task for task in tasks if task is not None), return_exceptions=True)
        timer_task.cancel()
        await asyncio.gather(timer_task, return_exceptions=True)
    await bounded_startup(writer.close(), recorder, "persistence_drained", startup_timeout_seconds)
    recorder.emit("persistence_drained")
    recorder.emit("observation_completed")
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
    synchronized_input_count = pipeline.synchronized_input_count
    reasons = (
        list(decision.rejection_reasons)
        if decision is not None
        else [
            f"{symbol}:{reason}"
            for symbol, reasons_for_symbol in pipeline.incomplete_state_reasons.items()
            for reason in (reasons_for_symbol or ["no_decision"])
        ]
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
            "thetadata": "success" if theta.subscription_acknowledged else "not_observed",
            "acknowledged_request_count": len(theta.acknowledged_ids),
            "rejected_or_unmatched": list(theta.diagnostics),
            "rejected_event_diagnostics": list(theta.rejected_event_diagnostics),
            "rejected_event_overflow": theta.rejected_event_overflow,
            "request_registry": [
                {
                    "request_id": request_id,
                    "root": request.contract.root,
                    "expiration": request.contract.expiration,
                    "strike": request.contract.strike,
                    "right": request.contract.right,
                    "request_type": request.req_type,
                    "add": request.add,
                    "connection_generation": request.generation,
                    "acknowledged": request_id in theta.acknowledged_ids,
                }
                for request_id, request in sorted(theta.request_registry.items())
            ],
            "acknowledged_contracts": [
                {
                    "root": contract.root,
                    "expiration": contract.expiration,
                    "strike": contract.strike,
                    "right": contract.right,
                }
                for contract in sorted(
                    theta.acknowledged_contracts,
                    key=lambda value: (value.root, value.expiration, value.strike, value.right),
                )
            ],
            "requests_by_type": {
                req_type: {
                    "requested": sum(
                        request.req_type == req_type for request in theta.request_registry.values()
                    ),
                    "acknowledged": sum(
                        theta.request_registry[request_id].req_type == req_type
                        for request_id in theta.acknowledged_ids
                        if request_id in theta.request_registry
                    ),
                    "rejected_or_unmatched": sum(
                        request_type == req_type for request_type in theta.rejected_request_types
                    ),
                }
                for req_type in request_types
            },
        },
        "provider_stream_status": {"thetadata": theta.stream_status},
        "historical_bootstrap": {
            "status": "success",
            "symbols": list(historical_symbols),
            "adjustment": historical.adjustment,
            "provenance": historical.source_provenance,
        },
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
        "trigger_reason_counts": dict(
            Counter(
                reason
                for decision_item in pipeline.decisions
                for reason in decision_item.triggering_change_reasons
            )
        ),
        "evaluation_skip_reasons": pipeline.metrics.skip_reasons or {},
        "synchronized_input_count": synchronized_input_count,
        "synchronized_symbols": sorted(
            symbol
            for symbol, reasons_for_symbol in pipeline.incomplete_state_reasons.items()
            if not reasons_for_symbol
        ),
        "incomplete_state_reasons": pipeline.incomplete_state_reasons,
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
