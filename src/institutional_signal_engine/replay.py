"""Replay recorded canonical events through the continuous pipeline."""

from collections.abc import Iterable
from datetime import UTC, datetime
from uuid import NAMESPACE_URL, UUID, uuid5

from .config import Settings
from .persistence import InMemoryRepository
from .pipeline import SignalPipeline
from .quote_book import QuoteConsumption
from .schemas import CanonicalEvent, Decision


def _ordered_consumed_inputs(
    events: Iterable[CanonicalEvent], quote_consumptions: Iterable[QuoteConsumption]
) -> list[CanonicalEvent]:
    """Reconstruct the live ingress order from persisted trades and consumed quotes."""
    unique_quotes: dict[object, CanonicalEvent] = {}
    for consumption in sorted(quote_consumptions, key=lambda value: value.consumption_order):
        unique_quotes.setdefault(consumption.quote_event_id, consumption.quote)
    inputs = [*unique_quotes.values(), *events]
    return sorted(
        inputs,
        key=lambda value: (
            value.ingest_order,
            0 if value.payload.get("provider_event_kind") == "quote" else 1,
            value.received_timestamp,
            value.normalized_timestamp,
            value.source,
            value.sequence,
            str(value.event_id),
        ),
    )


def replay(
    events: Iterable[CanonicalEvent], symbols: Iterable[str], settings: Settings
) -> tuple[Decision, ...]:
    del symbols
    ordered = sorted(
        events,
        key=lambda value: (
            value.ingest_order,
            value.received_timestamp,
            value.normalized_timestamp,
            value.source,
            value.sequence,
            str(value.event_id),
        ),
    )
    clock = [ordered[0].normalized_timestamp] if ordered else [datetime.min.replace(tzinfo=UTC)]
    run_id = (
        ordered[0].run_id
        if ordered and ordered[0].run_id != UUID(int=0)
        else uuid5(NAMESPACE_URL, "replay:" + ",".join(str(event.event_id) for event in ordered))
    )
    pipeline = SignalPipeline(
        settings, repository=InMemoryRepository(), now=lambda: clock[0], run_id=run_id
    )
    for event in ordered:
        clock[0] = event.normalized_timestamp
        if event.payload.get("provider_event_kind") == "sweep_timer":
            pipeline.tick()
        else:
            pipeline.process(event)
    return tuple(pipeline.decisions)


def replay_with_consumed_quotes(
    events: Iterable[CanonicalEvent],
    quote_consumptions: Iterable[QuoteConsumption],
    symbols: Iterable[str],
    settings: Settings,
) -> tuple[Decision, ...]:
    """Replay only the trades and the exact quotes live processing consumed."""
    del symbols
    inputs = _ordered_consumed_inputs(events, quote_consumptions)
    clock = [inputs[0].normalized_timestamp] if inputs else [datetime.min.replace(tzinfo=UTC)]
    run_id = inputs[0].run_id if inputs else UUID(int=0)
    pipeline = SignalPipeline(
        settings, repository=InMemoryRepository(), now=lambda: clock[0], run_id=run_id
    )
    for event in inputs:
        clock[0] = event.normalized_timestamp
        if event.payload.get("provider_event_kind") == "sweep_timer":
            pipeline.tick()
        else:
            pipeline.process(event)
    return tuple(pipeline.decisions)
