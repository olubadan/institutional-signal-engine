"""Replay recorded canonical events through the continuous pipeline."""

from collections.abc import Iterable
from datetime import UTC, datetime

from .config import Settings
from .persistence import InMemoryRepository
from .pipeline import SignalPipeline
from .schemas import CanonicalEvent, Decision


def replay(
    events: Iterable[CanonicalEvent], symbols: Iterable[str], settings: Settings
) -> tuple[Decision, ...]:
    del symbols
    ordered = sorted(
        events, key=lambda value: (value.normalized_timestamp, value.source, value.sequence)
    )
    clock = [ordered[0].normalized_timestamp] if ordered else [datetime.min.replace(tzinfo=UTC)]
    pipeline = SignalPipeline(settings, repository=InMemoryRepository(), now=lambda: clock[0])
    for event in ordered:
        clock[0] = event.normalized_timestamp
        pipeline.process(event)
    return tuple(pipeline.decisions)
