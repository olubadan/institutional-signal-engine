"""Replay recorded canonical events through the continuous pipeline."""

from collections.abc import Iterable
from datetime import UTC, datetime
from uuid import NAMESPACE_URL, UUID, uuid5

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
        pipeline.process(event)
    return tuple(pipeline.decisions)
