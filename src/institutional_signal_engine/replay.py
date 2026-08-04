"""Replay recorded canonical events through the same synchronization/decision path."""

from collections.abc import Iterable

from .config import Settings
from .persistence import InMemoryRepository
from .schemas import CanonicalEvent, Decision
from .synchronization import Synchronizer


def replay(
    events: Iterable[CanonicalEvent], symbols: Iterable[str], settings: Settings
) -> tuple[Decision, ...]:
    synchronizer = Synchronizer()
    repository = InMemoryRepository()
    for event in sorted(
        events, key=lambda value: (value.normalized_timestamp, value.source, value.sequence)
    ):
        repository.record_event(event)
        synchronizer.add(event)
    snapshots = []
    for symbol in sorted(symbols):
        timestamps = [
            event.normalized_timestamp for event in repository.events if event.symbol == symbol
        ]
        if timestamps:
            snapshot = synchronizer.snapshot(symbol, max(timestamps))
            if snapshot is not None:
                snapshots.append(snapshot)
    from .signals import decide

    return (decide(snapshots, settings),)
