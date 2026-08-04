"""Inward-owned ports. Vendor and infrastructure types stop at adapters."""

from collections.abc import AsyncIterator, Iterable
from typing import Protocol

from .schemas import CanonicalEvent, Decision


class MarketDataProvider(Protocol):
    async def events(self, symbols: Iterable[str]) -> AsyncIterator[CanonicalEvent]: ...

    async def health(self) -> dict[str, object]: ...


class EventRepository(Protocol):
    def record_event(self, event: CanonicalEvent) -> None: ...
    def record_decision(self, decision: Decision) -> None: ...
    def replay_events(self) -> Iterable[CanonicalEvent]: ...


class Clock(Protocol):
    def now(self) -> object: ...
