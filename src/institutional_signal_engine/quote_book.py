"""Bounded quote conflation and event-time classification windows."""

from collections import deque
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from typing import Any

from .schemas import CanonicalEvent


@dataclass(frozen=True)
class QuoteKey:
    provider: str
    instrument: tuple[str, ...]


@dataclass(frozen=True)
class QuoteConsumption:
    consumption_order: int
    quote_event_id: object
    trade_event_id: object | None
    quote_role: str
    quote: CanonicalEvent


@dataclass
class QuoteBookMetrics:
    quotes_received: int = 0
    quotes_superseded: int = 0
    quotes_consumed: int = 0


class QuoteBook:
    """Keep latest state O(1) and a bounded event-time classification window."""

    def __init__(self, window: timedelta = timedelta(seconds=5.5)) -> None:
        if window <= timedelta(milliseconds=500):
            raise ValueError("quote window must exceed the 500ms classification interval")
        self.window = window
        self._latest: dict[QuoteKey, CanonicalEvent] = {}
        self._windows: dict[QuoteKey, deque[CanonicalEvent]] = {}
        self._consumed: dict[object, QuoteConsumption] = {}
        self._consumption_order = 0
        self.metrics = QuoteBookMetrics()
        self.consumptions: list[QuoteConsumption] = []

    @staticmethod
    def key(event: CanonicalEvent) -> QuoteKey:
        payload: dict[str, Any] = event.payload
        instrument: tuple[str, ...]
        if event.kind.value == "options":
            contract = payload.get("contract", {})
            instrument = (
                str(contract.get("root", event.symbol)).upper(),
                str(contract.get("expiration", "")),
                str(contract.get("strike", "")),
                str(contract.get("right", "")).upper(),
            )
        else:
            instrument = (event.symbol.upper(),)
        return QuoteKey(event.source, instrument)

    def receive(self, quote: CanonicalEvent) -> None:
        if quote.payload.get("provider_event_kind") != "quote":
            raise ValueError("quote book accepts quote events only")
        key = self.key(quote)
        self.metrics.quotes_received += 1
        if key in self._latest:
            self.metrics.quotes_superseded += 1
        self._latest[key] = quote
        window = self._windows.setdefault(key, deque())
        window.append(quote)
        self._expire(key, quote.source_timestamp)

    def _expire(self, key: QuoteKey, timestamp: datetime) -> None:
        window = self._windows[key]
        cutoff = timestamp - self.window
        while window and window[0].source_timestamp < cutoff:
            window.popleft()

    def latest(self, trade: CanonicalEvent) -> CanonicalEvent | None:
        return self._latest.get(self.key(trade))

    def newest_valid_at_or_before(self, trade: CanonicalEvent) -> CanonicalEvent | None:
        key = self.key(trade)
        window = self._windows.get(key)
        if window is None:
            return None
        self._expire(key, trade.source_timestamp)
        for quote in reversed(window):
            if quote.source_timestamp <= trade.source_timestamp:
                return quote
        return None

    def consume(
        self, quote: CanonicalEvent, trade: CanonicalEvent | None = None
    ) -> QuoteConsumption:
        existing = self._consumed.get(quote.event_id)
        if existing is not None:
            if trade is not None and existing.trade_event_id is None:
                updated = replace(existing, trade_event_id=trade.event_id, quote_role="BOTH")
                self._consumed[quote.event_id] = updated
                self.consumptions[existing.consumption_order - 1] = updated
                return updated
            if trade is None and existing.trade_event_id is not None:
                updated = replace(existing, quote_role="BOTH")
                self._consumed[quote.event_id] = updated
                self.consumptions[existing.consumption_order - 1] = updated
                return updated
            return existing
        self._consumption_order += 1
        role = "BOTH" if trade is not None else "CURRENT_STATE"
        consumption = QuoteConsumption(
            self._consumption_order,
            quote.event_id,
            trade.event_id if trade is not None else None,
            role,
            quote,
        )
        self.consumptions.append(consumption)
        self._consumed[quote.event_id] = consumption
        self.metrics.quotes_consumed += 1
        return consumption

    def consume_for_trade(self, trade: CanonicalEvent) -> QuoteConsumption | None:
        quote = self.newest_valid_at_or_before(trade)
        return self.consume(quote, trade) if quote is not None else None

    def consume_current_state(self) -> tuple[QuoteConsumption, ...]:
        consumed: list[QuoteConsumption] = []
        for quote in self._latest.values():
            before = self._consumed.get(quote.event_id)
            record = self.consume(quote)
            if before is None or before.quote_role != record.quote_role:
                consumed.append(record)
        return tuple(consumed)

    def snapshot(self) -> dict[QuoteKey, CanonicalEvent]:
        return dict(self._latest)
