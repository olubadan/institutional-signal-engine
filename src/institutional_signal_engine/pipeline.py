"""Continuous, bounded event-to-decision processing."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from time import monotonic

from .config import Settings
from .indicators import classify_option_trade, session_key
from .persistence import InMemoryRepository
from .ports import EventRepository
from .schemas import CanonicalEvent, Decision, EventKind
from .signals import decide
from .synchronization import Synchronizer


@dataclass
class PipelineMetrics:
    provider_transport_latency_ms: list[float]
    processing_latency_ms: list[float]
    event_age_ms: list[float]
    late_events: int = 0
    stale_events: int = 0
    out_of_order_events: int = 0
    duplicate_events: int = 0


class SignalPipeline:
    def __init__(
        self,
        settings: Settings,
        repository: EventRepository | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.settings = settings
        self.repository: EventRepository = repository or InMemoryRepository()
        self.now = now or (lambda: datetime.now(UTC))
        self.synchronizer = Synchronizer()
        self.metrics = PipelineMetrics([], [], [])
        self._seen: set[object] = set()
        self._decided_states: set[tuple[object, ...]] = set()
        self._sessions: dict[str, str] = {}
        self._equity_volume: dict[str, int] = {}
        self._option_volume: dict[str, int] = {}
        self._ask_premium: dict[str, Decimal] = {}
        self._net_call_premium: dict[str, Decimal] = {}
        self.decisions: list[Decision] = []

    def process(self, event: CanonicalEvent) -> Decision | None:
        started = monotonic()
        processing_time = self.now().astimezone(UTC)
        self.metrics.provider_transport_latency_ms.append(
            max(0.0, (event.received_timestamp - event.source_timestamp).total_seconds() * 1000)
        )
        self.metrics.processing_latency_ms.append((monotonic() - started) * 1000)
        age_ms = max(0.0, (processing_time - event.source_timestamp).total_seconds() * 1000)
        self.metrics.event_age_ms.append(age_ms)
        if age_ms > self.synchronizer.max_staleness.total_seconds() * 1000:
            self.metrics.stale_events += 1
            return None
        if event.event_id in self._seen:
            self.metrics.duplicate_events += 1
            return None
        self._seen.add(event.event_id)
        event = self._enrich_state(event)
        if not self.synchronizer.add(event):
            self.metrics.out_of_order_events += 1
            return None
        self.repository.record_event(event)
        if event.kind == EventKind.EQUITY and event.symbol != "AAPL":
            kind = EventKind.MARKET_INDEX if event.symbol == "SPY" else EventKind.SECTOR_INDEX
            self.synchronizer.add(event.model_copy(update={"kind": kind, "symbol": "AAPL"}))
        snapshot = self.synchronizer.snapshot("AAPL", event.normalized_timestamp)
        if snapshot is None:
            return None
        state_id = tuple(sorted(snapshot.event_ids))
        if state_id in self._decided_states:
            return None
        self._decided_states.add(state_id)
        decision = decide([snapshot], self.settings)
        self.repository.record_decision(decision)
        self.decisions.append(decision)
        return decision

    def _enrich_state(self, event: CanonicalEvent) -> CanonicalEvent:
        payload = dict(event.payload)
        key = f"{event.kind.value}:{event.symbol}"
        current_session = session_key(event.source_timestamp)
        if self._sessions.get(key) != current_session:
            self._sessions[key] = current_session
            self._equity_volume[key] = 0
            self._option_volume[key] = 0
            self._ask_premium[key] = Decimal(0)
            self._net_call_premium[key] = Decimal(0)
        if event.kind in {EventKind.EQUITY, EventKind.MARKET_INDEX, EventKind.SECTOR_INDEX}:
            self._equity_volume[key] += int(payload.get("volume", 0))
            payload["volume"] = self._equity_volume[key]
        elif event.kind == EventKind.OPTIONS:
            size = int(payload.get("trade_size", payload.get("option_volume", 0)))
            self._option_volume[key] += size
            payload["option_volume"] = self._option_volume[key]
            price = payload.get("trade_price")
            quote = payload.get("quote_context", {})
            if price is not None:
                price_decimal = Decimal(str(price))
                bid = quote.get("bid")
                ask = quote.get("ask")
                classification = classify_option_trade(
                    price_decimal,
                    Decimal(str(bid)) if bid is not None else None,
                    Decimal(str(ask)) if ask is not None else None,
                )
                payload["trade_classification"] = classification
                premium = price_decimal * Decimal(size) * Decimal(100)
                if classification == "ask":
                    self._ask_premium[key] += premium
                    self._net_call_premium[key] += premium
                elif classification == "bid":
                    self._net_call_premium[key] -= premium
                payload["ask_premium"] = self._ask_premium[key]
                payload["call_premium"] = self._net_call_premium[key]
        return event.model_copy(update={"payload": payload})
