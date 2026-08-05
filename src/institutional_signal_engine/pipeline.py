"""Continuous, bounded event-to-decision processing."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from time import monotonic
from uuid import UUID, uuid4

from .config import Settings
from .indicators import (
    OptionContractIdentity,
    OptionQuoteContext,
    OptionTradeContext,
    classify_ask_side,
    regular_session,
    session_key,
)
from .persistence import InMemoryRepository
from .ports import EventRepository
from .schemas import CanonicalEvent, Decision, EventKind
from .signals import decide
from .synchronization import Synchronizer
from .thetadata_conditions import CONDITION_MAPPING_VERSION, eligible_condition


@dataclass
class PipelineMetrics:
    provider_transport_latency_ms: list[float]
    processing_latency_ms: list[float]
    event_age_ms: list[float]
    late_events: int = 0
    stale_events: int = 0
    out_of_order_events: int = 0
    duplicate_events: int = 0
    unknown_condition_events: int = 0


class SignalPipeline:
    def __init__(
        self,
        settings: Settings,
        repository: EventRepository | None = None,
        now: Callable[[], datetime] | None = None,
        run_id: UUID | None = None,
    ) -> None:
        self.settings = settings
        self.repository: EventRepository = repository or InMemoryRepository()
        self.now = now or (lambda: datetime.now(UTC))
        self.run_id = run_id or uuid4()
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
        if event.run_id == UUID(int=0):
            event = event.model_copy(update={"run_id": self.run_id})
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
        decision = decide([snapshot], self.settings).model_copy(update={"run_id": self.run_id})
        self.repository.record_decision(decision)
        self.decisions.append(decision)
        return decision

    def _enrich_state(self, event: CanonicalEvent) -> CanonicalEvent:
        payload = dict(event.payload)
        if payload.get("_state_enriched") is True:
            return event
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
            contract_data = payload.get("contract", {})
            right = str(contract_data.get("right", "")).upper()
            condition_code = payload.get("condition_code")
            # The mapping is intentionally numeric and versioned; no text matching occurs.
            eligible = (
                right == "C"
                and size >= 1
                and regular_session(event.source_timestamp)
                and isinstance(condition_code, int)
                and eligible_condition(condition_code)
            )
            payload["condition_mapping_version"] = CONDITION_MAPPING_VERSION
            payload["eligible_trade"] = eligible
            if isinstance(condition_code, int) and condition_code not in range(98):
                self.metrics.unknown_condition_events += 1
                payload["eligibility_reason"] = "unknown_condition_code"
            elif not eligible:
                payload["eligibility_reason"] = "excluded_or_non_call_or_session_or_size"
            if not eligible:
                payload["_state_enriched"] = True
                return event.model_copy(update={"payload": payload})
            self._option_volume[key] += size
            payload["option_volume"] = self._option_volume[key]
            price = payload.get("trade_price")
            quote = payload.get("quote_context", {})
            if price is not None:
                price_decimal = Decimal(str(price))
                identity = OptionContractIdentity(
                    str(contract_data.get("root", "")).upper(),
                    int(contract_data.get("expiration", 0)),
                    int(contract_data.get("strike", 0)),
                    right,
                )
                trade_time = event.source_timestamp
                quote_time = quote.get("timestamp")
                quote_context = None
                if quote.get("bid") is not None and quote.get("ask") is not None and quote_time:
                    quote_context = OptionQuoteContext(
                        Decimal(str(quote["bid"])),
                        Decimal(str(quote["ask"])),
                        datetime.fromisoformat(str(quote_time)),
                        identity,
                    )
                classification = classify_ask_side(
                    OptionTradeContext(price_decimal, trade_time, identity), quote_context
                )
                payload["trade_classification"] = classification
                premium = price_decimal * Decimal(size) * Decimal(100)
                if classification == "ask":
                    self._ask_premium[key] += premium
                    self._net_call_premium[key] += premium
                payload["ask_premium"] = self._ask_premium[key]
                payload["call_premium"] = self._net_call_premium[key]
        payload["_state_enriched"] = True
        return event.model_copy(update={"payload": payload})
