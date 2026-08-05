"""Continuous, bounded event-to-decision processing."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from time import monotonic
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

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
from .persistence_async import AsyncAuditWriter, AuditWrite
from .ports import EventRepository
from .quote_book import QuoteBook, QuoteConsumption
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
    timings: list["EventTiming"] | None = None
    evaluations_triggered: int = 0
    evaluations_skipped: int = 0
    skip_reasons: dict[str, int] | None = None
    persistence_backpressure_failure: bool = False
    trades_received: int = 0
    trades_processed: int = 0


@dataclass(frozen=True)
class EventTiming:
    provider: str
    event_kind: str
    source_timestamp: datetime
    received_timestamp: datetime
    processing_timestamp: datetime
    event_age_at_receipt_ms: float
    processing_duration_ms: float
    precision_conversion_required: bool
    timezone_conversion_required: bool
    processing_started_timestamp: datetime | None = None
    processing_completed_timestamp: datetime | None = None
    internal_queue_wait_ms: float | None = None
    total_age_at_completion_ms: float | None = None


class SignalPipeline:
    def __init__(
        self,
        settings: Settings,
        repository: EventRepository | None = None,
        now: Callable[[], datetime] | None = None,
        run_id: UUID | None = None,
        writer: AsyncAuditWriter | None = None,
        quote_book: QuoteBook | None = None,
    ) -> None:
        self.settings = settings
        self.repository: EventRepository = repository or InMemoryRepository()
        self.writer = writer
        self.quote_book = quote_book or QuoteBook(
            timedelta(milliseconds=500 + settings.allowed_lateness_seconds * 1000)
        )
        self.now = now or (lambda: datetime.now(UTC))
        self.run_id = run_id or uuid4()
        self._ingest_order = 0
        self._decision_order = 0
        self.synchronizer = Synchronizer()
        self.metrics = PipelineMetrics([], [], [], timings=[], skip_reasons={})
        self._seen: set[object] = set()
        self._decided_states: set[tuple[object, ...]] = set()
        self._sessions: dict[str, str] = {}
        self._equity_volume: dict[str, int] = {}
        self._option_volume: dict[str, int] = {}
        self._ask_premium: dict[str, Decimal] = {}
        self._net_call_premium: dict[str, Decimal] = {}
        self.decisions: list[Decision] = []
        self._last_evaluated: dict[str, object] = {}
        self.incomplete_run = False

    def process(self, event: CanonicalEvent) -> Decision | None:
        started = monotonic()
        processing_time = self.now().astimezone(UTC)
        age_at_receipt_ms = (
            event.received_timestamp - event.source_timestamp
        ).total_seconds() * 1000
        age_ms = (processing_time - event.source_timestamp).total_seconds() * 1000
        self.metrics.event_age_ms.append(age_ms)
        if event.payload.get("provider_event_kind") == "quote":
            self.quote_book.receive(event)
            self._record_timing(event, processing_time, age_at_receipt_ms, started)
            return None
        self.metrics.trades_received += 1
        if age_ms > self.synchronizer.max_staleness.total_seconds() * 1000:
            self.metrics.stale_events += 1
            self._record_timing(event, processing_time, age_at_receipt_ms, started)
            return None
        if event.event_id in self._seen:
            self.metrics.duplicate_events += 1
            self._record_timing(event, processing_time, age_at_receipt_ms, started)
            return None
        self._seen.add(event.event_id)
        quote_consumption = self.quote_book.consume_for_trade(event)
        if quote_consumption is not None:
            quote_consumption = QuoteConsumption(
                quote_consumption.consumption_order,
                quote_consumption.quote_event_id,
                quote_consumption.trade_event_id,
                quote_consumption.quote_role,
                quote_consumption.quote.model_copy(update={"run_id": self.run_id}),
            )
            if not self._enqueue(AuditWrite(quote_consumption=quote_consumption)):
                return None
            quote_payload = quote_consumption.quote.payload
            payload = dict(event.payload)
            payload["quote_context"] = {
                **dict(payload.get("quote_context", {})),
                "bid": quote_payload.get("bid", quote_payload.get("quote_context", {}).get("bid")),
                "ask": quote_payload.get("ask", quote_payload.get("quote_context", {}).get("ask")),
                "timestamp": quote_consumption.quote.source_timestamp.isoformat(),
                "contract": quote_payload.get("contract"),
            }
            event = event.model_copy(update={"payload": payload})
        event = self._enrich_state(event)
        self._ingest_order += 1
        event = event.model_copy(
            update={
                "run_id": self.run_id if event.run_id == UUID(int=0) else event.run_id,
                "ingest_order": self._ingest_order
                if event.ingest_order == 0
                else event.ingest_order,
            }
        )
        if not self._enqueue(AuditWrite(event=event)):
            return None
        self.metrics.trades_processed += 1
        if not self.synchronizer.add(event):
            self.metrics.out_of_order_events += 1
            self._record_timing(event, processing_time, age_at_receipt_ms, started)
            return None
        if event.kind == EventKind.EQUITY and event.symbol != "AAPL":
            kind = EventKind.MARKET_INDEX if event.symbol == "SPY" else EventKind.SECTOR_INDEX
            self.synchronizer.add(event.model_copy(update={"kind": kind, "symbol": "AAPL"}))
        snapshot = self.synchronizer.snapshot("AAPL", event.normalized_timestamp)
        if snapshot is None:
            self._record_timing(event, processing_time, age_at_receipt_ms, started)
            return None
        state_id = tuple(sorted(snapshot.event_ids))
        reasons = self._material_change_reasons(snapshot, event)
        if not reasons:
            self.metrics.evaluations_skipped += 1
            self._skip("unchanged_synchronized_state")
            self._record_timing(event, processing_time, age_at_receipt_ms, started)
            return None
        for consumption in self.quote_book.consume_current_state():
            current_consumption = QuoteConsumption(
                consumption.consumption_order,
                consumption.quote_event_id,
                consumption.trade_event_id,
                consumption.quote_role,
                consumption.quote.model_copy(update={"run_id": self.run_id}),
            )
            if not self._enqueue(AuditWrite(quote_consumption=current_consumption)):
                return None
        self._decided_states.add(state_id)
        decision = decide([snapshot], self.settings).model_copy(
            update={
                "run_id": self.run_id,
                "triggering_change_reasons": tuple(reasons),
                "synchronized_state_identity": ",".join(map(str, sorted(snapshot.event_ids))),
            }
        )
        self._decision_order += 1
        decision = decision.model_copy(update={"decision_order": self._decision_order})
        if not self._enqueue(AuditWrite(decision=decision)):
            return None
        self.metrics.evaluations_triggered += 1
        self.decisions.append(decision)
        self._record_timing(event, processing_time, age_at_receipt_ms, started)
        return decision

    def _enqueue(self, record: AuditWrite) -> bool:
        if self.writer is None:
            if record.event is not None:
                self.repository.record_event(record.event)
            if record.decision is not None:
                self.repository.record_decision(record.decision)
            return True
        accepted = self.writer.enqueue(record)
        if not accepted:
            self.incomplete_run = True
            self.metrics.persistence_backpressure_failure = True
        return accepted

    def _skip(self, reason: str) -> None:
        if self.metrics.skip_reasons is not None:
            self.metrics.skip_reasons[reason] = self.metrics.skip_reasons.get(reason, 0) + 1

    def _material_change_reasons(self, snapshot: object, event: CanonicalEvent) -> list[str]:
        from .schemas import SynchronizedInput

        assert isinstance(snapshot, SynchronizedInput)
        previous = self._last_evaluated.get(snapshot.symbol)
        self._last_evaluated[snapshot.symbol] = snapshot
        if previous is None:
            return ["initial_state"]
        assert isinstance(previous, SynchronizedInput)
        reasons: list[str] = []
        if (
            previous.price is not None
            and snapshot.price is not None
            and previous.price != 0
            and abs((snapshot.price - previous.price) / previous.price) >= Decimal("0.0001")
        ):
            reasons.append("underlying_price_threshold")
        if event.kind == EventKind.OPTIONS and event.payload.get("eligible_trade"):
            reasons.append("new_eligible_option_trade")
        for name, old, new, boundary in (
            ("rvol_boundary", previous.relative_volume, snapshot.relative_volume, Decimal(2)),
            (
                "equity_strength_boundary",
                previous.equity_delta,
                snapshot.equity_delta,
                Decimal(0),
            ),
            (
                "market_strength_boundary",
                previous.market_delta,
                snapshot.market_delta,
                Decimal(0),
            ),
            (
                "sector_strength_boundary",
                previous.sector_delta,
                snapshot.sector_delta,
                Decimal(0),
            ),
            (
                "call_premium_boundary",
                previous.call_premium,
                snapshot.call_premium,
                Decimal(500000),
            ),
            (
                "room_boundary",
                previous.distance_to_resistance,
                snapshot.distance_to_resistance,
                self.settings.thresholds.minimum_room,
            ),
            (
                "spread_boundary",
                previous.spread,
                snapshot.spread,
                self.settings.thresholds.maximum_spread,
            ),
            (
                "ask_side_boundary",
                previous.ask_side_percentage,
                snapshot.ask_side_percentage,
                Decimal(65),
            ),
        ):
            if old is not None and new is not None and (old < boundary) != (new < boundary):
                reasons.append(name)
        if previous.resistance_state != snapshot.resistance_state:
            reasons.append("resistance_state")
        if previous.quote_validity != snapshot.quote_validity:
            reasons.append("quote_validity_transition")
        return reasons

    def tick(self) -> Decision | None:
        """Timer-driven freshness check, independent of incoming events."""
        now = self.now().astimezone(UTC)
        current = self.synchronizer.snapshot("AAPL", now, allow_stale=True)
        if current is None:
            self._skip("freshness_window_expiry")
            return None
        previous = self._last_evaluated.get(current.symbol)
        if previous is None or not hasattr(previous, "as_of"):
            self._skip("freshness_window_expiry")
            return None
        if now - previous.as_of <= self.synchronizer.max_staleness:
            self._skip("freshness_window_not_expired")
            return None
        decision = decide([current], self.settings).model_copy(
            update={
                "run_id": self.run_id,
                "decision_id": uuid5(
                    NAMESPACE_URL,
                    f"freshness:{self.run_id}:{','.join(map(str, current.event_ids))}",
                ),
                "triggering_change_reasons": ("freshness_window_expiry",),
                "synchronized_state_identity": ",".join(map(str, sorted(current.event_ids))),
            }
        )
        self._decision_order += 1
        decision = decision.model_copy(update={"decision_order": self._decision_order})
        if not self._enqueue(AuditWrite(decision=decision)):
            return None
        self.metrics.evaluations_triggered += 1
        self.decisions.append(decision)
        self._last_evaluated[current.symbol] = current
        return decision

    def _record_timing(
        self,
        event: CanonicalEvent,
        processing_time: datetime,
        age_at_receipt_ms: float,
        started: float,
    ) -> None:
        processing_duration_ms = (monotonic() - started) * 1000
        completed = self.now().astimezone(UTC)
        queue_wait_ms = (processing_time - event.received_timestamp).total_seconds() * 1000
        total_age_ms = (completed - event.source_timestamp).total_seconds() * 1000
        self.metrics.provider_transport_latency_ms.append(age_at_receipt_ms)
        self.metrics.processing_latency_ms.append(processing_duration_ms)
        if self.metrics.timings is None:
            return
        timing_payload = event.payload.get("timestamp_conversion", {})
        self.metrics.timings.append(
            EventTiming(
                provider=event.source,
                event_kind=str(event.payload.get("provider_event_kind", event.kind.value)),
                source_timestamp=event.source_timestamp,
                received_timestamp=event.received_timestamp,
                processing_timestamp=processing_time,
                event_age_at_receipt_ms=age_at_receipt_ms,
                processing_duration_ms=processing_duration_ms,
                precision_conversion_required=bool(
                    timing_payload.get("precision_converted", False)
                ),
                timezone_conversion_required=bool(timing_payload.get("timezone_converted", False)),
                processing_started_timestamp=processing_time,
                processing_completed_timestamp=completed,
                internal_queue_wait_ms=queue_wait_ms,
                total_age_at_completion_ms=total_age_ms,
            )
        )

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
