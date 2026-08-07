"""Continuous, bounded event-to-decision processing."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from time import monotonic
from typing import cast
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from .config import Settings
from .indicators import (
    ET,
    IndicatorCalculator,
    OptionContractIdentity,
    OptionQuoteContext,
    OptionTradeContext,
    classify_ask_side,
    regular_session,
    session_key,
    session_phase,
)
from .persistence import InMemoryRepository
from .persistence_async import AsyncAuditWriter, AuditWrite
from .ports import EventRepository
from .quote_book import QuoteBook, QuoteConsumption
from .schemas import CanonicalEvent, Decision, EventKind
from .signals import decide
from .sweeps import SweepEngine
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
        indicator_calculator: IndicatorCalculator | None = None,
        symbols: tuple[str, ...] = ("AAPL",),
        sector_by_symbol: dict[str, str] | None = None,
    ) -> None:
        self.settings = settings
        self.repository: EventRepository = repository or InMemoryRepository()
        self.writer = writer
        self.quote_book = quote_book or QuoteBook(
            timedelta(milliseconds=500 + settings.allowed_lateness_seconds * 1000)
        )
        self.now = now or (lambda: datetime.now(UTC))
        self.indicator_calculator = indicator_calculator
        self.run_id = run_id or uuid4()
        self._ingest_order = 0
        self._decision_order = 0
        self.symbols = tuple(sorted({symbol.upper() for symbol in symbols})) or ("AAPL",)
        self.sector_by_symbol = {
            symbol: value.upper() for symbol, value in (sector_by_symbol or {}).items()
        }
        self._synchronizers = {symbol: Synchronizer() for symbol in self.symbols}
        # Retain the singular attribute for callers and older tests.
        self.synchronizer = self._synchronizers[self.symbols[0]]
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
        self._indicator_results: dict[str, tuple[datetime, dict[str, object]]] = {}
        self._last_session_phase: str | None = None
        self._last_session_date: str | None = None
        self._boundary_tokens_seen: set[tuple[str, str]] = set()
        self._pending_boundary_reasons: list[str] = []
        self._session_generation = 0
        self.sweeps = SweepEngine(self.run_id)
        self._pending_sweep_reasons: list[str] = []
        self._pending_closed_sweep_audits: list[dict[str, object]] = []
        self.incomplete_run = False
        self.synchronized_input_count = 0
        self.incomplete_state_reasons: dict[str, list[str]] = {
            symbol: [] for symbol in self.symbols
        }

    def process(self, event: CanonicalEvent) -> Decision | None:
        started = monotonic()
        processing_time = self.now().astimezone(UTC)
        self._handle_session_transition(event.source_timestamp)
        if event.payload.get("provider_event_kind") == "sweep_timer":
            self._record_timing(event, processing_time, 0.0, started)
            return None
        age_at_receipt_ms = (
            event.received_timestamp - event.source_timestamp
        ).total_seconds() * 1000
        age_ms = (processing_time - event.source_timestamp).total_seconds() * 1000
        self.metrics.event_age_ms.append(age_ms)
        if event.payload.get("provider_event_kind") == "quote":
            self._ingest_order += 1
            event = event.model_copy(
                update={
                    "run_id": self.run_id if event.run_id == UUID(int=0) else event.run_id,
                    "ingest_order": self._ingest_order
                    if event.ingest_order == 0
                    else event.ingest_order,
                }
            )
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
        event = self._apply_indicators(event)
        if (
            event.kind in {EventKind.EQUITY, EventKind.MARKET_INDEX, EventKind.SECTOR_INDEX}
            and event.source == "alpaca"
            and event.payload.get("_calculated_indicators") is not True
        ):
            payload = dict(event.payload)
            payload["feature_reasons"] = tuple(
                dict.fromkeys(
                    tuple(payload.get("feature_reasons", ()))
                    + ("missing_live_indicator_calculator",)
                )
            )
            event = event.model_copy(update={"payload": payload})
            self._enqueue(AuditWrite(event=event))
            self._record_timing(event, processing_time, age_at_receipt_ms, started)
            return None
        event = self._enrich_state(event)
        sweep_audit: dict[str, object] | None = None
        closed_sweep_audits: tuple[dict[str, object], ...] = ()
        if event.kind == EventKind.OPTIONS:
            sweep_update = self.sweeps.process(event)
            closed_sweep_audits = sweep_update.closed_audits
            payload = dict(event.payload)
            sweep_state = self.sweeps.snapshot()
            payload.update(sweep_state)
            if sweep_update.cluster is not None:
                sweep_audit = sweep_update.audit
                payload["sweep_cluster_id"] = str(sweep_update.cluster.cluster_id)
                payload["sweep_cluster_thresholds"] = (
                    sweep_update.audit.get("thresholds", {}) if sweep_update.audit else {}
                )
            if sweep_update.reason is not None:
                self._pending_sweep_reasons.append(sweep_update.reason)
            event = event.model_copy(update={"payload": payload})
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
        if sweep_audit is not None and not self._enqueue(AuditWrite(sweep=sweep_audit)):
            return None
        for audit in closed_sweep_audits:
            if not self._enqueue(AuditWrite(sweep=audit)):
                return None
        for audit in sweep_update.transition_audits if event.kind == EventKind.OPTIONS else ():
            if not self._enqueue(AuditWrite(sweep=audit)):
                return None
        for audit in self._pending_closed_sweep_audits:
            if not self._enqueue(AuditWrite(sweep=audit)):
                return None
        self._pending_closed_sweep_audits.clear()
        self.metrics.trades_processed += 1
        targets: list[str] = []
        if (
            event.kind in {EventKind.EQUITY, EventKind.OPTIONS}
            and event.symbol in self._synchronizers
        ):
            targets.append(event.symbol)
        elif event.kind == EventKind.EQUITY and event.symbol == "SPY":
            event = event.model_copy(update={"kind": EventKind.MARKET_INDEX})
            targets.extend(self.symbols)
        elif event.kind == EventKind.EQUITY and event.symbol == "XLK":
            event = event.model_copy(update={"kind": EventKind.SECTOR_INDEX})
            targets.extend(
                symbol
                for symbol in self.symbols
                if self.sector_by_symbol.get(symbol, "XLK") == "XLK"
            )
        elif event.kind == EventKind.MARKET_INDEX:
            targets.extend(self.symbols)
        elif event.kind == EventKind.SECTOR_INDEX:
            targets.extend(
                symbol
                for symbol in self.symbols
                if self.sector_by_symbol.get(symbol, event.symbol) == event.symbol
            )
        accepted = False
        for symbol in targets:
            target_event = event.model_copy(update={"symbol": symbol})
            if self._synchronizers[symbol].add(target_event):
                accepted = True
        if not accepted:
            self.metrics.out_of_order_events += 1
            self._record_timing(event, processing_time, age_at_receipt_ms, started)
            return None
        first_decision: Decision | None = None
        for symbol in dict.fromkeys(targets):
            snapshot = self._synchronizers[symbol].snapshot(symbol, event.normalized_timestamp)
            if snapshot is None:
                self.incomplete_state_reasons[symbol] = [
                    "missing_required_equity_options_market_or_sector_state"
                ]
                continue
            self.synchronized_input_count += 1
            self.incomplete_state_reasons[symbol] = []
            state_id = tuple(sorted(snapshot.event_ids))
            reasons = self._material_change_reasons(snapshot, event)
            if not reasons:
                self.metrics.evaluations_skipped += 1
                self._skip("unchanged_synchronized_state")
                continue
            for consumption in self.quote_book.consume_current_state():
                current_consumption = QuoteConsumption(
                    consumption.consumption_order,
                    consumption.quote_event_id,
                    consumption.trade_event_id,
                    consumption.quote_role,
                    consumption.quote.model_copy(update={"run_id": self.run_id}),
                )
                if not self._enqueue(AuditWrite(quote_consumption=current_consumption)):
                    return first_decision
            self._decided_states.add(state_id)
            decision = decide([snapshot], self.settings).model_copy(
                update={
                    "run_id": self.run_id,
                    "triggering_change_reasons": tuple(reasons),
                    "synchronized_state_identity": ",".join(map(str, sorted(snapshot.event_ids))),
                    "sweep_state": self.sweeps.snapshot(),
                    "indicator_provenance": {
                        **snapshot.provenance,
                        "evaluation_symbol": snapshot.symbol,
                    },
                }
            )
            self._decision_order += 1
            decision = decision.model_copy(update={"decision_order": self._decision_order})
            if not self._enqueue(AuditWrite(decision=decision)):
                return first_decision
            self.metrics.evaluations_triggered += 1
            self.decisions.append(decision)
            first_decision = first_decision or decision
            if any(reason.startswith("session_boundary_") for reason in reasons):
                self._pending_boundary_reasons.clear()
            self._pending_sweep_reasons.clear()
        self._record_timing(event, processing_time, age_at_receipt_ms, started)
        return first_decision

    def _enqueue(self, record: AuditWrite) -> bool:
        if self.writer is None:
            if record.event is not None:
                self.repository.record_event(record.event)
            if record.decision is not None:
                self.repository.record_decision(record.decision)
            if record.sweep is not None:
                self.repository.record_sweep(record.sweep)
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
        if previous is None:
            initial_reasons = [
                "initial_state",
                *self._pending_boundary_reasons,
                *self._pending_sweep_reasons,
            ]
            self._last_evaluated[snapshot.symbol] = snapshot
            return initial_reasons
        assert isinstance(previous, SynchronizedInput)
        self._last_evaluated[snapshot.symbol] = snapshot
        reasons: list[str] = []
        if self._pending_boundary_reasons:
            reasons.extend(self._pending_boundary_reasons)
        if self._pending_sweep_reasons:
            reasons.extend(self._pending_sweep_reasons)
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
        self._handle_session_transition(now)
        for audit in self._pending_closed_sweep_audits:
            if not self._enqueue(AuditWrite(sweep=audit)):
                return None
        self._pending_closed_sweep_audits.clear()
        sweep_update = self.sweeps.tick(now)
        self._refresh_sweep_state_in_synchronizer()
        if sweep_update.reason is not None:
            self._pending_sweep_reasons.append(sweep_update.reason)
            timer_event = CanonicalEvent(
                event_id=uuid5(
                    NAMESPACE_URL,
                    f"sweep-timer:{self.run_id}:{now.isoformat()}:{','.join(value.isoformat() for value in sweep_update.expired_timestamps)}",
                ),
                run_id=self.run_id,
                ingest_order=self._ingest_order + 1,
                kind=EventKind.OPTIONS,
                symbol="AAPL",
                source="engine",
                source_timestamp=now,
                received_timestamp=now,
                normalized_timestamp=now,
                sequence=self._ingest_order + 1,
                payload={
                    "provider_event_kind": "sweep_timer",
                    "sweep_expiry_timestamps": [
                        value.isoformat() for value in sweep_update.expired_timestamps
                    ],
                    "trigger_reason": sweep_update.reason,
                },
            )
            self._ingest_order += 1
            if not self._enqueue(AuditWrite(event=timer_event)):
                return None
            for audit in sweep_update.transition_audits:
                if not self._enqueue(AuditWrite(sweep=audit)):
                    return None
        if self._pending_sweep_reasons:
            current = self.synchronizer.snapshot(self.symbols[0], now, allow_stale=True)
            if current is not None:
                decision = decide([current], self.settings).model_copy(
                    update={
                        "run_id": self.run_id,
                        "decision_id": uuid5(
                            NAMESPACE_URL,
                            f"sweep:{self.run_id}:{','.join(self._pending_sweep_reasons)}:{current.event_ids}",
                        ),
                        "triggering_change_reasons": tuple(self._pending_sweep_reasons),
                        "synchronized_state_identity": ",".join(
                            map(str, sorted(current.event_ids))
                        ),
                        "sweep_state": self.sweeps.snapshot(),
                    }
                )
                self._decision_order += 1
                decision = decision.model_copy(update={"decision_order": self._decision_order})
                if not self._enqueue(AuditWrite(decision=decision)):
                    return None
                self.metrics.evaluations_triggered += 1
                self.decisions.append(decision)
                self._pending_sweep_reasons.clear()
                self._last_evaluated[current.symbol] = current
                return decision
        if self._pending_boundary_reasons:
            current = self.synchronizer.snapshot(self.symbols[0], now, allow_stale=True)
            if current is not None:
                decision = decide([current], self.settings).model_copy(
                    update={
                        "run_id": self.run_id,
                        "decision_id": uuid5(
                            NAMESPACE_URL,
                            f"boundary:{self.run_id}:{','.join(self._pending_boundary_reasons)}:{current.event_ids}",
                        ),
                        "triggering_change_reasons": tuple(self._pending_boundary_reasons),
                        "synchronized_state_identity": ",".join(
                            map(str, sorted(current.event_ids))
                        ),
                        "sweep_state": self.sweeps.snapshot(),
                    }
                )
                self._decision_order += 1
                decision = decision.model_copy(update={"decision_order": self._decision_order})
                if not self._enqueue(AuditWrite(decision=decision)):
                    return None
                self.metrics.evaluations_triggered += 1
                self.decisions.append(decision)
                self._pending_boundary_reasons.clear()
                self._last_evaluated[current.symbol] = current
                return decision
        current = self.synchronizer.snapshot(self.symbols[0], now, allow_stale=True)
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
        timer_event = CanonicalEvent(
            event_id=uuid5(
                NAMESPACE_URL,
                f"freshness-timer:{self.run_id}:{now.isoformat()}:{current.symbol}",
            ),
            run_id=self.run_id,
            ingest_order=self._ingest_order + 1,
            kind=EventKind.OPTIONS,
            symbol=current.symbol,
            source="engine",
            source_timestamp=now,
            received_timestamp=now,
            normalized_timestamp=now,
            sequence=self._ingest_order + 1,
            payload={
                "provider_event_kind": "sweep_timer",
                "sweep_expiry_timestamps": [
                    (previous.as_of + self.synchronizer.max_staleness).isoformat()
                ],
                "trigger_reason": "freshness_window_expiry",
            },
        )
        self._ingest_order += 1
        if not self._enqueue(AuditWrite(event=timer_event)):
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
                "sweep_state": self.sweeps.snapshot(),
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

    def _refresh_sweep_state_in_synchronizer(self) -> None:
        for symbol, synchronizer in self._synchronizers.items():
            current = synchronizer._events.get((symbol, EventKind.OPTIONS))
            if current is None:
                continue
            payload = dict(current.payload)
            payload.update(self.sweeps.snapshot())
            synchronizer._events[(current.symbol, EventKind.OPTIONS)] = current.model_copy(
                update={"payload": payload}
            )

    def _handle_session_transition(self, timestamp: datetime) -> None:
        local_date = timestamp.astimezone(ET).date().isoformat()
        phase = session_phase(timestamp)
        previous = self._last_session_phase
        if phase == "REGULAR" and previous != "REGULAR":
            token = (local_date, "open")
            if token not in self._boundary_tokens_seen:
                self._boundary_tokens_seen.add(token)
                self._session_generation += 1
                self._reset_session_state(local_date)
                self._pending_boundary_reasons.append("session_boundary_open")
        elif phase == "AFTER_HOURS" and previous == "REGULAR":
            token = (self._last_session_date or local_date, "close")
            if token not in self._boundary_tokens_seen:
                self._boundary_tokens_seen.add(token)
                self._pending_boundary_reasons.append("session_boundary_close")
                self._pending_closed_sweep_audits.extend(self.sweeps.close_session())
        self._last_session_phase = phase
        self._last_session_date = local_date

    def _reset_session_state(self, session_date: str) -> None:
        self._sessions.clear()
        self._equity_volume.clear()
        self._option_volume.clear()
        self._ask_premium.clear()
        self._net_call_premium.clear()
        self._last_evaluated.clear()
        self._indicator_results.clear()
        self.sweeps.reset_session(session_date)
        self._pending_sweep_reasons.clear()
        for synchronizer in self._synchronizers.values():
            synchronizer.reset_session()
        if self.indicator_calculator is not None:
            self.indicator_calculator.states.clear()
            self.indicator_calculator.resistance._levels.clear()
            self.indicator_calculator.resistance._metadata.clear()
            self.indicator_calculator.resistance._values.clear()

    def _apply_indicators(self, event: CanonicalEvent) -> CanonicalEvent:
        if self.indicator_calculator is None or event.kind not in {
            EventKind.EQUITY,
            EventKind.MARKET_INDEX,
            EventKind.SECTOR_INDEX,
        }:
            return event
        payload = dict(event.payload)
        price = payload.get("price")
        if price is None:
            return event
        result = self.indicator_calculator.equity(
            event.symbol,
            event.source_timestamp,
            Decimal(str(price)),
            int(payload.get("trade_volume", payload.get("volume", 0))),
            tuple(str(value) for value in payload.get("conditions", ())),
        )
        reasons = cast(tuple[str, ...], result["reasons"])
        delta = cast(Decimal | None, result["delta"])
        self._indicator_results[event.symbol] = (event.source_timestamp, result)
        payload.update(
            {
                "price": result["price"],
                "volume": result["volume"],
                "session_vwap": result["session_vwap"],
                "relative_volume": result["relative_volume"],
                "delta": result["delta"],
                "resistance_state": result["resistance_state"],
                "distance_to_resistance": result["distance_to_resistance"],
                "resistance_level": result["resistance_level"],
                "indicator_provenance": {
                    "historical": str(result["provenance"]),
                    "resistance": str(result["resistance_provenance"] or "missing"),
                    "adjustment": str(result["adjustment_metadata"]),
                    "session_generation": str(self._session_generation),
                    "resistance_source_sessions": ",".join(
                        str(value)
                        for value in cast(tuple[str, ...], result["resistance_source_sessions"])
                    ),
                },
                "feature_reasons": reasons,
                "_calculated_indicators": True,
            }
        )
        if event.symbol in self._synchronizers:
            market_entry = self._indicator_results.get("SPY")
            sector_entry = self._indicator_results.get(
                self.sector_by_symbol.get(event.symbol, "XLK")
            )
            market = market_entry[1] if market_entry is not None else None
            sector = sector_entry[1] if sector_entry is not None else None
            measurement = result["measurement_minute"]
            same_market_window = (
                market is not None
                and market.get("session") == result["session"]
                and market.get("measurement_minute") == measurement
            )
            same_sector_window = (
                sector is not None
                and sector.get("session") == result["session"]
                and sector.get("measurement_minute") == measurement
            )
            payload["relative_strength_vs_spy"] = IndicatorCalculator.relative_strength(
                delta,
                cast(Decimal | None, market.get("delta"))
                if same_market_window and market is not None
                else None,
            )
            payload["relative_strength_vs_sector"] = IndicatorCalculator.relative_strength(
                delta,
                cast(Decimal | None, sector.get("delta"))
                if same_sector_window and sector is not None
                else None,
            )
            if payload["relative_strength_vs_spy"] is None:
                payload["feature_reasons"] = tuple(payload["feature_reasons"]) + (
                    "missing_spy_baseline",
                )
            if payload["relative_strength_vs_sector"] is None:
                payload["feature_reasons"] = tuple(payload["feature_reasons"]) + (
                    "missing_sector_baseline",
                )
        return event.model_copy(update={"payload": payload})

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
                if classification == "unknown" and quote_context is not None:
                    quote_age = trade_time.astimezone(UTC) - quote_context.timestamp.astimezone(UTC)
                    if (
                        quote_context.bid > 0
                        and quote_context.ask >= quote_context.bid
                        and timedelta(0) <= quote_age <= timedelta(milliseconds=500)
                        and price_decimal <= quote_context.bid
                    ):
                        classification = "bid"
                payload["trade_classification"] = classification
                premium = price_decimal * Decimal(size) * Decimal(100)
                if classification == "ask":
                    self._ask_premium[key] += premium
                    self._net_call_premium[key] += premium
                payload["ask_premium"] = self._ask_premium[key]
                payload["call_premium"] = self._net_call_premium[key]
        payload["_state_enriched"] = True
        return event.model_copy(update={"payload": payload})
