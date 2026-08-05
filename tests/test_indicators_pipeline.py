from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

from institutional_signal_engine.config import Settings
from institutional_signal_engine.indicators import (
    HistoricalMarketDataPort,
    IndicatorCalculator,
    PreviousClose,
    classify_option_trade,
)
from institutional_signal_engine.pipeline import SignalPipeline
from institutional_signal_engine.schemas import CanonicalEvent, EventKind, SynchronizedInput
from institutional_signal_engine.signals import decide

NOW = datetime(2026, 8, 5, 14, 31, tzinfo=UTC)


class History(HistoricalMarketDataPort):
    def previous_close(self, symbol: str, session: str):
        return PreviousClose(symbol, Decimal(100), NOW, "fixture:previous-close")

    def cumulative_volume_baseline(self, symbol: str, minute: int):
        return (Decimal(100), Decimal(100))

    def completed_session_highs(self, symbol: str, session: str):
        return {
            f"2026-08-{day:02d}": Decimal(value)
            for day, value in [(1, 110), (2, 111), (3, 112), (4, 113), (5, 114)]
        }

    def adjustment_metadata(self, symbol: str, session: str) -> str:
        return "split"

    def provenance(self, symbol: str, session: str) -> str:
        return "fixture:historical-bootstrap"


def test_stateful_equity_features_use_previous_close_vwap_and_rvol():
    calculator = IndicatorCalculator(History())
    first = calculator.equity("AAPL", NOW, Decimal(101), 10, ["@"])
    second = calculator.equity("AAPL", NOW, Decimal(102), 10, ["@"])
    assert first["previous_close_return"] == Decimal("0.01")
    assert second["session_vwap"] == Decimal("101.5")
    assert second["relative_volume"] == Decimal("0.2")
    assert second["delta"] == Decimal("0.02")
    assert second["resistance_state"] == "OVERHEAD_RESISTANCE"
    assert second["resistance_provenance"] == "fixture:historical-bootstrap"


def test_missing_historical_baselines_fail_closed_without_zero_indicators():
    calculator = IndicatorCalculator()
    result = calculator.equity("AAPL", NOW, Decimal(101), 10, ["@"])
    assert result["delta"] is None
    assert result["relative_volume"] is None
    assert result["distance_to_resistance"] is None
    assert "missing_previous_close" in result["reasons"]
    assert "missing_rvol_baseline" in result["reasons"]
    assert "missing_resistance_baseline" in result["reasons"]


def test_excluded_or_out_of_session_equity_trade_does_not_change_volume():
    calculator = IndicatorCalculator(History())
    premarket = datetime(2026, 8, 5, 13, 29, tzinfo=UTC)
    result = calculator.equity("AAPL", premarket, Decimal(101), 1000, ["@"])
    assert result["volume"] == 0
    assert result["session_vwap"] is None


def test_calculated_indicators_reach_pipeline_event_and_decision_inputs():
    pipeline = SignalPipeline(Settings(), indicator_calculator=IndicatorCalculator(History()))
    raw = CanonicalEvent(
        event_id=uuid4(),
        kind=EventKind.EQUITY,
        symbol="AAPL",
        source="fixture",
        source_timestamp=NOW,
        received_timestamp=NOW,
        normalized_timestamp=NOW,
        sequence=1,
        payload={
            "provider_event_kind": "trade",
            "price": Decimal(101),
            "trade_volume": 10,
            "conditions": ("@",),
            "feature_reasons": ("requires_historical_baseline",),
        },
    )
    calculated = pipeline._apply_indicators(raw)
    assert calculated.payload["_calculated_indicators"] is True
    assert calculated.payload["delta"] == Decimal("0.01")
    assert calculated.payload["session_vwap"] == Decimal(101)
    assert calculated.payload["relative_volume"] == Decimal("0.1")
    assert calculated.payload["feature_reasons"] == (
        "missing_spy_baseline",
        "missing_sector_baseline",
    )
    assert calculated.payload["indicator_provenance"]["historical"] == "fixture:previous-close"


def test_indicator_provenance_is_carried_into_persisted_decision():
    item = SynchronizedInput(
        symbol="AAPL",
        as_of=NOW,
        price=Decimal(101),
        volume=100000,
        option_volume=2,
        open_interest=1,
        call_premium=Decimal(100000),
        spread=Decimal("0.01"),
        distance_to_resistance=Decimal(1),
        resistance_state="OVERHEAD_RESISTANCE",
        relative_volume=Decimal(2),
        equity_delta=Decimal("0.01"),
        market_delta=Decimal("0.01"),
        sector_delta=Decimal("0.01"),
        concurrent_positions=0,
        event_ids=(uuid4(),),
        provenance={"historical": "fixture:historical-bootstrap", "adjustment": "split"},
    )
    decision = decide([item], Settings())
    assert decision.indicator_provenance == item.provenance


def test_relative_strength_uses_identical_previous_close_window():
    pipeline = SignalPipeline(Settings(), indicator_calculator=IndicatorCalculator(History()))

    def raw(symbol: str, price: int, sequence: int) -> CanonicalEvent:
        return CanonicalEvent(
            event_id=uuid4(),
            kind=EventKind.EQUITY,
            symbol=symbol,
            source="fixture",
            source_timestamp=NOW,
            received_timestamp=NOW,
            normalized_timestamp=NOW,
            sequence=sequence,
            payload={
                "provider_event_kind": "trade",
                "price": price,
                "trade_volume": 10,
                "conditions": ("@",),
            },
        )

    pipeline._apply_indicators(raw("SPY", 101, 1))
    pipeline._apply_indicators(raw("XLK", 102, 1))
    aapl = pipeline._apply_indicators(raw("AAPL", 104, 1))
    assert aapl.payload["relative_strength_vs_spy"] == Decimal("0.03")
    assert aapl.payload["relative_strength_vs_sector"] == Decimal("0.02")


def test_trade_classification_preserves_unknown_between_quotes():
    assert classify_option_trade(Decimal("1.01"), Decimal(1), Decimal("1.01")) == "ask"
    assert classify_option_trade(Decimal("1.005"), Decimal(1), Decimal("1.01")) == "unknown"


def test_pipeline_persists_once_and_does_not_duplicate_unchanged_state():
    pipeline = SignalPipeline(Settings(), now=lambda: NOW)
    events = []
    for kind, symbol, payload in [
        (EventKind.EQUITY, "AAPL", {"price": 101, "volume": 100000, "spread": 0.01, "delta": 1}),
        (EventKind.EQUITY, "SPY", {"price": 501, "volume": 100000, "spread": 0.01, "delta": 1}),
        (EventKind.EQUITY, "XLK", {"price": 201, "volume": 100000, "spread": 0.01, "delta": 1}),
        (
            EventKind.OPTIONS,
            "AAPL",
            {
                "option_volume": 2,
                "open_interest": 1,
                "call_premium": 100000,
                "distance_to_resistance": 1,
            },
        ),
    ]:
        events.append(
            CanonicalEvent(
                event_id=uuid4(),
                kind=kind,
                symbol=symbol,
                source="fixture",
                source_timestamp=NOW,
                received_timestamp=NOW,
                normalized_timestamp=NOW,
                sequence=1,
                payload=payload,
            )
        )
    results = [pipeline.process(event) for event in events]
    assert results[-1] is not None
    assert pipeline.process(events[-1]) is None
    assert len(pipeline.decisions) == 1
    assert pipeline.metrics.duplicate_events == 1


def test_session_boundary_open_and_close_are_idempotent():
    pipeline = SignalPipeline(Settings(), now=lambda: NOW)
    open_time = datetime(2026, 8, 5, 13, 30, tzinfo=UTC)
    close_time = datetime(2026, 8, 5, 20, 0, tzinfo=UTC)
    pipeline._handle_session_transition(open_time)
    pipeline._handle_session_transition(open_time)
    assert pipeline._pending_boundary_reasons == ["session_boundary_open"]
    pipeline._pending_boundary_reasons.clear()
    pipeline._handle_session_transition(close_time)
    pipeline._handle_session_transition(close_time)
    assert pipeline._pending_boundary_reasons == ["session_boundary_close"]


def test_alpaca_placeholder_indicators_cannot_reach_decisions():
    pipeline = SignalPipeline(Settings(), now=lambda: NOW)
    event = CanonicalEvent(
        event_id=uuid4(),
        kind=EventKind.EQUITY,
        symbol="AAPL",
        source="alpaca",
        source_timestamp=NOW,
        received_timestamp=NOW,
        normalized_timestamp=NOW,
        sequence=1,
        payload={
            "provider_event_kind": "trade",
            "price": Decimal(101),
            "volume": 100000,
            "feature_reasons": ("requires_historical_baseline",),
        },
    )
    assert pipeline.process(event) is None
    assert pipeline.decisions == []
    assert pipeline.metrics.evaluations_triggered == 0


def test_tick_detects_boundary_without_market_event():
    current = [datetime(2026, 8, 5, 13, 29, tzinfo=UTC)]
    pipeline = SignalPipeline(Settings(), now=lambda: current[0])
    current[0] = datetime(2026, 8, 5, 13, 30, tzinfo=UTC)
    assert pipeline.tick() is None
    assert pipeline._pending_boundary_reasons == ["session_boundary_open"]
