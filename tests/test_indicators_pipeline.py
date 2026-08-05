from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

from institutional_signal_engine.config import Settings
from institutional_signal_engine.indicators import (
    HistoricalMarketDataPort,
    IndicatorCalculator,
    classify_option_trade,
)
from institutional_signal_engine.pipeline import SignalPipeline
from institutional_signal_engine.schemas import CanonicalEvent, EventKind

NOW = datetime(2026, 8, 5, 14, 31, tzinfo=UTC)


class History(HistoricalMarketDataPort):
    def previous_close(self, symbol: str, session: str):
        from institutional_signal_engine.indicators import PreviousClose

        return PreviousClose(symbol, Decimal(100), NOW, "fixture:previous-close")

    def cumulative_volume_baseline(self, symbol: str, minute: int):
        return (Decimal(100), Decimal(100))


def test_stateful_equity_features_use_previous_close_vwap_and_rvol():
    calculator = IndicatorCalculator(History())
    first = calculator.equity("AAPL", NOW, Decimal(101), 10, ["@"])
    second = calculator.equity("AAPL", NOW, Decimal(102), 10, ["@"])
    assert first["previous_close_return"] == Decimal("0.01")
    assert second["session_vwap"] == Decimal("101.5")
    assert second["relative_volume"] == Decimal("0.2")


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
