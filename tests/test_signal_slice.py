from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

from institutional_signal_engine.config import Settings, Thresholds
from institutional_signal_engine.replay import replay
from institutional_signal_engine.schemas import CanonicalEvent, EventKind, SynchronizedInput
from institutional_signal_engine.signals import decide, evaluate
from institutional_signal_engine.synchronization import Synchronizer

NOW = datetime(2026, 1, 1, tzinfo=UTC)


def item(symbol="AAPL", **overrides):
    values = {
        "price": Decimal(100),
        "volume": 100_000,
        "option_volume": 200,
        "open_interest": 100,
        "call_premium": Decimal(100000),
        "spread": Decimal("0.01"),
        "distance_to_resistance": Decimal("0.01"),
        "relative_volume": Decimal(2),
        "equity_delta": Decimal(1),
        "market_delta": Decimal(1),
        "sector_delta": Decimal(1),
        "relative_strength_vs_spy": Decimal(1),
        "relative_strength_vs_sector": Decimal(1),
        "qualifying_sweep_count": 3,
        "qualifying_sweep_premium": Decimal(500000),
        "session_sweep_gate": True,
        "most_recent_qualifying_sweep_timestamp": NOW,
        "first_signal_at": NOW,
        "concurrent_positions": 0,
        "event_ids": (uuid4(),),
    }
    values.update(overrides)
    as_of = values.pop("as_of", NOW)
    return SynchronizedInput(symbol=symbol, as_of=as_of, **values)


def test_formal_gates_boundary_and_reason():
    settings = Settings(thresholds=Thresholds(minimum_call_premium=Decimal(100000)))
    candidate = evaluate(item(call_premium=Decimal(99999)), settings)
    assert not next(g for g in candidate.gates if g.name == "options").passed
    assert (
        next(g for g in candidate.gates if g.name == "options").reason
        == "premium_or_volume_oi_threshold"
    )


def test_capacity_is_strictly_less_than_maximum():
    candidate = evaluate(item(concurrent_positions=1), Settings(capacity=1))
    assert not next(g for g in candidate.gates if g.name == "R").passed


def test_freshness_uses_only_most_recent_qualifying_sweep_timestamp():
    fresh = evaluate(
        item(
            as_of=NOW + timedelta(minutes=29, seconds=59),
            first_signal_at=NOW + timedelta(minutes=29, seconds=59),
            most_recent_qualifying_sweep_timestamp=NOW,
        ),
        Settings(),
    )
    expired = evaluate(
        item(
            as_of=NOW + timedelta(minutes=30),
            first_signal_at=NOW + timedelta(minutes=30),
            most_recent_qualifying_sweep_timestamp=NOW,
        ),
        Settings(),
    )
    assert next(g for g in fresh.gates if g.name == "F").passed
    assert not next(g for g in expired.gates if g.name == "F").passed


def test_lexicographic_ranking_and_no_signal_reasons():
    settings = Settings()
    decision = decide([item("MSFT", call_premium=Decimal(200000)), item("AAPL")], settings)
    assert decision.fire and decision.selected_symbol == "MSFT"
    rejected = decide([item(call_premium=Decimal(1))], settings)
    assert not rejected.fire and rejected.selected_symbol is None
    assert any("options" in reason for reason in rejected.rejection_reasons)


def test_replay_is_deterministic():
    events = []
    for sequence, kind, payload in [
        (
            1,
            EventKind.EQUITY,
            {
                "price": 100,
                "volume": 100000,
                "spread": 0.01,
                "relative_volume": 2,
                "delta": 1,
                "first_signal_at": NOW,
            },
        ),
        (
            1,
            EventKind.OPTIONS,
            {
                "option_volume": 200,
                "open_interest": 100,
                "call_premium": 100000,
                "distance_to_resistance": 0.01,
            },
        ),
        (1, EventKind.MARKET_INDEX, {"delta": 1}),
        (1, EventKind.SECTOR_INDEX, {"delta": 1}),
    ]:
        events.append(
            CanonicalEvent(
                event_id=uuid4(),
                kind=kind,
                symbol="AAPL",
                source=kind.value,
                source_timestamp=NOW,
                received_timestamp=NOW,
                normalized_timestamp=NOW,
                sequence=sequence,
                payload=payload,
            )
        )
    first = replay(events, ["AAPL"], Settings())
    second = replay(events, ["AAPL"], Settings())
    assert first == second


def test_sequences_are_independent_per_symbol_and_kind():
    synchronizer = Synchronizer()
    first = CanonicalEvent(
        event_id=uuid4(),
        kind=EventKind.EQUITY,
        symbol="AAPL",
        source="alpaca",
        source_timestamp=NOW,
        received_timestamp=NOW,
        normalized_timestamp=NOW,
        sequence=10,
        payload={},
    )
    second = first.model_copy(update={"event_id": uuid4(), "symbol": "SPY", "sequence": 1})
    assert synchronizer.add(first)
    assert synchronizer.add(second)
