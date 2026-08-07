from datetime import UTC, datetime
from uuid import uuid4

from institutional_signal_engine.config import Settings
from institutional_signal_engine.persistence import InMemoryRepository
from institutional_signal_engine.quote_book import QuoteConsumption
from institutional_signal_engine.replay import _ordered_consumed_inputs, replay
from institutional_signal_engine.schemas import CanonicalEvent, EventKind


def _event(kind: EventKind, sequence: int, payload: dict[str, object]) -> CanonicalEvent:
    timestamp = datetime(2026, 1, 1, tzinfo=UTC)
    return CanonicalEvent(
        event_id=uuid4(),
        kind=kind,
        symbol="AAPL",
        source=kind.value,
        source_timestamp=timestamp,
        received_timestamp=timestamp,
        normalized_timestamp=timestamp,
        sequence=sequence,
        payload=payload,
    )


def test_event_persistence_is_idempotent():
    repository = InMemoryRepository()
    event = _event(EventKind.EQUITY, 1, {"price": 1})
    repository.record_event(event)
    repository.record_event(event)
    assert tuple(repository.replay_events()) == (event,)


def test_replay_is_deterministic_for_identical_ordered_events():
    events = [
        _event(
            EventKind.EQUITY,
            1,
            {"price": 100, "volume": 100000, "spread": 0.01, "relative_volume": 2, "delta": 1},
        ),
        _event(
            EventKind.OPTIONS,
            1,
            {
                "option_volume": 2,
                "open_interest": 1,
                "call_premium": 100000,
                "distance_to_resistance": 1,
            },
        ),
        _event(EventKind.MARKET_INDEX, 1, {"delta": 1}),
        _event(EventKind.SECTOR_INDEX, 1, {"delta": 1}),
    ]
    assert replay(events, ["AAPL"], Settings()) == replay(events, ["AAPL"], Settings())


def test_replay_orders_consumed_quotes_by_ingress_before_trades():
    quote = _event(EventKind.EQUITY, 1, {"provider_event_kind": "quote", "price": 100})
    quote = quote.model_copy(update={"ingest_order": 1})
    trade = _event(EventKind.EQUITY, 2, {"provider_event_kind": "trade", "price": 100})
    trade = trade.model_copy(update={"ingest_order": 2})
    consumption = QuoteConsumption(1, quote.event_id, trade.event_id, "BOTH", quote)
    ordered = _ordered_consumed_inputs((trade,), (consumption,))
    assert [event.event_id for event in ordered] == [quote.event_id, trade.event_id]
