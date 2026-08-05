import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

from institutional_signal_engine.config import Settings
from institutional_signal_engine.persistence import InMemoryRepository
from institutional_signal_engine.persistence_async import AsyncAuditWriter, AuditWrite
from institutional_signal_engine.pipeline import SignalPipeline
from institutional_signal_engine.quote_book import QuoteBook
from institutional_signal_engine.schemas import CanonicalEvent, EventKind, SynchronizedInput

NOW = datetime(2026, 8, 5, 14, 31, tzinfo=UTC)


def event(
    kind: EventKind,
    symbol: str,
    source_timestamp: datetime,
    *,
    provider_kind: str = "quote",
    contract: dict[str, object] | None = None,
) -> CanonicalEvent:
    payload: dict[str, object] = {
        "provider_event_kind": provider_kind,
        "price": 100,
        "bid": 99,
        "ask": 101,
    }
    if contract is not None:
        payload["contract"] = contract
    return CanonicalEvent(
        event_id=uuid4(),
        kind=kind,
        symbol=symbol,
        source="fixture",
        source_timestamp=source_timestamp,
        received_timestamp=source_timestamp,
        normalized_timestamp=source_timestamp,
        sequence=1,
        payload=payload,
    )


def test_quote_slots_overwrite_and_instruments_are_independent():
    book = QuoteBook(window=timedelta(seconds=1))
    aapl = event(EventKind.EQUITY, "AAPL", NOW)
    spy = event(EventKind.EQUITY, "SPY", NOW)
    option_a = event(
        EventKind.OPTIONS,
        "AAPL",
        NOW,
        contract={"root": "AAPL", "expiration": 20260807, "strike": 310000, "right": "C"},
    )
    option_b = event(
        EventKind.OPTIONS,
        "AAPL",
        NOW,
        contract={"root": "AAPL", "expiration": 20260807, "strike": 315000, "right": "C"},
    )
    newer_aapl = aapl.model_copy(update={"event_id": uuid4(), "sequence": 2})
    for quote in (aapl, spy, option_a, option_b, newer_aapl):
        book.receive(quote)
    assert book.latest(aapl) == newer_aapl
    assert book.latest(spy) == spy
    assert book.latest(option_a) == option_a
    assert book.latest(option_b) == option_b
    assert (book.metrics.quotes_received, book.metrics.quotes_superseded) == (5, 1)


def test_trade_uses_newest_quote_at_or_before_and_expires_old_quotes():
    book = QuoteBook(window=timedelta(seconds=1))
    old = event(
        EventKind.OPTIONS,
        "AAPL",
        NOW,
        contract={"root": "AAPL", "expiration": 20260807, "strike": 310000, "right": "C"},
    )
    future = old.model_copy(
        update={
            "event_id": uuid4(),
            "source_timestamp": NOW + timedelta(milliseconds=10),
            "normalized_timestamp": NOW + timedelta(milliseconds=10),
        }
    )
    book.receive(old)
    book.receive(future)
    trade = event(
        EventKind.OPTIONS,
        "AAPL",
        NOW,
        provider_kind="trade",
        contract={"root": "AAPL", "expiration": 20260807, "strike": 310000, "right": "C"},
    )
    consumed = book.consume_for_trade(trade)
    assert consumed is not None and consumed.quote.event_id == old.event_id
    expired_trade = trade.model_copy(
        update={
            "event_id": uuid4(),
            "source_timestamp": NOW + timedelta(seconds=2),
            "normalized_timestamp": NOW + timedelta(seconds=2),
        }
    )
    assert book.consume_for_trade(expired_trade) is None


def test_async_writer_batches_and_drains():
    async def run() -> None:
        repository = InMemoryRepository()
        writer = AsyncAuditWriter(repository, soft_limit=2, hard_limit=4, batch_size=2)
        writer.start()
        for _ in range(3):
            assert writer.enqueue(AuditWrite(event=event(EventKind.EQUITY, "AAPL", NOW)))
        await writer.close()
        assert len(repository.events) == 3
        assert writer.metrics.batch_sizes
        assert writer.persistence_backpressure_failure is False

    asyncio.run(run())


def test_hard_limit_fails_closed_and_drains_accepted_records():
    async def run() -> None:
        repository = InMemoryRepository()
        writer = AsyncAuditWriter(repository, soft_limit=1, hard_limit=2, batch_size=10)
        writer.start()
        record = AuditWrite(event=event(EventKind.EQUITY, "AAPL", NOW))
        assert writer.enqueue(record)
        assert writer.enqueue(record)
        assert writer.enqueue(record) is False
        assert writer.persistence_backpressure_failure is True
        await writer.close()
        assert len(repository.events) == 1

    asyncio.run(run())


def test_material_change_boundaries_and_unchanged_skip():
    base = SynchronizedInput(
        symbol="AAPL",
        as_of=NOW,
        price=100,
        volume=100000,
        option_volume=10,
        open_interest=10,
        call_premium=400000,
        spread=Decimal("0.01"),
        distance_to_resistance=Decimal("0.003"),
        resistance_state="OVERHEAD_RESISTANCE",
        relative_volume=Decimal("1.9"),
        equity_delta=Decimal("-0.1"),
        market_delta=Decimal("-0.1"),
        sector_delta=Decimal("-0.1"),
        first_signal_at=None,
        concurrent_positions=0,
        event_ids=(uuid4(),),
        ask_side_percentage=Decimal(64),
        quote_validity="VALID",
    )
    pipeline = SignalPipeline(Settings())
    fixture_event = event(EventKind.EQUITY, "AAPL", NOW, provider_kind="trade")
    assert pipeline._material_change_reasons(base, fixture_event) == ["initial_state"]
    assert pipeline._material_change_reasons(base, fixture_event) == []
    changed = base.model_copy(
        update={
            "price": Decimal("100.02"),
            "relative_volume": Decimal(2),
            "equity_delta": Decimal("0.1"),
            "call_premium": Decimal(500001),
            "ask_side_percentage": Decimal(65),
            "quote_validity": "INVALID",
            "event_ids": (uuid4(),),
        }
    )
    reasons = pipeline._material_change_reasons(changed, fixture_event)
    assert {
        "underlying_price_threshold",
        "rvol_boundary",
        "equity_strength_boundary",
        "call_premium_boundary",
        "ask_side_boundary",
        "quote_validity_transition",
    }.issubset(reasons)
