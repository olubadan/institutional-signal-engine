from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

from institutional_signal_engine.config import Settings
from institutional_signal_engine.indicators import (
    OptionContractIdentity,
    OptionQuoteContext,
    OptionTradeContext,
    ResistanceCache,
    classify_ask_side,
)
from institutional_signal_engine.persistence import InMemoryRepository
from institutional_signal_engine.pipeline import SignalPipeline
from institutional_signal_engine.schemas import CanonicalEvent, EventKind
from institutional_signal_engine.thetadata_conditions import (
    CANCELLATION_CODES,
    CONDITION_MAPPING_VERSION,
    EXCLUDED_CODES,
    LATE_REPORT_CODES,
    MULTI_LEG_CODES,
    THETADATA_CONDITION_CODES,
    eligible_condition,
)

NOW = datetime(2026, 8, 5, 14, 0, tzinfo=UTC)
IDENTITY = OptionContractIdentity("AAPL", 20260807, 310000, "C")


def quote(bid: str, ask: str, age_ms: int = 100, future: bool = False, mismatch: bool = False):
    stamp = NOW + timedelta(milliseconds=age_ms) if future else NOW - timedelta(milliseconds=age_ms)
    return OptionQuoteContext(
        Decimal(bid),
        Decimal(ask),
        stamp,
        OptionContractIdentity("MSFT", 20260807, 310000, "C") if mismatch else IDENTITY,
    )


def test_exact_versioned_condition_mapping_and_exclusions():
    assert CONDITION_MAPPING_VERSION == "thetadata-trade-conditions-v3-20260805"
    assert THETADATA_CONDITION_CODES[0] == "REGULAR"
    assert THETADATA_CONDITION_CODES[35] == "SPREAD"
    assert THETADATA_CONDITION_CODES[40] == "CANC"
    assert THETADATA_CONDITION_CODES[65] == "OUT_OF_SEQ_PRE_MKT"
    assert MULTI_LEG_CODES == frozenset({35, 36, 37, 38, 67})
    assert CANCELLATION_CODES == frozenset({40, 41, 42, 43, 44})
    assert LATE_REPORT_CODES == frozenset({2, 5, 6, 13, 15, 26, 27, 28, 56, 57, 65})
    assert EXCLUDED_CODES == MULTI_LEG_CODES | CANCELLATION_CODES | LATE_REPORT_CODES | frozenset(
        {61, 77}
    )
    assert eligible_condition(0)
    assert not eligible_condition(35)
    assert not eligible_condition(40)
    assert not eligible_condition(999)


def test_ask_side_boundaries_and_quote_validity():
    trade = OptionTradeContext(Decimal("1.01"), NOW, IDENTITY)
    assert classify_ask_side(trade, quote("1.00", "1.01")) == "ask"
    assert (
        classify_ask_side(
            OptionTradeContext(Decimal("1.005"), NOW, IDENTITY), quote("1.00", "1.01")
        )
        == "unknown"
    )
    assert (
        classify_ask_side(OptionTradeContext(Decimal("1.00"), NOW, IDENTITY), quote("1.00", "1.01"))
        == "unknown"
    )
    assert classify_ask_side(trade, quote("0", "1.01")) == "unknown"
    assert classify_ask_side(trade, quote("1.02", "1.01")) == "unknown"
    assert classify_ask_side(trade, quote("1.00", "1.01", age_ms=501)) == "unknown"
    assert classify_ask_side(trade, quote("1.00", "1.01", future=True)) == "unknown"
    assert classify_ask_side(trade, quote("1.00", "1.01", mismatch=True)) == "unknown"


def test_resistance_ladder_freezes_and_supports_blue_sky():
    cache = ResistanceCache()
    highs = {f"2026-07-{day:02d}": Decimal(100) for day in range(1, 22)}
    highs.update({f"2026-06-{day:02d}": Decimal(120) for day in range(1, 252 - 20 + 1)})
    overhead = cache.calculate("AAPL", "2026-08-05", Decimal(101), highs, "split:v1", "fixture")
    assert overhead.state == "OVERHEAD_RESISTANCE"
    assert overhead.level == Decimal(120)
    assert overhead.distance == Decimal(19) / Decimal(101)
    frozen = cache.calculate(
        "AAPL", "2026-08-05", Decimal(1), {"2026-08-04": Decimal(2)}, "changed", "changed"
    )
    assert frozen == overhead
    blue_sky = ResistanceCache().calculate(
        "AAPL", "2026-08-05", Decimal(200), highs, "split:v1", "fixture"
    )
    assert blue_sky.state == "NO_OVERHEAD_RESISTANCE"
    assert blue_sky.level is None and blue_sky.distance is None


def test_run_id_is_attached_to_pipeline_events_and_decisions():
    run_id = uuid4()
    repository = InMemoryRepository()
    pipeline = SignalPipeline(Settings(), repository=repository, run_id=run_id, now=lambda: NOW)
    event = CanonicalEvent(
        event_id=uuid4(),
        kind=EventKind.EQUITY,
        symbol="AAPL",
        source="fixture",
        source_timestamp=NOW,
        received_timestamp=NOW,
        normalized_timestamp=NOW,
        sequence=1,
        payload={"price": 100, "volume": 1, "spread": 0.01},
    )
    pipeline.process(event)
    assert repository.events[0].run_id == run_id
    assert UUID(str(run_id)) == run_id
