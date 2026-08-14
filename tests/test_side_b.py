from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

from institutional_signal_engine.schemas import CanonicalEvent, EventKind
from institutional_signal_engine.side_b import (
    FORWARD_HORIZONS_SECONDS,
    ForwardOutcomeTracker,
    OutcomeAnchor,
    SideBJournal,
    replay_side_b,
)


def _equity(timestamp: datetime, price: str, sequence: int) -> CanonicalEvent:
    return CanonicalEvent(
        event_id=uuid4(),
        kind=EventKind.EQUITY,
        symbol="AAPL",
        source="alpaca",
        source_timestamp=timestamp,
        received_timestamp=timestamp + timedelta(milliseconds=2),
        normalized_timestamp=timestamp,
        sequence=sequence,
        payload={"provider_event_kind": "trade", "price": price, "volume": 10},
    )


def test_forward_outcomes_are_durable_and_replayable(tmp_path: Path) -> None:
    journal = SideBJournal(tmp_path / "SIDE_B.jsonl")
    tracker = ForwardOutcomeTracker(journal)
    t0 = datetime(2026, 8, 14, 13, 30, tzinfo=UTC)
    tracker.register_anchor(
        OutcomeAnchor("cluster-1", "AAPL", t0, Decimal(100), True, False, "UP", Decimal(2))
    )
    for index, horizon in enumerate(FORWARD_HORIZONS_SECONDS, 1):
        tracker.observe(_equity(t0 + timedelta(seconds=horizon), str(100 + index), index))
    result = replay_side_b(tmp_path / "SIDE_B.jsonl")
    assert result == {
        "schema_version": "SIDE_B_EQUITY_OUTCOMES_V1",
        "record_count": 14,
        "observation_count": 6,
        "outcome_count": 6,
        "filter_decision_count": 0,
        "anchor_count": 1,
        "comparison_count": 1,
        "duplicate_scientific_ids": [],
    }


def test_future_observations_do_not_change_anchor(tmp_path: Path) -> None:
    journal = SideBJournal(tmp_path / "SIDE_B.jsonl")
    tracker = ForwardOutcomeTracker(journal)
    t0 = datetime(2026, 8, 14, 13, 30, tzinfo=UTC)
    tracker.register_anchor(OutcomeAnchor("cluster-1", "AAPL", t0, Decimal(100)))
    tracker.observe(_equity(t0 + timedelta(seconds=5), "101", 1))
    records = [line for line in (tmp_path / "SIDE_B.jsonl").read_text().splitlines()]
    assert '"price_t0":"100"' in records[0]
    assert '"price_t0":"100"' in records[2]
