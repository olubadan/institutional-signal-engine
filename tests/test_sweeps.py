from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

from institutional_signal_engine.schemas import CanonicalEvent, EventKind
from institutional_signal_engine.sweeps import SweepEngine

NOW = datetime(2026, 8, 5, 14, 31, tzinfo=UTC)
CONTRACT = {"root": "AAPL", "expiration": 20260807, "strike": 310000, "right": "C"}


def trade(
    timestamp: datetime,
    size: int,
    exchange: str,
    classification: str = "ask",
    contract: dict[str, object] | None = None,
    price: Decimal = Decimal(100),
) -> CanonicalEvent:
    return CanonicalEvent(
        event_id=uuid4(),
        kind=EventKind.OPTIONS,
        symbol="AAPL",
        source="theta",
        source_timestamp=timestamp,
        received_timestamp=timestamp,
        normalized_timestamp=timestamp,
        sequence=1,
        payload={
            "provider_event_kind": "trade",
            "eligible_trade": True,
            "condition_mapping_version": "test",
            "contract": contract or CONTRACT,
            "trade_price": price,
            "trade_size": size,
            "exchange": exchange,
            "trade_classification": classification,
        },
    )


def qualifying_cluster(engine: SweepEngine, start: datetime = NOW) -> UUID:
    cluster_id = UUID(int=0)
    for offset, exchange in enumerate(("A", "B", "C")):
        update = engine.process(trade(start + timedelta(milliseconds=offset), 5, exchange))
        if update.cluster is not None:
            cluster_id = update.cluster.cluster_id
    assert update.reason == "NEW_QUALIFYING_SWEEP"
    return cluster_id


def test_cluster_identity_and_fixed_boundary():
    engine = SweepEngine(uuid4())
    first = engine.process(trade(NOW, 5, "A"))
    same = engine.process(trade(NOW + timedelta(seconds=1), 5, "B"))
    after = engine.process(trade(NOW + timedelta(seconds=1, milliseconds=1), 5, "C"))
    assert first.cluster is same.cluster
    assert after.cluster is not same.cluster
    assert same.cluster is not None and len(same.cluster.trades) == 2


def test_exact_identity_separates_contracts_and_only_calls_qualify():
    engine = SweepEngine(uuid4())
    put = engine.process(trade(NOW, 5, "A", contract={**CONTRACT, "right": "P"}))
    other_strike = engine.process(trade(NOW, 5, "B", contract={**CONTRACT, "strike": 311000}))
    assert put.reason is None and other_strike.reason is None
    assert put.cluster is None
    assert other_strike.cluster is not None and not other_strike.cluster.qualifying


def test_three_distinct_exchanges_and_duplicate_exchange_rule():
    engine = SweepEngine(uuid4())
    for exchange in ("A", "A", "B"):
        update = engine.process(trade(NOW, 5, exchange))
    assert update.reason is None
    update = engine.process(trade(NOW, 5, "C"))
    assert update.reason == "NEW_QUALIFYING_SWEEP"


def test_cluster_premium_and_directional_boundaries():
    engine = SweepEngine(uuid4())
    for exchange in ("A", "B", "C"):
        update = engine.process(trade(NOW, 5, exchange))
    assert update.cluster is not None
    assert update.audit is not None
    assert update.audit["aggregate_eligible_premium"] == "150000"
    assert update.audit["ask_side_percentage"] == "100"
    assert update.audit["unknown_premium_percentage"] == "0"

    failing = SweepEngine(uuid4())
    for exchange in ("A", "B", "C"):
        update = failing.process(trade(NOW, 4, exchange))
    assert update.reason is None


def test_unknown_premium_boundary_and_zero_classified_fail_closed():
    at_boundary = SweepEngine(uuid4())
    for exchange in ("A", "B", "C"):
        at_boundary.process(trade(NOW, 5, exchange))
    update = at_boundary.process(trade(NOW, 5, "D", "unknown"))
    assert update.cluster is not None and update.cluster.qualifying

    over = SweepEngine(uuid4())
    for exchange in ("A", "B", "C"):
        over.process(trade(NOW, 5, exchange))
    update = over.process(trade(NOW, 16, "D", "unknown"))
    assert update.reason == "SWEEP_QUALIFICATION_REVOKED"

    zero = SweepEngine(uuid4())
    for exchange in ("A", "B", "C"):
        update = zero.process(trade(NOW, 5, exchange, "unknown"))
    assert update.reason is None


def test_qualification_revoke_and_requalification_are_transitions():
    engine = SweepEngine(uuid4())
    qualifying_cluster(engine)
    revoked = engine.process(trade(NOW + timedelta(milliseconds=500), 16, "D", "unknown"))
    assert revoked.reason == "SWEEP_QUALIFICATION_REVOKED"
    requalified = engine.process(trade(NOW + timedelta(milliseconds=600), 30, "E", "ask"))
    assert requalified.reason == "NEW_QUALIFYING_SWEEP"
    assert requalified.cluster is revoked.cluster


def test_session_gate_requires_three_sweeps_and_five_hundred_thousand():
    engine = SweepEngine(uuid4())
    qualifying_cluster(engine, NOW)
    qualifying_cluster(engine, NOW + timedelta(seconds=2))
    qualifying_cluster(engine, NOW + timedelta(seconds=4))
    state = engine.snapshot()
    assert state["qualifying_sweep_count"] == 3
    assert state["qualifying_sweep_premium"] == "450000"
    assert state["session_sweep_gate"] is False
    qualifying_cluster(engine, NOW + timedelta(seconds=6))
    assert engine.snapshot()["session_sweep_gate"] is True


def test_freshness_expires_once_and_newer_sweep_refreshes():
    engine = SweepEngine(uuid4())
    qualifying_cluster(engine, NOW)
    assert (
        engine.tick(NOW + timedelta(minutes=30, milliseconds=2)).reason == "SWEEP_FRESHNESS_EXPIRED"
    )
    assert engine.tick(NOW + timedelta(minutes=30, seconds=1)).reason is None

    newer = SweepEngine(uuid4())
    qualifying_cluster(newer, NOW)
    qualifying_cluster(newer, NOW + timedelta(minutes=1))
    state = newer.snapshot()
    assert (
        state["most_recent_qualifying_sweep_timestamp"]
        == (NOW + timedelta(minutes=1, milliseconds=2)).isoformat()
    )


def test_premium_just_below_boundary_does_not_qualify():
    engine = SweepEngine(uuid4())
    for exchange in ("A", "B", "C"):
        update = engine.process(trade(NOW, 5, exchange, price=Decimal("99.99999")))
    assert update.cluster is not None
    assert update.audit["aggregate_eligible_premium"] == "149999.98500"
    assert update.reason is None


def test_cancellation_recomputes_qualification_and_uncorrelated_fails_closed():
    engine = SweepEngine(uuid4())
    first = trade(NOW, 5, "A")
    engine.process(first)
    second = trade(NOW + timedelta(milliseconds=1), 5, "B")
    engine.process(second)
    third = trade(NOW + timedelta(milliseconds=2), 5, "C")
    assert engine.process(third).reason == "NEW_QUALIFYING_SWEEP"

    correction = third.model_copy(
        update={
            "event_id": uuid4(),
            "payload": {"provider_event_kind": "cancel", "cancel_of": str(third.event_id)},
        }
    )
    revoked = engine.process(correction)
    assert revoked.reason == "SWEEP_QUALIFICATION_REVOKED"
    assert revoked.cluster is not None
    assert str(third.event_id) not in revoked.audit["constituent_trade_ids"]

    unknown = correction.model_copy(
        update={
            "event_id": uuid4(),
            "payload": {"provider_event_kind": "correction", "correction_of": str(uuid4())},
        }
    )
    assert engine.process(unknown).reason == "SWEEP_UNCORRELATED_CORRECTION"


def test_closed_cluster_audit_freezes_final_thresholds():
    engine = SweepEngine(uuid4())
    qualifying_cluster(engine)
    audits = engine.close_session()
    assert len(audits) == 1
    assert audits[0]["final"] is True
    assert audits[0]["thresholds"]["cluster_premium"] is True


def test_closed_qualifying_cluster_expires_for_freshness():
    engine = SweepEngine(uuid4())
    qualifying_cluster(engine)
    engine.process(trade(NOW + timedelta(seconds=2), 1, "D"))
    assert engine.tick(NOW + timedelta(minutes=30, seconds=2)).reason == ("SWEEP_FRESHNESS_EXPIRED")
    assert engine.snapshot()["most_recent_qualifying_sweep_timestamp"] is None
