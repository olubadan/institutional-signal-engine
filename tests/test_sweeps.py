from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

from institutional_signal_engine.schemas import CanonicalEvent, EventKind
from institutional_signal_engine.sweeps import (
    EXCHANGE_MAPPING_VERSION,
    SWEEP_FRESHNESS,
    THETADATA_EXCHANGE_CODES,
    SweepEngine,
)

NOW = datetime(2026, 8, 5, 14, 31, tzinfo=UTC)
CONTRACT = {"root": "AAPL", "expiration": 20260807, "strike": 310000, "right": "C"}
VALID_EXCHANGES = ("5", "31", "43")


def trade(
    timestamp: datetime,
    size: int,
    exchange: str,
    classification: str = "ask",
    contract: dict[str, object] | None = None,
    price: Decimal = Decimal(100),
    payload_overrides: dict[str, object] | None = None,
) -> CanonicalEvent:
    payload = {
        "provider_event_kind": "trade",
        "eligible_trade": True,
        "condition_mapping_version": "test",
        "contract": contract or CONTRACT,
        "trade_price": price,
        "trade_size": size,
        "exchange": exchange,
        "trade_classification": classification,
    }
    payload.update(payload_overrides or {})
    return CanonicalEvent(
        event_id=uuid4(),
        kind=EventKind.OPTIONS,
        symbol="AAPL",
        source="theta",
        source_timestamp=timestamp,
        received_timestamp=timestamp,
        normalized_timestamp=timestamp,
        sequence=1,
        payload=payload,
    )


def qualifying_cluster(engine: SweepEngine, start: datetime = NOW) -> UUID:
    cluster_id = UUID(int=0)
    for offset, exchange in enumerate(VALID_EXCHANGES):
        update = engine.process(trade(start + timedelta(milliseconds=offset), 5, exchange))
        if update.cluster is not None:
            cluster_id = update.cluster.cluster_id
    assert update.reason == "NEW_QUALIFYING_SWEEP"
    return cluster_id


def test_cluster_identity_and_fixed_boundary():
    engine = SweepEngine(uuid4())
    first = engine.process(trade(NOW, 5, "5"))
    same = engine.process(trade(NOW + timedelta(seconds=1), 5, "31"))
    after = engine.process(trade(NOW + timedelta(seconds=1, milliseconds=1), 5, "43"))
    assert first.cluster is same.cluster
    assert after.cluster is not same.cluster
    assert same.cluster is not None and len(same.cluster.trades) == 2


def test_exact_identity_separates_contracts_and_only_calls_qualify():
    engine = SweepEngine(uuid4())
    put = engine.process(trade(NOW, 5, "5", contract={**CONTRACT, "right": "P"}))
    other_strike = engine.process(trade(NOW, 5, "31", contract={**CONTRACT, "strike": 311000}))
    assert put.reason is None and other_strike.reason is None
    assert put.cluster is None
    assert other_strike.cluster is not None and not other_strike.cluster.qualifying


def test_three_distinct_exchanges_and_duplicate_exchange_rule():
    engine = SweepEngine(uuid4())
    for exchange in ("5", "5", "31"):
        update = engine.process(trade(NOW, 5, exchange))
    assert update.reason is None
    update = engine.process(trade(NOW, 5, "43"))
    assert update.reason == "NEW_QUALIFYING_SWEEP"


def test_cluster_premium_and_directional_boundaries():
    engine = SweepEngine(uuid4())
    for exchange in VALID_EXCHANGES:
        update = engine.process(trade(NOW, 5, exchange))
    assert update.cluster is not None
    assert update.audit is not None
    assert update.audit["aggregate_eligible_premium"] == "150000"
    assert update.audit["ask_side_percentage"] == "100"
    assert update.audit["unknown_premium_percentage"] == "0"

    failing = SweepEngine(uuid4())
    for exchange in VALID_EXCHANGES:
        update = failing.process(trade(NOW, 4, exchange))
    assert update.reason is None


def test_unknown_premium_boundary_and_zero_classified_fail_closed():
    at_boundary = SweepEngine(uuid4())
    for exchange in VALID_EXCHANGES:
        at_boundary.process(trade(NOW, 5, exchange))
    update = at_boundary.process(trade(NOW, 5, "46", "unknown"))
    assert update.cluster is not None and update.cluster.qualifying

    over = SweepEngine(uuid4())
    for exchange in VALID_EXCHANGES:
        over.process(trade(NOW, 5, exchange))
    update = over.process(trade(NOW, 16, "46", "unknown"))
    assert update.reason == "SWEEP_QUALIFICATION_REVOKED"

    zero = SweepEngine(uuid4())
    for exchange in VALID_EXCHANGES:
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
    expiry = NOW + timedelta(milliseconds=2) + SWEEP_FRESHNESS
    assert engine.tick(expiry - timedelta(milliseconds=1)).reason is None
    assert engine.tick(expiry).reason == "SWEEP_FRESHNESS_EXPIRED"
    assert engine.tick(expiry + timedelta(milliseconds=1)).reason is None

    newer = SweepEngine(uuid4())
    qualifying_cluster(newer, NOW)
    qualifying_cluster(newer, NOW + timedelta(minutes=1))
    state = newer.snapshot()
    assert (
        state["most_recent_qualifying_sweep_timestamp"]
        == (NOW + timedelta(minutes=1, milliseconds=2)).isoformat()
    )


def test_sweep_window_keeps_construction_independent_from_freshness():
    engine = SweepEngine(uuid4())
    qualifying_cluster(engine, NOW)
    original = next(iter(engine.active.values()))

    # The one-second construction window is strict for a new constituent;
    # thirty-minute freshness must never merge this into the old cluster.
    separated = engine.process(trade(NOW + timedelta(seconds=1, milliseconds=1), 5, "5"))
    assert separated.cluster is not original
    assert separated.cluster is not None
    assert separated.cluster.open_timestamp == NOW + timedelta(seconds=1, milliseconds=1)


def test_freshness_expiry_does_not_change_cluster_membership():
    engine = SweepEngine(uuid4())
    qualifying_cluster(engine, NOW)
    original = next(iter(engine.active.values()))
    expiry = original.last_timestamp + SWEEP_FRESHNESS
    assert engine.tick(expiry - timedelta(microseconds=1)).reason is None
    assert next(iter(engine.active.values())) is original
    assert engine.tick(expiry).reason == "SWEEP_FRESHNESS_EXPIRED"
    assert original.expired is True


def test_premium_just_below_boundary_does_not_qualify():
    engine = SweepEngine(uuid4())
    for exchange in VALID_EXCHANGES:
        update = engine.process(trade(NOW, 5, exchange, price=Decimal("99.99999")))
    assert update.cluster is not None
    assert update.audit["aggregate_eligible_premium"] == "149999.98500"
    assert update.reason is None


def test_cancellation_recomputes_qualification_and_uncorrelated_fails_closed():
    engine = SweepEngine(uuid4())
    first = trade(NOW, 5, "5")
    engine.process(first)
    second = trade(NOW + timedelta(milliseconds=1), 5, "31")
    engine.process(second)
    third = trade(NOW + timedelta(milliseconds=2), 5, "43")
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
    assert revoked.audit["cancellation_records"][-1]["original_record"]["event_id"] == str(
        third.event_id
    )
    assert revoked.audit["cancellation_records"][-1]["corrective_record"]["event_id"] == str(
        correction.event_id
    )

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


def test_missing_and_unknown_exchanges_fail_with_threshold_evidence():
    engine = SweepEngine(uuid4())
    for exchange in ("A", "B", "C", "", "999"):
        update = engine.process(trade(NOW, 5, exchange))
    assert update.cluster is not None
    assert update.cluster.qualifying is False
    assert update.audit["thresholds"]["three_exchanges"] is False
    assert update.audit["exchange_identifiers"] == ["", "999", "A", "B", "C"]


def test_numeric_thetadata_exchange_mapping_is_versioned_and_authoritative():
    assert EXCHANGE_MAPPING_VERSION == "thetadata-opra-exchanges-v1"
    assert THETADATA_EXCHANGE_CODES["5"] == "CBOE"
    assert THETADATA_EXCHANGE_CODES["31"] == "ISE_GEMINI"
    assert "999" not in THETADATA_EXCHANGE_CODES


def test_three_valid_numeric_exchange_codes_satisfy_participation():
    engine = SweepEngine(uuid4())
    for exchange in VALID_EXCHANGES:
        update = engine.process(trade(NOW, 5, exchange))
    assert update.cluster is not None and update.cluster.qualifying
    assert update.audit["thresholds"]["three_exchanges"] is True


def test_ask_percentage_exact_boundary_and_just_below():
    exact = SweepEngine(uuid4())
    exact_specs = (
        ("5", Decimal(975), "ask"),
        ("31", Decimal("262.5"), "bid"),
        ("43", Decimal("262.5"), "bid"),
    )
    for exchange, price, side in exact_specs:
        update = exact.process(trade(NOW, 1, exchange, side, price=price))
    assert update.cluster is not None and update.cluster.qualifying
    assert update.audit["thresholds"]["ask_side_percentage"] is True

    below = SweepEngine(uuid4())
    below_specs = (
        ("5", Decimal("974.85"), "ask"),
        ("31", Decimal("262.575"), "bid"),
        ("43", Decimal("262.575"), "bid"),
    )
    for exchange, price, side in below_specs:
        update = below.process(trade(NOW, 1, exchange, side, price=price))
    assert update.cluster is not None and update.cluster.qualifying is False
    assert update.audit["thresholds"]["ask_side_percentage"] is False


def test_session_reset_clears_sweep_count_and_premium():
    engine = SweepEngine(uuid4())
    qualifying_cluster(engine)
    assert engine.snapshot()["qualifying_sweep_count"] == 1
    engine.reset_session("2026-08-06")
    assert engine.snapshot()["qualifying_sweep_count"] == 0
    assert engine.snapshot()["qualifying_sweep_premium"] == "0"


def test_correction_replaces_trade_and_preserves_both_records():
    engine = SweepEngine(uuid4())
    first = trade(NOW, 5, "5")
    engine.process(first)
    second = trade(NOW + timedelta(milliseconds=1), 5, "31")
    engine.process(second)
    third = trade(NOW + timedelta(milliseconds=2), 5, "43")
    engine.process(third)
    corrective = trade(
        NOW + timedelta(milliseconds=3),
        1,
        "D",
        "bid",
        price=Decimal(1),
        payload_overrides={
            "provider_event_kind": "correction",
            "correction_of": str(third.event_id),
            "trade_price": Decimal(1),
            "trade_size": 1,
            "exchange": "D",
            "trade_classification": "bid",
        },
    )
    update = engine.process(corrective)
    assert update.cluster is not None
    corrected = update.cluster.trades[third.event_id]
    assert corrected.event_id == third.event_id
    assert corrected.payload["corrective_record_id"] == str(corrective.event_id)
    assert update.audit["correction_records"][-1]["original_record"]["event_id"] == str(
        third.event_id
    )
    assert update.audit["correction_records"][-1]["corrective_record"]["event_id"] == str(
        corrective.event_id
    )


def test_out_of_order_before_open_does_not_mutate_cluster():
    engine = SweepEngine(uuid4())
    first = engine.process(trade(NOW, 5, "5"))
    before = engine.process(trade(NOW - timedelta(milliseconds=1), 5, "31"))
    assert before.reason == "SWEEP_OUT_OF_ORDER_BEFORE_OPEN"
    assert before.cluster is None
    assert first.cluster is not None and len(first.cluster.trades) == 1


def test_transition_history_is_ordered_and_nonqualifying_audit_is_retained():
    nonqualifying = SweepEngine(uuid4())
    update = nonqualifying.process(trade(NOW, 1, "5"))
    assert update.audit is not None
    assert update.audit["qualification_state"] is False
    assert update.audit["thresholds"]["cluster_premium"] is False
    engine = SweepEngine(uuid4())
    qualified = None
    for exchange in VALID_EXCHANGES:
        qualified = engine.process(trade(NOW, 5, exchange))
    assert qualified is not None
    assert qualified.transition_audits
    transition = qualified.transition_audits[-1]
    assert transition["transition"] == "NEW_QUALIFYING_SWEEP"
    assert transition["transition_order"] == 1


def test_transition_audits_are_append_only_and_ordered():
    from institutional_signal_engine.persistence import InMemoryRepository

    engine = SweepEngine(uuid4())
    repository = InMemoryRepository()
    qualifying_cluster(engine)
    cluster = next(iter(engine.active.values()))
    repository.record_sweep(engine._audit(cluster, "TEST_TRANSITION"))
    revoked = engine.process(trade(NOW + timedelta(milliseconds=500), 16, "D", "unknown"))
    for audit in revoked.transition_audits:
        repository.record_sweep(audit)
    orders = [int(value["transition_order"]) for value in repository.sweep_transitions]
    assert orders == sorted(orders)
    assert len(repository.sweep_transitions) == 2
    assert [
        int(value["transition_order"])
        for value in repository.replay_sweep_transitions(engine.run_id)
    ] == orders


def test_synchronous_repository_writes_sweep_audits():
    from institutional_signal_engine.config import Settings
    from institutional_signal_engine.persistence import InMemoryRepository
    from institutional_signal_engine.persistence_async import AuditWrite
    from institutional_signal_engine.pipeline import SignalPipeline

    repository = InMemoryRepository()
    pipeline = SignalPipeline(Settings(), repository=repository)
    audit = {"run_id": str(uuid4()), "cluster_id": str(uuid4()), "transition": None}
    assert pipeline._enqueue(AuditWrite(sweep=audit))
    assert repository.sweeps == [audit]


def test_exact_expiry_timestamp_is_a_transition():
    engine = SweepEngine(uuid4())
    qualifying_cluster(engine)
    update = engine.tick(NOW + timedelta(milliseconds=2) + SWEEP_FRESHNESS)
    assert update.reason == "SWEEP_FRESHNESS_EXPIRED"
    assert update.transition_audits
    assert update.transition_audits[0]["transition"] == "SWEEP_FRESHNESS_EXPIRED"


def test_pipeline_freshness_expiry_re_evaluates_and_persists_timer():
    from institutional_signal_engine.config import Settings
    from institutional_signal_engine.persistence import InMemoryRepository
    from institutional_signal_engine.pipeline import SignalPipeline
    from institutional_signal_engine.schemas import EventKind

    expiry = NOW + timedelta(milliseconds=2) + SWEEP_FRESHNESS
    repository = InMemoryRepository()
    pipeline = SignalPipeline(Settings(), repository=repository, now=lambda: expiry)
    qualifying_cluster(pipeline.sweeps, NOW)
    pipeline._last_session_phase = "REGULAR"
    pipeline._last_session_date = "2026-08-05"

    def state_event(kind: EventKind, payload: dict[str, object]) -> CanonicalEvent:
        return CanonicalEvent(
            event_id=uuid4(),
            kind=kind,
            symbol="AAPL",
            source="fixture",
            source_timestamp=NOW,
            received_timestamp=NOW,
            normalized_timestamp=NOW,
            sequence=1,
            payload=payload,
        )

    base = {
        "price": Decimal(100),
        "volume": 100000,
        "spread": Decimal("0.01"),
        "distance_to_resistance": Decimal("0.01"),
        "resistance_state": "OVERHEAD_RESISTANCE",
        "relative_volume": Decimal(2),
        "delta": Decimal(1),
        "relative_strength_vs_spy": Decimal(1),
        "relative_strength_vs_sector": Decimal(1),
    }
    pipeline.synchronizer.add(state_event(EventKind.EQUITY, base))
    pipeline.synchronizer.add(
        state_event(
            EventKind.OPTIONS,
            {
                "option_volume": 200,
                "open_interest": 100,
                "call_premium": Decimal(100000),
                "ask_side_percentage": Decimal(100),
                "quote_validity": "VALID",
            },
        )
    )
    pipeline.synchronizer.add(state_event(EventKind.MARKET_INDEX, {"delta": Decimal(1)}))
    pipeline.synchronizer.add(state_event(EventKind.SECTOR_INDEX, {"delta": Decimal(1)}))
    decision = pipeline.tick()
    assert decision is not None
    assert "SWEEP_FRESHNESS_EXPIRED" in decision.triggering_change_reasons
    assert not next(g for g in decision.candidates[0].gates if g.name == "F").passed
    assert any(
        event.payload.get("provider_event_kind") == "sweep_timer" for event in repository.events
    )


def test_timer_driven_freshness_replays_field_by_field():
    from institutional_signal_engine.config import Settings
    from institutional_signal_engine.persistence import InMemoryRepository
    from institutional_signal_engine.pipeline import SignalPipeline
    from institutional_signal_engine.replay import replay
    from institutional_signal_engine.schemas import EventKind

    run_id = uuid4()
    clock = [NOW]
    repository = InMemoryRepository()
    pipeline = SignalPipeline(
        Settings(), repository=repository, now=lambda: clock[0], run_id=run_id
    )

    def event(kind: EventKind, sequence: int, payload: dict[str, object]) -> CanonicalEvent:
        return CanonicalEvent(
            event_id=uuid4(),
            run_id=run_id,
            kind=kind,
            symbol="AAPL",
            source="fixture",
            source_timestamp=NOW,
            received_timestamp=NOW,
            normalized_timestamp=NOW,
            sequence=sequence,
            payload=payload,
        )

    equity = event(
        EventKind.EQUITY,
        1,
        {
            "_calculated_indicators": True,
            "price": Decimal(100),
            "volume": 100000,
            "spread": Decimal("0.01"),
            "relative_volume": Decimal(2),
            "delta": Decimal(1),
            "relative_strength_vs_spy": Decimal(1),
            "relative_strength_vs_sector": Decimal(1),
            "distance_to_resistance": Decimal("0.01"),
            "resistance_state": "OVERHEAD_RESISTANCE",
        },
    )
    market = event(EventKind.MARKET_INDEX, 1, {"_calculated_indicators": True, "delta": Decimal(1)})
    sector = event(EventKind.SECTOR_INDEX, 1, {"_calculated_indicators": True, "delta": Decimal(1)})
    option_events = [
        event(
            EventKind.OPTIONS,
            index,
            {
                "_state_enriched": True,
                "provider_event_kind": "trade",
                "eligible_trade": True,
                "condition_code": 0,
                "contract": CONTRACT,
                "trade_price": Decimal(100),
                "trade_size": 5,
                "exchange": exchange,
                "trade_classification": "ask",
                "option_volume": index * 5,
                "open_interest": 100,
                "call_premium": Decimal(index * 50000),
            },
        )
        for index, exchange in enumerate(VALID_EXCHANGES, 1)
    ]
    for input_event in (equity, market, sector, *option_events):
        pipeline.process(input_event)
    clock[0] = NOW + SWEEP_FRESHNESS
    pipeline.tick()
    live_decisions = tuple(repository.decisions)
    replayed = replay(tuple(repository.events), ("AAPL",), Settings())
    assert live_decisions
    assert [value.model_dump(mode="json") for value in live_decisions] == [
        value.model_dump(mode="json") for value in replayed
    ]
    assert any(
        "SWEEP_FRESHNESS_EXPIRED" in value.triggering_change_reasons for value in live_decisions
    )
