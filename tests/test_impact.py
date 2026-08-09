from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

import pytest

from institutional_signal_engine.config import Settings
from institutional_signal_engine.impact import (
    DELTA_PROVENANCE_VERSION,
    IMPACT_COEFFICIENT,
    IMPACT_COEFFICIENT_SOURCE,
    IMPACT_DECAY_MINUTES,
    IMPACT_TARGET_MOVE,
    IMPACT_TARGET_MOVE_SOURCE,
    ImpactBaseline,
    ShadowImpactEngine,
    calculate_five_minute_baseline,
    delta_equivalent,
    evaluate_cluster,
    session_state,
)
from institutional_signal_engine.impact_coverage import CoverageCandidate, build_coverage_plan
from institutional_signal_engine.persistence import InMemoryRepository
from institutional_signal_engine.persistence_async import AsyncAuditWriter, AuditWrite
from institutional_signal_engine.pipeline import SignalPipeline
from institutional_signal_engine.schemas import CanonicalEvent, EventKind

NOW = datetime(2026, 8, 7, 14, 0, tzinfo=UTC)


def baseline(
    volume: Decimal = Decimal(19200), volatility: Decimal = Decimal("0.01"), samples: int = 20
) -> ImpactBaseline:
    return ImpactBaseline(
        "BAC", 5, volume, volatility, date(2026, 8, 7), samples, "fixture", "split-adjusted"
    )


def audit(
    *,
    cluster_id: str = "cluster-1",
    first: datetime = NOW,
    last: datetime = NOW,
    ask: str = "100",
    unknown: str = "0",
    exchanges: tuple[str, ...] = ("CBOE", "ISE", "MIAX"),
    control: bool = False,
) -> dict[str, object]:
    ids = [str(uuid4()), str(uuid4()), str(uuid4())]
    return {
        "cluster_id": cluster_id,
        "run_id": str(uuid4()),
        "root": "BAC",
        "expiration": 20260814,
        "strike": 40000,
        "right": "C",
        "constituent_trade_ids": ids,
        "first_constituent_timestamp": first.isoformat(),
        "last_constituent_timestamp": last.isoformat(),
        "aggregate_eligible_premium": "1000",
        "ask_side_percentage": ask,
        "unknown_premium_percentage": unknown,
        "exchange_set": list(exchanges),
        "qualification_state": control,
        "thresholds": {"cluster_premium": control},
    }


def events_for(
    item: dict[str, object], *, side: str = "ask", delta: str | None = "1"
) -> list[dict[str, object]]:
    return [
        {
            "trade_classification": side,
            "classification_confidence": 1,
            "trade_size": 4,
            "delta": delta,
            "delta_provenance": "ALPACA_PROVIDER_DELTA_V1" if delta is not None else None,
        }
        for _ in item["constituent_trade_ids"]  # type: ignore[union-attr]
    ]


def test_signed_and_gross_delta_equivalent_demand_and_cancellation():
    signed, gross, coherence, reasons = delta_equivalent(
        [
            {
                "trade_classification": "ask",
                "classification_confidence": 1,
                "delta": "0.5",
                "delta_provenance": "ALPACA_PROVIDER_DELTA_V1",
                "trade_size": 10,
            },
            {
                "trade_classification": "bid",
                "classification_confidence": 1,
                "delta": "0.5",
                "delta_provenance": "ALPACA_PROVIDER_DELTA_V1",
                "trade_size": 4,
            },
        ]
    )
    assert signed == Decimal(300)
    assert gross == Decimal(700)
    assert coherence == Decimal(300) / Decimal(700)
    assert reasons == ()


def test_unknown_is_not_force_classified_and_zero_denominator_is_explicit():
    signed, gross, coherence, reasons = delta_equivalent(
        [
            {
                "trade_classification": "unknown",
                "delta": "0.5",
                "delta_provenance": "ALPACA_PROVIDER_DELTA_V1",
                "trade_size": 1,
            }
        ]
    )
    assert signed == gross == Decimal(0)
    assert coherence == Decimal(0)
    assert reasons == ()


def test_missing_delta_is_fail_closed():
    signed, gross, coherence, reasons = delta_equivalent(
        [{"trade_classification": "ask", "trade_size": 1}]
    )
    assert signed is gross is coherence is None
    assert reasons == ("IMPACT_DELTA_UNAVAILABLE",)


def test_impact_constants_and_serialized_baseline_fields_are_separate():
    item = audit()
    result = evaluate_cluster(item, events_for(item), baseline())
    serialized = result.as_dict()
    assert IMPACT_COEFFICIENT == Decimal(1)
    assert IMPACT_COEFFICIENT_SOURCE == "RESEARCH_ASSUMPTION_V1"
    assert IMPACT_TARGET_MOVE == Decimal("0.0025")
    assert IMPACT_TARGET_MOVE_SOURCE == "OWNER_SELECTED_TARGET_UNDERLYING_MOVE"
    assert result.impact_coefficient == Decimal(1)
    assert result.impact_coefficient_source == "RESEARCH_ASSUMPTION_V1"
    assert result.target_move == Decimal("0.0025")
    assert result.target_move_source == "OWNER_SELECTED_TARGET_UNDERLYING_MOVE"
    assert result.expected_volatility == Decimal("0.01")
    assert result.expected_volume == Decimal(19200)
    assert serialized["impact_coefficient"] == "1"
    assert serialized["expected_volatility"] == "0.01"
    assert serialized["expected_volume"] == "19200"
    assert serialized["delta_provenance_version"] == DELTA_PROVENANCE_VERSION


@pytest.mark.parametrize(
    "provenance",
    ["ALPACA_PROVIDER_DELTA_V1", "THETADATA_PROVIDER_DELTA_V1", "DETERMINISTIC_OPTION_DELTA_V1"],
)
def test_delta_provenance_allowlist_accepts_provider_and_versioned_calculation(provenance: str):
    signed, gross, coherence, reasons = delta_equivalent(
        [
            {
                "trade_classification": "ask",
                "delta": "0.5",
                "delta_provenance": provenance,
                "trade_size": 1,
            }
        ]
    )
    assert signed == gross == Decimal(50)
    assert coherence == Decimal(1)
    assert reasons == ()


@pytest.mark.parametrize(
    ("event", "reason"),
    [
        (
            {"trade_classification": "ask", "delta": "0.5", "trade_size": 1},
            "IMPACT_DELTA_PROVENANCE_UNAVAILABLE",
        ),
        (
            {
                "trade_classification": "ask",
                "delta": "0.5",
                "delta_provenance": "UNSUPPORTED",
                "trade_size": 1,
            },
            "IMPACT_DELTA_PROVENANCE_UNAVAILABLE",
        ),
        ({"trade_classification": "ask", "trade_size": 1}, "IMPACT_DELTA_UNAVAILABLE"),
    ],
)
def test_numeric_delta_without_supported_provenance_fails_closed(
    event: dict[str, object], reason: str
):
    signed, gross, coherence, reasons = delta_equivalent([event])
    assert signed is gross is coherence is None
    assert reasons == (reason,)


def test_mixed_delta_provenance_preserves_raw_evidence_and_blocks_cluster():
    item = audit()
    events = events_for(item)
    events[1] = {**events[1], "delta_provenance": "UNSUPPORTED"}
    result = evaluate_cluster(item, events, baseline())
    assert result.shadow_qualified is False
    assert "IMPACT_DELTA_PROVENANCE_UNAVAILABLE" in result.failed_reasons
    assert result.delta_evidence[1]["supplied_value"] == "1"
    assert result.delta_evidence[1]["provenance_state"] == "UNSUPPORTED"
    replayed = result.as_dict()
    assert replayed["delta_evidence"] == list(result.delta_evidence)


@pytest.mark.parametrize(
    ("volatility", "samples", "expected"),
    [(Decimal("0.01"), 19, False)],
)
def test_baseline_insufficiency_blocks_shadow(volatility: Decimal, samples: int, expected: bool):
    item = audit()
    result = evaluate_cluster(
        item, events_for(item), baseline(volatility=volatility, samples=samples)
    )
    assert result.shadow_qualified is expected
    assert "IMPACT_BASELINE_INSUFFICIENT" in result.failed_reasons


def test_exact_z_one_boundary_and_control_shadow_independence():
    item = audit(control=True)
    result = evaluate_cluster(item, events_for(item), baseline())
    assert result.z_score == Decimal(1)
    assert result.shadow_qualified is True
    assert result.control_qualified is True
    assert result.comparison == "CONTROL_PASS_SHADOW_PASS"


def test_below_and_above_z_boundaries():
    item = audit()
    below = evaluate_cluster(item, events_for(item, delta="0.99"), baseline())
    above = evaluate_cluster(item, events_for(item, delta="1.01"), baseline())
    assert below.z_score is not None and below.z_score < 1
    assert below.shadow_qualified is False
    assert above.z_score is not None and above.z_score > 1
    assert above.shadow_qualified is True


def test_coherence_and_threshold_evidence_are_separate():
    item = audit(ask="64.99")
    result = evaluate_cluster(item, events_for(item), baseline())
    assert result.shadow_qualified is False
    assert result.thresholds["ask_side_percentage"] is False
    assert "IMPACT_THRESHOLD_ASK_SIDE_PERCENTAGE_FAILED" in result.failed_reasons


def test_fixed_event_time_window_is_inclusive():
    item = audit(last=NOW + timedelta(seconds=1))
    assert evaluate_cluster(item, events_for(item), baseline()).thresholds[
        "fixed_one_second_window"
    ]
    item = audit(last=NOW + timedelta(seconds=1, milliseconds=1))
    assert not evaluate_cluster(item, events_for(item), baseline()).thresholds[
        "fixed_one_second_window"
    ]


def test_session_decay_is_fresh_only_and_not_additive_z():
    first = evaluate_cluster(audit(cluster_id="1"), events_for(audit()), baseline())
    second_audit = audit(
        cluster_id="2", first=NOW + timedelta(minutes=1), last=NOW + timedelta(minutes=1)
    )
    second = evaluate_cluster(second_audit, events_for(second_audit), baseline())
    state = session_state("BAC", NOW + timedelta(minutes=1), (first, second), baseline())
    assert state.fresh_cluster_count == 2
    assert state.decayed_signed_demand < first.signed_delta_demand + second.signed_delta_demand  # type: ignore[operator]
    expired = session_state(
        "BAC", NOW + timedelta(minutes=IMPACT_DECAY_MINUTES), (first,), baseline()
    )
    assert expired.fresh_cluster_count == 0


def test_three_fresh_clusters_positive_demand_passes_session_gate():
    results = []
    for index in range(3):
        item = audit(
            cluster_id=str(index),
            first=NOW + timedelta(seconds=index),
            last=NOW + timedelta(seconds=index),
        )
        results.append(evaluate_cluster(item, events_for(item), baseline()))
    state = session_state("BAC", NOW + timedelta(seconds=3), results, baseline())
    assert state.fresh_cluster_count == 3
    assert state.passed is True


def test_baseline_uses_completed_sessions_and_persists_provenance():
    bars: list[dict[str, object]] = []
    day = datetime(2026, 7, 1, tzinfo=UTC)
    completed = 0
    while completed < 20:
        if day.weekday() < 5:
            timestamp = day.replace(hour=14)
            completed += 1
        else:
            day += timedelta(days=1)
            continue
        for minute in range(5):
            bars.append(
                {
                    "timestamp": timestamp + timedelta(minutes=minute),
                    "volume": 100,
                    "close": Decimal(100 + minute),
                }
            )
        day += timedelta(days=1)
    result = calculate_five_minute_baseline(
        "BAC", bars, date(2026, 8, 7), NOW, "split-adjusted", "alpaca:fixture-bars"
    )
    assert result.sample_size == 20
    assert result.expected_volume == Decimal(500)
    assert result.source == "ALPACA_HISTORICAL_BARS"
    assert result.provenance == "alpaca:fixture-bars"


def test_coverage_keeps_unresolved_and_enforces_separate_limits():
    candidates = tuple(
        CoverageCandidate(
            "BAC",
            20260814,
            40000 + index,
            "C",
            None,
            False,
            index + 1,
            3,
            ("quote_classification",),
            "UNRESOLVED",
        )
        for index in range(3)
    )
    plan = build_coverage_plan(candidates, trade_limit=2, quote_limit=3)
    assert len(plan.selected) == 2
    assert plan.complete_conditional_coverage is False
    assert plan.status == "CAPACITY_CONSTRAINED_COVERAGE"
    assert plan.excluded[0]["reason"] == "CAPACITY_CONSTRAINED_COVERAGE"


def test_coverage_excludes_only_proven_hard_bound_candidates():
    excluded = CoverageCandidate(
        "BAC", 20260814, 40000, "C", Decimal("0.5"), True, 1, 1, (), "PROVABLY_EXCLUDABLE"
    )
    plan = build_coverage_plan((excluded,))
    assert plan.selected == ()
    assert plan.complete_conditional_coverage is True


def test_shared_engine_persists_cluster_and_session_evidence():
    run_id = uuid4()
    engine = ShadowImpactEngine(run_id, {"BAC": baseline()})
    item = audit()
    for event_id, payload in zip(item["constituent_trade_ids"], events_for(item), strict=True):  # type: ignore[arg-type]
        engine.accept_event(UUID(str(event_id)), payload)
    cluster, session = engine.process_audit(item)
    assert cluster["model_version"] == "SHADOW_IMPACT_V1"
    assert cluster["impact_coefficient"] == "1"
    assert cluster["impact_coefficient_source"] == "RESEARCH_ASSUMPTION_V1"
    assert cluster["target_move"] == "0.0025"
    assert cluster["target_move_source"] == "OWNER_SELECTED_TARGET_UNDERLYING_MOVE"
    assert cluster["expected_volume"] == str(baseline().expected_volume)
    assert cluster["expected_volatility"] == str(baseline().expected_volatility)
    assert session["run_id"] == str(run_id)
    replay = ShadowImpactEngine.replay(run_id, (item,), engine.events, {"BAC": baseline()})
    assert replay == (cluster,)


async def test_async_writer_drains_shadow_cluster_and_session_audits():
    repository = InMemoryRepository()
    writer = AsyncAuditWriter(repository, soft_limit=4, hard_limit=8, batch_size=2)
    cluster = {"run_id": str(uuid4()), "cluster_id": "c1", "model_version": "SHADOW_IMPACT_V1"}
    session = {"run_id": cluster["run_id"], "symbol": "BAC", "as_of": NOW.isoformat()}
    writer.start()
    assert writer.enqueue(AuditWrite(impact_cluster=cluster))
    assert writer.enqueue(AuditWrite(impact_session=session))
    await writer.drain()
    assert repository.impact_clusters == [cluster]
    assert repository.impact_sessions == [session]


def test_shadow_is_attached_to_shared_pipeline_and_not_a_second_ingress_path():
    run_id = uuid4()
    repository = InMemoryRepository()
    engine = ShadowImpactEngine(run_id, {"BAC": baseline()})
    pipeline = SignalPipeline(
        Settings(),
        repository=repository,
        symbols=("BAC",),
        run_id=run_id,
        impact_engine=engine,
        now=lambda: NOW,
    )
    for sequence, exchange in enumerate(("5", "31", "43"), 1):
        event_time = NOW + timedelta(milliseconds=sequence)
        event = CanonicalEvent(
            event_id=uuid4(),
            run_id=run_id,
            kind=EventKind.OPTIONS,
            symbol="BAC",
            source="theta",
            source_timestamp=event_time,
            received_timestamp=event_time,
            normalized_timestamp=event_time,
            sequence=sequence,
            payload={
                "contract": {"root": "BAC", "expiration": 20260814, "strike": 40000, "right": "C"},
                "condition_code": 0,
                "trade_price": Decimal(100),
                "trade_size": 5,
                "exchange": exchange,
                "delta": Decimal("0.5"),
                "delta_provenance": "ALPACA_PROVIDER_DELTA_V1",
                "quote_context": {
                    "bid": Decimal(99),
                    "ask": Decimal(100),
                    "timestamp": event_time.isoformat(),
                    "contract": {
                        "root": "BAC",
                        "expiration": 20260814,
                        "strike": 40000,
                        "right": "C",
                    },
                },
            },
        )
        pipeline.process(event)
    assert len(repository.events) == 3
    assert len(repository.impact_clusters) >= 1
    assert all(value["model_version"] == "SHADOW_IMPACT_V1" for value in repository.impact_clusters)
    assert len(pipeline.impact_results) >= 1
