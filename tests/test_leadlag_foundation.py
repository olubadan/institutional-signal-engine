from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from institutional_signal_engine.config import Settings
from institutional_signal_engine.leadlag import (
    LeadLagOutcome,
    T0Lifecycle,
    economic_detection_clock,
    evaluate_synthetic_scenario,
    generate_synthetic_scenario,
    map_existing_t0,
)
from institutional_signal_engine.persistence import InMemoryRepository
from institutional_signal_engine.pipeline import SignalPipeline
from institutional_signal_engine.providers.alpaca import AlpacaEquitiesProvider
from institutional_signal_engine.providers.thetadata import ThetaDataOptionsProvider
from institutional_signal_engine.schemas import CanonicalEvent, EventKind


def test_temporal_evidence_contract_preserves_existing_event_clocks():
    from institutional_signal_engine.leadlag import TemporalEvidence

    source = datetime(2026, 1, 2, 14, 30, tzinfo=UTC)
    event = CanonicalEvent(
        event_id=uuid4(), kind=EventKind.OPTIONS, symbol="AAPL", source="thetadata",
        source_timestamp=source, received_timestamp=source + timedelta(milliseconds=40),
        normalized_timestamp=source + timedelta(milliseconds=41), sequence=7, payload={"contract": {"root": "AAPL"}},
    )
    evidence = TemporalEvidence.from_canonical_event(
        event, received_monotonic_ns=123, canonical_acceptance_timestamp=source + timedelta(milliseconds=42),
        processing_timestamp=source + timedelta(milliseconds=43),
    )
    assert evidence.provider_event_timestamp == source
    assert evidence.received_wall_timestamp != evidence.normalization_timestamp
    assert evidence.received_monotonic_ns == 123
    assert evidence.processing_timestamp > evidence.canonical_acceptance_timestamp


def test_temporal_evidence_reads_runtime_capture_fields_without_collapsing_clocks():
    from institutional_signal_engine.leadlag import TemporalEvidence, clock_domain_duration_seconds

    source = datetime(2026, 1, 2, 14, 30, tzinfo=UTC)
    event = CanonicalEvent(
        event_id=uuid4(), kind=EventKind.OPTIONS, symbol="AAPL", source="thetadata",
        source_timestamp=source, received_timestamp=source + timedelta(milliseconds=40),
        normalized_timestamp=source + timedelta(milliseconds=41), sequence=7,
        payload={
            "_received_monotonic_ns": 123,
            "_pipeline_admission_timestamp": (source + timedelta(milliseconds=42)).isoformat(),
        },
    )
    evidence = TemporalEvidence.from_canonical_event(event)
    assert evidence.received_monotonic_ns == 123
    assert evidence.pipeline_admission_timestamp == source + timedelta(milliseconds=42)
    assert evidence.canonical_acceptance_timestamp == source + timedelta(milliseconds=42)
    assert evidence.provider_event_timestamp != evidence.canonical_acceptance_timestamp
    assert evidence.clock_domains["provider_event"] == "provider_event_clock"
    with pytest.raises(ValueError):
        clock_domain_duration_seconds(
            source, source + timedelta(seconds=1),
            start_domain="provider_event_clock", end_domain="local_wall_clock",
        )


def test_local_provider_normalization_captures_monotonic_receive_without_network():
    provider = AlpacaEquitiesProvider("wss://offline.invalid/stream", "fixture-key", "fixture-secret")
    event = provider._normalize({
        "T": "t", "t": "2026-01-02T14:30:00Z", "S": "AAPL", "i": 1,
        "p": 100, "s": 10, "c": [], "x": "D",
    }, ingress_wall_timestamp=datetime(2026, 1, 2, 14, 30, tzinfo=UTC), ingress_monotonic_ns=123)
    assert event is not None
    assert event.payload["_received_monotonic_ns"] == 123
    assert isinstance(event.payload["_normalized_monotonic_ns"], int)


def test_theta_normalization_accepts_socket_ingress_clock_without_network():
    provider = ThetaDataOptionsProvider("ws://offline.invalid/events", "fixture-secret")
    event = provider._normalize({
        "header": {"type": "TRADE"},
        "contract": {"root": "AAPL", "expiration": 20260807, "strike": 310000, "right": "C"},
        "trade": {"date": "20260102", "ms_of_day": 52200000, "sequence": 7, "size": 5, "price": "100", "exchange": "5"},
    }, ingress_wall_timestamp=datetime(2026, 1, 2, 14, 30, tzinfo=UTC), ingress_monotonic_ns=456)
    assert event is not None
    assert event.payload["_received_monotonic_ns"] == 456
    assert isinstance(event.payload["_normalized_monotonic_ns"], int)


def test_pipeline_records_canonical_acceptance_timestamp_offline():
    now = datetime(2026, 1, 2, 14, 30, tzinfo=UTC)
    repository = InMemoryRepository()
    pipeline = SignalPipeline(Settings(), repository=repository, now=lambda: now)
    pipeline.process(CanonicalEvent(
        event_id=uuid4(), kind=EventKind.EQUITY, symbol="AAPL", source="fixture",
        source_timestamp=now, received_timestamp=now, normalized_timestamp=now, sequence=1,
        payload={"provider_event_kind": "trade", "price": 100, "volume": 1},
    ))
    assert repository.events[-1].payload["_pipeline_admission_timestamp"] == now.isoformat()
    receipt = repository.replay_journal_receipts()[0]
    assert receipt["event_id"] == str(repository.events[-1].event_id)
    assert receipt["acceptance_stage"] == "in_memory_repository_record_event"
    assert receipt["clock_domain"] == "local_wall_clock"


def test_t0_maps_current_sweep_fields_and_marks_missing_clocks():
    base = datetime(2026, 1, 2, 14, 30, tzinfo=UTC)
    lifecycle = map_existing_t0({
        "first_constituent_timestamp": base.isoformat(),
        "last_constituent_timestamp": (base + timedelta(milliseconds=600)).isoformat(),
        "close_timestamp": (base + timedelta(seconds=1)).isoformat(),
        "qualification_state": True,
        "final": True,
    })
    assert lifecycle.t0_onset == base
    assert lifecycle.t0_cluster_complete == base + timedelta(seconds=1)
    assert lifecycle.t0_qualify is None
    assert "t0_qualify_timestamp_not_captured" in lifecycle.gaps
    assert "t0_emit_timestamp_not_captured" in lifecycle.gaps


def test_active_cluster_does_not_claim_completion_from_last_constituent():
    base = datetime(2026, 1, 2, 14, 30, tzinfo=UTC)
    lifecycle = map_existing_t0({
        "first_constituent_timestamp": base.isoformat(),
        "last_constituent_timestamp": (base + timedelta(milliseconds=600)).isoformat(),
        "qualification_state": True,
        "final": False,
    })
    assert lifecycle.t0_cluster_complete is None
    assert "t0_cluster_complete_not_captured_or_cluster_still_growing" in lifecycle.gaps


def test_t0_valid_order_and_actionable_clock_are_distinct_from_onset():
    base = datetime(2026, 1, 2, 14, 30, tzinfo=UTC)
    lifecycle = T0Lifecycle(base, base + timedelta(seconds=1), base + timedelta(seconds=2), base + timedelta(seconds=3))
    assert lifecycle.valid
    assert economic_detection_clock(lifecycle) == base + timedelta(seconds=3)


def test_t0_rejects_lookahead_clock_ordering():
    base = datetime(2026, 1, 2, 14, 30, tzinfo=UTC)
    qualification_before_later_constituent = T0Lifecycle(
        base, base + timedelta(seconds=2), base + timedelta(seconds=1), base + timedelta(seconds=3)
    )
    assert qualification_before_later_constituent.valid
    invalid_emit = T0Lifecycle(base, base + timedelta(seconds=1), base + timedelta(seconds=2), base + timedelta(seconds=1))
    assert "emit_before_qualify" in invalid_emit.validation_errors()
    invalid_qualify = T0Lifecycle(base, base + timedelta(seconds=1), base - timedelta(seconds=1), None)
    assert "qualify_before_onset" in invalid_qualify.validation_errors()
    with pytest.raises(ValueError):
        economic_detection_clock(T0Lifecycle(base, base + timedelta(seconds=1), None, None))


def test_t0_nonqualifying_completed_cluster_is_not_promoted():
    base = datetime(2026, 1, 2, 14, 30, tzinfo=UTC)
    lifecycle = map_existing_t0({
        "first_constituent_timestamp": base.isoformat(),
        "last_constituent_timestamp": (base + timedelta(seconds=1)).isoformat(),
        "qualification_state": False,
        "final": True,
    })
    assert lifecycle.t0_qualify is None
    assert "cluster_not_qualified" in lifecycle.gaps


def test_t0_emit_before_qualification_is_rejected_and_onset_is_not_actionable():
    base = datetime(2026, 1, 2, 14, 30, tzinfo=UTC)
    invalid = T0Lifecycle(base, base, None, base + timedelta(seconds=1))
    assert "emit_without_qualify" in invalid.validation_errors()
    with pytest.raises(ValueError):
        economic_detection_clock(T0Lifecycle(base, base, None, None))


@pytest.mark.parametrize("lag", [5.0, 30.0, 120.0])
def test_deterministic_lab_reproduces_known_options_lead_lags(lag: float):
    first = generate_synthetic_scenario(10, LeadLagOutcome.OPTIONS_FIRST.value, lag)
    second = generate_synthetic_scenario(10, LeadLagOutcome.OPTIONS_FIRST.value, lag)
    assert first.visible_hash == second.visible_hash
    result = evaluate_synthetic_scenario(first)
    assert result.true_lag_seconds == lag
    assert result.observed_outcome == LeadLagOutcome.OPTIONS_FIRST.value
    assert result.lag_error_seconds == 0


@pytest.mark.parametrize("outcome", [
    LeadLagOutcome.EQUITY_FIRST.value,
    LeadLagOutcome.SIMULTANEOUS.value,
    LeadLagOutcome.NO_RESPONSE.value,
    LeadLagOutcome.PRE_EXISTING_EQUITY_MOVE.value,
])
def test_deterministic_lab_covers_negative_and_nonpositive_outcomes(outcome: str):
    result = evaluate_synthetic_scenario(generate_synthetic_scenario(20, outcome, 5.0))
    assert result.true_outcome == outcome
    assert result.visible_hash
    if outcome == LeadLagOutcome.NO_RESPONSE.value:
        assert result.observed_outcome == LeadLagOutcome.NO_RESPONSE.value


def test_lab_variations_are_seeded_and_ground_truth_is_not_in_visible_events():
    scenario = generate_synthetic_scenario(
        99, LeadLagOutcome.OPTIONS_FIRST.value, 30.0, jitter_ms=20,
        irregular_sampling=True, out_of_order=True, duplicates=True,
        missing_observations=True, changing_volatility=True,
    )
    assert "true_lag_seconds" not in {key for event in scenario.visible_events for key in event.payload}
    assert scenario.hidden_ground_truth["true_lag_seconds"] == 30.0
    assert evaluate_synthetic_scenario(scenario).duplicate_count == 1


def test_observed_classification_is_unchanged_when_hidden_label_changes():
    scenario = generate_synthetic_scenario(101, LeadLagOutcome.PRE_EXISTING_EQUITY_MOVE.value, 5.0)
    changed_truth = replace(
        scenario,
        hidden_ground_truth={
            **scenario.hidden_ground_truth,
            "true_outcome": LeadLagOutcome.EQUITY_FIRST.value,
            "true_lag_seconds": -5.0,
        },
    )
    original = evaluate_synthetic_scenario(scenario)
    changed = evaluate_synthetic_scenario(changed_truth)
    assert original.observed_outcome == LeadLagOutcome.PRE_EXISTING_EQUITY_MOVE.value
    assert changed.observed_outcome == original.observed_outcome
    assert changed.observed_lag_seconds == original.observed_lag_seconds


def test_out_of_order_arrival_is_real_and_reconstructible_separately_from_provider_time():
    scenario = generate_synthetic_scenario(202, out_of_order=True)
    provider_order = [
        event.event_id
        for event in sorted(scenario.visible_events, key=lambda event: (event.provider_timestamp, event.sequence))
    ]
    receive_order = [event.event_id for event in scenario.visible_events]
    assert provider_order != receive_order
    assert evaluate_synthetic_scenario(scenario).out_of_order_count > 0
    assert [event.received_timestamp for event in scenario.visible_events] == sorted(
        event.received_timestamp for event in scenario.visible_events
    )


def test_duplicate_and_out_of_order_variations_are_distinct():
    duplicate = evaluate_synthetic_scenario(generate_synthetic_scenario(203, duplicates=True))
    inverted = evaluate_synthetic_scenario(generate_synthetic_scenario(204, out_of_order=True))
    assert duplicate.duplicate_count == 1
    assert duplicate.out_of_order_count == 0
    assert inverted.duplicate_count == 0
    assert inverted.out_of_order_count > 0
