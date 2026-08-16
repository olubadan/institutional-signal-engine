"""Offline temporal evidence and deterministic lead/lag fixtures.

This module is deliberately independent of provider clients and trading code.
The fixture laboratory is an evaluation aid: its hidden ground truth is never
part of the event stream supplied to a detector and can never establish a
live-market result.
"""

from __future__ import annotations

import hashlib
import json
import random
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from itertools import pairwise
from statistics import median
from typing import Any


class LeadLagOutcome(StrEnum):
    OPTIONS_FIRST = "OPTIONS_FIRST"
    EQUITY_FIRST = "EQUITY_FIRST"
    SIMULTANEOUS = "SIMULTANEOUS"
    NO_RESPONSE = "NO_RESPONSE"
    PRE_EXISTING_EQUITY_MOVE = "PRE_EXISTING_EQUITY_MOVE"
    AMBIGUOUS = "AMBIGUOUS"


def _utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        raise ValueError("temporal evidence timestamps must be timezone-aware")
    return value.astimezone(UTC)


def _parse_payload_timestamp(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return _utc(value)
    return datetime.fromisoformat(str(value)).astimezone(UTC)


@dataclass(frozen=True)
class TemporalEvidence:
    """A lossless-enough receipt for the clocks currently observable."""

    receipt_id: str
    source: str
    symbol: str
    instrument: str | None
    provider_event_timestamp: datetime | None
    provider_sequence: int | str | None
    received_wall_timestamp: datetime | None
    received_monotonic_ns: int | None
    normalization_timestamp: datetime | None
    canonical_acceptance_timestamp: datetime | None
    processing_timestamp: datetime | None
    emission_timestamp: datetime | None
    timestamp_precision: str | None
    timestamp_uncertainty_ns: int | None
    provenance: str
    duplicate: bool
    out_of_order: bool

    def __post_init__(self) -> None:
        for name in (
            "provider_event_timestamp",
            "received_wall_timestamp",
            "normalization_timestamp",
            "canonical_acceptance_timestamp",
            "processing_timestamp",
            "emission_timestamp",
        ):
            object.__setattr__(self, name, _utc(getattr(self, name)))

    @classmethod
    def from_canonical_event(
        cls,
        event: Any,
        *,
        receipt_id: str | None = None,
        processing_timestamp: datetime | None = None,
        emission_timestamp: datetime | None = None,
        received_monotonic_ns: int | None = None,
        canonical_acceptance_timestamp: datetime | None = None,
        timestamp_precision: str | None = None,
        timestamp_uncertainty_ns: int | None = None,
        duplicate: bool = False,
        out_of_order: bool = False,
    ) -> TemporalEvidence:
        """Adapt the existing CanonicalEvent without changing its schema."""

        payload = event.payload
        return cls(
            receipt_id=receipt_id or str(event.event_id),
            source=str(event.source),
            symbol=str(event.symbol),
            instrument=(str(payload["contract"]) if payload.get("contract") else None),
            provider_event_timestamp=event.source_timestamp,
            provider_sequence=event.sequence,
            received_wall_timestamp=event.received_timestamp,
            received_monotonic_ns=received_monotonic_ns
            if received_monotonic_ns is not None
            else payload.get("_received_monotonic_ns"),
            normalization_timestamp=event.normalized_timestamp,
            canonical_acceptance_timestamp=canonical_acceptance_timestamp
            or _parse_payload_timestamp(payload.get("_canonical_acceptance_timestamp")),
            processing_timestamp=processing_timestamp,
            emission_timestamp=emission_timestamp,
            timestamp_precision=timestamp_precision
            or payload.get("timestamp_precision")
            or payload.get("timestamp_conversion", {}).get("precision"),
            timestamp_uncertainty_ns=timestamp_uncertainty_ns,
            provenance=f"canonical_event:{event.event_id}",
            duplicate=duplicate,
            out_of_order=out_of_order,
        )

    def as_dict(self) -> dict[str, object]:
        result = asdict(self)
        for key, value in tuple(result.items()):
            if isinstance(value, datetime):
                result[key] = value.isoformat()
        return result


@dataclass(frozen=True)
class T0Lifecycle:
    """The four option-side clocks, with explicit missing-observation gaps."""

    t0_onset: datetime | None
    t0_cluster_complete: datetime | None
    t0_qualify: datetime | None
    t0_emit: datetime | None
    evidence_refs: tuple[str, ...] = ()
    gaps: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for name in ("t0_onset", "t0_cluster_complete", "t0_qualify", "t0_emit"):
            object.__setattr__(self, name, _utc(getattr(self, name)))

    @property
    def valid(self) -> bool:
        return not self.validation_errors()

    def validation_errors(self) -> tuple[str, ...]:
        errors: list[str] = []
        if self.t0_onset and self.t0_cluster_complete and self.t0_cluster_complete < self.t0_onset:
            errors.append("cluster_complete_before_onset")
        if self.t0_qualify and self.t0_onset and self.t0_qualify < self.t0_onset:
            errors.append("qualify_before_onset")
        if self.t0_emit and self.t0_qualify is None:
            errors.append("emit_without_qualify")
        if self.t0_emit and self.t0_qualify and self.t0_emit < self.t0_qualify:
            errors.append("emit_before_qualify")
        return tuple(errors)

    def as_dict(self) -> dict[str, object]:
        return {
            "t0_onset": self.t0_onset.isoformat() if self.t0_onset else None,
            "t0_cluster_complete": (
                self.t0_cluster_complete.isoformat() if self.t0_cluster_complete else None
            ),
            "t0_qualify": self.t0_qualify.isoformat() if self.t0_qualify else None,
            "t0_emit": self.t0_emit.isoformat() if self.t0_emit else None,
            "evidence_refs": list(self.evidence_refs),
            "gaps": list(self.gaps),
            "validation_errors": list(self.validation_errors()),
        }


def map_existing_t0(
    audit: Mapping[str, object],
    *,
    qualification_timestamp: datetime | None = None,
    emission_timestamp: datetime | None = None,
) -> T0Lifecycle:
    """Bind t0 names to current sweep audit fields without inventing clocks.

    Current ``SweepEngine`` audits expose first/last constituent timestamps and
    qualification state. They do not expose qualification or emission times;
    those remain explicit gaps until the runtime records them.
    """

    def parse(name: str) -> datetime | None:
        value = audit.get(name)
        if value is None:
            return None
        if isinstance(value, datetime):
            return _utc(value)
        return datetime.fromisoformat(str(value)).astimezone(UTC)

    onset = parse("first_constituent_timestamp")
    complete = parse("cluster_complete_timestamp")
    if complete is None and bool(audit.get("final")):
        complete = parse("close_timestamp")
    qualify = qualification_timestamp or parse("qualification_timestamp")
    emit = emission_timestamp or parse("emission_timestamp")
    gaps: list[str] = []
    if onset is None:
        gaps.append("t0_onset_not_captured")
    if complete is None:
        gaps.append("t0_cluster_complete_not_captured_or_cluster_still_growing")
    if bool(audit.get("qualification_state")) and qualify is None:
        gaps.append("t0_qualify_timestamp_not_captured")
    if not bool(audit.get("qualification_state")):
        gaps.append("cluster_not_qualified")
    if emit is None:
        gaps.append("t0_emit_timestamp_not_captured")
    return T0Lifecycle(
        onset,
        complete,
        qualify,
        emit,
        evidence_refs=tuple(
            name
            for name, value in (
                ("first_constituent_timestamp", onset),
                ("last_constituent_timestamp", complete),
                ("qualification_timestamp", qualify),
                ("emission_timestamp", emit),
            )
            if value is not None
        ),
        gaps=tuple(gaps),
    )


def economic_detection_clock(lifecycle: T0Lifecycle) -> datetime:
    """Return the earliest actionable clock, never the scientific onset."""

    if lifecycle.t0_emit is not None:
        return lifecycle.t0_emit
    if lifecycle.t0_qualify is not None:
        return lifecycle.t0_qualify
    raise ValueError("actionable detection clock is unavailable; onset is not a substitute")


@dataclass(frozen=True)
class SyntheticEvent:
    event_id: str
    stream: str
    symbol: str
    provider_timestamp: datetime
    received_timestamp: datetime
    received_monotonic_ns: int
    sequence: int
    payload: Mapping[str, object]

    def as_dict(self) -> dict[str, object]:
        return {
            "event_id": self.event_id,
            "stream": self.stream,
            "symbol": self.symbol,
            "provider_timestamp": self.provider_timestamp.isoformat(),
            "received_timestamp": self.received_timestamp.isoformat(),
            "received_monotonic_ns": self.received_monotonic_ns,
            "sequence": self.sequence,
            "payload": dict(self.payload),
        }


@dataclass(frozen=True)
class SyntheticScenario:
    scenario_id: str
    seed: int
    configuration: Mapping[str, object]
    visible_events: tuple[SyntheticEvent, ...]
    hidden_ground_truth: Mapping[str, object]

    def visible_payload(self) -> tuple[dict[str, object], ...]:
        return tuple(event.as_dict() for event in self.visible_events)

    def as_dict(self) -> dict[str, object]:
        return {
            "scenario_id": self.scenario_id,
            "scenario_version": "LEAD_LAG_TEMPORAL_LAB_V1",
            "seed": self.seed,
            "configuration": dict(self.configuration),
            "visible_events": list(self.visible_payload()),
            "hidden_ground_truth": dict(self.hidden_ground_truth),
        }

    @property
    def visible_hash(self) -> str:
        encoded = json.dumps(self.visible_payload(), sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class TemporalEvaluation:
    true_outcome: str
    true_lag_seconds: float | None
    observed_outcome: str
    observed_lag_seconds: float | None
    lag_error_seconds: float | None
    detection_delay_seconds: float | None
    duplicate_count: int
    out_of_order_count: int
    missing_observation_count: int
    visible_hash: str

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def _price_at(timestamp: datetime, onset: datetime | None, rng: random.Random, changing: bool) -> float:
    baseline = 100.0
    noise = rng.uniform(-0.00015, 0.00015) if changing else 0.0
    if onset is not None and timestamp >= onset:
        return baseline * (1.0 + 0.006 + noise)
    return baseline * (1.0 + noise)


def generate_synthetic_scenario(
    seed: int,
    outcome: str = "OPTIONS_FIRST",
    lag_seconds: float = 5.0,
    *,
    jitter_ms: int = 0,
    irregular_sampling: bool = False,
    out_of_order: bool = False,
    duplicates: bool = False,
    missing_observations: bool = False,
    bursty_options: bool = True,
    changing_volatility: bool = False,
) -> SyntheticScenario:
    """Generate deterministic raw observations and separate hidden truth."""

    if outcome not in {value.value for value in LeadLagOutcome}:
        raise ValueError(f"unknown synthetic outcome: {outcome}")
    rng = random.Random(seed)
    base = datetime(2026, 1, 2, 14, 30, tzinfo=UTC)
    option_onset = base + timedelta(seconds=20)
    if outcome == LeadLagOutcome.OPTIONS_FIRST:
        equity_onset = option_onset + timedelta(seconds=lag_seconds)
        true_lag = float(lag_seconds)
    elif outcome in {LeadLagOutcome.EQUITY_FIRST, LeadLagOutcome.PRE_EXISTING_EQUITY_MOVE}:
        true_lag = -abs(float(lag_seconds))
        equity_onset = option_onset + timedelta(seconds=true_lag)
    elif outcome == LeadLagOutcome.SIMULTANEOUS:
        true_lag = min(0.05, max(0.0, lag_seconds))
        equity_onset = option_onset + timedelta(seconds=true_lag)
    else:
        true_lag = None
        equity_onset = None

    option_offsets = (0.0, 0.2, 0.6) if bursty_options else (0.0,)
    events: list[SyntheticEvent] = []
    sequence = 0

    def add(stream: str, timestamp: datetime, payload: dict[str, object]) -> None:
        nonlocal sequence
        sequence += 1
        if out_of_order and stream == "options":
            latency_ms = 300
        elif out_of_order and stream == "equity" and timestamp == option_onset:
            latency_ms = 5
        else:
            latency_ms = 50 + (rng.randint(0, jitter_ms) if jitter_ms else 0)
        receive = timestamp + timedelta(milliseconds=latency_ms)
        events.append(
            SyntheticEvent(
                event_id=f"{seed}-{sequence:04d}",
                stream=stream,
                symbol="AAPL",
                provider_timestamp=timestamp,
                received_timestamp=receive,
                received_monotonic_ns=sequence * 1_000_000_000 + latency_ms * 1_000_000,
                sequence=sequence,
                payload=payload,
            )
        )

    for offset in option_offsets:
        add(
            "options",
            option_onset + timedelta(seconds=offset),
            {"provider_event_kind": "trade", "pressure": "call", "premium": 250_000},
        )

    sample_offsets = (
        (0, 1, 2, 7, 12, 19, 30, 60, 120, 150, 180)
        if irregular_sampling
        else tuple(range(0, 181, 5))
    )
    for offset in sample_offsets:
        timestamp = base + timedelta(seconds=offset)
        if equity_onset is not None and timestamp >= equity_onset:
            payload_price = _price_at(timestamp, equity_onset, rng, changing_volatility)
        else:
            payload_price = _price_at(timestamp, None, rng, changing_volatility)
        if missing_observations and offset == sample_offsets[-2]:
            continue
        add(
            "equity",
            timestamp,
            {"provider_event_kind": "trade", "price": round(payload_price, 6)},
        )

    duplicate_count = 0
    if duplicates and events:
        events.append(events[-1])
        duplicate_count = 1
    if out_of_order:
        events = sorted(events, key=lambda event: (event.received_timestamp, -event.sequence))

    configuration = {
        "outcome": outcome,
        "lag_seconds": lag_seconds,
        "jitter_ms": jitter_ms,
        "irregular_sampling": irregular_sampling,
        "out_of_order": out_of_order,
        "duplicates": duplicates,
        "missing_observations": missing_observations,
        "bursty_options": bursty_options,
        "changing_volatility": changing_volatility,
    }
    ground_truth = {
        "true_outcome": outcome,
        "true_lag_seconds": true_lag,
        "t0_onset": option_onset.isoformat(),
        "t1_onset": equity_onset.isoformat() if equity_onset else None,
        "duplicate_count": duplicate_count,
        "ground_truth_is_not_visible": True,
    }
    return SyntheticScenario(
        scenario_id=f"lead-lag-{seed}",
        seed=seed,
        configuration=configuration,
        visible_events=tuple(events),
        hidden_ground_truth=ground_truth,
    )


def _fixture_response_time(events: tuple[SyntheticEvent, ...]) -> datetime | None:
    equity = sorted((event for event in events if event.stream == "equity"), key=lambda e: e.provider_timestamp)
    if len(equity) < 2:
        return None
    baseline = median(float(str(event.payload["price"])) for event in equity[:2])
    for event in equity[2:]:
        if abs(float(str(event.payload["price"])) - baseline) / baseline >= 0.002:
            return event.provider_timestamp
    return None


def _classify_visible_observations(
    events: tuple[SyntheticEvent, ...],
) -> tuple[str, float | None, float | None]:
    """Classify only from visible observations; hidden truth is not consulted."""

    unique = {event.event_id: event for event in events}
    options = tuple(event for event in unique.values() if event.stream == "options")
    option_onset = min((event.provider_timestamp for event in options), default=None)
    equity_onset = _fixture_response_time(tuple(unique.values()))
    if option_onset is None or equity_onset is None:
        observed = (
            LeadLagOutcome.NO_RESPONSE.value
            if equity_onset is None
            else LeadLagOutcome.AMBIGUOUS.value
        )
        return observed, None, None
    observed_lag = (equity_onset - option_onset).total_seconds()
    detection_delay = (
        next(
            event.received_timestamp
            for event in unique.values()
            if event.provider_timestamp == equity_onset
        )
        - equity_onset
    ).total_seconds()
    if abs(observed_lag) <= 0.050:
        observed = LeadLagOutcome.SIMULTANEOUS.value
    elif observed_lag > 0:
        observed = LeadLagOutcome.OPTIONS_FIRST.value
    else:
        observed = LeadLagOutcome.PRE_EXISTING_EQUITY_MOVE.value
    return observed, observed_lag, detection_delay


def evaluate_synthetic_scenario(scenario: SyntheticScenario) -> TemporalEvaluation:
    """Evaluate visible observations against hidden truth using a fixture detector."""

    events = scenario.visible_events
    observed, observed_lag, delay = _classify_visible_observations(events)
    unique = {event.event_id: event for event in events}
    true_lag = scenario.hidden_ground_truth["true_lag_seconds"]
    true_lag_float = float(str(true_lag)) if true_lag is not None else None
    return TemporalEvaluation(
        true_outcome=str(scenario.hidden_ground_truth["true_outcome"]),
        true_lag_seconds=true_lag_float,
        observed_outcome=observed,
        observed_lag_seconds=observed_lag,
        lag_error_seconds=(observed_lag - true_lag_float)
        if observed_lag is not None and true_lag_float is not None
        else None,
        detection_delay_seconds=delay,
        duplicate_count=len(events) - len(unique),
        out_of_order_count=sum(1 for left, right in pairwise(events) if right.sequence < left.sequence),
        missing_observation_count=int(bool(scenario.configuration["missing_observations"])),
        visible_hash=scenario.visible_hash,
    )
