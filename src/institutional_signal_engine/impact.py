"""Research-only mathematical footprint model evaluated from shared evidence.

This module deliberately has no provider or persistence dependencies.  The
live pipeline may call it after the existing sweep/feature path has accepted
an event; replay calls the same pure functions over persisted evidence.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import cast
from uuid import UUID
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

IMPACT_MODEL_VERSION = "SHADOW_IMPACT_V1"
CONTROL_MODEL_VERSION = "CONTROL_V1"
IMPACT_BASELINE_VERSION = "impact-baseline-5m-v1"
IMPACT_COVERAGE_VERSION = "impact-coverage-v1"
IMPACT_PRIORITY_VERSION = "impact-priority-v1"
IMPACT_REQUIRED_EVIDENCE_VERSION = "impact-evidence-fields-v1"
IMPACT_PI = Decimal("0.0025")
IMPACT_TARGET_MOVE = Decimal(1)
IMPACT_TARGET_MOVE_SOURCE = "RESEARCH_ASSUMPTION_V1"
IMPACT_HORIZON_MINUTES = 5
IMPACT_DECAY_MINUTES = 30
IMPACT_MIN_COHERENCE = Decimal("0.65")
IMPACT_MIN_ASK_PERCENTAGE = Decimal(65)
IMPACT_MAX_UNKNOWN_PERCENTAGE = Decimal(50)
IMPACT_MIN_EXCHANGES = 3
IMPACT_REQUIRED_EVIDENCE_FIELDS: tuple[str, ...] = (
    "delta",
    "quote_classification",
    "volatility_baseline",
    "volume_baseline",
    "exchange_identity",
)


@dataclass(frozen=True)
class ImpactBaseline:
    symbol: str
    horizon_minutes: int
    expected_volume: Decimal
    expected_volatility: Decimal
    effective_date: date
    sample_size: int
    provenance: str
    adjustment_metadata: str
    source: str = "ALPACA_HISTORICAL_BARS"
    version: str = IMPACT_BASELINE_VERSION

    @property
    def sufficient(self) -> bool:
        return (
            self.horizon_minutes == IMPACT_HORIZON_MINUTES
            and self.sample_size >= 20
            and self.expected_volume > 0
            and self.expected_volatility >= 0
            and self.source == "ALPACA_HISTORICAL_BARS"
        )


ImpactBaselineMap = Mapping[str, ImpactBaseline] | Mapping[tuple[str, int], ImpactBaseline]


@dataclass(frozen=True)
class ImpactClusterResult:
    cluster_id: str
    run_id: str
    symbol: str
    contract: tuple[str, int, int, str]
    first_timestamp: datetime
    last_timestamp: datetime
    nominal_premium: Decimal
    signed_delta_demand: Decimal | None
    gross_delta_activity: Decimal | None
    coherence: Decimal | None
    baseline: ImpactBaseline | None
    impact_coefficient: Decimal | None
    target_move: Decimal
    estimated_impact: Decimal | None
    z_score: Decimal | None
    fresh_window_seconds: int
    control_qualified: bool
    shadow_qualified: bool
    thresholds: Mapping[str, bool]
    failed_reasons: tuple[str, ...]
    comparison: str
    model_version: str = IMPACT_MODEL_VERSION
    target_move_source: str = IMPACT_TARGET_MOVE_SOURCE

    def as_dict(self) -> dict[str, object]:
        baseline = None
        if self.baseline is not None:
            baseline = {
                "symbol": self.baseline.symbol,
                "horizon_minutes": self.baseline.horizon_minutes,
                "expected_volume": str(self.baseline.expected_volume),
                "expected_volatility": str(self.baseline.expected_volatility),
                "effective_date": self.baseline.effective_date.isoformat(),
                "sample_size": self.baseline.sample_size,
                "provenance": self.baseline.provenance,
                "adjustment_metadata": self.baseline.adjustment_metadata,
                "source": self.baseline.source,
                "version": self.baseline.version,
            }
        return {
            "cluster_id": self.cluster_id,
            "run_id": self.run_id,
            "symbol": self.symbol,
            "contract": {
                "root": self.contract[0],
                "expiration": self.contract[1],
                "strike": self.contract[2],
                "right": self.contract[3],
            },
            "first_timestamp": self.first_timestamp.isoformat(),
            "last_timestamp": self.last_timestamp.isoformat(),
            "nominal_premium": str(self.nominal_premium),
            "signed_delta_demand": (
                str(self.signed_delta_demand) if self.signed_delta_demand is not None else None
            ),
            "gross_delta_activity": (
                str(self.gross_delta_activity) if self.gross_delta_activity is not None else None
            ),
            "coherence": str(self.coherence) if self.coherence is not None else None,
            "baseline": baseline,
            "impact_coefficient": (
                str(self.impact_coefficient) if self.impact_coefficient is not None else None
            ),
            "target_move": str(self.target_move),
            "target_move_source": self.target_move_source,
            "estimated_impact": (
                str(self.estimated_impact) if self.estimated_impact is not None else None
            ),
            "z_score": str(self.z_score) if self.z_score is not None else None,
            "fresh_window_seconds": self.fresh_window_seconds,
            "control_qualified": self.control_qualified,
            "shadow_qualified": self.shadow_qualified,
            "thresholds": dict(self.thresholds),
            "failed_reasons": list(self.failed_reasons),
            "comparison": self.comparison,
            "model_version": self.model_version,
        }


@dataclass(frozen=True)
class ImpactTimerTransition:
    run_id: str
    symbol: str
    timestamp: datetime
    reason: str
    cluster_ids: tuple[str, ...]
    model_version: str = IMPACT_MODEL_VERSION

    def as_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "symbol": self.symbol,
            "timestamp": self.timestamp.isoformat(),
            "reason": self.reason,
            "cluster_ids": list(self.cluster_ids),
            "model_version": self.model_version,
        }


@dataclass(frozen=True)
class ImpactSessionState:
    symbol: str
    as_of: datetime
    fresh_cluster_count: int
    decayed_signed_demand: Decimal
    session_z_score: Decimal | None
    passed: bool
    model_version: str = IMPACT_MODEL_VERSION

    def as_dict(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "as_of": self.as_of.isoformat(),
            "fresh_cluster_count": self.fresh_cluster_count,
            "decayed_signed_demand": str(self.decayed_signed_demand),
            "session_z_score": (
                str(self.session_z_score) if self.session_z_score is not None else None
            ),
            "passed": self.passed,
            "model_version": self.model_version,
        }


class ShadowImpactEngine:
    """Stateful shadow calculation attached to the shared sweep evidence.

    It receives already-normalized event payloads and sweep audits from the
    main pipeline.  It never subscribes, normalizes, classifies, or persists
    independently; callers persist the returned dictionaries through the
    normal audit writer.
    """

    def __init__(
        self,
        run_id: UUID,
        baselines: ImpactBaselineMap | None = None,
    ) -> None:
        self.run_id = run_id
        self.baselines: dict[str | tuple[str, int], ImpactBaseline] = {}
        for key, value in (baselines or {}).items():
            self.baselines[key.upper() if isinstance(key, str) else (key[0].upper(), key[1])] = (
                value
            )
        self.events: dict[str, Mapping[str, object]] = {}
        self.clusters: dict[str, ImpactClusterResult] = {}
        self.sessions: list[dict[str, object]] = []

    def _baseline(self, symbol: str, timestamp: datetime) -> ImpactBaseline | None:
        local = timestamp.astimezone(ET)
        minute_index = (local.hour * 60 + local.minute) - 570
        return self.baselines.get((symbol.upper(), minute_index)) or self.baselines.get(
            symbol.upper()
        )

    def accept_event(self, event_id: UUID, payload: Mapping[str, object]) -> None:
        self.events[str(event_id)] = dict(payload)

    def process_audit(
        self, audit: Mapping[str, object]
    ) -> tuple[dict[str, object], dict[str, object]]:
        event_payloads_list: list[Mapping[str, object]] = []
        constituent_ids = cast(Iterable[object], audit.get("constituent_trade_ids", ()))
        for value in constituent_ids:
            event_id = str(value)
            if event_id in self.events:
                event_payloads_list.append(self.events[event_id])
        event_payloads = tuple(event_payloads_list)
        symbol = str(audit.get("root", "")).upper()
        result = evaluate_cluster(
            audit,
            event_payloads,
            self._baseline(symbol, _timestamp(audit["last_constituent_timestamp"])),
        )
        self.clusters[result.cluster_id] = result
        state = session_state(
            symbol,
            result.last_timestamp,
            self.clusters.values(),
            self._baseline(symbol, result.last_timestamp),
        )
        session = {
            "run_id": str(self.run_id),
            **state.as_dict(),
        }
        self.sessions.append(session)
        return result.as_dict() | {"run_id": str(self.run_id)}, session

    def tick(self, now: datetime) -> tuple[dict[str, object], ...]:
        """Persist deterministic decay/expiry snapshots without new input."""
        outputs: list[dict[str, object]] = []
        symbols = {key if isinstance(key, str) else key[0] for key in self.baselines}
        for symbol in sorted(symbols):
            state = session_state(symbol, now, self.clusters.values(), self._baseline(symbol, now))
            outputs.append(
                {"run_id": str(self.run_id), **state.as_dict(), "trigger": "IMPACT_DECAY_TICK"}
            )
        return tuple(outputs)

    @classmethod
    def replay(
        cls,
        run_id: UUID,
        audits: Iterable[Mapping[str, object]],
        events: Mapping[str, Mapping[str, object]],
        baselines: ImpactBaselineMap,
    ) -> tuple[dict[str, object], ...]:
        engine = cls(run_id, baselines)
        for event_id, payload in events.items():
            engine.events[event_id] = payload
        return tuple(engine.process_audit(audit)[0] for audit in audits)


def _decimal(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _timestamp(value: object) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(UTC)
    return datetime.fromisoformat(str(value)).astimezone(UTC)


def _contract_from_audit(audit: Mapping[str, object]) -> tuple[str, int, int, str]:
    return (
        str(audit.get("root", "")).upper(),
        int(str(audit.get("expiration", 0))),
        int(str(audit.get("strike", 0))),
        str(audit.get("right", "")).upper(),
    )


def _comparison(control: bool, shadow: bool) -> str:
    if control and shadow:
        return "CONTROL_PASS_SHADOW_PASS"
    if control:
        return "CONTROL_PASS_SHADOW_FAIL"
    if shadow:
        return "CONTROL_FAIL_SHADOW_PASS"
    return "CONTROL_FAIL_SHADOW_FAIL"


def delta_equivalent(
    events: Sequence[Mapping[str, object]],
) -> tuple[Decimal | None, Decimal | None, Decimal | None, tuple[str, ...]]:
    """Return signed/gross delta demand and coherence without inventing delta.

    ``trade_classification`` is produced by the shared authoritative quote
    classifier.  A supplied delta must carry a provider or calculation
    provenance; absent or invalid values are explicitly unavailable.
    """
    signed = Decimal(0)
    gross = Decimal(0)
    reasons: list[str] = []
    any_delta = False
    for event in events:
        delta = _decimal(event.get("delta", event.get("option_delta")))
        if delta is None or not event.get("delta_provenance", event.get("option_delta_provenance")):
            reasons.append("IMPACT_DELTA_UNAVAILABLE")
            continue
        any_delta = True
        side = str(event.get("trade_classification", "unknown")).lower()
        confidence = event.get("classification_confidence", 1)
        if side not in {"ask", "bid"} or _decimal(confidence) != Decimal(1):
            continue
        size = _decimal(event.get("trade_size"))
        if size is None or size < 1:
            continue
        signed += (Decimal(1) if side == "ask" else Decimal(-1)) * delta * Decimal(100) * size
        gross += abs(delta) * Decimal(100) * size
    if not any_delta:
        return None, None, None, tuple(dict.fromkeys(reasons))
    coherence = abs(signed) / gross if gross > 0 else Decimal(0)
    return signed, gross, coherence, tuple(dict.fromkeys(reasons))


def evaluate_cluster(
    audit: Mapping[str, object],
    events: Sequence[Mapping[str, object]],
    baseline: ImpactBaseline | None,
) -> ImpactClusterResult:
    """Evaluate one fixed-window cluster and compare it with CONTROL_V1."""
    contract = _contract_from_audit(audit)
    control = bool(audit.get("qualification_state"))
    signed, gross, coherence, delta_reasons = delta_equivalent(events)
    coefficient: Decimal | None = None
    estimated: Decimal | None = None
    z_score: Decimal | None = None
    failed = list(delta_reasons)
    if baseline is None or not baseline.sufficient:
        failed.append("IMPACT_BASELINE_INSUFFICIENT")
    elif signed is not None and baseline.expected_volume > 0:
        coefficient = baseline.expected_volatility
        estimated = (
            coefficient * IMPACT_TARGET_MOVE * (abs(signed) / baseline.expected_volume).sqrt()
        )
        z_score = estimated / IMPACT_PI
    elif signed is None:
        failed.append("IMPACT_DELTA_UNAVAILABLE")
    ask = _decimal(audit.get("ask_side_percentage"))
    unknown = _decimal(audit.get("unknown_premium_percentage"))
    exchange_count = len(cast(Sequence[object], audit.get("exchange_set", ())))
    thresholds: dict[str, bool] = {
        "impact_z": z_score is not None and z_score >= Decimal(1),
        "directional_coherence": coherence is not None and coherence >= IMPACT_MIN_COHERENCE,
        "three_exchanges": exchange_count >= IMPACT_MIN_EXCHANGES,
        "ask_side_percentage": ask is not None and ask >= IMPACT_MIN_ASK_PERCENTAGE,
        "unknown_premium_percentage": unknown is not None
        and unknown <= IMPACT_MAX_UNKNOWN_PERCENTAGE,
        "fixed_one_second_window": (
            _timestamp(audit["last_constituent_timestamp"])
            - _timestamp(audit["first_constituent_timestamp"])
        )
        <= timedelta(seconds=1),
    }
    for name, passed in thresholds.items():
        if not passed:
            failed.append(f"IMPACT_THRESHOLD_{name.upper()}_FAILED")
    shadow = all(thresholds.values()) and not any(
        reason in {"IMPACT_BASELINE_INSUFFICIENT", "IMPACT_DELTA_UNAVAILABLE"} for reason in failed
    )
    return ImpactClusterResult(
        cluster_id=str(audit["cluster_id"]),
        run_id=str(audit["run_id"]),
        symbol=str(audit.get("root", "")).upper(),
        contract=contract,
        first_timestamp=_timestamp(audit["first_constituent_timestamp"]),
        last_timestamp=_timestamp(audit["last_constituent_timestamp"]),
        nominal_premium=_decimal(audit.get("aggregate_eligible_premium")) or Decimal(0),
        signed_delta_demand=signed,
        gross_delta_activity=gross,
        coherence=coherence,
        baseline=baseline,
        impact_coefficient=coefficient,
        target_move=IMPACT_TARGET_MOVE,
        estimated_impact=estimated,
        z_score=z_score,
        fresh_window_seconds=IMPACT_DECAY_MINUTES * 60,
        control_qualified=control,
        shadow_qualified=shadow,
        thresholds=thresholds,
        failed_reasons=tuple(dict.fromkeys(failed)),
        comparison=_comparison(control, shadow),
    )


def session_state(
    symbol: str,
    as_of: datetime,
    clusters: Iterable[ImpactClusterResult],
    baseline: ImpactBaseline | None,
) -> ImpactSessionState:
    """Compute fresh-count and exponentially decayed signed demand."""
    now = as_of.astimezone(UTC)
    tau = Decimal(IMPACT_DECAY_MINUTES * 60)
    fresh: list[ImpactClusterResult] = []
    demand = Decimal(0)
    for cluster in clusters:
        if cluster.symbol != symbol or not cluster.shadow_qualified:
            continue
        age = Decimal(str((now - cluster.last_timestamp).total_seconds()))
        # The timer fires at the exact 30-minute boundary; the cluster is
        # fresh only strictly before that deterministic expiry instant.
        if age < 0 or age >= tau:
            continue
        fresh.append(cluster)
        if cluster.signed_delta_demand is not None:
            demand += (-(age / tau)).exp() * cluster.signed_delta_demand
    score = None
    if baseline is not None and baseline.sufficient and baseline.expected_volume > 0:
        score = (baseline.expected_volatility / IMPACT_PI) * (
            abs(demand) / baseline.expected_volume
        ).sqrt()
    passed = len(fresh) >= 3 and score is not None and score >= 1 and demand > 0
    return ImpactSessionState(symbol, now, len(fresh), demand, score, passed)


def calculate_five_minute_baseline(
    symbol: str,
    bars: Sequence[Mapping[str, object]],
    session_date: date,
    target_timestamp: datetime,
    adjustment_metadata: str,
    provenance: str,
) -> ImpactBaseline:
    """Build a frozen same-time-of-day baseline from split-adjusted bars.

    Inputs are completed-session one-minute Alpaca bars.  The function groups
    each session's five-minute window beginning at the target minute and uses
    only sessions before ``session_date``.  At least twenty sessions are
    required; no proxy or fabricated fallback is returned.
    """
    target_local = target_timestamp.astimezone(ET)
    target_minute = target_local.hour * 60 + target_local.minute
    by_session: dict[date, list[Mapping[str, object]]] = {}
    for bar in bars:
        timestamp = _timestamp(bar["timestamp"])
        local_date = timestamp.date()
        if local_date >= session_date or timestamp.weekday() >= 5:
            continue
        by_session.setdefault(local_date, []).append(bar)
    volumes: list[Decimal] = []
    returns: list[Decimal] = []
    for day, day_bars in sorted(by_session.items()):
        candidates = sorted(day_bars, key=lambda value: _timestamp(value["timestamp"]))
        selected = [
            bar
            for bar in candidates
            if target_minute
            <= _timestamp(bar["timestamp"]).astimezone(ET).hour * 60
            + _timestamp(bar["timestamp"]).astimezone(ET).minute
            < target_minute + IMPACT_HORIZON_MINUTES
        ]
        if len(selected) < IMPACT_HORIZON_MINUTES:
            continue
        volumes.append(sum((Decimal(str(bar["volume"])) for bar in selected), Decimal(0)))
        first = _decimal(selected[0]["close"])
        last = _decimal(selected[-1]["close"])
        if first is not None and first > 0 and last is not None:
            returns.append((last - first) / first)
    if returns:
        mean = sum(returns, Decimal(0)) / Decimal(len(returns))
        variance = sum((value - mean) ** 2 for value in returns) / Decimal(len(returns))
        volatility = variance.sqrt()
    else:
        volatility = Decimal(0)
    return ImpactBaseline(
        symbol=symbol.upper(),
        horizon_minutes=IMPACT_HORIZON_MINUTES,
        expected_volume=(sum(volumes, Decimal(0)) / Decimal(len(volumes)))
        if volumes
        else Decimal(0),
        expected_volatility=volatility,
        effective_date=session_date,
        sample_size=len(volumes),
        provenance=provenance,
        adjustment_metadata=adjustment_metadata,
    )


def calculate_five_minute_baselines(
    symbol: str,
    bars: Sequence[Mapping[str, object]],
    session_date: date,
    adjustment_metadata: str,
    provenance: str,
) -> dict[tuple[str, int], ImpactBaseline]:
    """Calculate the frozen baseline book for each regular-session minute."""
    grouped: dict[date, dict[int, tuple[Decimal, Decimal]]] = {}
    for bar in bars:
        timestamp = _timestamp(bar["timestamp"]).astimezone(ET)
        if timestamp.date() >= session_date or timestamp.weekday() >= 5:
            continue
        minute_index = timestamp.hour * 60 + timestamp.minute - 570
        if not 0 <= minute_index < 390:
            continue
        close = _decimal(bar.get("close"))
        volume = _decimal(bar.get("volume"))
        if close is not None and volume is not None:
            grouped.setdefault(timestamp.date(), {})[minute_index] = (volume, close)
    result: dict[tuple[str, int], ImpactBaseline] = {}
    for minute_index in range(386):
        volumes: list[Decimal] = []
        returns: list[Decimal] = []
        for day_values in grouped.values():
            window = [day_values.get(index) for index in range(minute_index, minute_index + 5)]
            if any(value is None for value in window):
                continue
            values = cast(list[tuple[Decimal, Decimal]], window)
            volumes.append(sum((value[0] for value in values), Decimal(0)))
            first, last = values[0][1], values[-1][1]
            if first > 0:
                returns.append((last - first) / first)
        if len(volumes) < 20:
            continue
        mean = sum(returns, Decimal(0)) / Decimal(len(returns)) if returns else Decimal(0)
        variance = (
            sum(((value - mean) ** 2 for value in returns), Decimal(0)) / Decimal(len(returns))
            if returns
            else Decimal(0)
        )
        result[(symbol.upper(), minute_index)] = ImpactBaseline(
            symbol.upper(),
            IMPACT_HORIZON_MINUTES,
            sum(volumes, Decimal(0)) / Decimal(len(volumes)),
            variance.sqrt(),
            session_date,
            len(volumes),
            provenance,
            adjustment_metadata,
        )
    return result
