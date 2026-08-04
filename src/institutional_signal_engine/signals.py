"""Pure implementation of the formal S/F/R/E model and ranking rule."""

from decimal import Decimal

from .config import Settings
from .schemas import Candidate, Decision, GateResult, SynchronizedInput


def _gate(name: str, passed: bool, reason: str) -> GateResult:
    return GateResult(name=name, passed=passed, reason="passed" if passed else reason)


def evaluate(item: SynchronizedInput, settings: Settings, ordinal: int = 0) -> Candidate:
    t = settings.thresholds
    ratio = (
        (Decimal(item.option_volume) / Decimal(item.open_interest))
        if item.open_interest
        else Decimal(0)
    )
    liquidity = item.volume >= t.minimum_volume and item.spread <= t.maximum_spread
    options = (
        item.call_premium >= t.minimum_call_premium and ratio >= t.minimum_option_volume_oi_ratio
    )
    equity = item.equity_delta > 0
    market = item.market_delta > 0
    sector = item.sector_delta > 0
    signal = liquidity and options and equity and market and sector
    if item.first_signal_at is None:
        freshness_score = Decimal(0)
        freshness = False
    else:
        elapsed = max(Decimal(0), Decimal((item.as_of - item.first_signal_at).total_seconds()))
        freshness_score = max(Decimal(0), Decimal(1) - elapsed / Decimal(t.freshness_seconds))
        freshness = freshness_score >= t.minimum_decay and options and equity
    room = (
        item.distance_to_resistance >= t.minimum_room
        and item.volume >= t.minimum_volume
        and item.spread <= t.maximum_spread
        and item.concurrent_positions < settings.capacity
    )
    gates = (
        _gate("liquidity", liquidity, "volume_or_spread_threshold"),
        _gate("options", options, "premium_or_volume_oi_threshold"),
        _gate("equity", equity, "equity_delta_not_positive"),
        _gate("market", market, "market_delta_not_positive"),
        _gate("sector", sector, "sector_delta_not_positive"),
        _gate("S", signal, "signal_validity_failed"),
        _gate("F", freshness, "freshness_failed"),
        _gate("R", room, "room_or_capacity_failed"),
        _gate("E", signal and freshness and room, "executability_failed"),
    )
    return Candidate(
        symbol=item.symbol,
        freshness_score=freshness_score,
        call_premium=item.call_premium,
        option_volume_oi_ratio=ratio,
        relative_volume=item.relative_volume,
        distance_to_resistance=item.distance_to_resistance,
        ordinal=ordinal,
        gates=gates,
    )


def rank(candidates: list[Candidate]) -> list[Candidate]:
    return sorted(
        candidates,
        key=lambda c: (
            -c.freshness_score,
            -c.call_premium,
            -c.option_volume_oi_ratio,
            -c.relative_volume,
            -c.distance_to_resistance,
            c.ordinal,
            c.symbol,
        ),
    )


def decide(items: list[SynchronizedInput], settings: Settings) -> Decision:
    evaluated = [evaluate(item, settings, ordinal) for ordinal, item in enumerate(items)]
    ranked = rank(evaluated)
    executable = [
        candidate for candidate in ranked if all(g.passed for g in candidate.gates if g.name == "E")
    ]
    reasons = tuple(
        f"{candidate.symbol}:{gate.name}:{gate.reason}"
        for candidate in ranked
        for gate in candidate.gates
        if not gate.passed
    )
    event_ids = tuple(event_id for item in items for event_id in item.event_ids)
    timestamp = max(
        (item.as_of for item in items),
        default=__import__("datetime").datetime.now(__import__("datetime").timezone.utc),
    )
    return Decision(
        decision_id=Decision.deterministic_id(event_ids, timestamp),
        decided_at=timestamp,
        selected_symbol=executable[0].symbol if executable else None,
        fire=bool(executable),
        candidates=tuple(ranked),
        rejection_reasons=reasons,
        input_event_ids=event_ids,
        config_version=settings.config_version,
        engine_version=settings.engine_version,
    )
