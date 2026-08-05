"""Deterministic, Decimal-based stateful market indicators.

Only formulas explicitly specified by the owner are implemented here. Missing
historical, open-interest, or resistance inputs remain missing and are reported
as reason codes; zero is never used as a valid substitute.
"""

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class HistoricalBar:
    symbol: str
    timestamp: datetime
    close: Decimal
    volume: int
    provenance: str


@dataclass(frozen=True)
class PreviousClose:
    symbol: str
    close: Decimal
    as_of: datetime
    provenance: str


class HistoricalMarketDataPort:
    """Typed port for previous close and prior-session cumulative volumes."""

    def previous_close(self, symbol: str, session: str) -> PreviousClose | None:
        raise NotImplementedError

    def cumulative_volume_baseline(self, symbol: str, minute: int) -> tuple[Decimal, ...]:
        raise NotImplementedError


@dataclass
class IndicatorState:
    session: str | None = None
    cumulative_volume: int = 0
    cumulative_notional: Decimal = Decimal(0)
    cumulative_option_volume: int = 0
    ask_premium: Decimal = Decimal(0)
    net_call_premium: Decimal = Decimal(0)
    previous_close: PreviousClose | None = None
    minute_volume: dict[int, int] = field(default_factory=lambda: defaultdict(int))

    def reset(self, session: str) -> None:
        if self.session != session:
            self.session = session
            self.cumulative_volume = 0
            self.cumulative_notional = Decimal(0)
            self.cumulative_option_volume = 0
            self.ask_premium = Decimal(0)
            self.net_call_premium = Decimal(0)
            self.minute_volume.clear()


def session_key(timestamp: datetime) -> str:
    local = timestamp.astimezone(ET)
    return local.date().isoformat()


def eligible_equity_trade(conditions: Iterable[str]) -> bool:
    """Use the provider's regular-sale marker and reject correction/cancel tags."""
    tags = {str(value).upper() for value in conditions}
    return "@" in tags and not tags.intersection({"C", "E", "Z"})


def classify_option_trade(price: Decimal, bid: Decimal | None, ask: Decimal | None) -> str:
    """Classify from the contemporaneous quote: ask, bid, or unknown."""
    if ask is not None and price >= ask:
        return "ask"
    if bid is not None and price <= bid:
        return "bid"
    return "unknown"


class IndicatorCalculator:
    def __init__(self, historical: HistoricalMarketDataPort | None = None) -> None:
        self.historical = historical
        self.states: dict[str, IndicatorState] = defaultdict(IndicatorState)

    def equity(
        self,
        symbol: str,
        timestamp: datetime,
        price: Decimal,
        volume: int,
        conditions: Iterable[str] = (),
    ) -> dict[str, object]:
        state = self.states[symbol]
        session = session_key(timestamp)
        state.reset(session)
        eligible = eligible_equity_trade(conditions) if conditions else True
        if eligible:
            state.cumulative_volume += volume
            state.cumulative_notional += price * Decimal(volume)
            minute = (timestamp.astimezone(ET).hour * 60 + timestamp.astimezone(ET).minute) - 570
            state.minute_volume[minute] += volume
        baseline = (
            ()
            if self.historical is None
            else self.historical.cumulative_volume_baseline(symbol, minute)
        )
        previous = (
            None if self.historical is None else self.historical.previous_close(symbol, session)
        )
        state.previous_close = previous
        reasons: list[str] = []
        if previous is None or previous.close == 0:
            reasons.append("missing_previous_close")
            previous_return: Decimal | None = None
        else:
            previous_return = (price - previous.close) / previous.close
        average = sum(baseline, Decimal(0)) / Decimal(len(baseline)) if baseline else None
        current_cumulative = sum(state.minute_volume.get(index, 0) for index in range(minute + 1))
        rvol = None
        if average is not None and average != Decimal(0):
            rvol = Decimal(current_cumulative) / average
        if rvol is None:
            reasons.append("missing_rvol_baseline")
        vwap = (
            state.cumulative_notional / Decimal(state.cumulative_volume)
            if state.cumulative_volume
            else None
        )
        if vwap is None:
            reasons.append("missing_session_volume")
        return {
            "price": price,
            "volume": state.cumulative_volume,
            "session_vwap": vwap,
            "relative_volume": rvol,
            "previous_close_return": previous_return,
            "reasons": tuple(reasons),
            "provenance": previous.provenance if previous else "missing",
        }

    @staticmethod
    def relative_strength(
        symbol_return: Decimal | None, benchmark_return: Decimal | None
    ) -> Decimal | None:
        return (
            None
            if symbol_return is None or benchmark_return is None
            else symbol_return - benchmark_return
        )


@dataclass(frozen=True)
class OptionEvidence:
    option_volume: int | None
    open_interest: int | None
    ask_premium: Decimal | None
    net_call_premium: Decimal | None
    distance_to_resistance: Decimal | None
    reasons: tuple[str, ...] = ()


def option_evidence(
    state: IndicatorState, open_interest: int | None, distance_to_resistance: Decimal | None
) -> OptionEvidence:
    reasons: list[str] = []
    if open_interest is None or open_interest <= 0:
        reasons.append("missing_or_zero_open_interest")
    if distance_to_resistance is None:
        reasons.append("missing_resistance_distance")
    return OptionEvidence(
        state.cumulative_option_volume,
        open_interest,
        state.ask_premium,
        state.net_call_premium,
        distance_to_resistance,
        tuple(reasons),
    )
