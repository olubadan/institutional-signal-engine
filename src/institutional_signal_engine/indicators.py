"""Deterministic, Decimal-based stateful market indicators.

Only formulas explicitly specified by the owner are implemented here. Missing
historical, open-interest, or resistance inputs remain missing and are reported
as reason codes; zero is never used as a valid substitute.
"""

from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, time
from decimal import Decimal
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
REGULAR_OPEN = time(9, 30)
REGULAR_CLOSE = time(16, 0)


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

    def completed_session_highs(self, symbol: str, session: str) -> Mapping[str, Decimal]:
        """Return split-adjusted highs from completed sessions before ``session``."""
        del symbol, session
        return {}

    def adjustment_metadata(self, symbol: str, session: str) -> str:
        del symbol, session
        return "missing"

    def provenance(self, symbol: str, session: str) -> str:
        del symbol, session
        return "missing"


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


@dataclass(frozen=True)
class OptionContractIdentity:
    root: str
    expiration: int
    strike: int
    right: str


@dataclass(frozen=True)
class OptionQuoteContext:
    bid: Decimal
    ask: Decimal
    timestamp: datetime
    contract: OptionContractIdentity


@dataclass(frozen=True)
class OptionTradeContext:
    price: Decimal
    timestamp: datetime
    contract: OptionContractIdentity


def regular_session(timestamp: datetime) -> bool:
    local = timestamp.astimezone(ET)
    return REGULAR_OPEN <= local.time() <= REGULAR_CLOSE


def session_phase(timestamp: datetime) -> str:
    """Return the deterministic ET phase used by boundary handling."""
    local = timestamp.astimezone(ET)
    if local.weekday() >= 5:
        return "AFTER_HOURS"
    if local.time() < REGULAR_OPEN:
        return "PREMARKET"
    if local.time() < REGULAR_CLOSE:
        return "REGULAR"
    return "AFTER_HOURS"


def classify_ask_side(trade: OptionTradeContext, quote: OptionQuoteContext | None) -> str:
    """Apply the owner-defined quote validity and ask-side boundary exactly."""
    if quote is None:
        return "unknown"
    trade_time = trade.timestamp.astimezone(UTC)
    quote_time = quote.timestamp.astimezone(UTC)
    valid = (
        quote.bid > 0
        and quote.ask > 0
        and quote.ask >= quote.bid
        and quote_time <= trade_time
        and (trade_time - quote_time).total_seconds() <= Decimal("0.5")
        and quote.contract == trade.contract
    )
    if not valid:
        return "unknown"
    midpoint = (quote.bid + quote.ask) / Decimal(2)
    return (
        "ask"
        if trade.price >= quote.ask - Decimal("0.01") and trade.price > midpoint
        else "unknown"
    )


@dataclass(frozen=True)
class ResistanceLevel:
    state: str
    level: Decimal | None
    distance: Decimal | None
    source_sessions: tuple[str, ...]
    adjustment_metadata: str
    provenance: str


class ResistanceCache:
    def __init__(self) -> None:
        self._levels: dict[tuple[str, str], list[tuple[int, Decimal, tuple[str, ...]]]] = {}
        self._metadata: dict[tuple[str, str], tuple[str, str]] = {}
        self._values: dict[tuple[str, str], ResistanceLevel] = {}

    def calculate(
        self,
        symbol: str,
        session: str,
        current_price: Decimal,
        completed_session_highs: dict[str, Decimal],
        adjustment_metadata: str,
        provenance: str,
    ) -> ResistanceLevel:
        key = (symbol, session)
        levels = self._levels.get(key)
        if levels is None:
            ordered = sorted(completed_session_highs.items(), reverse=True)
            levels = []
            for count in (5, 20, 252):
                window = ordered[:count]
                if len(window) >= count:
                    levels.append(
                        (count, max(value for _, value in window), tuple(day for day, _ in window))
                    )
            self._levels[key] = levels
            self._metadata[key] = (adjustment_metadata, provenance)
        cached_adjustment, cached_provenance = self._metadata[key]
        selected = next((item for item in levels if item[1] > current_price), None)
        if selected is None:
            result = ResistanceLevel(
                "NO_OVERHEAD_RESISTANCE", None, None, (), cached_adjustment, cached_provenance
            )
        else:
            result = ResistanceLevel(
                "OVERHEAD_RESISTANCE",
                selected[1],
                (selected[1] - current_price) / current_price,
                selected[2],
                cached_adjustment,
                cached_provenance,
            )
        self._values[key] = result
        return result


class IndicatorCalculator:
    def __init__(self, historical: HistoricalMarketDataPort | None = None) -> None:
        self.historical = historical
        self.states: dict[str, IndicatorState] = defaultdict(IndicatorState)
        self.resistance = ResistanceCache()

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
        local = timestamp.astimezone(ET)
        minute = (local.hour * 60 + local.minute) - 570
        eligible = (eligible_equity_trade(conditions) if conditions else True) and regular_session(
            timestamp
        )
        if eligible:
            state.cumulative_volume += volume
            state.cumulative_notional += price * Decimal(volume)
            state.minute_volume[minute] += volume
        baseline: tuple[Decimal, ...] = ()
        previous: PreviousClose | None = None
        completed_highs: Mapping[str, Decimal] = {}
        adjustment_metadata = "missing"
        history_provenance = "missing"
        if self.historical is not None:
            baseline = self.historical.cumulative_volume_baseline(symbol, minute)
            previous = self.historical.previous_close(symbol, session)
            completed_highs = self.historical.completed_session_highs(symbol, session)
            adjustment_metadata = self.historical.adjustment_metadata(symbol, session)
            history_provenance = self.historical.provenance(symbol, session)
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
        resistance: ResistanceLevel | None = None
        if completed_highs:
            resistance = self.resistance.calculate(
                symbol,
                session,
                price,
                dict(completed_highs),
                adjustment_metadata,
                history_provenance,
            )
        else:
            reasons.append("missing_resistance_baseline")
        return {
            "price": price,
            "volume": state.cumulative_volume,
            "session_vwap": vwap,
            "relative_volume": rvol,
            "previous_close_return": previous_return,
            "delta": previous_return,
            "resistance_state": resistance.state if resistance else None,
            "distance_to_resistance": resistance.distance if resistance else None,
            "resistance_level": resistance.level if resistance else None,
            "resistance_provenance": resistance.provenance if resistance else None,
            "resistance_source_sessions": resistance.source_sessions if resistance else (),
            "reasons": tuple(reasons),
            "provenance": previous.provenance if previous else history_provenance,
            "adjustment_metadata": adjustment_metadata,
            "session": session,
            "measurement_minute": minute,
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
    return OptionEvidence(
        state.cumulative_option_volume,
        open_interest,
        state.ask_premium,
        state.net_call_premium,
        distance_to_resistance,
        tuple(reasons),
    )
