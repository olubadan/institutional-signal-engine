"""Immutable, provenance-carrying historical indicator bootstrap data."""

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from types import MappingProxyType

from .impact import ImpactBaseline
from .indicators import HistoricalMarketDataPort, PreviousClose


@dataclass(frozen=True)
class HistoricalBootstrap(HistoricalMarketDataPort):
    session: str
    previous_closes: Mapping[str, PreviousClose]
    cumulative_profiles: Mapping[str, Mapping[int, tuple[Decimal, ...]]]
    completed_highs: Mapping[str, Mapping[str, Decimal]]
    adjustment: str
    source_provenance: str
    impact_baselines: Mapping[tuple[str, int], ImpactBaseline] = MappingProxyType({})

    def __post_init__(self) -> None:
        object.__setattr__(self, "previous_closes", MappingProxyType(dict(self.previous_closes)))
        object.__setattr__(
            self, "cumulative_profiles", MappingProxyType(dict(self.cumulative_profiles))
        )
        object.__setattr__(self, "completed_highs", MappingProxyType(dict(self.completed_highs)))
        object.__setattr__(self, "impact_baselines", MappingProxyType(dict(self.impact_baselines)))

    def previous_close(self, symbol: str, session: str) -> PreviousClose | None:
        if session != self.session:
            return None
        return self.previous_closes.get(symbol)

    def cumulative_volume_baseline(self, symbol: str, minute: int) -> tuple[Decimal, ...]:
        return self.cumulative_profiles.get(symbol, {}).get(minute, ())

    def completed_session_highs(self, symbol: str, session: str) -> Mapping[str, Decimal]:
        if session != self.session:
            return {}
        return self.completed_highs.get(symbol, {})

    def adjustment_metadata(self, symbol: str, session: str) -> str:
        return (
            self.adjustment
            if session == self.session and symbol in self.previous_closes
            else "missing"
        )

    def provenance(self, symbol: str, session: str) -> str:
        return (
            self.source_provenance
            if session == self.session and symbol in self.previous_closes
            else "missing"
        )
