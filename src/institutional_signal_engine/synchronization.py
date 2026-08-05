"""Deterministic event synchronization with lateness and staleness policy."""

from datetime import UTC, datetime, timedelta

from .schemas import CanonicalEvent, EventKind, SynchronizedInput


class Synchronizer:
    def __init__(
        self,
        max_lateness: timedelta = timedelta(seconds=5),
        max_staleness: timedelta = timedelta(seconds=30),
    ) -> None:
        self.max_lateness = max_lateness
        self.max_staleness = max_staleness
        self._events: dict[tuple[str, EventKind], CanonicalEvent] = {}
        self._last_sequence: dict[tuple[str, str, EventKind], int] = {}

    def reset_session(self) -> None:
        """Drop prior-session synchronized state exactly once at session open."""
        self._events.clear()
        self._last_sequence.clear()

    def add(self, event: CanonicalEvent) -> bool:
        sequence_key = (event.source, event.symbol, event.kind)
        previous = self._last_sequence.get(sequence_key, -1)
        if event.sequence <= previous:
            return False
        self._last_sequence[sequence_key] = event.sequence
        key = (event.symbol, event.kind)
        current = self._events.get(key)
        if current is None or event.normalized_timestamp >= current.normalized_timestamp:
            self._events[key] = event
            return True
        return False

    def snapshot(
        self, symbol: str, as_of: datetime, positions: int = 0, allow_stale: bool = False
    ) -> SynchronizedInput | None:
        as_of = as_of.astimezone(UTC)
        required = {kind: self._events.get((symbol, kind)) for kind in EventKind}
        if any(event is None for event in required.values()):
            return None
        events = tuple(event for event in required.values() if event is not None)
        assert len(events) == len(EventKind)
        if not allow_stale and any(
            as_of - event.normalized_timestamp > self.max_staleness for event in events
        ):
            return None
        equity = next(event for event in events if event.kind == EventKind.EQUITY).payload
        options = next(event for event in events if event.kind == EventKind.OPTIONS).payload
        market = next(event for event in events if event.kind == EventKind.MARKET_INDEX).payload
        sector = next(event for event in events if event.kind == EventKind.SECTOR_INDEX).payload
        return SynchronizedInput(
            symbol=symbol,
            as_of=as_of,
            price=equity.get("price"),
            volume=equity.get("volume"),
            spread=equity.get("spread"),
            option_volume=options.get("option_volume"),
            open_interest=options.get("open_interest"),
            call_premium=options.get("call_premium"),
            distance_to_resistance=equity.get("distance_to_resistance"),
            resistance_state=equity.get("resistance_state"),
            relative_volume=equity.get("relative_volume"),
            equity_delta=equity.get("delta"),
            market_delta=market.get("delta"),
            sector_delta=sector.get("delta"),
            relative_strength_vs_spy=equity.get("relative_strength_vs_spy"),
            relative_strength_vs_sector=equity.get("relative_strength_vs_sector"),
            first_signal_at=equity.get("first_signal_at"),
            concurrent_positions=positions,
            event_ids=tuple(event.event_id for event in events),
            ask_side_percentage=options.get("ask_side_percentage"),
            quote_validity=options.get("quote_validity"),
            qualifying_sweep_count=int(options.get("qualifying_sweep_count", 0)),
            qualifying_sweep_premium=options.get("qualifying_sweep_premium", 0),
            session_sweep_gate=bool(options.get("session_sweep_gate", False)),
            most_recent_qualifying_sweep_timestamp=options.get(
                "most_recent_qualifying_sweep_timestamp"
            ),
            provenance={
                f"{name}:{key}": value
                for name, payload in (
                    ("equity", equity),
                    ("market", market),
                    ("sector", sector),
                )
                for key, value in payload.get("indicator_provenance", {}).items()
            },
            indicator_reasons=tuple(
                sorted(
                    set(
                        equity.get("feature_reasons", ())
                        + options.get("feature_reasons", ())
                        + market.get("feature_reasons", ())
                        + sector.get("feature_reasons", ())
                    )
                )
            ),
        )
