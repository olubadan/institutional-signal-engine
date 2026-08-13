from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid5

from institutional_signal_engine.config import Settings
from institutional_signal_engine.mathematical_pipeline import MathematicalPipeline, replay_semantics
from institutional_signal_engine.persistence import InMemoryRepository
from institutional_signal_engine.schemas import CanonicalEvent, EventKind

NOW = datetime(2026, 8, 13, 14, 31, tzinfo=UTC)
RUN_ID = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
CONTRACT = {"root": "AAPL", "expiration": 20260821, "strike": 310000, "right": "C"}


def event(
    kind: EventKind, sequence: int, symbol: str, payload: dict[str, object]
) -> CanonicalEvent:
    timestamp = NOW + timedelta(milliseconds=sequence)
    return CanonicalEvent(
        event_id=uuid5(NAMESPACE_URL, f"assembled:{kind.value}:{symbol}:{sequence}"),
        run_id=RUN_ID,
        kind=kind,
        symbol=symbol,
        source="thetadata-hermetic" if kind is EventKind.OPTIONS else "alpaca-hermetic",
        source_timestamp=timestamp,
        received_timestamp=timestamp,
        normalized_timestamp=timestamp,
        sequence=sequence,
        payload=payload,
    )


def build_events() -> tuple[CanonicalEvent, ...]:
    shared = {
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
    }
    events = [
        event(EventKind.EQUITY, 1, "AAPL", shared),
        event(
            EventKind.MARKET_INDEX, 2, "SPY", {"_calculated_indicators": True, "delta": Decimal(1)}
        ),
        event(
            EventKind.SECTOR_INDEX, 3, "XLK", {"_calculated_indicators": True, "delta": Decimal(1)}
        ),
    ]
    for index, exchange in enumerate(("5", "31", "43"), 4):
        events.append(
            event(
                EventKind.OPTIONS,
                index,
                "AAPL",
                {
                    "_state_enriched": True,
                    "provider_event_kind": "trade",
                    "eligible_trade": True,
                    "condition_mapping_version": "thetadata-opra-conditions-v1",
                    "contract": CONTRACT,
                    "trade_price": Decimal(100),
                    "trade_size": 5,
                    "exchange": exchange,
                    "trade_classification": "ask",
                    "option_volume": index * 5,
                    "open_interest": 100,
                    "call_premium": Decimal(index * 50000),
                    "delta": Decimal(1),
                    "delta_provenance": "DETERMINISTIC_OPTION_DELTA_V1",
                },
            )
        )
    return tuple(events)


def test_assembled_path_and_fresh_replay(tmp_path: Path) -> None:
    settings = Settings()
    repository = InMemoryRepository()
    pipeline = MathematicalPipeline(
        settings, repository, RUN_ID, ("AAPL",), baselines={}, now=lambda: NOW
    )
    events = build_events()
    for item in events:
        pipeline.process_accepted(item)
    live = pipeline.snapshot()
    replay = replay_semantics(events, settings, ("AAPL",), RUN_ID, baselines={})
    assert live["accepted_event_ids"] == replay["accepted_event_ids"]
    assert live["decisions"] == replay["decisions"]
    assert live["feature_vectors"] == replay["feature_vectors"]
    assert live["comparisons"] == replay["comparisons"]
    assert live["control_evaluations"] > 0
    assert live["shadow_evaluations"] > 0
    assert live["feature_vectors"]
    assert live["comparisons"]
    assert live["shadow_live_scoring_enabled"] is False
    assert live["orders_constructed"] == live["orders_submitted"] == 0
    assert repository.sweeps
    assert repository.shared_feature_vectors
    assert repository.model_comparisons
