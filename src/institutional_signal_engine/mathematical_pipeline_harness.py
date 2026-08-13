"""Provider-free execution harness for the assembled mathematical path."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import cast
from uuid import NAMESPACE_URL, UUID, uuid5

from .config import Settings
from .mathematical_pipeline import MathematicalPipeline, replay_semantics
from .persistence import InMemoryRepository
from .schemas import CanonicalEvent, EventKind

RUN_ID = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
NOW = datetime(2026, 8, 13, 14, 31, tzinfo=UTC)
CONTRACT = {"root": "AAPL", "expiration": 20260821, "strike": 310000, "right": "C"}


def _event(
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


def fixture_events() -> tuple[CanonicalEvent, ...]:
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
        _event(EventKind.EQUITY, 1, "AAPL", shared),
        _event(
            EventKind.MARKET_INDEX, 2, "SPY", {"_calculated_indicators": True, "delta": Decimal(1)}
        ),
        _event(
            EventKind.SECTOR_INDEX, 3, "XLK", {"_calculated_indicators": True, "delta": Decimal(1)}
        ),
    ]
    for index, exchange in enumerate(("5", "31", "43"), 4):
        events.append(
            _event(
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


def run(output: Path) -> dict[str, object]:
    if output.exists():
        raise RuntimeError("OUTPUT_EXISTS")
    output.mkdir(mode=0o700, parents=True)
    settings = Settings()
    repository = InMemoryRepository()
    events = fixture_events()
    pipeline = MathematicalPipeline(
        settings, repository, RUN_ID, ("AAPL",), baselines={}, now=lambda: NOW
    )
    rejected = events[0].model_copy(
        update={"event_id": uuid5(NAMESPACE_URL, "assembled:rejected-before-activation")}
    )
    pipeline.record_rejected(rejected, "not_active_acknowledged")
    for event in events:
        pipeline.process_accepted(event)
    live = pipeline.snapshot()
    replayed = replay_semantics(events, settings, ("AAPL",), RUN_ID, baselines={})
    fields = ("decisions", "control_evaluation_records", "feature_vectors", "comparisons")
    replay_equal = all(live[field] == replayed[field] for field in fields)
    vectors = cast(list[dict[str, object]], live["feature_vectors"])
    comparisons = cast(list[dict[str, object]], live["comparisons"])
    control_count = int(cast(int, live["control_evaluations"]))
    shadow_count = int(cast(int, live["shadow_evaluations"]))
    artifacts: dict[str, object] = {
        "EVENTS.json": [event.model_dump(mode="json") for event in events],
        "ADMISSION.json": {
            "accepted_event_ids": live["accepted_event_ids"],
            "rejected_event_ids": live["rejected_event_ids"],
            "rejection_reason": "not_active_acknowledged",
        },
        "SWEEP_CLUSTERS.json": repository.sweeps,
        "CONTROL_EVALUATIONS.json": live["control_evaluation_records"],
        "SHARED_FEATURE_VECTORS.json": vectors,
        "SHADOW_DECISIONS.json": [item["impact_inputs"] for item in vectors],
        "MODEL_COMPARISON.json": comparisons,
        "REPLAY.json": {"fresh_process": replayed, "exact_semantic_equality": replay_equal},
        "SAFETY.json": {
            "trading_enabled": False,
            "orders_constructed": 0,
            "orders_submitted": 0,
            "shadow_live_scoring_enabled": False,
        },
    }
    for name, value in artifacts.items():
        (output / name).write_text(
            json.dumps(value, default=str, sort_keys=True, indent=2) + "\n", encoding="utf-8"
        )
    summary: dict[str, object] = {
        "status": "SUCCESS" if replay_equal and control_count and shadow_count else "FAIL",
        "run_id": str(RUN_ID),
        "accepted_events": len(events),
        "control_evaluations": control_count,
        "shadow_evaluations": shadow_count,
        "shared_feature_vectors": len(vectors),
        "comparisons": len(comparisons),
        "fresh_process_replay_equal": replay_equal,
        "trading_enabled": False,
        "orders_constructed": 0,
        "orders_submitted": 0,
        "shadow_live_scoring_enabled": False,
        "artifact_root": str(output),
    }
    (output / "SUMMARY.json").write_text(
        json.dumps(summary, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )
    return summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(run(args.output), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
