"""Provider-free benchmark for scientific evidence callback overhead."""

from __future__ import annotations

import asyncio
import json
import statistics
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import NAMESPACE_URL, UUID, uuid5

from institutional_signal_engine.config import Settings
from institutional_signal_engine.journal import Journal
from institutional_signal_engine.mathematical_pipeline import MathematicalPipeline
from institutional_signal_engine.persistence import InMemoryRepository
from institutional_signal_engine.schemas import CanonicalEvent, EventKind

RUN_ID = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
NOW = datetime(2026, 8, 13, 14, 31, tzinfo=UTC)
CONTRACT = {"root": "AAPL", "expiration": 20260821, "strike": 310000, "right": "C"}


def build_events() -> tuple[CanonicalEvent, ...]:
    def make(
        kind: EventKind, sequence: int, symbol: str, payload: dict[str, object]
    ) -> CanonicalEvent:
        timestamp = NOW + timedelta(milliseconds=sequence)
        return CanonicalEvent(
            event_id=uuid5(NAMESPACE_URL, f"benchmark:{kind.value}:{symbol}:{sequence}"),
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
        make(EventKind.EQUITY, 1, "AAPL", shared),
        make(
            EventKind.MARKET_INDEX, 2, "SPY", {"_calculated_indicators": True, "delta": Decimal(1)}
        ),
        make(
            EventKind.SECTOR_INDEX, 3, "XLK", {"_calculated_indicators": True, "delta": Decimal(1)}
        ),
    ]
    for sequence, exchange in enumerate(("5", "31", "43"), 4):
        events.append(
            make(
                EventKind.OPTIONS,
                sequence,
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
                    "option_volume": sequence * 5,
                    "open_interest": 100,
                    "call_premium": Decimal(sequence * 50000),
                    "delta": Decimal(1),
                    "delta_provenance": "DETERMINISTIC_OPTION_DELTA_V1",
                },
            )
        )
    return tuple(events)


async def run(with_evidence: bool, repetitions: int = 20) -> float:
    durations: list[float] = []
    for _ in range(repetitions):
        journal = Journal(RUN_ID)
        pending: list[dict[str, object]] = []

        def sink(
            kind: str,
            payload: dict[str, object],
            pending: list[dict[str, object]] = pending,
        ) -> None:
            pending.append({"kind": kind, "payload": payload})

        def flush(
            event: CanonicalEvent,
            pending: list[dict[str, object]] = pending,
            journal: Journal = journal,
        ) -> None:
            journal.append(
                "event.accepted",
                timestamp=NOW,
                event_id=str(event.event_id),
                scientific_evidence=list(pending) if with_evidence else [],
            )
            pending.clear()

        pipeline = MathematicalPipeline(
            Settings(),
            InMemoryRepository(),
            RUN_ID,
            ("AAPL",),
            baselines={},
            now=lambda: NOW,
            async_shadow=True,
            evidence_sink=sink if with_evidence else None,
        )
        started = time.perf_counter()
        for event in build_events():
            pipeline.process_accepted(event)
            flush(event)
        await pipeline.drain_shadow()
        if pending:
            journal.append(
                "scientific.evidence_batch",
                timestamp=NOW,
                scientific_evidence={"records": list(pending)},
            )
        durations.append(time.perf_counter() - started)
    return statistics.median(durations)


async def main() -> None:
    baseline = await run(False)
    evidence = await run(True)
    result = {
        "schema_version": "SCIENTIFIC_EVIDENCE_OVERHEAD_BENCHMARK_V1",
        "workload": "provider-free canonical assembled option/equity path",
        "repetitions": 20,
        "baseline_seconds_median": baseline,
        "evidence_seconds_median": evidence,
        "incremental_seconds": evidence - baseline,
        "incremental_percent": ((evidence - baseline) / baseline * 100) if baseline else None,
        "semantics_changed": False,
        "live_provider_contact": False,
        "orders_constructed": 0,
        "orders_submitted": 0,
    }
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    asyncio.run(main())
