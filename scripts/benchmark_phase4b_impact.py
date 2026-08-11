"""Hermetic SHADOW_IMPACT_V1 overhead benchmark.

The workload is deterministic and models persisted three-trade sweep clusters.
The shared measurement calls the existing SweepCluster.recompute feature path;
the combined measurement calls that path followed by impact scoring.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from statistics import median
from time import perf_counter_ns
from uuid import NAMESPACE_URL, uuid5

from institutional_signal_engine.impact import ImpactBaseline, evaluate_cluster
from institutional_signal_engine.schemas import CanonicalEvent, EventKind
from institutional_signal_engine.sweeps import SweepCluster, SweepKey

WORKLOAD_CLUSTERS = 256
CONSTITUENTS_PER_CLUSTER = 3
WARMUP_SAMPLES = 3
MEASURED_SAMPLES = 15
NOW = datetime(2026, 8, 7, 14, 0, tzinfo=UTC)


def workload() -> tuple[tuple[SweepCluster, dict[str, object]], ...]:
    rows: list[tuple[SweepCluster, dict[str, object]]] = []
    for index in range(WORKLOAD_CLUSTERS):
        run_id = uuid5(NAMESPACE_URL, f"benchmark-run:{index}")
        cluster_id = uuid5(NAMESPACE_URL, f"benchmark-cluster:{index}")
        key = SweepKey("BAC", 20260814, 40000 + index, "C")
        cluster = SweepCluster(cluster_id, run_id, key, NOW, NOW + timedelta(seconds=1))
        for offset, exchange in enumerate(("5", "31", "43")):
            event_id = uuid5(NAMESPACE_URL, f"benchmark-event:{index}:{offset}")
            cluster.trades[event_id] = CanonicalEvent(
                event_id=event_id,
                run_id=run_id,
                kind=EventKind.OPTIONS,
                symbol="BAC",
                source="fixture",
                source_timestamp=NOW + timedelta(milliseconds=offset),
                received_timestamp=NOW,
                normalized_timestamp=NOW,
                sequence=offset,
                payload={
                    "contract": {
                        "root": "BAC",
                        "expiration": 20260814,
                        "strike": 40000 + index,
                        "right": "C",
                    },
                    "trade_price": Decimal(100),
                    "trade_size": 4,
                    "trade_classification": "ask",
                    "delta": Decimal("0.5"),
                    "delta_provenance": "ALPACA_PROVIDER_DELTA_V1",
                    "exchange": exchange,
                    "eligible_trade": True,
                },
            )
        audit = cluster.recompute()
        rows.append((cluster, audit))
    return tuple(rows)


def shared(rows: tuple[tuple[SweepCluster, dict[str, object]], ...]) -> None:
    for cluster, _ in rows:
        cluster.recompute()


def combined(
    rows: tuple[tuple[SweepCluster, dict[str, object]], ...], baseline: ImpactBaseline
) -> None:
    for cluster, audit in rows:
        cluster.recompute()
        evaluate_cluster(audit, tuple(event.payload for event in cluster.trades.values()), baseline)


def stats(values: list[float]) -> dict[str, float]:
    ordered = sorted(values)

    def percentile(fraction: float) -> float:
        position = (len(ordered) - 1) * fraction
        lower = int(position)
        upper = min(lower + 1, len(ordered) - 1)
        return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)

    return {
        "p50_ms": median(ordered),
        "p95_ms": percentile(0.95),
        "maximum_ms": max(ordered),
    }


def measure(function: object, *args: object) -> list[float]:
    callable_function = function  # type: ignore[assignment]
    for _ in range(WARMUP_SAMPLES):
        callable_function(*args)  # type: ignore[operator]
    values: list[float] = []
    for _ in range(MEASURED_SAMPLES):
        started = perf_counter_ns()
        callable_function(*args)  # type: ignore[operator]
        values.append((perf_counter_ns() - started) / 1_000_000)
    return values


def main() -> None:
    rows = workload()
    baseline = ImpactBaseline(
        "BAC", 5, Decimal(19200), Decimal("0.01"), NOW.date(), 20, "fixture", "split"
    )
    shared_values = measure(shared, rows)
    combined_values = measure(combined, rows, baseline)
    incremental_values = [
        combined - shared for combined, shared in zip(combined_values, shared_values, strict=True)
    ]
    shared_summary = stats(shared_values)
    combined_summary = stats(combined_values)
    incremental_summary = stats(incremental_values)
    shared_p50 = shared_summary["p50_ms"]
    print(
        json.dumps(
            {
                "environment": "local hermetic Python 3.12 benchmark",
                "workload_clusters": WORKLOAD_CLUSTERS,
                "constituents_per_cluster": CONSTITUENTS_PER_CLUSTER,
                "warmup_samples": WARMUP_SAMPLES,
                "measured_samples": MEASURED_SAMPLES,
                "shared_control_feature_processing_ms": shared_summary,
                "incremental_shadow_scoring_ms": incremental_summary,
                "combined_processing_ms": combined_summary,
                "overhead_percent_p50": 100 * incremental_summary["p50_ms"] / shared_p50,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
