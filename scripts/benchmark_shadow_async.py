"""501-cluster benchmark for the asynchronous SHADOW handoff."""

from __future__ import annotations

import asyncio
import json
from datetime import date
from decimal import Decimal
from statistics import median
from time import perf_counter_ns
from uuid import UUID, uuid4

import benchmark_phase4b_impact

from institutional_signal_engine.impact import ImpactBaseline, ShadowImpactEngine, evaluate_cluster
from institutional_signal_engine.shadow_async import AsyncShadowWorker

MEASURED_SAMPLES = 9


def stats(values: list[float]) -> dict[str, float]:
    ordered = sorted(values)
    return {
        "p50_ms": median(ordered),
        "p95_ms": ordered[min(len(ordered) - 1, int(len(ordered) * 0.95))],
        "maximum_ms": max(ordered),
    }


def prepared_rows() -> tuple[tuple[object, dict[str, object], dict[str, object]], ...]:
    benchmark_phase4b_impact.WORKLOAD_CLUSTERS = 501
    rows = benchmark_phase4b_impact.workload()
    run_id = str(uuid4())
    prepared: list[tuple[object, dict[str, object], dict[str, object]]] = []
    for cluster, audit in rows:
        normalized = dict(audit)
        normalized["run_id"] = run_id
        payloads = {
            str(event_id): dict(event.payload) for event_id, event in cluster.trades.items()
        }
        prepared.append((cluster, normalized, payloads))
    return tuple(prepared)


def control_only(rows: tuple[tuple[object, dict[str, object], dict[str, object]], ...]) -> None:
    for cluster, _, _ in rows:
        cluster.recompute()


def synchronous_shadow(
    rows: tuple[tuple[object, dict[str, object], dict[str, object]], ...],
    baseline: ImpactBaseline,
) -> None:
    for cluster, _, payloads in rows:
        audit = cluster.recompute()
        evaluate_cluster(audit, tuple(payloads.values()), baseline)


async def async_handoff(
    rows: tuple[tuple[object, dict[str, object], dict[str, object]], ...],
    baseline: ImpactBaseline,
) -> dict[str, object]:
    run_id = UUID(str(rows[0][1]["run_id"]))
    results = 0

    async def sink(_item: object, _result: dict[str, object], _session: dict[str, object]) -> None:
        nonlocal results
        results += 1

    worker = AsyncShadowWorker(
        ShadowImpactEngine(run_id, {"BAC": baseline}), sink, maxsize=len(rows) + 10
    )
    worker.start()
    accepted = 0
    total_started = perf_counter_ns()
    enqueue_ns = 0
    for cluster, _, payloads in rows:
        audit = cluster.recompute()
        enqueue_started = perf_counter_ns()
        accepted += int(
            worker.enqueue_snapshot(run_id, audit, payloads, baseline, benchmark_phase4b_impact.NOW)
        )
        enqueue_ns += perf_counter_ns() - enqueue_started
    enqueue_ms = enqueue_ns / 1_000_000
    total_ms = (perf_counter_ns() - total_started) / 1_000_000
    await worker.drain()
    return {
        "enqueue_ms": enqueue_ms,
        "total_control_plus_enqueue_ms": total_ms,
        "completed": results,
        "dropped": worker.metrics.rejected_full,
        "max_queue_depth": worker.metrics.max_depth,
        "queue_wait_ms": stats(worker.metrics.queue_wait_ms),
        "worker_result_latency_ms": stats(worker.metrics.result_latency_ms),
    }


async def main() -> None:
    rows = prepared_rows()
    baseline = ImpactBaseline(
        "BAC", 5, Decimal(19200), Decimal("0.01"), date(2026, 8, 7), 20, "fixture", "split"
    )
    control_values: list[float] = []
    sync_values: list[float] = []
    async_values: list[float] = []
    async_enqueue_values: list[float] = []
    async_details: dict[str, object] = {}
    for _ in range(MEASURED_SAMPLES):
        started = perf_counter_ns()
        control_only(rows)
        control_values.append((perf_counter_ns() - started) / 1_000_000)
        started = perf_counter_ns()
        synchronous_shadow(rows, baseline)
        sync_values.append((perf_counter_ns() - started) / 1_000_000)
        async_details = await async_handoff(rows, baseline)
        async_values.append(float(async_details["total_control_plus_enqueue_ms"]))
        async_enqueue_values.append(float(async_details["enqueue_ms"]))
    control = stats(control_values)
    sync = stats(sync_values)
    handoff = stats(async_values)
    enqueue = stats(async_enqueue_values)
    print(
        json.dumps(
            {
                "environment": "local hermetic Python benchmark",
                "workload_clusters": len(rows),
                "constituents_per_cluster": 3,
                "measured_samples": MEASURED_SAMPLES,
                "control_only_ms": control,
                "synchronous_shadow_total_ms": sync,
                "synchronous_incremental_ms": {
                    "p50_ms": sync["p50_ms"] - control["p50_ms"],
                },
                "async_total_control_plus_enqueue_ms": handoff,
                "async_enqueue_critical_path_ms": enqueue,
                "async_enqueue_over_control_percent": 100 * enqueue["p50_ms"] / control["p50_ms"]
                if control["p50_ms"]
                else None,
                "last_async_run": async_details,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
