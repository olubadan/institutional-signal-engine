"""Replay-safe asynchronous SHADOW_IMPACT_V1 evaluation.

The queue carries an immutable snapshot of a completed cluster.  It never
reads live state while evaluating a work item, so the worker cannot observe
future equity outcomes or later corrections.  Work-item identity is derived
from the complete snapshot and results are idempotent by that identity.
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal
from time import monotonic
from typing import cast
from uuid import UUID

from .impact import ImpactBaseline, ShadowImpactEngine


def _json_value(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


@dataclass(frozen=True, slots=True)
class ShadowWorkItem:
    """Immutable, cluster-time-only input to the SHADOW worker."""

    run_id: UUID
    cluster_id: str
    audit: Mapping[str, object]
    events: tuple[Mapping[str, object], ...]
    baseline: ImpactBaseline | None
    enqueued_at: datetime
    work_item_id: str
    enqueued_monotonic: float = 0.0

    @classmethod
    def create(
        cls,
        run_id: UUID,
        audit: Mapping[str, object],
        events: Sequence[Mapping[str, object]],
        baseline: ImpactBaseline | None,
        enqueued_at: datetime,
        *,
        copy_inputs: bool = True,
        enqueued_monotonic: float | None = None,
    ) -> ShadowWorkItem:
        # The identity binds the immutable cluster identity and constituent
        # ordering. The complete audit/events/baseline snapshot remains in
        # the work item and is persisted/replayed separately; hashing that
        # full JSON payload here would put serialization on ingestion's hot
        # path without adding identity information.
        cluster_id = str(audit["cluster_id"])
        work_item_id = cls.identity(run_id, audit)
        return cls(
            run_id=run_id,
            cluster_id=cluster_id,
            audit=dict(audit) if copy_inputs else audit,
            events=tuple(dict(event) for event in events) if copy_inputs else tuple(events),
            baseline=baseline,
            enqueued_at=enqueued_at,
            work_item_id=work_item_id,
            enqueued_monotonic=monotonic() if enqueued_monotonic is None else enqueued_monotonic,
        )

    @staticmethod
    def identity(run_id: UUID, audit: Mapping[str, object]) -> str:
        cluster_id = str(audit["cluster_id"])
        last_timestamp = audit.get("last_constituent_timestamp")
        premium = audit.get("aggregate_eligible_premium")
        transition = audit.get("transition")
        transition_order = audit.get("transition_order")
        return "shadow-work:" + ":".join(
            (
                str(run_id),
                cluster_id,
                str(last_timestamp),
                str(premium),
                str(transition),
                str(transition_order),
            )
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "version": "SHADOW_WORK_ITEM_V1",
            "work_item_id": self.work_item_id,
            "run_id": str(self.run_id),
            "cluster_id": self.cluster_id,
            "audit": _json_value(self.audit),
            "events": _json_value(self.events),
            "baseline": _json_value(self.baseline.__dict__) if self.baseline is not None else None,
            "enqueued_at": self.enqueued_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> ShadowWorkItem:
        raw_baseline = value.get("baseline")
        baseline = None
        if isinstance(raw_baseline, Mapping):
            baseline = ImpactBaseline(
                symbol=str(raw_baseline["symbol"]),
                horizon_minutes=int(raw_baseline["horizon_minutes"]),
                expected_volume=Decimal(str(raw_baseline["expected_volume"])),
                expected_volatility=Decimal(str(raw_baseline["expected_volatility"])),
                effective_date=date.fromisoformat(str(raw_baseline["effective_date"])),
                sample_size=int(raw_baseline["sample_size"]),
                provenance=str(raw_baseline["provenance"]),
                adjustment_metadata=str(raw_baseline["adjustment_metadata"]),
                source=str(raw_baseline.get("source", "ALPACA_HISTORICAL_BARS")),
                version=str(raw_baseline.get("version", "impact-baseline-5m-v1")),
            )
        enqueued_at = datetime.fromisoformat(str(value["enqueued_at"])).astimezone(UTC)
        raw_audit = value.get("audit")
        raw_events = value.get("events")
        if not isinstance(raw_audit, Mapping) or not isinstance(raw_events, (list, tuple)):
            raise TypeError("invalid shadow work item payload")
        return cls(
            run_id=UUID(str(value["run_id"])),
            cluster_id=str(value["cluster_id"]),
            audit=dict(raw_audit),
            events=tuple(dict(item) for item in raw_events if isinstance(item, Mapping)),
            baseline=baseline,
            enqueued_at=enqueued_at,
            work_item_id=str(value["work_item_id"]),
            enqueued_monotonic=monotonic(),
        )


@dataclass(frozen=True, slots=True)
class ShadowWorkRequest:
    """Small immutable handoff placed on the ingestion-side bounded queue."""

    run_id: UUID
    audit: Mapping[str, object]
    event_payloads: Mapping[str, Mapping[str, object]]
    baseline: ImpactBaseline | None
    enqueued_at: datetime
    enqueued_monotonic: float


@dataclass
class ShadowQueueMetrics:
    enqueued: int = 0
    completed: int = 0
    duplicate_suppressed: int = 0
    rejected_full: int = 0
    failed: int = 0
    max_depth: int = 0
    queue_wait_ms: list[float] = field(default_factory=list)
    result_latency_ms: list[float] = field(default_factory=list)


ResultSink = Callable[
    [ShadowWorkItem, dict[str, object], dict[str, object]], Awaitable[None] | None
]
WorkItemSink = Callable[[dict[str, object]], bool]


class AsyncShadowWorker:
    """Bounded, sequential, replay-safe worker for immutable cluster snapshots."""

    def __init__(
        self,
        engine: ShadowImpactEngine,
        result_sink: ResultSink,
        *,
        maxsize: int = 20_000,
        work_item_sink: WorkItemSink | None = None,
    ) -> None:
        if maxsize <= 0:
            raise ValueError("maxsize must be positive")
        self.engine = engine
        self.result_sink = result_sink
        self.work_item_sink = work_item_sink
        self.maxsize = maxsize
        self._queue: deque[ShadowWorkItem | ShadowWorkRequest] = deque()
        self._wake = asyncio.Event()
        self._busy = False
        self.metrics = ShadowQueueMetrics()
        self._pending: set[str] = set()
        self._completed: set[str] = set()
        self._task: asyncio.Task[None] | None = None
        self._stopping = False

    def start(self) -> None:
        if self._task is None:
            self._stopping = False
            self._task = asyncio.create_task(self._run())

    def enqueue(self, item: ShadowWorkItem) -> bool:
        if item.run_id != self.engine.run_id:
            raise ValueError("shadow work item run_id mismatch")
        if item.work_item_id in self._pending or item.work_item_id in self._completed:
            self.metrics.duplicate_suppressed += 1
            return True
        if self.work_item_sink is not None and not self.work_item_sink(item.as_dict()):
            self.metrics.rejected_full += 1
            return False
        if len(self._queue) >= self.maxsize:
            self.metrics.rejected_full += 1
            return False
        self._queue.append(item)
        if not self._wake.is_set():
            self._wake.set()
        self._pending.add(item.work_item_id)
        return True

    def recover(
        self,
        records: Sequence[Mapping[str, object]],
        *,
        completed_work_item_ids: Sequence[str] = (),
    ) -> int:
        """Requeue persisted work items after a process restart."""
        self._completed.update(completed_work_item_ids)
        recovered = 0
        for record in records:
            item = ShadowWorkItem.from_dict(record)
            if self.enqueue(item):
                recovered += 1
        return recovered

    def enqueue_snapshot(
        self,
        run_id: UUID,
        audit: Mapping[str, object],
        event_payloads: Mapping[str, Mapping[str, object]],
        baseline: ImpactBaseline | None,
        enqueued_at: datetime,
    ) -> bool:
        if run_id != self.engine.run_id:
            raise ValueError("shadow work item run_id mismatch")
        if len(self._queue) >= self.maxsize:
            self.metrics.rejected_full += 1
            return False
        request = ShadowWorkRequest(
            run_id, audit, event_payloads, baseline, enqueued_at, monotonic()
        )
        self._queue.append(request)
        if not self._wake.is_set():
            self._wake.set()
        return True

    async def _run(self) -> None:
        while not self._stopping or self._queue:
            if not self._queue:
                self._wake.clear()
                await self._wake.wait()
                continue
            queued = self._queue.popleft()
            self._busy = True
            self.metrics.enqueued += 1
            self.metrics.max_depth = max(self.metrics.max_depth, len(self._queue) + 1)
            item = (
                queued if isinstance(queued, ShadowWorkItem) else self._materialize_request(queued)
            )
            if isinstance(queued, ShadowWorkRequest) and item.work_item_id in self._completed:
                self.metrics.duplicate_suppressed += 1
                self._busy = False
                continue
            if isinstance(queued, ShadowWorkRequest):
                self._pending.add(item.work_item_id)
            started = monotonic()
            self.metrics.queue_wait_ms.append(
                (monotonic() - item.enqueued_monotonic) * 1000
                if item.enqueued_monotonic > 0
                else 0.0
            )
            try:
                if (
                    isinstance(queued, ShadowWorkRequest)
                    and self.work_item_sink is not None
                    and not self.work_item_sink(item.as_dict())
                ):
                    raise RuntimeError("SHADOW_WORK_ITEM_PERSISTENCE_BACKPRESSURE")
                event_ids = item.audit.get("constituent_trade_ids", ())
                if not isinstance(event_ids, (list, tuple)):
                    raise TypeError("invalid constituent event identities")
                for event_id, payload in zip(event_ids, item.events, strict=True):
                    self.engine.events[str(event_id)] = dict(payload)
                # Keep the ingestion event loop responsive while retaining one
                # deterministic evaluation order inside the worker.
                result, session = await asyncio.to_thread(self.engine.process_audit, item.audit)
                sink_result = self.result_sink(item, result, session)
                if sink_result is not None:
                    await sink_result
                self._completed.add(item.work_item_id)
                self.metrics.completed += 1
                self.metrics.result_latency_ms.append((monotonic() - started) * 1000)
            except Exception:
                self.metrics.failed += 1
                raise
            finally:
                self._pending.discard(item.work_item_id)
                self._busy = False

    @staticmethod
    def _materialize_request(request: ShadowWorkRequest) -> ShadowWorkItem:
        event_ids = cast(Sequence[object], request.audit.get("constituent_trade_ids", ()))
        return ShadowWorkItem.create(
            request.run_id,
            request.audit,
            tuple(
                request.event_payloads[str(event_id)]
                for event_id in event_ids
                if str(event_id) in request.event_payloads
            ),
            request.baseline,
            request.enqueued_at,
            copy_inputs=False,
            enqueued_monotonic=request.enqueued_monotonic,
        )

    async def drain(self) -> None:
        while self._queue or self._busy:
            await asyncio.sleep(0)
        if self._task is not None:
            self._stopping = True
            self._wake.set()
            await self._task
            self._task = None

    async def close(self) -> None:
        await self.drain()

    @property
    def backlog(self) -> int:
        return len(self._queue)
