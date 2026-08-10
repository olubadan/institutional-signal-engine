"""Asynchronous, bounded audit-write queue with explicit backpressure."""

import asyncio
import logging
from dataclasses import dataclass
from time import monotonic
from typing import Any

from .ports import EventRepository
from .schemas import CanonicalEvent, Decision

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AuditWrite:
    event: CanonicalEvent | None = None
    decision: Decision | None = None
    quote_consumption: Any | None = None
    sweep: Any | None = None
    impact_cluster: dict[str, object] | None = None
    impact_session: dict[str, object] | None = None
    probe_id: str | None = None


@dataclass
class WriteQueueMetrics:
    queue_depth_samples: list[int]
    batch_sizes: list[int]
    database_write_latency_ms: list[float]
    soft_limit_crossings: int = 0
    hard_limit_failures: int = 0
    time_above_soft_limit: float = 0.0
    probe_failures: int = 0


class AsyncAuditWriter:
    def __init__(
        self,
        repository: EventRepository,
        soft_limit: int = 10_000,
        hard_limit: int = 20_000,
        batch_size: int = 100,
        flush_interval: float = 0.05,
    ) -> None:
        if not 0 < soft_limit < hard_limit:
            raise ValueError("queue limits must satisfy 0 < soft < hard")
        self.repository = repository
        self.soft_limit = soft_limit
        self.hard_limit = hard_limit
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.queue: asyncio.Queue[AuditWrite] = asyncio.Queue(maxsize=hard_limit)
        self.metrics = WriteQueueMetrics([], [], [])
        self.persistence_backpressure_failure = False
        self._above_soft_started: float | None = None
        self._task: asyncio.Task[None] | None = None
        self._stopping = False

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run())

    def enqueue(self, record: AuditWrite) -> bool:
        depth = self.queue.qsize()
        self.metrics.queue_depth_samples.append(depth)
        if depth >= self.hard_limit:
            self.persistence_backpressure_failure = True
            self.metrics.hard_limit_failures += 1
            return False
        if depth >= self.soft_limit and self._above_soft_started is None:
            self._above_soft_started = monotonic()
            self.metrics.soft_limit_crossings += 1
            logger.warning("audit_write_queue_above_soft_limit", extra={"depth": depth})
        self.queue.put_nowait(record)
        return True

    async def _run(self) -> None:
        while not self._stopping or not self.queue.empty():
            batch: list[AuditWrite] = []
            try:
                batch.append(await asyncio.wait_for(self.queue.get(), self.flush_interval))
            except TimeoutError:
                continue
            while len(batch) < self.batch_size:
                try:
                    batch.append(self.queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            started = monotonic()
            for record in batch:
                if record.event is not None:
                    self.repository.record_event(record.event)
                if record.decision is not None:
                    self.repository.record_decision(record.decision)
                if record.quote_consumption is not None:
                    recorder = getattr(self.repository, "record_quote_consumption", None)
                    if recorder is not None:
                        recorder(record.quote_consumption)
                if record.sweep is not None:
                    self.repository.record_sweep(record.sweep)
                if record.impact_cluster is not None:
                    self.repository.record_impact_cluster(record.impact_cluster)
                if record.impact_session is not None:
                    self.repository.record_impact_session(record.impact_session)
                if record.probe_id is not None:
                    probe = getattr(self.repository, "write_drain_probe", None)
                    if probe is None or not probe(record.probe_id):
                        self.metrics.probe_failures += 1
                self.queue.task_done()
            flush = getattr(self.repository, "flush", None)
            if flush is not None:
                flush()
            self.metrics.batch_sizes.append(len(batch))
            self.metrics.database_write_latency_ms.append((monotonic() - started) * 1000)
            if self._above_soft_started is not None and self.queue.qsize() < self.soft_limit:
                self.metrics.time_above_soft_limit += monotonic() - self._above_soft_started
                self._above_soft_started = None

    async def drain(self) -> None:
        await self.queue.join()
        if self._task is not None:
            self._stopping = True
            await self._task
            self._task = None

    async def close(self) -> None:
        await self.drain()
