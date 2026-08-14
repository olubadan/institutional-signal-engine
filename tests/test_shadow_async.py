from datetime import UTC, datetime
from uuid import uuid4

import pytest
from test_impact import audit, baseline, events_for

from institutional_signal_engine.impact import ShadowImpactEngine
from institutional_signal_engine.persistence import InMemoryRepository
from institutional_signal_engine.persistence_async import AsyncAuditWriter, AuditWrite
from institutional_signal_engine.shadow_async import AsyncShadowWorker, ShadowWorkItem


@pytest.mark.asyncio
async def test_async_shadow_worker_preserves_result_and_suppresses_duplicates():
    run_id = uuid4()
    item_audit = audit(cluster_id="cluster-async")
    item_audit["run_id"] = str(run_id)
    item = ShadowWorkItem.create(
        run_id,
        item_audit,
        events_for(item_audit),
        baseline(),
        datetime.now(UTC),
    )
    results: list[tuple[dict[str, object], dict[str, object]]] = []

    async def sink(
        _item: ShadowWorkItem, result: dict[str, object], session: dict[str, object]
    ) -> None:
        results.append((result, session))

    worker = AsyncShadowWorker(ShadowImpactEngine(run_id, {"BAC": baseline()}), sink)
    worker.start()
    assert worker.enqueue(item)
    assert worker.enqueue(item)
    await worker.drain()

    assert len(results) == 1
    assert results[0][0]["cluster_id"] == "cluster-async"
    assert worker.metrics.duplicate_suppressed == 1
    assert worker.metrics.completed == 1
    assert worker.backlog == 0


@pytest.mark.asyncio
async def test_async_shadow_worker_rejects_cross_run_items():
    run_id = uuid4()
    item_audit = audit(cluster_id="cluster-cross")
    item = ShadowWorkItem.create(
        uuid4(), item_audit, events_for(item_audit), baseline(), datetime.now(UTC)
    )

    async def sink(
        _item: ShadowWorkItem, _result: dict[str, object], _session: dict[str, object]
    ) -> None:
        return None

    worker = AsyncShadowWorker(ShadowImpactEngine(run_id, {"BAC": baseline()}), sink)
    with pytest.raises(ValueError, match="run_id mismatch"):
        worker.enqueue(item)


def test_work_item_identity_binds_run_cluster_inputs_and_baseline():
    run_id = uuid4()
    item_audit = audit(cluster_id="cluster-id")
    item_audit["run_id"] = str(run_id)
    first = ShadowWorkItem.create(
        run_id, item_audit, events_for(item_audit), baseline(), datetime(2026, 8, 14, tzinfo=UTC)
    )
    second = ShadowWorkItem.create(
        run_id,
        {**item_audit, "aggregate_eligible_premium": "1001"},
        events_for(item_audit),
        baseline(),
        datetime(2026, 8, 14, tzinfo=UTC),
    )
    assert first.work_item_id != second.work_item_id
    assert first.as_dict()["run_id"] == str(run_id)


@pytest.mark.asyncio
async def test_work_items_are_recoverable_from_the_existing_durable_writer():
    run_id = uuid4()
    item_audit = audit(cluster_id="cluster-recover")
    item_audit["run_id"] = str(run_id)
    item = ShadowWorkItem.create(
        run_id, item_audit, events_for(item_audit), baseline(), datetime.now(UTC)
    )
    repository = InMemoryRepository()
    writer = AsyncAuditWriter(repository, soft_limit=2, hard_limit=4, batch_size=4)
    writer.start()
    assert writer.enqueue(AuditWrite(shadow_work_item=item.as_dict()))
    await writer.drain()
    recovered = tuple(repository.replay_shadow_work_items(run_id))
    assert len(recovered) == 1
    reconstructed = ShadowWorkItem.from_dict(recovered[0])
    assert reconstructed.work_item_id == item.work_item_id
    assert reconstructed.audit == item.audit

    results: list[str] = []

    async def sink(
        recovered_item: ShadowWorkItem,
        _result: dict[str, object],
        _session: dict[str, object],
    ) -> None:
        results.append(recovered_item.work_item_id)

    worker = AsyncShadowWorker(ShadowImpactEngine(run_id, {"BAC": baseline()}), sink)
    worker.start()
    assert worker.recover(recovered) == 1
    await worker.drain()
    assert results == [item.work_item_id]
