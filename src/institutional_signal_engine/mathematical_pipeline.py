"""Assembly seam for live admission, model processing, and offline replay."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any, cast
from uuid import NAMESPACE_URL, UUID, uuid5

from .config import Settings
from .impact import ImpactBaseline, ShadowImpactEngine
from .journal import canonical_json, sha256_bytes
from .persistence import InMemoryRepository
from .pipeline import SignalPipeline
from .schemas import CanonicalEvent
from .shadow_async import AsyncShadowWorker, ShadowWorkItem

SHARED_FEATURE_VECTOR_VERSION = "SHARED_FEATURE_VECTOR_V1"
MODEL_COMPARISON_VERSION = "CONTROL_SHADOW_COMPARISON_V1"
SCIENTIFIC_EVIDENCE_VERSION = "MATHEMATICAL_EVIDENCE_V1"


def _scientific_id(kind: str, *parts: object) -> str:
    return str(
        uuid5(NAMESPACE_URL, ":".join(("ise-scientific-v1", kind, *(str(part) for part in parts))))
    )


class MathematicalPipeline:
    """Compose the authoritative pipeline and preserve replayable outputs."""

    def __init__(
        self,
        settings: Settings,
        repository: Any,
        run_id: UUID,
        symbols: Iterable[str],
        *,
        baselines: Mapping[str, ImpactBaseline]
        | Mapping[tuple[str, int], ImpactBaseline]
        | None = None,
        now: Any | None = None,
        indicator_calculator: Any | None = None,
        async_shadow: bool = False,
        shadow_queue_size: int = 20_000,
        shadow_work_item_sink: Callable[[dict[str, object]], bool] | None = None,
        result_observer: Callable[[], None] | None = None,
        evidence_sink: Callable[[str, Mapping[str, object]], None] | None = None,
    ) -> None:
        self.settings = settings
        self.repository = repository
        self.run_id = run_id
        self.symbols = tuple(sorted({str(symbol).upper() for symbol in symbols}))
        self.impact_engine = ShadowImpactEngine(run_id, baselines)
        self.async_shadow = async_shadow
        self.result_observer = result_observer
        self.evidence_sink = evidence_sink
        self._audit_snapshots: dict[str, Mapping[str, object]] = {}
        self.shadow_worker: AsyncShadowWorker | None = None
        self.pipeline = SignalPipeline(
            settings,
            repository=repository,
            run_id=run_id,
            symbols=self.symbols or ("AAPL",),
            now=now,
            indicator_calculator=indicator_calculator,
            impact_engine=None if async_shadow else self.impact_engine,
            shadow_audit_sink=self._enqueue_shadow_audit if async_shadow else None,
        )
        if async_shadow:
            self.shadow_worker = AsyncShadowWorker(
                self.impact_engine,
                self._capture_async_result,
                maxsize=shadow_queue_size,
                work_item_sink=shadow_work_item_sink,
            )
            self.shadow_worker.start()
        self.accepted_event_ids: list[str] = []
        self.rejected_event_ids: list[str] = []
        self.feature_vectors: list[dict[str, object]] = []
        self.comparisons: list[dict[str, object]] = []
        self.control_evaluations: list[dict[str, object]] = []
        self._seen_decisions: set[str] = set()
        self._seen_scientific_controls: set[str] = set()

    def process_accepted(self, event: CanonicalEvent) -> Any:
        self.accepted_event_ids.append(str(event.event_id))
        result = self.pipeline.process(event)
        self._capture_new_outputs()
        if self.result_observer is not None:
            self.result_observer()
        return result

    def _emit_evidence(self, kind: str, payload: Mapping[str, object]) -> None:
        if self.evidence_sink is not None:
            self.evidence_sink(
                kind, {"schema_version": SCIENTIFIC_EVIDENCE_VERSION, **dict(payload)}
            )

    def _enqueue_shadow_audit(
        self, audit: dict[str, object], event_payloads: Mapping[str, Mapping[str, object]]
    ) -> None:
        if self.shadow_worker is None:
            return
        enqueued_at = self._audit_timestamp(audit)
        baseline = self.impact_engine._baseline(str(audit["root"]), enqueued_at)
        if not self.shadow_worker.enqueue_snapshot(
            self.run_id,
            audit,
            event_payloads,
            baseline,
            enqueued_at,
        ):
            raise RuntimeError("SHADOW_QUEUE_BACKPRESSURE")
        work_item_id = ShadowWorkItem.identity(self.run_id, audit)
        cluster_id = str(audit["cluster_id"])
        feature_vector_id = _scientific_id("feature-vector", self.run_id, cluster_id)
        shadow_evaluation_id = _scientific_id("shadow-evaluation", self.run_id, work_item_id)
        if audit.get("transition") == "CLUSTER_CLOSED":
            self._emit_evidence(
                "cluster.completed",
                {
                    "cluster_evidence_id": _scientific_id("cluster", self.run_id, cluster_id),
                    "cluster_id": cluster_id,
                    "feature_vector_id": feature_vector_id,
                    "symbol": audit.get("symbol"),
                    "constituent_event_ids": list(
                        cast(Sequence[object], audit.get("constituent_trade_ids", ()))
                    ),
                    "cluster_audit": dict(audit),
                },
            )
        self._emit_evidence(
            "shadow.enqueued",
            {
                "work_item_id": work_item_id,
                "shadow_evaluation_id": shadow_evaluation_id,
                "feature_vector_id": feature_vector_id,
                "cluster_id": cluster_id,
                "enqueued_at": enqueued_at.isoformat(),
                "queue_state": "ENQUEUED",
            },
        )

    @staticmethod
    def _audit_timestamp(audit: Mapping[str, object]) -> datetime:
        value = audit["last_constituent_timestamp"]
        if isinstance(value, datetime):
            return value
        return datetime.fromisoformat(str(value))

    async def _capture_async_result(
        self,
        _item: ShadowWorkItem,
        result: dict[str, object],
        session: dict[str, object],
    ) -> None:
        self._audit_snapshots.setdefault(str(result["cluster_id"]), dict(_item.audit))
        self.pipeline.impact_results.append(result)
        self.repository.record_impact_cluster(result)
        self.repository.record_impact_session(session)
        self._emit_evidence(
            "shadow.completed",
            {
                "work_item_id": _item.work_item_id,
                "shadow_evaluation_id": _scientific_id(
                    "shadow-evaluation", self.run_id, _item.work_item_id
                ),
                "feature_vector_id": _scientific_id(
                    "feature-vector", self.run_id, str(result["cluster_id"])
                ),
                "cluster_id": str(result["cluster_id"]),
                "enqueued_at": _item.enqueued_at.isoformat(),
                "completed_at": datetime.now(UTC).isoformat(),
                "result": result,
                "session": session,
                "status": "COMPLETED",
            },
        )
        self._capture_new_outputs()
        if self.result_observer is not None:
            self.result_observer()

    async def drain_shadow(self) -> None:
        if self.shadow_worker is None:
            return
        await self.shadow_worker.drain()

    def record_rejected(self, event: CanonicalEvent, reason: str) -> None:
        self.rejected_event_ids.append(str(event.event_id))
        recorder = getattr(self.repository, "record_pipeline_admission", None)
        if recorder is not None:
            recorder(
                {
                    "version": SHARED_FEATURE_VECTOR_VERSION,
                    "run_id": str(self.run_id),
                    "event_id": str(event.event_id),
                    "disposition": "REJECTED",
                    "reason": reason,
                }
            )

    def _capture_new_outputs(self) -> None:
        for decision in self.pipeline.decisions:
            decision_id = str(decision.decision_id)
            if decision_id in self._seen_decisions:
                continue
            self._seen_decisions.add(decision_id)
            evaluation = {
                "version": "CONTROL_EVALUATION_V1",
                "run_id": str(self.run_id),
                "decision_id": decision_id,
                "model_version": "CONTROL_V1",
                "thresholds": self.settings.thresholds.model_dump(mode="json"),
                "threshold_provenance": "Settings.thresholds authoritative runtime configuration",
                "inputs": decision.model_dump(mode="json"),
                "result": decision.fire,
                "failure_reasons": list(decision.rejection_reasons),
            }
            self.control_evaluations.append(evaluation)
            self._emit_evidence("control.evaluated", evaluation)
            recorder = getattr(self.repository, "record_control_evaluation", None)
            if recorder is not None:
                recorder(evaluation)
        existing = {str(item.get("cluster_id")) for item in self.feature_vectors}
        latest_results: dict[str, dict[str, object]] = {}
        for raw_result in self.pipeline.impact_results:
            latest_results[str(raw_result.get("cluster_id"))] = dict(raw_result)
        for cluster_id, result in latest_results.items():
            if cluster_id in existing:
                continue
            audit: dict[str, object] = dict(
                self._audit_snapshots.get(cluster_id)
                or next(
                    (
                        item
                        for item in reversed(tuple(getattr(self.repository, "sweeps", ())))
                        if str(item.get("cluster_id")) == cluster_id
                    ),
                    {},
                )
            )
            vector: dict[str, object] = {
                "version": SHARED_FEATURE_VECTOR_VERSION,
                "run_id": str(self.run_id),
                "cluster_id": cluster_id,
                "feature_vector_id": _scientific_id("feature-vector", self.run_id, cluster_id),
                "source_event_ids": list(
                    cast(Sequence[object], audit.get("constituent_trade_ids", []))
                ),
                "impact_inputs": dict(result),
                "control_inputs": {
                    "control_qualified": result.get("control_qualified"),
                    "control_model_version": "CONTROL_V1",
                    "configuration_provenance": "Settings.thresholds authoritative runtime configuration",
                    "thresholds": self.settings.thresholds.model_dump(mode="json"),
                },
            }
            vector["immutable_input_sha256"] = sha256_bytes(
                canonical_json(
                    {
                        "cluster_id": cluster_id,
                        "source_event_ids": vector["source_event_ids"],
                        "impact_inputs": vector["impact_inputs"],
                        "control_inputs": vector["control_inputs"],
                    }
                )
            )
            control_evaluation_id = _scientific_id("control-evaluation", self.run_id, cluster_id)
            shadow_evaluation_id = _scientific_id(
                "shadow-evaluation", self.run_id, ShadowWorkItem.identity(self.run_id, audit)
            )
            control_evaluation = {
                "version": "CONTROL_EVALUATION_V1",
                "evaluation_id": control_evaluation_id,
                "run_id": str(self.run_id),
                "cluster_id": cluster_id,
                "feature_vector_id": vector["feature_vector_id"],
                "model_version": "CONTROL_V1",
                "thresholds": self.settings.thresholds.model_dump(mode="json"),
                "threshold_provenance": "Settings.thresholds authoritative runtime configuration",
                "inputs": vector["control_inputs"],
                "result": result.get("control_qualified"),
                "failure_reasons": list(cast(Sequence[object], result.get("failed_reasons", []))),
            }
            if control_evaluation_id not in self._seen_scientific_controls:
                self._seen_scientific_controls.add(control_evaluation_id)
                self.control_evaluations.append(control_evaluation)
                self._emit_evidence("control.evaluated", control_evaluation)
                recorder = getattr(self.repository, "record_control_evaluation", None)
                if recorder is not None:
                    recorder(control_evaluation)
            self.feature_vectors.append(vector)
            comparison: dict[str, object] = {
                "version": MODEL_COMPARISON_VERSION,
                "comparison_id": _scientific_id("model-comparison", self.run_id, cluster_id),
                "comparison_type": "MODEL_ONLY",
                "run_id": str(self.run_id),
                "cluster_id": cluster_id,
                "feature_vector_id": vector["feature_vector_id"],
                "control_evaluation_id": control_evaluation_id,
                "shadow_evaluation_id": shadow_evaluation_id,
                "realized_outcome_complete": False,
                "control_result": result.get("control_qualified"),
                "shadow_result": result.get("shadow_qualified"),
                "comparison": result.get("comparison"),
                "failed_reasons": list(cast(Sequence[object], result.get("failed_reasons", []))),
            }
            self.comparisons.append(comparison)
            self._emit_evidence("feature_vector.persisted", vector)
            self._emit_evidence("comparison.completed", comparison)
            recorder = getattr(self.repository, "record_shared_feature_vector", None)
            if recorder is not None:
                recorder(vector)
            recorder = getattr(self.repository, "record_model_comparison", None)
            if recorder is not None:
                recorder(comparison)

    def snapshot(self) -> dict[str, object]:
        self._capture_new_outputs()
        return {
            "run_id": str(self.run_id),
            "accepted_event_ids": list(self.accepted_event_ids),
            "rejected_event_ids": list(self.rejected_event_ids),
            "decisions": [item.model_dump(mode="json") for item in self.pipeline.decisions],
            "control_evaluation_records": list(self.control_evaluations),
            "sweeps": list(getattr(self.repository, "sweeps", ())),
            "feature_vectors": list(self.feature_vectors),
            "comparisons": list(self.comparisons),
            "control_evaluations": len(self.control_evaluations),
            "shadow_evaluations": len(self.pipeline.impact_results),
            "shadow_live_scoring_enabled": False,
            "orders_constructed": 0,
            "orders_submitted": 0,
        }


def replay_semantics(
    events: Iterable[CanonicalEvent],
    settings: Settings,
    symbols: Iterable[str],
    run_id: UUID,
    *,
    baselines: Mapping[str, ImpactBaseline]
    | Mapping[tuple[str, int], ImpactBaseline]
    | None = None,
) -> dict[str, object]:
    """Re-run only persisted canonical events in a fresh pipeline instance."""
    repository = InMemoryRepository()
    ordered = sorted(
        events,
        key=lambda value: (
            value.ingest_order,
            value.received_timestamp,
            value.normalized_timestamp,
            value.sequence,
            str(value.event_id),
        ),
    )
    clock = [ordered[0].normalized_timestamp if ordered else datetime.min.replace(tzinfo=UTC)]
    pipeline = MathematicalPipeline(
        settings, repository, run_id, symbols, baselines=baselines, now=lambda: clock[0]
    )
    for event in ordered:
        clock[0] = event.normalized_timestamp
        pipeline.process_accepted(event)
    return pipeline.snapshot()
