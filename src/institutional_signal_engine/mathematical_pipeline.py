"""Assembly seam for live admission, model processing, and offline replay."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID

from .config import Settings
from .impact import ImpactBaseline, ShadowImpactEngine
from .persistence import InMemoryRepository
from .pipeline import SignalPipeline
from .schemas import CanonicalEvent

SHARED_FEATURE_VECTOR_VERSION = "SHARED_FEATURE_VECTOR_V1"
MODEL_COMPARISON_VERSION = "CONTROL_SHADOW_COMPARISON_V1"


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
    ) -> None:
        self.settings = settings
        self.repository = repository
        self.run_id = run_id
        self.symbols = tuple(sorted({str(symbol).upper() for symbol in symbols}))
        self.impact_engine = ShadowImpactEngine(run_id, baselines)
        self.pipeline = SignalPipeline(
            settings,
            repository=repository,
            run_id=run_id,
            symbols=self.symbols or ("AAPL",),
            now=now,
            indicator_calculator=indicator_calculator,
            impact_engine=self.impact_engine,
        )
        self.accepted_event_ids: list[str] = []
        self.rejected_event_ids: list[str] = []
        self.feature_vectors: list[dict[str, object]] = []
        self.comparisons: list[dict[str, object]] = []
        self.control_evaluations: list[dict[str, object]] = []
        self._seen_decisions: set[str] = set()

    def process_accepted(self, event: CanonicalEvent) -> Any:
        self.accepted_event_ids.append(str(event.event_id))
        result = self.pipeline.process(event)
        self._capture_new_outputs()
        return result

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
            if result.get("control_qualified") is not True:
                continue
            audit: dict[str, object] = next(
                (
                    item
                    for item in reversed(tuple(getattr(self.repository, "sweeps", ())))
                    if str(item.get("cluster_id")) == cluster_id
                ),
                {},
            )
            vector: dict[str, object] = {
                "version": SHARED_FEATURE_VECTOR_VERSION,
                "run_id": str(self.run_id),
                "cluster_id": cluster_id,
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
            self.feature_vectors.append(vector)
            comparison: dict[str, object] = {
                "version": MODEL_COMPARISON_VERSION,
                "run_id": str(self.run_id),
                "cluster_id": cluster_id,
                "control_result": result.get("control_qualified"),
                "shadow_result": result.get("shadow_qualified"),
                "comparison": result.get("comparison"),
                "failed_reasons": list(cast(Sequence[object], result.get("failed_reasons", []))),
            }
            self.comparisons.append(comparison)
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
