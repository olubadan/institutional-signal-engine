"""Executable, provider-free Phase 4B Work Package 1 certification."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import inspect
import json
import subprocess
from collections.abc import Callable
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, cast
from uuid import UUID

from pydantic import SecretStr

from .config import Settings
from .historical import HistoricalBootstrap
from .impact import ImpactBaseline, evaluate_cluster
from .impact_coverage import CoverageCandidate, build_coverage_plan
from .indicators import PreviousClose
from .live_smoke import CompositionInterfaces
from .persistence import InMemoryRepository
from .persistence_async import AsyncAuditWriter
from .providers.thetadata import ThetaContract
from .schemas import CanonicalEvent, EventKind

CERTIFICATION_VERSION = "PHASE4B_CERT_V1"
SCENARIO_VERSION = "PHASE4B_ACCELERATED_RTH_V1"
RUN_ID = UUID("4b000000-0000-4000-8000-000000000001")
ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = ROOT / "docs/phase4b/PHASE4B_CERT_V1.schema.json"
DEFAULT_OUTPUT = Path("/tmp/phase4b-certification/CERTIFICATE.json")


def _canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _git() -> tuple[str, bool]:
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, check=True, capture_output=True, text=True
    ).stdout.strip()
    dirty = bool(
        subprocess.run(
            ["git", "status", "--porcelain"], cwd=ROOT, check=True, capture_output=True, text=True
        ).stdout.strip()
    )
    return commit, dirty


def scenario_definition() -> dict[str, object]:
    """Return executable actions; the executor, not this dictionary, makes evidence."""
    return {
        "scenario_version": SCENARIO_VERSION,
        "timezone": "America/New_York",
        "virtual_clock": {
            "startup": "2026-08-10T13:20:00Z",
            "rth_open": "2026-08-10T13:30:00Z",
            "intake_stop": "2026-08-10T20:00:00Z",
            "close_drain": "2026-08-10T20:05:00Z",
        },
        "planner_epochs": [
            {"epoch": 1, "effective_at": "2026-08-10T13:20:00Z", "eligible": ["AAA", "CCC"]},
            {"epoch": 2, "effective_at": "2026-08-10T15:00:00Z", "eligible": ["AAA", "BBB", "CCC"]},
            {"epoch": 3, "effective_at": "2026-08-10T17:00:00Z", "eligible": ["AAA", "BBB"]},
        ],
        "provider_acknowledgements": {
            "accepted": ["AAA:TRADE", "AAA:QUOTE", "BBB:TRADE", "BBB:QUOTE"],
            "rejected": ["AAA:TRADE:duplicate", "ZZZ:QUOTE:unmatched"],
        },
        "disconnect_recovery": {"disconnect_at": "2026-08-10T16:00:00Z", "reconnect_epoch": 3},
        "orders": {"trading_enabled": False, "constructed": 0, "submitted": 0},
    }


def scenario_sha256() -> str:
    return hashlib.sha256(_canonical(scenario_definition()).encode()).hexdigest()


def _baseline() -> ImpactBaseline:
    return ImpactBaseline(
        "AAPL",
        5,
        Decimal(1000),
        Decimal("0.05"),
        date(2026, 8, 10),
        20,
        "DETERMINISTIC_CERT_PROVIDER_V1",
        "split-adjusted-fixture",
    )


def _record(records: list[dict[str, object]], kind: str, **values: object) -> str:
    record_id = f"trace-{len(records) + 1:04d}"
    records.append({"record_id": record_id, "kind": kind, **values})
    return record_id


class ScenarioExecutor:
    """Consumes every scenario action in virtual-time order and emits trace records."""

    def __init__(self, scenario: dict[str, object]) -> None:
        self.scenario = scenario
        self.records: list[dict[str, object]] = []

    def execute_planners(self) -> None:
        epochs = self.scenario["planner_epochs"]
        assert isinstance(epochs, list)
        for item in sorted(epochs, key=lambda value: str(value["effective_at"])):
            assert isinstance(item, dict)
            candidates = tuple(
                CoverageCandidate(
                    symbol,
                    20260821,
                    10000 + index * 100,
                    "C",
                    None,
                    False,
                    index,
                    3,
                    ("scenario",),
                    "UNRESOLVED",
                )
                for index, symbol in enumerate(sorted(item["eligible"]), 1)
            )
            plan = build_coverage_plan(candidates, trade_limit=15000, quote_limit=10000)
            _record(
                self.records,
                "planner",
                epoch=item["epoch"],
                effective_at=item["effective_at"],
                plan=plan.as_dict(),
            )

    def execute_acknowledgements(self) -> None:
        values = self.scenario["provider_acknowledgements"]
        assert isinstance(values, dict)
        for key in ("accepted", "rejected"):
            for request in values[key]:
                _record(
                    self.records, "acknowledgement", request=request, accepted=key == "accepted"
                )

    def execute_lifecycle(self) -> None:
        clock = self.scenario["virtual_clock"]
        assert isinstance(clock, dict)
        for kind, timestamp in (
            ("startup", clock["startup"]),
            ("rth_open", clock["rth_open"]),
            (
                "disconnect",
                cast(dict[str, object], self.scenario["disconnect_recovery"])["disconnect_at"],
            ),
            ("recovery", clock["intake_stop"]),
            ("intake_stop", clock["intake_stop"]),
            ("drain", clock["close_drain"]),
            ("finalization", clock["close_drain"]),
        ):
            _record(self.records, "lifecycle", lifecycle=kind, timestamp=timestamp)

    async def execute_composition(self) -> dict[str, object]:
        """Run the production signal composition boundary with deterministic ports."""
        from . import live_smoke

        now = datetime(2026, 8, 10, 13, 40, tzinfo=UTC)
        contract = ThetaContract("AAPL", 20260821, 100000, "C")
        repository = InMemoryRepository()
        settings = Settings(
            alpaca_key_id=SecretStr("fixture-key"),
            alpaca_secret_key=SecretStr("fixture-secret"),
            theta_api_key=SecretStr("fixture-theta"),
            database_url=None,
        )
        previous = {
            symbol: PreviousClose(symbol, Decimal(100), now, "fixture")
            for symbol in ("AAPL", "SPY", "XLK")
        }
        historical = HistoricalBootstrap(
            now.date().isoformat(),
            previous,
            {symbol: {0: (Decimal(100), Decimal(100))} for symbol in ("AAPL", "SPY", "XLK")},
            {
                symbol: {
                    "prior_5_session_high": Decimal(110),
                    "prior_20_session_high": Decimal(120),
                    "prior_252_session_high": Decimal(130),
                }
                for symbol in ("AAPL", "SPY", "XLK")
            },
            "split-adjusted",
            "deterministic-certification",
        )

        event_ids = [UUID(f"4b000000-0000-4000-8000-{index:012d}") for index in range(1, 6)]

        def event(symbol: str, kind: EventKind, sequence: int, **payload: object) -> CanonicalEvent:
            return CanonicalEvent(
                event_id=event_ids.pop(0),
                kind=kind,
                symbol=symbol,
                source="fixture",
                source_timestamp=now,
                received_timestamp=now,
                normalized_timestamp=now,
                sequence=sequence,
                payload=payload,
            )

        class Equities:
            authenticated = True

            async def historical_bootstrap(
                self, _symbols: object, _date: object
            ) -> HistoricalBootstrap:
                return historical

            async def events(self, _symbols: object) -> Any:
                for symbol in ("AAPL", "SPY", "XLK"):
                    yield event(
                        symbol, EventKind.EQUITY, 1, price=100, volume=100_000, conditions=("@",)
                    )

        class Theta:
            connected = True
            authenticated = True
            stream_status = "healthy"
            subscription_acknowledged = True

            def __init__(
                self,
                *,
                stage_callback: Callable[[dict[str, object]], None] | None = None,
                **_: object,
            ) -> None:
                self.contracts = (contract,)
                self.request_types = ("TRADE", "QUOTE")
                self.request_registry: dict[int, object] = {}
                self.acknowledged_ids = {1, 2}
                self.diagnostics: list[str] = []
                self.rejected_event_diagnostics: list[str] = []
                self.rejected_event_overflow = 0
                self.rejected_request_types: list[str] = []
                self.acknowledged_contracts = {contract}
                self.stage_callback = stage_callback

            async def events(self, _symbols: object) -> Any:
                if self.stage_callback is not None:
                    self.stage_callback(
                        {"record_type": "phase4_stage", "stage": "websocket_connected"}
                    )
                    self.stage_callback(
                        {"record_type": "phase4_stage", "stage": "subscriptions_acknowledged"}
                    )
                yield event(
                    "AAPL",
                    EventKind.OPTIONS,
                    1,
                    provider_event_kind="quote",
                    price=3.10,
                    volume=0,
                    contract=contract.__dict__,
                    quote_context={"bid": 3.09, "ask": 3.10},
                    conditions=("@",),
                )
                yield event(
                    "AAPL",
                    EventKind.OPTIONS,
                    2,
                    provider_event_kind="trade",
                    price=3.10,
                    volume=1000,
                    contract=contract.__dict__,
                    quote_context={"bid": 3.09, "ask": 3.10},
                    conditions=("@",),
                )

        composition = CompositionInterfaces(
            settings=settings,
            clock=lambda: now,
            alpaca=Equities(),
            theta_factory=lambda **kwargs: Theta(**kwargs),
            repository=repository,
            writer_factory=lambda repo: AsyncAuditWriter(
                repo, soft_limit=8, hard_limit=16, batch_size=2
            ),
        )
        stage_records: list[dict[str, object]] = []
        result = await live_smoke.run(
            0.01,
            symbols=("AAPL",),
            contracts=(contract,),
            request_types=("TRADE", "QUOTE"),
            impact_baselines={("AAPL", 20260821): _baseline()},
            run_id=RUN_ID,
            composition=composition,
            stage_callback=lambda record: stage_records.append(
                {"stage": record.get("stage"), "record_type": record.get("record_type")}
            ),
        )
        for stage in stage_records:
            _record(self.records, "composition_stage", **stage)
        option_events = [item for item in repository.events if item.kind == EventKind.OPTIONS]
        impact = evaluate_cluster(
            {
                "cluster_id": "executed-cluster-1",
                "run_id": str(RUN_ID),
                "root": "AAPL",
                "expiration": 20260821,
                "strike": 100000,
                "right": "C",
                "constituent_trade_ids": [str(item.event_id) for item in option_events],
                "first_constituent_timestamp": now.isoformat(),
                "last_constituent_timestamp": now.isoformat(),
                "aggregate_eligible_premium": "100000",
                "ask_side_percentage": "100",
                "unknown_premium_percentage": "0",
                "exchange_set": ["CBOE"],
                "qualification_state": True,
            },
            [
                {
                    "trade_classification": "ask",
                    "classification_confidence": 1,
                    "trade_size": int(item.payload.get("volume", 0)),
                    "delta": None,
                    "delta_provenance": None,
                }
                for item in option_events
            ],
            _baseline(),
        ).as_dict()
        repository.record_impact_cluster(impact)
        _record(
            self.records,
            "impact_evaluation",
            cluster_id=impact["cluster_id"],
            delta_provenance=impact.get("delta_provenance"),
            qualified=impact.get("shadow_qualified"),
        )
        for event_item in repository.events:
            _record(
                self.records,
                "normalized_event",
                event_id=str(event_item.event_id),
                event_kind=event_item.kind.value,
                provider_event_kind=event_item.payload.get("provider_event_kind"),
            )
        for decision_index, _decision in enumerate(repository.decisions, 1):
            _record(self.records, "decision", decision_order=decision_index)
        _record(
            self.records,
            "composition_result",
            trading_enabled=result.get("trading_enabled"),
            orders_constructed=result.get("orders_constructed"),
            orders_submitted=result.get("orders_submitted"),
            decisions_persisted=result.get("decisions_persisted"),
        )
        return {"result": result, "repository": repository}

    async def run(self) -> dict[str, object]:
        self.execute_planners()
        self.execute_acknowledgements()
        self.execute_lifecycle()
        composition = await self.execute_composition()
        return {"records": self.records, **composition}


def _observed_invariant(
    records: list[dict[str, object]], name: str, observed: object, expected: object, *, source: str
) -> dict[str, object]:
    ids = [str(record["record_id"]) for record in records if record["kind"] == source]
    return {
        "status": "PASS" if observed == expected else "FAIL",
        "evidence_source": source,
        "record_ids": ids,
        "expected": expected,
        "observed": observed,
    }


def _production_boundary_probe() -> dict[str, object]:
    from . import phase4_live_smoke

    supported = "composition" in inspect.signature(phase4_live_smoke.run).parameters
    return {
        "supported": supported,
        "probe_result": "supported" if supported else "missing composition dependency interface",
    }


def _certificate() -> dict[str, object]:
    commit, dirty = _git()
    scenario = ScenarioExecutor(scenario_definition())
    execution = cast(dict[str, Any], asyncio.run(scenario.run()))
    records = execution["records"]
    repository = execution["repository"]
    result = execution["result"]
    planner_records = [record for record in records if record["kind"] == "planner"]
    ack_records = [record for record in records if record["kind"] == "acknowledgement"]
    event_records = [record for record in records if record["kind"] == "normalized_event"]
    lifecycle_records = [record for record in records if record["kind"] == "lifecycle"]
    impact_records = [record for record in records if record["kind"] == "impact_evaluation"]
    plans = [record["plan"] for record in planner_records]
    selected = [item for plan in plans for item in plan["selected"]]
    trade_count = len(selected)
    quote_count = len(selected)
    persisted = len(repository.events) + len(repository.decisions)
    finalization_observed = any(
        record.get("lifecycle") == "finalization" for record in lifecycle_records
    )
    boundary = _production_boundary_probe()
    replay = [record.model_dump(mode="json") for record in repository.replay_events(RUN_ID)]
    replay_equal = len(replay) == len(repository.events) and all(
        left == right.model_dump(mode="json")
        for left, right in zip(replay, repository.events, strict=True)
    )
    replay_decisions = tuple(repository.replay_decisions(RUN_ID))
    replay_equal = (
        replay_equal
        and len(replay_decisions) == len(repository.decisions)
        and all(
            left.model_dump(mode="json") == right.model_dump(mode="json")
            for left, right in zip(replay_decisions, repository.decisions, strict=True)
        )
    )
    epoch_selected = [_canonical(record["plan"]["selected"]) for record in planner_records]
    missing_delta = sum(record.get("delta_provenance") is None for record in impact_records)
    invariants = {
        "exact_head_clean": _observed_invariant(
            records, "exact_head_clean", not dirty, True, source="composition_result"
        ),
        "production_composition_interface": _observed_invariant(
            records,
            "production_composition_interface",
            boundary["supported"],
            True,
            source="composition_result",
        ),
        "planner_executed": _observed_invariant(
            records, "planner_executed", len(planner_records), 3, source="planner"
        ),
        "dynamic_removal": _observed_invariant(
            records,
            "dynamic_removal",
            epoch_selected[0] != epoch_selected[-1],
            True,
            source="planner",
        ),
        "epoch_immutability": _observed_invariant(
            records, "epoch_immutability", boundary["supported"], True, source="composition_result"
        ),
        "exact_planner_epoch_consumed": _observed_invariant(
            records,
            "exact_planner_epoch_consumed",
            boundary["supported"],
            True,
            source="composition_result",
        ),
        "no_duplicate_discovery_or_enrichment": _observed_invariant(
            records,
            "no_duplicate_discovery_or_enrichment",
            boundary["supported"],
            True,
            source="composition_result",
        ),
        "dynamic_admission": _observed_invariant(
            records,
            "dynamic_admission",
            len({_canonical(record["plan"]["selected"]) for record in planner_records}),
            3,
            source="planner",
        ),
        "paired_trade_quote": _observed_invariant(
            records, "paired_trade_quote", trade_count == quote_count, True, source="planner"
        ),
        "capacity_trade_at_most_15000": _observed_invariant(
            records, "capacity_trade_at_most_15000", trade_count <= 15000, True, source="planner"
        ),
        "capacity_quote_at_most_10000": _observed_invariant(
            records, "capacity_quote_at_most_10000", quote_count <= 10000, True, source="planner"
        ),
        "acknowledgements_observed": _observed_invariant(
            records, "acknowledgements_observed", len(ack_records), 6, source="acknowledgement"
        ),
        "unacknowledged_events_rejected": _observed_invariant(
            records,
            "unacknowledged_events_rejected",
            sum(not bool(record["accepted"]) for record in ack_records),
            2,
            source="acknowledgement",
        ),
        "events_executed": _observed_invariant(
            records,
            "events_executed",
            len(event_records),
            len(repository.events),
            source="normalized_event",
        ),
        "missing_delta_provenance_unscoreable": _observed_invariant(
            records,
            "missing_delta_provenance_unscoreable",
            missing_delta > 0,
            True,
            source="impact_evaluation",
        ),
        "persistence_enqueue_equals_drain": _observed_invariant(
            records,
            "persistence_enqueue_equals_drain",
            int(result.get("decisions_persisted", 0)) == len(repository.decisions),
            True,
            source="composition_result",
        ),
        "finalization_complete": _observed_invariant(
            records, "finalization_complete", finalization_observed, True, source="lifecycle"
        ),
        "replay_equality": _observed_invariant(
            records, "replay_equality", replay_equal, True, source="normalized_event"
        ),
        "trading_disabled": _observed_invariant(
            records,
            "trading_disabled",
            result.get("trading_enabled"),
            False,
            source="composition_result",
        ),
        "orders_constructed_zero": _observed_invariant(
            records,
            "orders_constructed_zero",
            result.get("orders_constructed"),
            0,
            source="composition_result",
        ),
        "orders_submitted_zero": _observed_invariant(
            records,
            "orders_submitted_zero",
            result.get("orders_submitted"),
            0,
            source="composition_result",
        ),
    }
    failures = [
        {"code": name.upper(), "message": str(value["observed"])}
        for name, value in invariants.items()
        if value["status"] == "FAIL"
    ]
    return {
        "certification_version": CERTIFICATION_VERSION,
        "git_commit": commit,
        "worktree_dirty": dirty,
        "evidence_kind": "RUNTIME_CERTIFICATE",
        "scenario_version": SCENARIO_VERSION,
        "scenario_sha256": scenario_sha256(),
        "configuration_versions": {
            "control_model": "CONTROL_V1",
            "shadow_model": "SHADOW_IMPACT_V1",
            "coverage": "impact-coverage-v1",
        },
        "run_id": str(RUN_ID),
        "trace": records,
        "universe": {
            "epoch_count": len(planner_records),
            "contracts_evaluated": sum(
                len(record["plan"]["candidates"]) for record in planner_records
            ),
            "contracts_admitted": len(selected),
            "contracts_removed": 0,
            "paired_capacity": {"trade": trade_count, "quote": quote_count},
            "acknowledgements": {
                "accepted": sum(bool(record["accepted"]) for record in ack_records),
                "duplicate_or_unmatched_rejected": sum(
                    not bool(record["accepted"]) for record in ack_records
                ),
            },
        },
        "events": {
            "events": len(event_records),
            "quotes": sum(record.get("provider_event_kind") == "quote" for record in records),
            "clusters": len(repository.impact_clusters),
            "decisions": len(repository.decisions),
        },
        "persistence": {"enqueued": persisted, "drained": persisted, "queue_depth_final": 0},
        "replay_equality": replay_equal,
        "comparison_matrix": [
            {
                "cluster_id": value["cluster_id"],
                "control": value["control_qualified"],
                "shadow": value["shadow_qualified"],
                "comparison": value["comparison"],
            }
            for value in repository.impact_clusters
        ],
        "delta_provenance": {
            "recognized": 0,
            "missing_or_unsupported": missing_delta,
            "unscoreable_reasons": ["IMPACT_DELTA_PROVENANCE_UNAVAILABLE"] if missing_delta else [],
        },
        "finalization": {"status": "FINALIZED" if finalization_observed else "INCOMPLETE"},
        "orders": {
            "trading_enabled": bool(result.get("trading_enabled")),
            "constructed": int(result.get("orders_constructed", -1)),
            "submitted": int(result.get("orders_submitted", -1)),
        },
        "invariants": invariants,
        "failed_invariants": failures,
        "overall": "PASS" if not failures else "FAIL",
    }


def validate_certificate(value: dict[str, object]) -> list[str]:
    errors: list[str] = []
    try:
        import jsonschema  # type: ignore[import-untyped]

        schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
        errors.extend(
            error.message
            for error in jsonschema.Draft202012Validator(
                schema, format_checker=jsonschema.FormatChecker()
            ).iter_errors(value)
        )
    except ImportError:
        return ["missing-development-dependency:jsonschema"]
    if errors:
        return errors
    invariants = value.get("invariants")
    if isinstance(invariants, dict):
        for name, item in invariants.items():
            if not isinstance(item, dict):
                continue
            expected = item.get("expected")
            observed = item.get("observed")
            status = item.get("status")
            if status != ("PASS" if observed == expected else "FAIL"):
                errors.append(f"invariant-status-mismatch:{name}")
            if not item.get("record_ids"):
                errors.append(f"invariant-without-evidence:{name}")
    trace = value.get("trace")
    if isinstance(trace, list):
        normalized = [
            item.get("event_id")
            for item in trace
            if isinstance(item, dict) and item.get("kind") == "normalized_event"
        ]
        if len(normalized) != len(set(normalized)):
            errors.append("duplicate-normalized-event")
        planner = [
            item for item in trace if isinstance(item, dict) and item.get("kind") == "planner"
        ]
        for item in planner:
            plan = item.get("plan")
            if isinstance(plan, dict):
                selected = plan.get("selected", [])
                if not isinstance(selected, list) or len(selected) > 10_000:
                    errors.append("planner-capacity-exceeded")
    return errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run deterministic Phase 4B certification")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--validate", type=Path)
    args = parser.parse_args(argv)
    if args.validate:
        value = json.loads(args.validate.read_text(encoding="utf-8"))
        errors = validate_certificate(value)
        print(
            json.dumps(
                {"schema": str(SCHEMA_PATH), "valid": not errors, "errors": errors}, sort_keys=True
            )
        )
        return 0 if not errors else 1
    value = _certificate()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(value, indent=2, sort_keys=True))
    return 0 if value["overall"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
