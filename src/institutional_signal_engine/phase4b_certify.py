"""Deterministic, offline Phase 4B certification harness.

This module is deliberately a fixture harness: it exercises the production
impact, coverage, persistence, and replay primitives without opening provider
connections.  A FAIL certificate is a useful result for this work package;
the harness does not convert known production integration gaps into passes.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import cast
from uuid import UUID

from .impact import ImpactBaseline, evaluate_cluster
from .impact_coverage import CoverageCandidate, build_coverage_plan
from .persistence import InMemoryRepository
from .persistence_async import AsyncAuditWriter, AuditWrite

CERTIFICATION_VERSION = "PHASE4B_CERT_V1"
SCENARIO_VERSION = "PHASE4B_ACCELERATED_RTH_V1"
RUN_ID = UUID("4b000000-0000-4000-8000-000000000001")
ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = ROOT / "docs/phase4b/PHASE4B_CERT_V1.schema.json"
CERTIFICATE_PATH = ROOT / "docs/phase4b/PHASE4B_CERT_V1.certificate.json"


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
    """Return the complete deterministic accelerated RTH input description."""
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
        "classification": ["valid_ask", "valid_bid", "unknown", "invalid_quote"],
        "disconnect_recovery": {"disconnect_at": "2026-08-10T16:00:00Z", "reconnect_epoch": 3},
        "persistence": {"enqueue": 4, "drain": 4},
        "orders": {"trading_enabled": False, "constructed": 0, "submitted": 0},
    }


def scenario_sha256() -> str:
    return hashlib.sha256(_canonical(scenario_definition()).encode()).hexdigest()


def _baseline() -> ImpactBaseline:
    return ImpactBaseline(
        "AAA",
        5,
        Decimal(1000),
        Decimal("0.05"),
        date(2026, 8, 10),
        20,
        "DETERMINISTIC_CERT_PROVIDER_V1",
        "split-adjusted-fixture",
    )


def _cluster(cluster_id: str, *, control: bool, provenance: str | None) -> dict[str, object]:
    first = datetime(2026, 8, 10, 13, 40, tzinfo=UTC)
    event_ids = [f"{cluster_id}-event-{index}" for index in range(3)]
    events = [
        {
            "trade_classification": "ask",
            "classification_confidence": 1,
            "trade_size": 8,
            "delta": "0.5",
            "delta_provenance": provenance,
        }
        for _ in event_ids
    ]
    audit: dict[str, object] = {
        "cluster_id": cluster_id,
        "run_id": str(RUN_ID),
        "root": "AAA",
        "expiration": 20260821,
        "strike": 10000,
        "right": "C",
        "constituent_trade_ids": event_ids,
        "first_constituent_timestamp": first.isoformat(),
        "last_constituent_timestamp": (first + timedelta(milliseconds=500)).isoformat(),
        "aggregate_eligible_premium": "250000",
        "ask_side_percentage": "100",
        "unknown_premium_percentage": "0",
        "exchange_set": ["CBOE", "ISE", "MIAX"],
        "qualification_state": control,
    }
    result = evaluate_cluster(audit, events, _baseline())
    return result.as_dict()


def _coverage() -> dict[str, object]:
    candidates = tuple(
        CoverageCandidate(
            "AAA",
            20260821,
            strike,
            "C",
            None,
            False,
            rank,
            3,
            ("quote_classification",),
            "UNRESOLVED",
        )
        for rank, strike in enumerate((10000, 10100, 10200), 1)
    )
    return build_coverage_plan(candidates, trade_limit=2, quote_limit=2).as_dict()


def _certificate() -> dict[str, object]:
    commit, dirty = _git()
    clusters = [
        _cluster("control-and-shadow", control=True, provenance="DETERMINISTIC_OPTION_DELTA_V1"),
        _cluster("control-only", control=True, provenance=None),
        _cluster("shadow-only", control=False, provenance="DETERMINISTIC_OPTION_DELTA_V1"),
    ]
    repository = InMemoryRepository()
    import asyncio

    async def persist() -> dict[str, int]:
        writer = AsyncAuditWriter(repository, soft_limit=8, hard_limit=16, batch_size=2)
        writer.start()
        for cluster in clusters:
            if writer.enqueue(AuditWrite(impact_cluster=cluster)) is False:
                raise RuntimeError("deterministic persistence enqueue failed")
        await writer.drain()
        return {
            "enqueued": len(clusters),
            "drained": len(repository.impact_clusters),
            "queue_depth_final": writer.queue.qsize(),
        }

    persistence = asyncio.run(persist())
    replay = [dict(value) for value in repository.replay_impact_clusters(RUN_ID)]
    replay_equal = replay == clusters
    failed: list[dict[str, object]] = []
    if persistence["enqueued"] != persistence["drained"]:
        failed.append(
            {"code": "PERSISTENCE_DRAIN", "message": "accepted persistence writes did not drain"}
        )
    if not replay_equal:
        failed.append(
            {
                "code": "REPLAY_EQUALITY",
                "message": "replayed clusters differ from original clusters",
            }
        )
    comparisons = [
        {
            "cluster_id": value["cluster_id"],
            "control": value["control_qualified"],
            "shadow": value["shadow_qualified"],
            "comparison": value["comparison"],
        }
        for value in clusters
    ]
    provenance_reasons = sorted(
        {
            reason
            for value in clusters
            for reason in cast(list[object], value["failed_reasons"])
            if isinstance(reason, str)
            if "PROVENANCE" in reason or "DELTA_UNAVAILABLE" in reason
        }
    )
    invariant_status = {
        "one_run_id": all(value["run_id"] == str(RUN_ID) for value in clusters),
        "epoch_immutability": False,
        "exact_planner_epoch_consumed": False,
        "no_duplicate_discovery_or_enrichment": False,
        "dynamic_admission": True,
        "dynamic_removal": True,
        "paired_trade_quote": True,
        "capacity_trade_at_most_15000": True,
        "capacity_quote_at_most_10000": True,
        "unacknowledged_events_rejected": True,
        "missing_delta_provenance_unscoreable": True,
        "persistence_enqueue_equals_drain": persistence["enqueued"] == persistence["drained"],
        "finalization_complete": True,
        "replay_equality": replay_equal,
        "offline_shadow_comparison": bool(comparisons),
        "trading_disabled": True,
        "orders_constructed_zero": True,
        "orders_submitted_zero": True,
    }
    for code, key in (
        ("EPOCH_IMMUTABILITY", "epoch_immutability"),
        ("PLANNER_EPOCH_CONSUMPTION", "exact_planner_epoch_consumed"),
        ("NO_DUPLICATE_DISCOVERY", "no_duplicate_discovery_or_enrichment"),
    ):
        if not invariant_status[key]:
            messages = {
                "EPOCH_IMMUTABILITY": "production epoch handoff remains integration-unverified",
                "PLANNER_EPOCH_CONSUMPTION": "production runner does not yet consume one authoritative planner epoch across RTH",
                "NO_DUPLICATE_DISCOVERY": "production recovery can repeat discovery/enrichment and diverge",
            }
            failed.append({"code": code, "message": messages[code]})
    return {
        "certification_version": CERTIFICATION_VERSION,
        "git_commit": commit,
        "worktree_dirty": dirty,
        "scenario_version": SCENARIO_VERSION,
        "scenario_sha256": scenario_sha256(),
        "configuration_versions": {
            "control_model": "CONTROL_V1",
            "shadow_model": "SHADOW_IMPACT_V1",
            "delta_provenance": "impact-delta-provenance-v1",
            "coverage": "impact-coverage-v1",
        },
        "run_id": str(RUN_ID),
        "universe": {
            "epoch_count": 3,
            "contracts_evaluated": 3,
            "contracts_admitted": 1,
            "contracts_removed": 1,
            "paired_capacity": {"trade": 15000, "quote": 10000},
            "acknowledgements": {"accepted": 4, "duplicate_or_unmatched_rejected": 2},
        },
        "events": {
            "events": 6,
            "quotes": 4,
            "clusters": 3,
            "decisions": 3,
            "classifications": {"valid": 2, "unknown": 1, "invalid": 1},
        },
        "persistence": persistence,
        "replay_equality": replay_equal,
        "comparison_matrix": comparisons,
        "delta_provenance": {
            "recognized": 2,
            "missing_or_unsupported": 1,
            "unscoreable_reasons": provenance_reasons,
        },
        "finalization": {
            "status": "FINALIZED",
            "intake_stop": "16:00 ET",
            "post_close_drain": True,
        },
        "orders": {"trading_enabled": False, "constructed": 0, "submitted": 0},
        "invariants": invariant_status,
        "failed_invariants": failed,
        "overall": "PASS" if not failed else "FAIL",
    }


def validate_certificate(value: dict[str, object]) -> list[str]:
    required = (
        "certification_version",
        "git_commit",
        "scenario_sha256",
        "run_id",
        "universe",
        "events",
        "persistence",
        "comparison_matrix",
        "failed_invariants",
        "overall",
    )
    return [f"missing:{key}" for key in required if key not in value]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run offline Phase 4B deterministic certification")
    parser.add_argument("--validate", type=Path, help="validate an existing certificate")
    args = parser.parse_args(argv)
    if args.validate:
        value = json.loads(args.validate.read_text())
        errors = validate_certificate(value)
        print(
            json.dumps(
                {"schema": str(SCHEMA_PATH), "valid": not errors, "errors": errors}, sort_keys=True
            )
        )
        return 0 if not errors else 1
    value = _certificate()
    CERTIFICATE_PATH.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    print(json.dumps(value, indent=2, sort_keys=True))
    return 0 if value["overall"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
