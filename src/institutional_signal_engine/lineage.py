"""Run-scoped universe lineage and deterministic finalization records."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date
from uuid import UUID

from .contract_mapping import MappingResult
from .impact_coverage import CoveragePlan
from .providers.thetadata import ThetaContract
from .universe import UniverseSelection

LINEAGE_VERSION = "phase4b-universe-lineage-v1"


def contract_key(contract: ThetaContract) -> tuple[str, int, int, str]:
    return contract.root, contract.expiration, contract.strike, contract.right


def _identity(contract: ThetaContract | None) -> dict[str, object] | None:
    if contract is None:
        return None
    return {
        "root": contract.root,
        "expiration": contract.expiration,
        "strike": contract.strike,
        "right": contract.right,
    }


def _transition(
    run_id: UUID,
    order: int,
    stage: str,
    state: str,
    result: MappingResult,
    reason: str | None = None,
) -> dict[str, object]:
    return {
        "run_id": str(run_id),
        "transition_order": order,
        "stage": stage,
        "state": state,
        "symbol": result.source.underlying_symbol,
        "provider_identity": {
            "contract_id": result.source.contract_id,
            "occ_symbol": result.source.occ_symbol,
        },
        "canonical_identity": _identity(result.theta_contract),
        "reason": reason,
        "lineage_version": LINEAGE_VERSION,
    }


def build_contract_transitions(
    run_id: UUID,
    mappings: Iterable[MappingResult],
    coarse: tuple[UniverseSelection, ...],
    coarse_exclusions: tuple[dict[str, object], ...],
    enrichment_records: tuple[dict[str, object], ...],
    plan: CoveragePlan,
    requests: tuple[dict[str, object], ...] = (),
) -> tuple[dict[str, object], ...]:
    """Emit an immutable, terminal explanation for every discovered contract."""
    coarse_keys = {contract_key(c) for s in coarse for c in s.contracts}
    coarse_provider_ids = {
        str(item.get("provider_symbol"))
        for selection in coarse
        for item in selection.contract_evidence
        if item.get("provider_symbol") is not None
    }
    enriched_by_key: dict[tuple[str, int, int, str], dict[str, object]] = {}
    for record in enrichment_records:
        try:
            key = (
                str(record["root"]),
                int(str(record["expiration"])),
                int(str(record["strike"])),
                str(record["right"]),
            )
        except (KeyError, TypeError, ValueError):
            continue
        enriched_by_key[key] = record
    candidates = {
        contract_key(ThetaContract(c.symbol, c.expiration, c.strike, c.right)): c
        for c in plan.candidates
    }
    selected = {
        contract_key(ThetaContract(c.symbol, c.expiration, c.strike, c.right))
        for c in plan.selected
    }
    request_keys = {
        (
            str(item.get("root")),
            int(str(item.get("expiration"))),
            int(str(item.get("strike"))),
            str(item.get("right")),
        )
        for item in requests
        if all(field in item for field in ("root", "expiration", "strike", "right"))
    }
    acknowledged_keys = {
        (
            str(item.get("root")),
            int(str(item.get("expiration"))),
            int(str(item.get("strike"))),
            str(item.get("right")),
        )
        for item in requests
        if bool(item.get("acknowledged"))
    }
    exclusion_by_key: dict[tuple[str, int, int, str], dict[str, object]] = {}
    exclusion_by_provider: dict[str, dict[str, object]] = {}
    for item in coarse_exclusions:
        provider_symbol = item.get("provider_symbol")
        if provider_symbol is not None:
            exclusion_by_provider[str(provider_symbol)] = item
        identity = item.get("canonical_identity")
        if isinstance(identity, dict) and all(
            field in identity for field in ("root", "expiration", "strike", "right")
        ):
            exclusion_by_key[
                (
                    str(identity["root"]),
                    int(str(identity["expiration"])),
                    int(str(identity["strike"])),
                    str(identity["right"]),
                )
            ] = item
    output: list[dict[str, object]] = []
    order = 0
    for result in mappings:
        order += 1
        if not result.accepted or result.theta_contract is None:
            output.append(
                _transition(
                    run_id,
                    order,
                    "MAPPING_REJECTED",
                    "MAPPING_REJECTED",
                    result,
                    result.rejection_reason or "CANONICAL_IDENTITY_UNAVAILABLE",
                )
            )
            continue
        contract = result.theta_contract
        key = contract_key(contract)
        output.append(_transition(run_id, order, "MAPPED", "MAPPED", result))
        if key not in coarse_keys or result.source.occ_symbol not in coarse_provider_ids:
            excluded = exclusion_by_provider.get(
                result.source.occ_symbol, exclusion_by_key.get(key, {})
            )
            output.append(
                _transition(
                    run_id,
                    order + 1,
                    "COARSE_SHORTLIST",
                    "COARSE_EXCLUDED",
                    result,
                    str(excluded.get("reason", "COARSE_SHORTLIST_EXCLUDED")),
                )
            )
            order += 1
            continue
        output.append(_transition(run_id, order + 1, "COARSE_SHORTLIST", "COARSE_SELECTED", result))
        order += 1
        enrichment = enriched_by_key.get(key)
        reasons = (
            enrichment.get("rejection_reasons", ())
            if enrichment
            else ("ENRICHMENT_RECORD_MISSING",)
        )
        if isinstance(reasons, (list, tuple)) and reasons:
            output.append(
                _transition(
                    run_id, order + 1, "ENRICHED", "ENRICHMENT_EXCLUDED", result, str(reasons[0])
                )
            )
            order += 1
            continue
        output.append(_transition(run_id, order + 1, "ENRICHED", "ENRICHED", result))
        order += 1
        candidate = candidates.get(key)
        if candidate is None:
            output.append(
                _transition(
                    run_id,
                    order + 1,
                    "COVERAGE_CLASSIFIED",
                    "PLANNER_EXCLUDED",
                    result,
                    "COVERAGE_CANDIDATE_MISSING",
                )
            )
            order += 1
            continue
        output.append(
            _transition(run_id, order + 1, "COVERAGE_CLASSIFIED", candidate.coverage_class, result)
        )
        order += 1
        if candidate.coverage_class == "PROVABLY_EXCLUDABLE":
            output.append(
                _transition(
                    run_id,
                    order + 1,
                    "COVERAGE_CLASSIFIED",
                    "U_X_EXCLUDED",
                    result,
                    "PROVABLY_EXCLUDABLE_U_X",
                )
            )
            order += 1
            continue
        if key not in selected:
            output.append(
                _transition(
                    run_id,
                    order + 1,
                    "PLANNER_SELECTED",
                    "PLANNER_EXCLUDED",
                    result,
                    "CAPACITY_CONSTRAINED_COVERAGE",
                )
            )
            order += 1
            continue
        output.append(
            _transition(run_id, order + 1, "PLANNER_SELECTED", "PLANNER_SELECTED", result)
        )
        order += 1
        output.append(_transition(run_id, order + 1, "ALLOCATED", "ALLOCATED", result))
        order += 1
        if key not in request_keys:
            output.append(
                _transition(
                    run_id,
                    order + 1,
                    "REQUESTED",
                    "REQUEST_FAILED",
                    result,
                    "REQUEST_NOT_SUBMITTED",
                )
            )
        elif key in acknowledged_keys:
            output.append(_transition(run_id, order + 1, "ACKNOWLEDGED", "ACKNOWLEDGED", result))
        else:
            output.append(
                _transition(
                    run_id,
                    order + 1,
                    "ACKNOWLEDGED",
                    "ACKNOWLEDGEMENT_FAILED",
                    result,
                    "ACKNOWLEDGEMENT_FAILED",
                )
            )
        order += 1
    return tuple(output)


@dataclass(frozen=True)
class RunUniverseFinalization:
    run_id: UUID
    session_date: date
    transitions: tuple[dict[str, object], ...]
    coverage_plan: dict[str, object]
    final_allocation: tuple[dict[str, object], ...]
    trade_requests: tuple[dict[str, object], ...]
    quote_requests: tuple[dict[str, object], ...]
    acknowledgements: tuple[dict[str, object], ...]
    engine_commit: str | None
    policy_versions: dict[str, str]

    def record(self) -> dict[str, object]:
        return {
            "lineage_version": LINEAGE_VERSION,
            "run_id": str(self.run_id),
            "session_date": self.session_date.isoformat(),
            "contract_transitions": list(self.transitions),
            "coverage_plan": self.coverage_plan,
            "final_allocation": list(self.final_allocation),
            "trade_requests": list(self.trade_requests),
            "quote_requests": list(self.quote_requests),
            "acknowledgements": list(self.acknowledgements),
            "engine_commit": self.engine_commit,
            "policy_versions": dict(self.policy_versions),
        }


def replay_finalization(record: dict[str, object]) -> dict[str, object]:
    """Reconstruct the persisted finalization projection without live providers."""
    transitions = record.get("contract_transitions", [])
    if not isinstance(transitions, list):
        raise TypeError("invalid_contract_transitions")
    if not all(isinstance(item, dict) and "run_id" in item for item in transitions):
        raise ValueError("invalid_transition_record")
    allocation = record.get("final_allocation")
    plan = record.get("coverage_plan")
    if not isinstance(allocation, list) or not isinstance(plan, dict):
        raise TypeError("invalid_final_allocation")
    selected = plan.get("selected", [])
    if not isinstance(selected, list):
        raise TypeError("invalid_coverage_selection")
    selected_keys = {
        (
            str(item.get("symbol")),
            int(str(item.get("expiration"))),
            int(str(item.get("strike"))),
            str(item.get("right")),
        )
        for item in selected
        if isinstance(item, dict)
    }
    allocation_keys = {
        (
            str(item.get("root")),
            int(str(item.get("expiration"))),
            int(str(item.get("strike"))),
            str(item.get("right")),
        )
        for item in allocation
        if isinstance(item, dict)
    }
    if not allocation_keys.issubset(selected_keys):
        raise ValueError("allocation_not_subset_of_planner")
    raw_requests = (record.get("trade_requests", []), record.get("quote_requests", []))
    if any(not isinstance(value, list) for value in raw_requests):
        raise ValueError("invalid_request_plan")
    requests = tuple(value for value in raw_requests if isinstance(value, list))
    for request_group in requests:
        for request in request_group:
            if not isinstance(request, dict):
                raise TypeError("invalid_request")
            key = (
                str(request.get("root")),
                int(str(request.get("expiration"))),
                int(str(request.get("strike"))),
                str(request.get("right")),
            )
            if key not in allocation_keys:
                raise ValueError("request_not_in_allocation")
    # The persisted transition order is the replay order. Reconstructing the
    # projection must not sort by stage or lose corrective/failed states.
    return dict(record)
