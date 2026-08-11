"""Deterministic conditional coverage analysis for SHADOW_IMPACT_V1."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Literal, cast

from .impact import (
    IMPACT_COVERAGE_VERSION,
    IMPACT_PRIORITY_VERSION,
    IMPACT_REQUIRED_EVIDENCE_FIELDS,
    IMPACT_REQUIRED_EVIDENCE_VERSION,
)
from .universe import UniverseSelection

CoverageClass = Literal["MUST_OBSERVE", "PROVABLY_EXCLUDABLE", "UNRESOLVED"]
PILOT_COVERAGE_POPULATION_VERSION = "phase4b-pilot-coverage-population-v1"
UNKNOWN_EVIDENCE_POLICY_VERSION = "phase4b-unresolved-priority-v1"


@dataclass(frozen=True)
class CoverageCandidate:
    symbol: str
    expiration: int
    strike: int
    right: str
    upper_z: Decimal | None
    hard_upper_bound: bool
    liquidity_rank: int
    candidate_count: int
    available_evidence_fields: tuple[str, ...]
    coverage_class: CoverageClass
    epistemic_inputs: tuple[dict[str, str], ...] = ()
    missing_evidence_reasons: tuple[str, ...] = ()
    evidence_provenance: tuple[str, ...] = ()
    preprocessing_stage: str = "FINAL_LIQUIDITY_QUALIFIED"
    distance_from_underlying: Decimal | None = None

    @property
    def coverage_set(self) -> str:
        return {
            "MUST_OBSERVE": "U_M",
            "PROVABLY_EXCLUDABLE": "U_X",
            "UNRESOLVED": "U_R",
        }[self.coverage_class]

    @property
    def impact_capability(self) -> Decimal:
        if self.coverage_class == "PROVABLY_EXCLUDABLE":
            return Decimal(0)
        if self.upper_z is None:
            return Decimal("0.5")
        return self.upper_z / (Decimal(1) + self.upper_z)

    @property
    def liquidity_quality(self) -> Decimal:
        if self.candidate_count <= 1:
            return Decimal(1)
        return Decimal(1) - Decimal(self.liquidity_rank - 1) / Decimal(self.candidate_count - 1)

    @property
    def evidence_completeness(self) -> Decimal:
        return Decimal(
            len(set(self.available_evidence_fields) & set(IMPACT_REQUIRED_EVIDENCE_FIELDS))
        ) / Decimal(len(IMPACT_REQUIRED_EVIDENCE_FIELDS))

    @property
    def priority(self) -> Decimal:
        return self.impact_capability * self.liquidity_quality * self.evidence_completeness

    @property
    def key(self) -> tuple[object, ...]:
        return (
            -self.priority,
            0 if self.coverage_class == "MUST_OBSERVE" else 1,
            -self.impact_capability,
            -self.liquidity_quality,
            -self.evidence_completeness,
            self.symbol,
            self.distance_from_underlying is None,
            self.distance_from_underlying or Decimal(0),
            self.expiration,
            self.strike,
            self.right,
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "expiration": self.expiration,
            "strike": self.strike,
            "right": self.right,
            "upper_z": str(self.upper_z) if self.upper_z is not None else None,
            "hard_upper_bound": self.hard_upper_bound,
            "coverage_class": self.coverage_class,
            "coverage_set": self.coverage_set,
            "epistemic_inputs": [dict(value) for value in self.epistemic_inputs],
            "missing_evidence_reasons": list(self.missing_evidence_reasons),
            "evidence_provenance": list(self.evidence_provenance),
            "preprocessing_stage": self.preprocessing_stage,
            "unresolved_priority_policy_version": UNKNOWN_EVIDENCE_POLICY_VERSION,
            "impact_capability": str(self.impact_capability),
            "liquidity_quality": str(self.liquidity_quality),
            "evidence_completeness": str(self.evidence_completeness),
            "available_evidence_fields": list(self.available_evidence_fields),
            "liquidity_rank": self.liquidity_rank,
            "candidate_count": self.candidate_count,
            "distance_from_underlying": (
                str(self.distance_from_underlying)
                if self.distance_from_underlying is not None
                else None
            ),
            "priority": str(self.priority),
            "coverage_version": IMPACT_COVERAGE_VERSION,
            "priority_version": IMPACT_PRIORITY_VERSION,
            "required_evidence_version": IMPACT_REQUIRED_EVIDENCE_VERSION,
            "required_evidence_fields": list(IMPACT_REQUIRED_EVIDENCE_FIELDS),
        }


@dataclass(frozen=True)
class CoveragePlan:
    candidates: tuple[CoverageCandidate, ...]
    selected: tuple[CoverageCandidate, ...]
    excluded: tuple[dict[str, object], ...]
    trade_limit: int
    quote_limit: int
    complete_conditional_coverage: bool
    status: str
    max_contracts_per_symbol: int = 1_000

    def as_dict(self) -> dict[str, object]:
        return {
            "coverage_version": IMPACT_COVERAGE_VERSION,
            "trade_limit": self.trade_limit,
            "quote_limit": self.quote_limit,
            "candidates": [candidate.as_dict() for candidate in self.candidates],
            "selected": [candidate.as_dict() for candidate in self.selected],
            "excluded": list(self.excluded),
            "complete_conditional_coverage": self.complete_conditional_coverage,
            "status": self.status,
            "candidate_population_version": PILOT_COVERAGE_POPULATION_VERSION,
            "candidate_population": "ALPACA_DISCOVERED_20_SYMBOL_PHASE4_PILOT_AFTER_LIQUIDITY",
            "theoretical_u_star": [candidate.as_dict() for candidate in self._theoretical()],
            "counts": {
                "candidates": len(self.candidates),
                "u_m": sum(candidate.coverage_set == "U_M" for candidate in self.candidates),
                "u_x": sum(candidate.coverage_set == "U_X" for candidate in self.candidates),
                "u_r": sum(candidate.coverage_set == "U_R" for candidate in self.candidates),
                "u_star": len(self._theoretical()),
                "selected": len(self.selected),
                "capacity_excluded": sum(
                    value.get("reason") == "CAPACITY_CONSTRAINED_COVERAGE"
                    for value in self.excluded
                ),
            },
            "paired_stream_limit": min(self.trade_limit, self.quote_limit),
            "max_contracts_per_symbol": self.max_contracts_per_symbol,
        }

    def _theoretical(self) -> tuple[CoverageCandidate, ...]:
        return tuple(
            candidate
            for candidate in self.candidates
            if candidate.coverage_class in {"MUST_OBSERVE", "UNRESOLVED"}
        )


def build_coverage_plan(
    candidates: tuple[CoverageCandidate, ...],
    trade_limit: int = 15_000,
    quote_limit: int = 10_000,
    max_contracts_per_symbol: int = 1_000,
) -> CoveragePlan:
    """Allocate U* without silently truncating and preserve every exclusion."""
    theoretical = tuple(
        candidate
        for candidate in candidates
        if candidate.coverage_class in {"MUST_OBSERVE", "UNRESOLVED"}
    )
    if min(trade_limit, quote_limit, max_contracts_per_symbol) < 1:
        raise ValueError("coverage limits must be positive")
    ordered = tuple(sorted(theoretical, key=lambda candidate: candidate.key))
    capacity = min(trade_limit, quote_limit)
    selected_list: list[CoverageCandidate] = []
    selected_per_symbol: dict[str, int] = {}
    for candidate in ordered:
        if len(selected_list) >= capacity:
            break
        count = selected_per_symbol.get(candidate.symbol, 0)
        if count >= max_contracts_per_symbol:
            continue
        selected_list.append(candidate)
        selected_per_symbol[candidate.symbol] = count + 1
    selected = tuple(selected_list)
    capacity_excluded = tuple(
        {
            **candidate.as_dict(),
            "rank": rank,
            "binding_capacity": "TRADE_AND_QUOTE"
            if trade_limit == quote_limit
            else (
                "TRADE"
                if len(ordered) > trade_limit
                else "QUOTE"
                if len(ordered) > quote_limit
                else "MAX_CONTRACTS_PER_SYMBOL"
            ),
            "reason": "CAPACITY_CONSTRAINED_COVERAGE",
        }
        for rank, candidate in enumerate(ordered, 1)
        if candidate not in selected and candidate.coverage_class in {"MUST_OBSERVE", "UNRESOLVED"}
    )
    non_observed = tuple(
        {
            **candidate.as_dict(),
            "rank": None,
            "binding_capacity": None,
            "reason": "PROVABLY_EXCLUDABLE_U_X",
        }
        for candidate in candidates
        if candidate.coverage_class == "PROVABLY_EXCLUDABLE"
    )
    excluded = capacity_excluded + non_observed
    complete = (
        len(ordered) <= trade_limit
        and len(ordered) <= quote_limit
        and all(
            sum(candidate.symbol == symbol for candidate in ordered) <= max_contracts_per_symbol
            for symbol in {candidate.symbol for candidate in ordered}
        )
    )
    return CoveragePlan(
        candidates=candidates,
        selected=selected,
        excluded=excluded,
        trade_limit=trade_limit,
        quote_limit=quote_limit,
        complete_conditional_coverage=complete,
        status="CONDITIONAL_ZERO_MISS" if complete else "CAPACITY_CONSTRAINED_COVERAGE",
        max_contracts_per_symbol=max_contracts_per_symbol,
    )


def build_pilot_coverage_candidates(
    selections: tuple[UniverseSelection, ...],
    baseline_symbols: frozenset[str] = frozenset(),
) -> tuple[CoverageCandidate, ...]:
    """Build replayable U_M/U_X/U_R candidates from the bounded pilot output.

    Prospective delta-equivalent demand bounds are not available from the
    current provider evidence, so these candidates remain U_R. This helper
    never turns missing evidence into a hard exclusion or an impact claim.
    """
    candidates: list[CoverageCandidate] = []
    for selection in selections:
        evidence_by_key = {
            (
                int(str(value.get("expiration", 0))),
                int(str(value.get("strike", 0))),
                str(value.get("right", "")),
            ): value
            for value in selection.contract_evidence
        }
        ordered = tuple(
            sorted(
                set(selection.contracts),
                key=lambda contract: (contract.expiration, contract.strike, contract.right),
            )
        )
        for rank, contract in enumerate(ordered, 1):
            evidence = evidence_by_key.get(
                (contract.expiration, contract.strike, contract.right), {}
            )
            has_quote = bool(evidence.get("bid_price") and evidence.get("ask_price"))
            has_baseline = selection.symbol.upper() in baseline_symbols
            inputs = (
                {"field": "prospective_delta_equivalent_upper_bound", "status": "UNKNOWN"},
                {
                    "field": "quote_classification",
                    "status": "OBSERVED" if has_quote else "UNKNOWN",
                },
                {
                    "field": "volume_baseline",
                    "status": "OBSERVED" if has_baseline else "UNKNOWN",
                },
                {
                    "field": "volatility_baseline",
                    "status": "OBSERVED" if has_baseline else "UNKNOWN",
                },
                {"field": "delta", "status": "UNKNOWN"},
                {"field": "exchange_identity", "status": "UNKNOWN"},
            )
            missing = (
                "PROSPECTIVE_DELTA_EQUIVALENT_UPPER_BOUND_UNAVAILABLE",
                "DELTA_PROVENANCE_UNAVAILABLE",
                "EXCHANGE_PARTICIPATION_UNAVAILABLE",
            )
            candidates.append(
                CoverageCandidate(
                    selection.symbol,
                    contract.expiration,
                    contract.strike,
                    contract.right,
                    None,
                    False,
                    rank,
                    len(ordered),
                    tuple(
                        value["field"]
                        for value in inputs
                        if value["status"] == "OBSERVED"
                        and value["field"] in IMPACT_REQUIRED_EVIDENCE_FIELDS
                    ),
                    "UNRESOLVED",
                    inputs,
                    missing,
                    ("ALPACA_CONTRACT_CATALOG", "ALPACA_OPTION_SNAPSHOT"),
                    distance_from_underlying=(
                        abs(
                            Decimal(contract.strike) / Decimal(1000)
                            - Decimal(str(evidence.get("underlying_price", 0)))
                        )
                        if evidence.get("underlying_price") is not None
                        else None
                    ),
                )
            )
    return tuple(candidates)


def _candidate_from_dict(value: dict[str, object]) -> CoverageCandidate:
    raw_class = str(value.get("coverage_class", "UNRESOLVED"))
    if raw_class not in {"MUST_OBSERVE", "PROVABLY_EXCLUDABLE", "UNRESOLVED"}:
        raise ValueError("invalid_coverage_class")
    raw_inputs = value.get("epistemic_inputs", [])
    input_values = raw_inputs if isinstance(raw_inputs, (list, tuple)) else ()
    inputs = tuple(
        {"field": str(item.get("field")), "status": str(item.get("status"))}
        for item in input_values
        if isinstance(item, dict) and "field" in item and "status" in item
    )
    required_fields = value.get("available_evidence_fields", [])
    missing_reasons = value.get("missing_evidence_reasons", [])
    provenance = value.get("evidence_provenance", [])
    required_values = required_fields if isinstance(required_fields, (list, tuple)) else ()
    missing_values = missing_reasons if isinstance(missing_reasons, (list, tuple)) else ()
    provenance_values = provenance if isinstance(provenance, (list, tuple)) else ()
    raw_upper = value.get("upper_z")
    return CoverageCandidate(
        str(value["symbol"]),
        int(str(value["expiration"])),
        int(str(value["strike"])),
        str(value["right"]),
        Decimal(str(raw_upper)) if raw_upper is not None else None,
        bool(value.get("hard_upper_bound", False)),
        int(str(value.get("liquidity_rank", 1))),
        int(str(value.get("candidate_count", 1))),
        tuple(str(item) for item in required_values),
        raw_class,  # type: ignore[arg-type]
        inputs,
        tuple(str(item) for item in missing_values),
        tuple(str(item) for item in provenance_values),
        str(value.get("preprocessing_stage", "FINAL_LIQUIDITY_QUALIFIED")),
        Decimal(str(value["distance_from_underlying"]))
        if value.get("distance_from_underlying") is not None
        else None,
    )


def replay_coverage_plan(record: dict[str, object]) -> CoveragePlan:
    """Reconstruct a coverage plan using only its persisted candidate evidence."""
    raw_candidates = record.get("candidates", [])
    candidates = (
        tuple(
            _candidate_from_dict(value)
            for value in cast(list[object] | tuple[object, ...], raw_candidates)
            if isinstance(value, dict)
        )
        if isinstance(raw_candidates, (list, tuple))
        else ()
    )
    return build_coverage_plan(
        candidates,
        trade_limit=int(str(record["trade_limit"])),
        quote_limit=int(str(record["quote_limit"])),
        max_contracts_per_symbol=int(str(record.get("max_contracts_per_symbol", 1000))),
    )
