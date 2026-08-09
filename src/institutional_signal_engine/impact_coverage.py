"""Deterministic conditional coverage analysis for SHADOW_IMPACT_V1."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

from .impact import (
    IMPACT_COVERAGE_VERSION,
    IMPACT_PRIORITY_VERSION,
    IMPACT_REQUIRED_EVIDENCE_FIELDS,
    IMPACT_REQUIRED_EVIDENCE_VERSION,
)

CoverageClass = Literal["MUST_OBSERVE", "PROVABLY_EXCLUDABLE", "UNRESOLVED"]


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
            "impact_capability": str(self.impact_capability),
            "liquidity_quality": str(self.liquidity_quality),
            "evidence_completeness": str(self.evidence_completeness),
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
        }


def build_coverage_plan(
    candidates: tuple[CoverageCandidate, ...],
    trade_limit: int = 15_000,
    quote_limit: int = 10_000,
) -> CoveragePlan:
    """Allocate U* without silently truncating and preserve every exclusion."""
    theoretical = tuple(
        candidate
        for candidate in candidates
        if candidate.coverage_class in {"MUST_OBSERVE", "UNRESOLVED"}
    )
    ordered = tuple(sorted(theoretical, key=lambda candidate: candidate.key))
    capacity = min(trade_limit, quote_limit)
    selected = ordered[:capacity]
    excluded = tuple(
        {
            **candidate.as_dict(),
            "rank": rank,
            "binding_capacity": "TRADE_AND_QUOTE"
            if trade_limit == quote_limit
            else ("TRADE" if len(ordered) > trade_limit else "QUOTE"),
            "reason": "CAPACITY_CONSTRAINED_COVERAGE",
        }
        for rank, candidate in enumerate(ordered, 1)
        if candidate not in selected
    )
    complete = len(ordered) <= trade_limit and len(ordered) <= quote_limit
    return CoveragePlan(
        candidates=candidates,
        selected=selected,
        excluded=excluded,
        trade_limit=trade_limit,
        quote_limit=quote_limit,
        complete_conditional_coverage=complete,
        status="CONDITIONAL_ZERO_MISS" if complete else "CAPACITY_CONSTRAINED_COVERAGE",
    )
