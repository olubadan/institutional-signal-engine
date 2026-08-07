"""Deterministic joining and final qualification of Phase 4 evidence."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from .contract_mapping import MappingResult
from .providers.alpaca_option_snapshots import OptionQuoteEvidence
from .providers.thetadata import ThetaContract
from .providers.thetadata_open_interest import OpenInterestEvidence
from .universe import (
    PHASE4_LIQUIDITY_EVIDENCE_SOURCE,
    PHASE4_SWEEP_OBSERVATION_POLICY_VERSION,
    UniverseSelection,
)

PHASE4_OBSERVATION_SPREAD_POLICY_VERSION = "phase4-observation-spread-v1"
PHASE4_OBSERVATION_MAX_ABSOLUTE_SPREAD = Decimal("0.05")
PHASE4_OBSERVATION_MAX_PROPORTIONAL_SPREAD = Decimal("0.20")


@dataclass(frozen=True)
class EnrichmentResult:
    selections: tuple[UniverseSelection, ...]
    records: tuple[dict[str, object], ...]
    diagnostics: tuple[dict[str, object], ...] = ()


def finalize_liquidity(
    coarse: tuple[UniverseSelection, ...],
    mappings: tuple[MappingResult, ...],
    quotes: dict[ThetaContract, OptionQuoteEvidence],
    open_interest: dict[ThetaContract, OpenInterestEvidence],
    now: datetime,
    max_quote_age_seconds: int = 60,
    maximum_spread: Decimal = Decimal("0.01"),
    minimum_quote_size: int = 1,
    alpaca_open_interest: dict[ThetaContract, int | None] | None = None,
    require_open_interest: bool = True,
    policy_version: str = PHASE4_SWEEP_OBSERVATION_POLICY_VERSION,
    spread_policy_version: str | None = None,
    maximum_proportional_spread: Decimal | None = None,
) -> EnrichmentResult:
    if not require_open_interest and policy_version != PHASE4_SWEEP_OBSERVATION_POLICY_VERSION:
        raise ValueError("phase4_observation_policy_required")
    mapping_by_contract = {
        result.theta_contract: result
        for result in mappings
        if result.accepted and result.theta_contract is not None
    }
    selections: list[UniverseSelection] = []
    records: list[dict[str, object]] = []
    diagnostics: list[dict[str, object]] = []
    observation_spread = not require_open_interest
    effective_absolute = (
        PHASE4_OBSERVATION_MAX_ABSOLUTE_SPREAD if observation_spread else maximum_spread
    )
    effective_proportional = (
        PHASE4_OBSERVATION_MAX_PROPORTIONAL_SPREAD
        if observation_spread and maximum_proportional_spread is None
        else maximum_proportional_spread
    )
    for selection in coarse:
        accepted: list[ThetaContract] = []
        evidence: list[dict[str, object]] = []
        for contract in selection.contracts:
            mapping = mapping_by_contract.get(contract)
            quote = quotes.get(contract)
            oi = open_interest.get(contract)
            reasons: list[str] = []
            if mapping is None:
                reasons.append("catalog_identity_missing")
            if quote is None:
                reasons.append("quote_snapshot_missing_or_identity_mismatch")
            elif not quote.valid(now, max_quote_age_seconds, minimum_quote_size):
                reasons.append("quote_snapshot_invalid_stale_crossed_or_zero_size")
            elif quote.spread is None or quote.spread > effective_absolute:
                reasons.append("spread_threshold_failed")
            elif (
                effective_proportional is not None
                and quote.spread_percentage is not None
                and quote.spread_percentage > effective_proportional
            ):
                reasons.append("proportional_spread_threshold_failed")
            alpaca_oi = (alpaca_open_interest or {}).get(contract)
            if require_open_interest and oi is None:
                reasons.append("dated_open_interest_missing_or_identity_mismatch")
            elif require_open_interest and oi is not None and oi.open_interest <= 0:
                reasons.append("dated_open_interest_missing_or_zero")
            if not reasons and quote is not None:
                accepted.append(contract)
                effective_oi = oi.open_interest if oi is not None else alpaca_oi
                oi_available = effective_oi is not None and effective_oi > 0
                if require_open_interest and not oi_available:
                    raise AssertionError("required_open_interest_passed_without_evidence")
                evidence.append(
                    {
                        "root": contract.root,
                        "expiration": contract.expiration,
                        "strike": contract.strike,
                        "right": contract.right,
                        "provider_symbol": quote.provider_symbol,
                        "feed": quote.feed,
                        "snapshot_timestamp": quote.snapshot_timestamp.isoformat(),
                        "bid_price": str(quote.bid_price),
                        "bid_size": quote.bid_size,
                        "ask_price": str(quote.ask_price),
                        "ask_size": quote.ask_size,
                        "spread": str(quote.spread),
                        "spread_percentage": str(quote.spread_percentage),
                        "latest_trade_price": str(quote.latest_trade_price)
                        if quote.latest_trade_price is not None
                        else None,
                        "latest_trade_timestamp": quote.latest_trade_timestamp.isoformat()
                        if quote.latest_trade_timestamp is not None
                        else None,
                        "oi_evidence_source": oi.source if oi is not None else None,
                        "oi_reported_at": oi.reported_at.isoformat() if oi is not None else None,
                        "oi_effective_date": oi.effective_date.isoformat()
                        if oi is not None
                        else None,
                        "open_interest": effective_oi,
                        "oi_date_source": (
                            oi.source if oi is not None else "ALPACA_UNDATED" if alpaca_oi else None
                        ),
                        "open_interest_verified_as_of": oi.verified_as_of
                        if oi is not None
                        else False,
                        "evidence_quality": "PHASE4_OBSERVATIONAL",
                        "oi_available_at_subscription": oi_available,
                        "signal_eligible_at_subscription": oi_available,
                        "subscription_purpose": "PHASE4_SWEEP_OBSERVATION",
                        "selection_stage": "FINAL_LIQUIDITY_QUALIFIED",
                        "symbol_liquidity_evidence_source": PHASE4_LIQUIDITY_EVIDENCE_SOURCE,
                        "symbol_liquidity_verified": False,
                        "policy_version": policy_version,
                    }
                )
            records.append(
                {
                    "symbol": selection.symbol,
                    "root": contract.root,
                    "expiration": contract.expiration,
                    "strike": contract.strike,
                    "right": contract.right,
                    "selection_stage": "FINAL_LIQUIDITY_QUALIFIED"
                    if not reasons
                    else "COARSE_SHORTLIST",
                    "rejection_reasons": tuple(reasons),
                    "catalog_symbol": mapping.source.occ_symbol if mapping else None,
                }
            )
            price_bucket = (
                "missing"
                if quote is None or quote.bid_price is None or quote.ask_price is None
                else "lt_0.10"
                if quote.ask_price < Decimal("0.10")
                else "0.10_to_1"
                if quote.ask_price < Decimal(1)
                else "gte_1"
            )
            abs_bucket = (
                "missing"
                if quote is None or quote.spread is None
                else "lt_0.01"
                if quote.spread < Decimal("0.01")
                else "0.01_to_0.05"
                if quote.spread <= Decimal("0.05")
                else "gt_0.05"
            )
            prop_bucket = (
                "missing"
                if quote is None or quote.spread_percentage is None
                else "lt_0.20"
                if quote.spread_percentage < Decimal("0.20")
                else "0.20_to_1"
                if quote.spread_percentage <= Decimal(1)
                else "gt_1"
            )
            diagnostics.append(
                {
                    "symbol": selection.symbol,
                    "option_price_bucket": price_bucket,
                    "absolute_spread_bucket": abs_bucket,
                    "proportional_spread_bucket": prop_bucket,
                    "absolute_limit": str(effective_absolute),
                    "proportional_limit": str(effective_proportional)
                    if effective_proportional is not None
                    else None,
                    "policy_version": spread_policy_version
                    or (
                        PHASE4_OBSERVATION_SPREAD_POLICY_VERSION
                        if observation_spread
                        else "production"
                    ),
                    "reason": reasons[0] if reasons else "accepted",
                }
            )
        selections.append(
            UniverseSelection(
                selection.symbol,
                bool(accepted),
                selection.expiration if accepted else None,
                tuple(accepted),
                () if accepted else ("final_liquidity_evidence_failed",),
                (*selection.provenance, "quote_snapshot", "optional_open_interest"),
                selection.provider_responses,
                tuple(evidence),
                PHASE4_LIQUIDITY_EVIDENCE_SOURCE,
                False,
                policy_version,
            )
        )
    return EnrichmentResult(tuple(selections), tuple(records), tuple(diagnostics))
