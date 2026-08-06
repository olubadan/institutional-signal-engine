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
    PHASE4_OBSERVATION_POLICY_VERSION,
    UniverseSelection,
)


@dataclass(frozen=True)
class EnrichmentResult:
    selections: tuple[UniverseSelection, ...]
    records: tuple[dict[str, object], ...]


def finalize_liquidity(
    coarse: tuple[UniverseSelection, ...],
    mappings: tuple[MappingResult, ...],
    quotes: dict[ThetaContract, OptionQuoteEvidence],
    open_interest: dict[ThetaContract, OpenInterestEvidence],
    now: datetime,
    max_quote_age_seconds: int = 60,
    maximum_spread: Decimal = Decimal("0.01"),
    minimum_quote_size: int = 1,
) -> EnrichmentResult:
    mapping_by_contract = {
        result.theta_contract: result
        for result in mappings
        if result.accepted and result.theta_contract is not None
    }
    selections: list[UniverseSelection] = []
    records: list[dict[str, object]] = []
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
            elif quote.spread is None or quote.spread > maximum_spread:
                reasons.append("spread_threshold_failed")
            if oi is None:
                reasons.append("dated_open_interest_missing_or_identity_mismatch")
            elif oi.open_interest <= 0:
                reasons.append("dated_open_interest_missing_or_zero")
            if not reasons and quote is not None and oi is not None:
                accepted.append(contract)
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
                        "oi_evidence_source": oi.source,
                        "oi_reported_at": oi.reported_at.isoformat(),
                        "oi_effective_date": oi.effective_date.isoformat(),
                        "open_interest": oi.open_interest,
                        "open_interest_verified_as_of": oi.verified_as_of,
                        "selection_stage": "FINAL_LIQUIDITY_QUALIFIED",
                        "symbol_liquidity_evidence_source": PHASE4_LIQUIDITY_EVIDENCE_SOURCE,
                        "symbol_liquidity_verified": False,
                        "evidence_quality": "PHASE4_OBSERVATIONAL",
                        "policy_version": PHASE4_OBSERVATION_POLICY_VERSION,
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
        selections.append(
            UniverseSelection(
                selection.symbol,
                bool(accepted),
                selection.expiration if accepted else None,
                tuple(accepted),
                () if accepted else ("final_liquidity_evidence_failed",),
                (*selection.provenance, "quote_snapshot", "dated_open_interest"),
                selection.provider_responses,
                tuple(evidence),
                PHASE4_LIQUIDITY_EVIDENCE_SOURCE,
                False,
                PHASE4_OBSERVATION_POLICY_VERSION,
            )
        )
    return EnrichmentResult(tuple(selections), tuple(records))
