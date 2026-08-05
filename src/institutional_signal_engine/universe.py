"""Typed option-universe selection and reconciliation."""

from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol
from uuid import UUID

from .providers.thetadata import ThetaContract

PILOT_SYMBOLS: tuple[str, ...] = (
    "AAPL",
    "MSFT",
    "NVDA",
    "AMZN",
    "META",
    "GOOGL",
    "TSLA",
    "AMD",
    "JPM",
    "BAC",
    "XOM",
    "CVX",
    "UNH",
    "LLY",
    "COST",
    "WMT",
    "NFLX",
    "AVGO",
    "ORCL",
    "CRM",
)


@dataclass(frozen=True)
class ContractEvidence:
    contract: ThetaContract
    underlying_price: Decimal
    bid: Decimal | None
    ask: Decimal | None
    trade_volume: int | None
    open_interest: int | None


class OptionUniverseProvider(Protocol):
    def expirations(self, symbol: str, as_of: int) -> tuple[int, ...]: ...
    def strikes(self, symbol: str, expiration: int) -> tuple[int, ...]: ...
    def evidence(
        self, symbol: str, expiration: int, strikes: tuple[int, ...]
    ) -> tuple[ContractEvidence, ...]: ...


class ContractUniverseSelector:
    def __init__(self, max_contracts: int = 15_000) -> None:
        self.max_contracts = max_contracts

    def select_calls(
        self, evidence: tuple[ContractEvidence, ...], today: int
    ) -> tuple[ThetaContract, ...]:
        valid = [
            item
            for item in evidence
            if item.contract.right == "C"
            and item.contract.expiration >= today
            and item.trade_volume is not None
            and item.trade_volume > 0
            and item.open_interest is not None
            and item.open_interest > 0
            and item.bid is not None
            and item.ask is not None
            and item.ask >= item.bid
        ]
        valid.sort(
            key=lambda item: (
                item.contract.expiration,
                abs(Decimal(item.contract.strike) / Decimal(1000) - item.underlying_price),
                -int(item.trade_volume or 0),
                item.contract.strike,
            )
        )
        return tuple(item.contract for item in valid[: self.max_contracts])


@dataclass(frozen=True)
class SymbolEligibilityEvidence:
    symbol: str
    market_cap: Decimal | None
    average_daily_dollar_volume: Decimal | None
    listed_options: bool | None
    average_options_volume: Decimal | None

    def evaluate(self) -> tuple[bool, tuple[str, ...]]:
        reasons: list[str] = []
        if self.market_cap is None or self.market_cap < Decimal(10000000000):
            reasons.append("market_cap_below_10b_or_missing")
        if self.average_daily_dollar_volume is None or self.average_daily_dollar_volume < Decimal(
            50000000
        ):
            reasons.append("average_daily_dollar_volume_below_50m_or_missing")
        if self.listed_options is not True:
            reasons.append("listed_options_missing_or_false")
        if self.average_options_volume is None or self.average_options_volume < Decimal(2000):
            reasons.append("average_options_volume_below_2000_or_missing")
        return not reasons, tuple(reasons)


@dataclass(frozen=True)
class UniverseSelection:
    symbol: str
    included: bool
    expiration: int | None
    contracts: tuple[ThetaContract, ...]
    rejection_reasons: tuple[str, ...]
    provenance: tuple[str, ...]
    provider_responses: tuple[dict[str, object], ...]


class UniverseLimitExceeded(ValueError):
    """The plan cannot be connected because it exceeds the Standard limit."""


class Phase4UniverseSelector:
    """Deterministically select the bounded liquid pilot universe."""

    def __init__(
        self,
        option_provider: OptionUniverseProvider,
        max_contracts: int = 15_000,
        max_moneyness_distance: Decimal = Decimal("0.20"),
    ) -> None:
        self.option_provider = option_provider
        self.max_contracts = max_contracts
        self.max_moneyness_distance = max_moneyness_distance

    def select_symbol(self, evidence: SymbolEligibilityEvidence, today: int) -> UniverseSelection:
        symbol = evidence.symbol.upper()
        eligible, reasons = evidence.evaluate()
        if not eligible:
            return UniverseSelection(symbol, False, None, (), reasons, ("eligibility",), ())
        responses: list[dict[str, object]] = []
        for expiration in sorted(self.option_provider.expirations(symbol, today)):
            if expiration < today:
                continue
            strikes = tuple(sorted(set(self.option_provider.strikes(symbol, expiration))))
            responses.append(
                {
                    "endpoint": "strikes",
                    "symbol": symbol,
                    "expiration": expiration,
                    "count": len(strikes),
                }
            )
            candidate_evidence = self.option_provider.evidence(symbol, expiration, strikes)
            selected = tuple(
                sorted(
                    {
                        item.contract
                        for item in candidate_evidence
                        if self._qualifies_contract(item)
                    },
                    key=lambda contract: (contract.expiration, contract.strike, contract.right),
                )
            )
            if selected:
                return UniverseSelection(
                    symbol,
                    True,
                    expiration,
                    selected,
                    (),
                    ("eligibility", "nearest_future_expiration", "liquidity", "moneyness"),
                    tuple(responses),
                )
        return UniverseSelection(
            symbol,
            False,
            None,
            (),
            ("no_liquid_near_term_call_contract",),
            ("eligibility", "expiration_discovery", "strike_discovery", "liquidity", "moneyness"),
            tuple(responses),
        )

    def select_pilot(
        self,
        evidence: tuple[SymbolEligibilityEvidence, ...],
        today: int,
    ) -> tuple[UniverseSelection, ...]:
        selections = tuple(
            self.select_symbol(item, today)
            for item in sorted(evidence, key=lambda value: value.symbol)
        )
        total = sum(len(item.contracts) for item in selections)
        if total > self.max_contracts:
            raise UniverseLimitExceeded(f"subscription_plan_exceeds_{self.max_contracts}_contracts")
        return selections

    def _qualifies_contract(self, item: ContractEvidence) -> bool:
        if item.contract.right != "C" or item.trade_volume is None or item.trade_volume < 1:
            return False
        if item.open_interest is None or item.open_interest < 1:
            return False
        if item.bid is None or item.ask is None or item.bid <= 0 or item.ask < item.bid:
            return False
        if item.underlying_price <= 0:
            return False
        strike_dollars = Decimal(item.contract.strike) / Decimal(1000)
        return (
            abs(strike_dollars - item.underlying_price) / item.underlying_price
            <= self.max_moneyness_distance
        )


def subscription_plan(selections: tuple[UniverseSelection, ...]) -> tuple[ThetaContract, ...]:
    """Return a stable, duplicate-free Standard subscription plan."""
    contracts = {
        contract
        for selection in selections
        if selection.included
        for contract in selection.contracts
    }
    return tuple(
        sorted(
            contracts, key=lambda value: (value.root, value.expiration, value.strike, value.right)
        )
    )


def selection_audits(
    run_id: UUID,
    selections: tuple[UniverseSelection, ...],
    plan: tuple[ThetaContract, ...],
) -> tuple[dict[str, object], ...]:
    """Create sanitized, replayable audit payloads for every pilot symbol."""
    plan_values = [
        {
            "root": contract.root,
            "expiration": contract.expiration,
            "strike": contract.strike,
            "right": contract.right,
        }
        for contract in plan
    ]
    return tuple(
        {
            "run_id": str(run_id),
            "symbol": selection.symbol,
            "included": selection.included,
            "expiration": selection.expiration,
            "contracts": [
                {
                    "root": contract.root,
                    "expiration": contract.expiration,
                    "strike": contract.strike,
                    "right": contract.right,
                }
                for contract in selection.contracts
            ],
            "rejection_reasons": list(selection.rejection_reasons),
            "provenance": list(selection.provenance),
            "provider_responses": list(selection.provider_responses),
            "subscription_plan": plan_values,
        }
        for selection in selections
    )


def reconcile(
    previous: tuple[ThetaContract, ...], selected: tuple[ThetaContract, ...]
) -> tuple[tuple[ThetaContract, ...], tuple[ThetaContract, ...]]:
    old, new = set(previous), set(selected)
    return tuple(
        sorted(new - old, key=lambda value: (value.expiration, value.strike, value.right))
    ), tuple(sorted(old - new, key=lambda value: (value.expiration, value.strike, value.right)))
