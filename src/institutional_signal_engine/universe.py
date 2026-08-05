"""Typed option-universe selection and reconciliation."""

from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol

from .providers.thetadata import ThetaContract


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


def reconcile(
    previous: tuple[ThetaContract, ...], selected: tuple[ThetaContract, ...]
) -> tuple[tuple[ThetaContract, ...], tuple[ThetaContract, ...]]:
    old, new = set(previous), set(selected)
    return tuple(
        sorted(new - old, key=lambda value: (value.expiration, value.strike, value.right))
    ), tuple(sorted(old - new, key=lambda value: (value.expiration, value.strike, value.right)))
