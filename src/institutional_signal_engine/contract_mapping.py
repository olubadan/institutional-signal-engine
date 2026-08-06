"""Canonical Alpaca/OCC/ThetaData option-contract mapping."""

import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

from .providers.thetadata import ThetaContract

CONTRACT_MAPPING_VERSION = "alpaca-occ-thetadata-v1"


@dataclass(frozen=True)
class CanonicalOptionIdentity:
    root: str
    expiration: int
    strike: int
    right: str
    multiplier: int = 100

    def theta_contract(self) -> ThetaContract:
        return ThetaContract(self.root, self.expiration, self.strike, self.right)

    def occ_symbol(self) -> str:
        expiration = (
            datetime.strptime(str(self.expiration), "%Y%m%d").replace(tzinfo=UTC).strftime("%y%m%d")
        )
        right = self.right
        return f"{self.root:<6}{expiration}{right}{self.strike:08d}"


@dataclass(frozen=True)
class AlpacaOptionContract:
    contract_id: str
    occ_symbol: str
    underlying_symbol: str
    root_symbol: str
    expiration: date
    strike: Decimal
    right: str
    style: str | None
    status: str
    tradable: bool
    open_interest: int | None
    open_interest_date: date | None
    close_price: Decimal | None
    close_price_date: date | None
    multiplier: int | None
    received_at: datetime
    original_fields: dict[str, object]

    @classmethod
    def from_payload(cls, payload: dict[str, Any], received_at: datetime) -> "AlpacaOptionContract":
        required = (
            "id",
            "symbol",
            "underlying_symbol",
            "expiration_date",
            "strike_price",
            "type",
            "status",
            "tradable",
        )
        missing = [field for field in required if field not in payload]
        if missing:
            raise ValueError("missing_fields:" + ",".join(missing))
        expiration = date.fromisoformat(str(payload["expiration_date"]))
        multiplier_value = payload.get("multiplier")
        multiplier = int(multiplier_value) if multiplier_value is not None else None
        return cls(
            str(payload["id"]),
            str(payload["symbol"]),
            str(payload["underlying_symbol"]).upper(),
            str(payload.get("root_symbol") or str(payload["underlying_symbol"])).upper(),
            expiration,
            Decimal(str(payload["strike_price"])),
            str(payload["type"]).upper(),
            str(payload["style"]) if payload.get("style") is not None else None,
            str(payload["status"]).lower(),
            bool(payload["tradable"]),
            int(payload["open_interest"]) if payload.get("open_interest") is not None else None,
            date.fromisoformat(str(payload["open_interest_date"]))
            if payload.get("open_interest_date")
            else None,
            Decimal(str(payload["close_price"]))
            if payload.get("close_price") is not None
            else None,
            date.fromisoformat(str(payload["close_price_date"]))
            if payload.get("close_price_date")
            else None,
            multiplier,
            received_at.astimezone(UTC),
            dict(payload),
        )


@dataclass(frozen=True)
class MappingResult:
    source: AlpacaOptionContract
    canonical: CanonicalOptionIdentity | None
    theta_contract: ThetaContract | None
    accepted: bool
    failed_field: str | None
    rejection_reason: str | None
    mapping_version: str = CONTRACT_MAPPING_VERSION

    def record(self) -> dict[str, object]:
        return {
            "source_provider": "alpaca",
            "contract_id": self.source.contract_id,
            "occ_symbol": self.source.occ_symbol,
            "original_fields": self.source.original_fields,
            "canonical": {
                "root": self.canonical.root,
                "expiration": self.canonical.expiration,
                "strike": self.canonical.strike,
                "right": self.canonical.right,
                "multiplier": self.canonical.multiplier,
            }
            if self.canonical is not None
            else None,
            "theta_contract": {
                "root": self.theta_contract.root,
                "expiration": self.theta_contract.expiration,
                "strike": self.theta_contract.strike,
                "right": self.theta_contract.right,
            }
            if self.theta_contract is not None
            else None,
            "mapping_version": self.mapping_version,
            "conversion_timestamp": self.source.received_at.isoformat(),
            "accepted": self.accepted,
            "failed_field": self.failed_field,
            "rejection_reason": self.rejection_reason,
        }


def decode_occ_symbol(symbol: str) -> CanonicalOptionIdentity:
    value = symbol.strip()
    padded = value
    if len(value) == 19:
        root_match = re.fullmatch(r"([A-Z0-9]{1,6})(\d{6})([CP])(\d{8})", value.upper())
        if root_match is None:
            raise ValueError("occ_symbol_encoding")
        padded = f"{root_match.group(1):<6}{root_match.group(2)}{root_match.group(3)}{root_match.group(4)}"
    if len(padded) != 21:
        raise ValueError("occ_symbol_length")
    root = padded[:6].strip().upper()
    if not root:
        raise ValueError("occ_root_missing")
    try:
        expiration = int(
            datetime.strptime(padded[6:12], "%y%m%d").replace(tzinfo=UTC).strftime("%Y%m%d")
        )
        right = padded[12]
        strike = int(padded[13:21])
    except (TypeError, ValueError) as exc:
        raise ValueError("occ_symbol_encoding") from exc
    if right not in {"C", "P"} or strike <= 0:
        raise ValueError("occ_symbol_contract_fields")
    return CanonicalOptionIdentity(root, expiration, strike, right)


def _field_identity(contract: AlpacaOptionContract) -> CanonicalOptionIdentity:
    scaled = contract.strike * Decimal(1000)
    if scaled != scaled.to_integral_value():
        raise ValueError("strike_not_exactly_tenths_of_cent")
    if contract.right not in {"CALL", "PUT", "C", "P"}:
        raise ValueError("unsupported_right")
    right = "C" if contract.right in {"CALL", "C"} else "P"
    return CanonicalOptionIdentity(
        contract.root_symbol.upper(),
        int(contract.expiration.strftime("%Y%m%d")),
        int(scaled),
        right,
    )


def map_alpaca_contract(contract: AlpacaOptionContract) -> MappingResult:
    try:
        field_identity = _field_identity(contract)
    except ValueError as exc:
        return MappingResult(contract, None, None, False, "fields", str(exc))
    try:
        occ_identity = decode_occ_symbol(contract.occ_symbol)
    except ValueError as exc:
        return MappingResult(contract, None, None, False, "occ_symbol", str(exc))
    if field_identity != occ_identity:
        for field in ("root", "expiration", "strike", "right"):
            if getattr(field_identity, field) != getattr(occ_identity, field):
                return MappingResult(
                    contract, None, None, False, field, f"occ_field_mismatch:{field}"
                )
        return MappingResult(contract, None, None, False, "occ_symbol", "occ_identity_mismatch")
    if contract.multiplier not in {None, 100}:
        return MappingResult(contract, None, None, False, "multiplier", "unsupported_deliverable")
    if contract.underlying_symbol != field_identity.root:
        return MappingResult(contract, None, None, False, "underlying_symbol", "root_mismatch")
    if contract.status != "active" or not contract.tradable:
        return MappingResult(contract, None, None, False, "status", "inactive_or_not_tradable")
    return MappingResult(
        contract, field_identity, field_identity.theta_contract(), True, None, None
    )


def round_trip_validate(result: MappingResult) -> MappingResult:
    if not result.accepted or result.canonical is None or result.theta_contract is None:
        return result
    canonical = result.canonical
    theta = result.theta_contract
    if theta != canonical.theta_contract():
        return MappingResult(
            result.source, None, None, False, "theta_contract", "theta_identity_mismatch"
        )
    if decode_occ_symbol(result.source.occ_symbol) != canonical:
        return MappingResult(result.source, None, None, False, "occ_symbol", "round_trip_mismatch")
    return result
