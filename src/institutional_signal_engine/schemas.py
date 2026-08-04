"""Canonical, provider-independent event and decision schemas."""

from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import BaseModel, ConfigDict, Field, field_validator


def utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return value.astimezone(UTC)


class EventKind(StrEnum):
    EQUITY = "equity"
    OPTIONS = "options"
    MARKET_INDEX = "market_index"
    SECTOR_INDEX = "sector_index"


class CanonicalEvent(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    event_id: UUID
    kind: EventKind
    symbol: str
    source: str
    source_timestamp: datetime
    received_timestamp: datetime
    normalized_timestamp: datetime
    sequence: int = Field(ge=0)
    payload: dict[str, Any]

    _utc_source = field_validator("source_timestamp")(utc)
    _utc_received = field_validator("received_timestamp")(utc)
    _utc_normalized = field_validator("normalized_timestamp")(utc)


class SynchronizedInput(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    symbol: str
    as_of: datetime
    price: Decimal
    volume: int
    option_volume: int
    open_interest: int
    call_premium: Decimal
    spread: Decimal
    distance_to_resistance: Decimal
    relative_volume: Decimal
    equity_delta: Decimal
    market_delta: Decimal
    sector_delta: Decimal
    first_signal_at: datetime | None = None
    concurrent_positions: int = Field(ge=0)
    event_ids: tuple[UUID, ...]

    _utc_as_of = field_validator("as_of")(utc)
    _utc_first = field_validator("first_signal_at")(lambda v: utc(v) if v else v)


class GateResult(BaseModel):
    model_config = ConfigDict(frozen=True)
    name: str
    passed: bool
    reason: str


class Candidate(BaseModel):
    model_config = ConfigDict(frozen=True)
    symbol: str
    freshness_score: Decimal
    call_premium: Decimal
    option_volume_oi_ratio: Decimal
    relative_volume: Decimal
    distance_to_resistance: Decimal
    ordinal: int
    gates: tuple[GateResult, ...]


class Decision(BaseModel):
    model_config = ConfigDict(frozen=True)
    decision_id: UUID
    decided_at: datetime
    selected_symbol: str | None
    fire: bool
    candidates: tuple[Candidate, ...]
    rejection_reasons: tuple[str, ...]
    input_event_ids: tuple[UUID, ...]
    config_version: str
    engine_version: str

    _utc_decided = field_validator("decided_at")(utc)

    @classmethod
    def deterministic_id(cls, event_ids: tuple[UUID, ...], timestamp: datetime) -> UUID:
        return uuid5(
            NAMESPACE_URL, f"decision:{timestamp.isoformat()}:{','.join(map(str, event_ids))}"
        )
