"""Canonical, provider-independent event and decision schemas."""

from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import BaseModel, ConfigDict, Field, field_validator

from .thetadata_conditions import CONDITION_MAPPING_VERSION


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
    run_id: UUID = UUID(int=0)
    ingest_order: int = Field(default=0, ge=0)
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
    price: Decimal | None
    volume: int | None
    option_volume: int | None
    open_interest: int | None
    call_premium: Decimal | None
    spread: Decimal | None
    distance_to_resistance: Decimal | None
    resistance_state: str | None = None
    relative_volume: Decimal | None
    equity_delta: Decimal | None
    market_delta: Decimal | None
    sector_delta: Decimal | None
    first_signal_at: datetime | None = None
    concurrent_positions: int = Field(ge=0)
    event_ids: tuple[UUID, ...]
    indicator_reasons: tuple[str, ...] = ()
    provenance: dict[str, str] = Field(default_factory=dict)
    ask_side_percentage: Decimal | None = None
    quote_validity: str | None = None

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


class CandidateCounters(BaseModel):
    model_config = ConfigDict(frozen=True)
    candidates_evaluated: int = Field(ge=0)
    candidates_passing_S: int = Field(ge=0)
    candidates_passing_S_and_F_and_R: int = Field(ge=0)
    executable_candidates: int = Field(ge=0)


class Decision(BaseModel):
    model_config = ConfigDict(frozen=True)
    decision_id: UUID
    run_id: UUID = UUID(int=0)
    decision_order: int = Field(default=0, ge=0)
    decided_at: datetime
    selected_symbol: str | None
    fire: bool
    candidates: tuple[Candidate, ...]
    rejection_reasons: tuple[str, ...]
    input_event_ids: tuple[UUID, ...]
    config_version: str
    engine_version: str
    condition_mapping_version: str = CONDITION_MAPPING_VERSION
    triggering_change_reasons: tuple[str, ...] = ()
    synchronized_state_identity: str = ""
    counters: CandidateCounters = CandidateCounters(
        candidates_evaluated=0,
        candidates_passing_S=0,
        candidates_passing_S_and_F_and_R=0,
        executable_candidates=0,
    )

    _utc_decided = field_validator("decided_at")(utc)

    @classmethod
    def deterministic_id(cls, event_ids: tuple[UUID, ...], timestamp: datetime) -> UUID:
        del timestamp
        return uuid5(NAMESPACE_URL, f"decision:{','.join(map(str, event_ids))}")
