"""Non-authoritative journal, receipt, hashing, and verification primitives."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path
from types import MappingProxyType
from typing import Any, cast
from uuid import UUID


def canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def sha256(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def sha256_bytes(value: str) -> str:
    """Digest the exact UTF-8 serialized bytes, without reparsing or normalizing."""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


JOURNAL_KIND_SESSION_START = "session.start"
JOURNAL_KIND_CONFIGURATION = "configuration.loaded"
JOURNAL_KIND_PROVIDER_READY = "provider.ready"
JOURNAL_KIND_DISCOVERY_START = "discovery.start"
JOURNAL_KIND_DISCOVERY_COMPLETE = "discovery.complete"
JOURNAL_KIND_ENRICHMENT_START = "enrichment.start"
JOURNAL_KIND_ENRICHMENT_COMPLETE = "enrichment.complete"
JOURNAL_KIND_EPOCH_CREATED = "epoch.created"
JOURNAL_KIND_EPOCH_PERSISTED = "epoch.persisted"
JOURNAL_KIND_SUBSCRIPTION_COMMAND = "subscription.command"
JOURNAL_KIND_SUBSCRIPTION_ACKNOWLEDGEMENT = "subscription.acknowledgement"
JOURNAL_KIND_EPOCH_ACTIVATED = "epoch.activated"
JOURNAL_KIND_EVENT_ACCEPTED = "event.accepted"
JOURNAL_KIND_EVENT_REJECTED = "event.rejected"
JOURNAL_KIND_CLOCK_ADVANCED = "clock.advanced"
JOURNAL_KIND_REEVALUATION_START = "reevaluation.start"
JOURNAL_KIND_REEVALUATION_NOOP = "reevaluation.noop"
JOURNAL_KIND_PROVIDER_DISCONNECTED = "provider.disconnected"
JOURNAL_KIND_PROVIDER_RECONNECTED = "provider.reconnected"
JOURNAL_KIND_EPOCH_RESTORED = "epoch.restored"
JOURNAL_KIND_INTAKE_STOPPED = "intake.stopped"
JOURNAL_KIND_PERSISTENCE_DRAINED = "persistence.drained"
JOURNAL_KIND_SESSION_FINALIZED = "session.finalized"
JOURNAL_KIND_CLUSTER_COMPLETED = "cluster.completed"
JOURNAL_KIND_CONTROL_EVALUATED = "control.evaluated"
JOURNAL_KIND_FEATURE_VECTOR_PERSISTED = "feature_vector.persisted"
JOURNAL_KIND_SHADOW_ENQUEUED = "shadow.enqueued"
JOURNAL_KIND_SHADOW_COMPLETED = "shadow.completed"
JOURNAL_KIND_COMPARISON_COMPLETED = "comparison.completed"

MATERIAL_RECORD_KINDS = frozenset(
    {
        JOURNAL_KIND_SESSION_START,
        JOURNAL_KIND_CONFIGURATION,
        JOURNAL_KIND_PROVIDER_READY,
        JOURNAL_KIND_DISCOVERY_START,
        JOURNAL_KIND_DISCOVERY_COMPLETE,
        JOURNAL_KIND_ENRICHMENT_START,
        JOURNAL_KIND_ENRICHMENT_COMPLETE,
        JOURNAL_KIND_EPOCH_CREATED,
        JOURNAL_KIND_EPOCH_PERSISTED,
        JOURNAL_KIND_SUBSCRIPTION_COMMAND,
        JOURNAL_KIND_SUBSCRIPTION_ACKNOWLEDGEMENT,
        JOURNAL_KIND_EPOCH_ACTIVATED,
        JOURNAL_KIND_EVENT_ACCEPTED,
        JOURNAL_KIND_EVENT_REJECTED,
        JOURNAL_KIND_CLOCK_ADVANCED,
        JOURNAL_KIND_REEVALUATION_START,
        JOURNAL_KIND_REEVALUATION_NOOP,
        JOURNAL_KIND_PROVIDER_DISCONNECTED,
        JOURNAL_KIND_PROVIDER_RECONNECTED,
        JOURNAL_KIND_EPOCH_RESTORED,
        JOURNAL_KIND_INTAKE_STOPPED,
        JOURNAL_KIND_PERSISTENCE_DRAINED,
        JOURNAL_KIND_SESSION_FINALIZED,
        JOURNAL_KIND_CLUSTER_COMPLETED,
        JOURNAL_KIND_CONTROL_EVALUATED,
        JOURNAL_KIND_FEATURE_VECTOR_PERSISTED,
        JOURNAL_KIND_SHADOW_ENQUEUED,
        JOURNAL_KIND_SHADOW_COMPLETED,
        JOURNAL_KIND_COMPARISON_COMPLETED,
    }
)

ZERO_DIGEST = "0" * 64


class JournalFailure(ValueError):
    """A normal verification/projection failure with a stable code."""

    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(f"{code}:{detail}" if detail else code)
        self.code = code
        self.detail = detail


class JournalRepository:
    """Repository boundary for canonical serialized journals."""

    def save(self, run_id: UUID, serialized: str) -> str:
        raise NotImplementedError

    def load(self, identity: str) -> str:
        raise NotImplementedError


def persistence_object_identity(run_id: UUID, serialized: str) -> str:
    return f"journal://{run_id}/{sha256_bytes(serialized)}"


@dataclass(frozen=True)
class AcceptanceContract:
    """Immutable contract value; construction alone grants no certification authority."""

    contract_version: str
    certificate_version: str
    expected_run_id: UUID
    allowed_record_kinds: frozenset[str]
    expected_epoch_memberships: tuple[tuple[str, ...], ...]
    expected_clock_boundaries: tuple[str, ...]
    expected_rth_start: str
    expected_rth_stop: str
    expected_intake_stop: str
    expected_accepted_events: tuple[tuple[str, int, str, str], ...]
    expected_rejected_events: tuple[tuple[str, int, str, str], ...]
    expected_restoration_membership: tuple[str, ...]
    required_invariants: tuple[str, ...]
    allowed_lifecycle_order: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.allowed_record_kinds != MATERIAL_RECORD_KINDS:
            raise JournalFailure("CONTRACT_RECORD_VOCABULARY_INVALID")
        if not self.contract_version or not self.certificate_version:
            raise JournalFailure("CONTRACT_VERSION_INVALID")


class FileJournalRepository(JournalRepository):
    def __init__(self, directory: Path = Path("/tmp/institutional-signal-engine-journals")) -> None:
        self._directory = directory

    def save(self, run_id: UUID, serialized: str) -> str:
        self._directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        digest = sha256_bytes(serialized)
        identity = persistence_object_identity(run_id, serialized)
        target = self._directory / f"{run_id}-{digest}.json"
        if target.exists():
            if target.read_text(encoding="utf-8") != serialized:
                raise JournalFailure("PERSISTENCE_OBJECT_IMMUTABLE", identity)
            return identity
        temporary = target.with_suffix(".tmp")
        temporary.write_text(serialized, encoding="utf-8")
        os.chmod(temporary, 0o600)
        temporary.replace(target)
        return identity

    def load(self, identity: str) -> str:
        try:
            prefix, digest = identity.rsplit("/", 1)
            run_id = UUID(prefix.removeprefix("journal://"))
            if len(digest) != 64 or any(
                character not in "0123456789abcdef" for character in digest
            ):
                raise ValueError("invalid_digest")
        except ValueError as exc:
            raise JournalFailure("PERSISTENCE_IDENTITY_INVALID", identity) from exc
        target = self._directory / f"{run_id}-{digest}.json"
        try:
            return target.read_text(encoding="utf-8")
        except FileNotFoundError as exc:
            raise JournalFailure("PERSISTED_JOURNAL_MISSING", identity) from exc


class InMemoryJournalRepository(JournalRepository):
    def __init__(self) -> None:
        self._values: dict[str, str] = {}

    def save(self, run_id: UUID, serialized: str) -> str:
        identity = persistence_object_identity(run_id, serialized)
        if identity in self._values and self._values[identity] != serialized:
            raise JournalFailure("PERSISTENCE_OBJECT_IMMUTABLE", identity)
        self._values[identity] = serialized
        return identity

    def load(self, identity: str) -> str:
        try:
            return self._values[identity]
        except KeyError as exc:
            raise JournalFailure("PERSISTED_JOURNAL_MISSING", identity) from exc


@dataclass(frozen=True)
class PersistenceReceipt:
    """Untrusted persistence claim unless derived inside the authoritative operation."""

    contract_version: str
    run_id: UUID
    journal_root_digest: str
    journal_byte_sha256: str
    journal_record_count: int
    persistence_identity: str
    persistence_completed: bool
    receipt_digest: str

    def commitment(self) -> dict[str, object]:
        return {
            "contract_version": self.contract_version,
            "run_id": str(self.run_id),
            "journal_root_digest": self.journal_root_digest,
            "journal_byte_sha256": self.journal_byte_sha256,
            "journal_record_count": self.journal_record_count,
            "persistence_identity": self.persistence_identity,
            "persistence_completed": self.persistence_completed,
        }

    def as_dict(self) -> dict[str, object]:
        return {**self.commitment(), "receipt_digest": self.receipt_digest}

    def serialize(self) -> str:
        return canonical_json(self.as_dict())

    @classmethod
    def deserialize(cls, serialized: str) -> PersistenceReceipt:
        try:
            raw = cast(Mapping[str, object], json.loads(serialized))
            if type(raw["persistence_completed"]) is not bool:
                raise TypeError("persistence_completed")
            if type(raw["journal_record_count"]) is not int:
                raise TypeError("journal_record_count")
            return cls(
                contract_version=str(raw["contract_version"]),
                run_id=UUID(str(raw["run_id"])),
                journal_root_digest=str(raw["journal_root_digest"]),
                journal_byte_sha256=str(raw["journal_byte_sha256"]),
                journal_record_count=raw["journal_record_count"],
                persistence_identity=str(raw["persistence_identity"]),
                persistence_completed=raw["persistence_completed"],
                receipt_digest=str(raw["receipt_digest"]),
            )
        except Exception as exc:
            raise JournalFailure("PERSISTENCE_RECEIPT_DESERIALIZATION_FAILED") from exc

    @classmethod
    def create(
        cls,
        contract: AcceptanceContract,
        journal: VerifiedJournal,
        persistence_identity: str,
    ) -> PersistenceReceipt:
        receipt = cls(
            contract_version=contract.contract_version,
            run_id=journal.run_id,
            journal_root_digest=journal.root_digest,
            journal_byte_sha256=sha256_bytes(journal.canonical_serialization),
            journal_record_count=journal.seal.record_count,
            persistence_identity=persistence_identity,
            persistence_completed=True,
            receipt_digest="",
        )
        return replace(receipt, receipt_digest=sha256(receipt.commitment()))


@dataclass(frozen=True)
class ReplayReceipt:
    """Untrusted replay claim unless derived inside the authoritative operation."""

    contract_version: str
    run_id: UUID
    persistence_identity: str
    original_root_digest: str
    reconstructed_root_digest: str
    original_byte_sha256: str
    reconstructed_byte_sha256: str
    original_record_count: int
    reconstructed_record_count: int
    original_projection_digest: str
    replayed_projection_digest: str
    exact_equality: bool
    receipt_digest: str

    def commitment(self) -> dict[str, object]:
        return {
            "contract_version": self.contract_version,
            "run_id": str(self.run_id),
            "persistence_identity": self.persistence_identity,
            "original_root_digest": self.original_root_digest,
            "reconstructed_root_digest": self.reconstructed_root_digest,
            "original_byte_sha256": self.original_byte_sha256,
            "reconstructed_byte_sha256": self.reconstructed_byte_sha256,
            "original_record_count": self.original_record_count,
            "reconstructed_record_count": self.reconstructed_record_count,
            "original_projection_digest": self.original_projection_digest,
            "replayed_projection_digest": self.replayed_projection_digest,
            "exact_equality": self.exact_equality,
        }

    def as_dict(self) -> dict[str, object]:
        return {**self.commitment(), "receipt_digest": self.receipt_digest}

    def serialize(self) -> str:
        return canonical_json(self.as_dict())

    @classmethod
    def deserialize(cls, serialized: str) -> ReplayReceipt:
        try:
            raw = cast(Mapping[str, object], json.loads(serialized))
            if type(raw["exact_equality"]) is not bool:
                raise TypeError("exact_equality")
            if (
                type(raw["original_record_count"]) is not int
                or type(raw["reconstructed_record_count"]) is not int
            ):
                raise TypeError("record_count")
            return cls(
                contract_version=str(raw["contract_version"]),
                run_id=UUID(str(raw["run_id"])),
                persistence_identity=str(raw["persistence_identity"]),
                original_root_digest=str(raw["original_root_digest"]),
                reconstructed_root_digest=str(raw["reconstructed_root_digest"]),
                original_byte_sha256=str(raw["original_byte_sha256"]),
                reconstructed_byte_sha256=str(raw["reconstructed_byte_sha256"]),
                original_record_count=raw["original_record_count"],
                reconstructed_record_count=raw["reconstructed_record_count"],
                original_projection_digest=str(raw["original_projection_digest"]),
                replayed_projection_digest=str(raw["replayed_projection_digest"]),
                exact_equality=raw["exact_equality"],
                receipt_digest=str(raw["receipt_digest"]),
            )
        except Exception as exc:
            raise JournalFailure("REPLAY_RECEIPT_DESERIALIZATION_FAILED") from exc

    @classmethod
    def from_values(cls, **values: object) -> ReplayReceipt:
        receipt = cls(**values, receipt_digest="")  # type: ignore[arg-type]
        return cls(**values, receipt_digest=sha256(receipt.commitment()))  # type: ignore[arg-type]


@dataclass(frozen=True)
class JournalRecord:
    sequence: int
    kind: str
    timestamp: datetime
    run_id: UUID
    parent_sequence: int | None
    operation_id: str
    cause_operation_id: str | None
    correlation_id: str
    previous_record_digest: str
    epoch_sequence: int | None
    epoch_id: str | None
    contract_identity: str | None
    command_id: str | None
    ack_id: str | None
    payload_digest: str
    record_digest: str
    payload: Mapping[str, object]

    def digest_input(self) -> dict[str, object]:
        return {
            "sequence": self.sequence,
            "kind": self.kind,
            "timestamp": self.timestamp.isoformat(),
            "run_id": str(self.run_id),
            "parent_sequence": self.parent_sequence,
            "operation_id": self.operation_id,
            "cause_operation_id": self.cause_operation_id,
            "correlation_id": self.correlation_id,
            "previous_record_digest": self.previous_record_digest,
            "epoch_sequence": self.epoch_sequence,
            "epoch_id": self.epoch_id,
            "contract_identity": self.contract_identity,
            "command_id": self.command_id,
            "ack_id": self.ack_id,
            "payload_digest": self.payload_digest,
            "payload": dict(self.payload),
        }

    def as_dict(self) -> dict[str, object]:
        return {**self.digest_input(), "record_digest": self.record_digest}

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> JournalRecord:
        payload = dict(cast(Mapping[str, object], value.get("payload", {})))
        return cls(
            sequence=int(cast(int, value["sequence"])),
            kind=str(value["kind"]),
            timestamp=datetime.fromisoformat(str(value["timestamp"])),
            run_id=UUID(str(value["run_id"])),
            parent_sequence=(
                int(cast(int, value["parent_sequence"]))
                if value.get("parent_sequence") is not None
                else None
            ),
            operation_id=str(value["operation_id"]),
            cause_operation_id=(
                str(value["cause_operation_id"])
                if value.get("cause_operation_id") is not None
                else None
            ),
            correlation_id=str(value["correlation_id"]),
            previous_record_digest=str(value["previous_record_digest"]),
            epoch_sequence=(
                int(cast(int, value["epoch_sequence"]))
                if value.get("epoch_sequence") is not None
                else None
            ),
            epoch_id=str(value["epoch_id"]) if value.get("epoch_id") is not None else None,
            contract_identity=(
                str(value["contract_identity"])
                if value.get("contract_identity") is not None
                else None
            ),
            command_id=str(value["command_id"]) if value.get("command_id") is not None else None,
            ack_id=str(value["ack_id"]) if value.get("ack_id") is not None else None,
            payload_digest=str(value["payload_digest"]),
            record_digest=str(value["record_digest"]),
            payload=MappingProxyType(payload),
        )


@dataclass(frozen=True)
class JournalSeal:
    run_id: UUID
    record_count: int
    terminal_sequence: int
    terminal_digest: str
    seal_digest: str

    def commitment(self) -> dict[str, object]:
        return {
            "run_id": str(self.run_id),
            "record_count": self.record_count,
            "terminal_sequence": self.terminal_sequence,
            "terminal_digest": self.terminal_digest,
        }

    def as_dict(self) -> dict[str, object]:
        return {**self.commitment(), "seal_digest": self.seal_digest}

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> JournalSeal:
        return cls(
            run_id=UUID(str(value["run_id"])),
            record_count=int(cast(int, value["record_count"])),
            terminal_sequence=int(cast(int, value["terminal_sequence"])),
            terminal_digest=str(value["terminal_digest"]),
            seal_digest=str(value["seal_digest"]),
        )


@dataclass(frozen=True)
class VerifiedJournal:
    """Low-level verified value, never a certificate capability or authority token."""

    run_id: UUID
    contract_version: str
    records: tuple[JournalRecord, ...]
    seal: JournalSeal
    canonical_serialization: str

    @property
    def root_digest(self) -> str:
        return self.seal.terminal_digest

    def records_by_kind(self, kind: str) -> tuple[JournalRecord, ...]:
        return tuple(record for record in self.records if record.kind == kind)


class Journal:
    def __init__(self, run_id: UUID, clock: Any = None) -> None:
        self._run_id = run_id
        self._clock = clock
        self._records: list[JournalRecord] = []
        self._seal: JournalSeal | None = None

    @property
    def run_id(self) -> UUID:
        return self._run_id

    @property
    def records(self) -> tuple[JournalRecord, ...]:
        return tuple(self._records)

    @property
    def sealed(self) -> bool:
        return self._seal is not None

    @property
    def seal_value(self) -> JournalSeal | None:
        return self._seal

    @property
    def sequence(self) -> int:
        return len(self._records)

    def append(
        self,
        kind: str,
        *,
        timestamp: datetime | None = None,
        parent_sequence: int | None = None,
        operation_id: str | None = None,
        correlation_id: str | None = None,
        epoch_sequence: int | None = None,
        epoch_id: str | None = None,
        contract_identity: str | None = None,
        command_id: str | None = None,
        ack_id: str | None = None,
        **payload: object,
    ) -> JournalRecord:
        if self._seal is not None:
            raise JournalFailure("JOURNAL_SEALED")
        if self._clock is not None:
            timestamp = cast(datetime, self._clock())
        if timestamp is None:
            raise JournalFailure("TIMESTAMP_REQUIRED")
        safe_payload = {key: value for key, value in payload.items() if value is not None}
        payload_digest = sha256(safe_payload)
        stable_operation_id = operation_id or f"{kind}:{len(self._records)}"
        cause_operation_id = (
            self._records[parent_sequence].operation_id if parent_sequence is not None else None
        )
        record = JournalRecord(
            sequence=len(self._records),
            kind=kind,
            timestamp=timestamp,
            run_id=self._run_id,
            parent_sequence=parent_sequence,
            operation_id=stable_operation_id,
            cause_operation_id=cause_operation_id,
            correlation_id=correlation_id or stable_operation_id,
            previous_record_digest=(
                self._records[-1].record_digest if self._records else ZERO_DIGEST
            ),
            epoch_sequence=epoch_sequence,
            epoch_id=epoch_id,
            contract_identity=contract_identity,
            command_id=command_id,
            ack_id=ack_id,
            payload_digest=payload_digest,
            record_digest="",
            payload=MappingProxyType(safe_payload),
        )
        record = JournalRecord(
            **{**record.__dict__, "record_digest": sha256(record.digest_input())}
        )
        self._records.append(record)
        return record

    def seal(self) -> JournalSeal:
        if self._seal is not None:
            return self._seal
        if not self._records:
            raise JournalFailure("EMPTY_JOURNAL")
        commitment = {
            "run_id": str(self._run_id),
            "record_count": len(self._records),
            "terminal_sequence": self._records[-1].sequence,
            "terminal_digest": self._records[-1].record_digest,
        }
        self._seal = JournalSeal(
            run_id=self._run_id,
            record_count=len(self._records),
            terminal_sequence=self._records[-1].sequence,
            terminal_digest=self._records[-1].record_digest,
            seal_digest=sha256(commitment),
        )
        return self._seal

    def as_dict(self) -> dict[str, object]:
        return {
            "run_id": str(self._run_id),
            "records": [record.as_dict() for record in self._records],
            "seal": self._seal.as_dict() if self._seal is not None else None,
        }

    def serialize(self) -> str:
        return canonical_json(self.as_dict())

    @classmethod
    def deserialize(cls, data: str) -> Journal:
        try:
            raw = cast(Mapping[str, object], json.loads(data))
            journal = cls(UUID(str(raw["run_id"])))
            records = cast(list[Mapping[str, object]], raw.get("records", []))
            journal._records = [JournalRecord.from_dict(item) for item in records]
            if raw.get("seal") is not None:
                journal._seal = JournalSeal.from_dict(cast(Mapping[str, object], raw["seal"]))
            return journal
        except JournalFailure:
            raise
        except Exception as exc:
            raise JournalFailure("DESERIALIZATION_FAILED", type(exc).__name__) from exc

    def records_by_kind(self, kind: str) -> tuple[JournalRecord, ...]:
        return tuple(record for record in self._records if record.kind == kind)

    def epoch_records(self) -> tuple[JournalRecord, ...]:
        return self.records_by_kind(JOURNAL_KIND_EPOCH_CREATED)

    def subscription_commands(self) -> tuple[JournalRecord, ...]:
        return self.records_by_kind(JOURNAL_KIND_SUBSCRIPTION_COMMAND)

    def subscription_acks(self) -> tuple[JournalRecord, ...]:
        return self.records_by_kind(JOURNAL_KIND_SUBSCRIPTION_ACKNOWLEDGEMENT)

    def accepted_events(self) -> tuple[JournalRecord, ...]:
        return self.records_by_kind(JOURNAL_KIND_EVENT_ACCEPTED)

    def rejected_events(self) -> tuple[JournalRecord, ...]:
        return self.records_by_kind(JOURNAL_KIND_EVENT_REJECTED)

    def lifecycle_records(self) -> tuple[JournalRecord, ...]:
        kinds = {
            JOURNAL_KIND_PROVIDER_DISCONNECTED,
            JOURNAL_KIND_PROVIDER_RECONNECTED,
            JOURNAL_KIND_EPOCH_RESTORED,
            JOURNAL_KIND_INTAKE_STOPPED,
            JOURNAL_KIND_PERSISTENCE_DRAINED,
            JOURNAL_KIND_SESSION_FINALIZED,
        }
        return tuple(record for record in self._records if record.kind in kinds)

    def verify_integrity(self) -> tuple[bool, str]:
        try:
            verify_structural(self)
        except JournalFailure as exc:
            return False, exc.code
        return True, "ok"


def _one(records: tuple[JournalRecord, ...], kind: str) -> JournalRecord:
    found = tuple(record for record in records if record.kind == kind)
    if not found:
        raise JournalFailure("MISSING_REQUIRED_RECORD", kind)
    if len(found) != 1:
        raise JournalFailure("DUPLICATE_RECORD", kind)
    return found[0]


def verify_structural(journal: Journal) -> None:
    records = journal.records
    seal = journal.seal_value
    if seal is None:
        raise JournalFailure("UNSEALED_JOURNAL")
    if not records:
        raise JournalFailure("EMPTY_JOURNAL")
    expected_previous = ZERO_DIGEST
    previous_timestamp: datetime | None = None
    seen: set[int] = set()
    operation_ids: set[str] = set()
    for expected_sequence, record in enumerate(records):
        if record.sequence != expected_sequence or record.sequence in seen:
            raise JournalFailure("SEQUENCE_INVALID", str(record.sequence))
        seen.add(record.sequence)
        if record.run_id != journal.run_id:
            raise JournalFailure("RUN_ID_MISMATCH", str(record.sequence))
        if not record.operation_id or not record.correlation_id:
            raise JournalFailure("OPERATION_ID_INVALID", str(record.sequence))
        if record.operation_id in operation_ids:
            raise JournalFailure("OPERATION_ID_DUPLICATE", record.operation_id)
        operation_ids.add(record.operation_id)
        if previous_timestamp is not None and record.timestamp < previous_timestamp:
            raise JournalFailure("TIMESTAMP_ORDER_INVALID", str(record.sequence))
        if record.payload_digest != sha256(dict(record.payload)):
            raise JournalFailure("PAYLOAD_DIGEST_MISMATCH", str(record.sequence))
        if record.previous_record_digest != expected_previous:
            raise JournalFailure("CHAIN_BROKEN", str(record.sequence))
        if record.record_digest != sha256(record.digest_input()):
            raise JournalFailure("RECORD_DIGEST_MISMATCH", str(record.sequence))
        if record.parent_sequence is not None:
            if record.parent_sequence not in seen:
                raise JournalFailure("CAUSAL_PARENT_MISSING", str(record.sequence))
            if record.parent_sequence >= record.sequence:
                raise JournalFailure("CAUSAL_PARENT_ORDER", str(record.sequence))
            parent = records[record.parent_sequence]
            if record.cause_operation_id != parent.operation_id:
                raise JournalFailure("CAUSAL_CORRELATION_INVALID", str(record.sequence))
        elif record.cause_operation_id is not None:
            raise JournalFailure("CAUSAL_CORRELATION_INVALID", str(record.sequence))
        expected_previous = record.record_digest
        previous_timestamp = record.timestamp
    if seal.run_id != journal.run_id:
        raise JournalFailure("SEAL_RUN_ID_MISMATCH")
    if seal.record_count != len(records):
        raise JournalFailure("SEAL_RECORD_COUNT_MISMATCH")
    if seal.terminal_sequence != records[-1].sequence:
        raise JournalFailure("SEAL_TERMINAL_SEQUENCE_MISMATCH")
    if seal.terminal_digest != records[-1].record_digest:
        raise JournalFailure("SEAL_TERMINAL_DIGEST_MISMATCH")
    if seal.seal_digest != sha256(seal.commitment()):
        raise JournalFailure("SEAL_DIGEST_MISMATCH")


def verify_semantics(journal: Journal, contract: AcceptanceContract) -> None:
    records = journal.records
    by_sequence = {record.sequence: record for record in records}
    for record in records:
        if record.kind not in contract.allowed_record_kinds:
            raise JournalFailure("UNKNOWN_RECORD_KIND", record.kind)

    required_single = (
        JOURNAL_KIND_SESSION_START,
        JOURNAL_KIND_CONFIGURATION,
        JOURNAL_KIND_PROVIDER_READY,
        JOURNAL_KIND_INTAKE_STOPPED,
        JOURNAL_KIND_PERSISTENCE_DRAINED,
        JOURNAL_KIND_SESSION_FINALIZED,
    )
    singles = {kind: _one(records, kind) for kind in required_single}
    final_sequence = singles[JOURNAL_KIND_SESSION_FINALIZED].sequence
    if final_sequence != len(records) - 1:
        raise JournalFailure("RECORD_AFTER_FINALIZATION")
    if records[0].kind != JOURNAL_KIND_SESSION_START:
        raise JournalFailure("SESSION_START_NOT_FIRST")

    def parent_is(record: JournalRecord, kind: str) -> JournalRecord:
        if record.parent_sequence is None:
            raise JournalFailure("CAUSAL_PARENT_WRONG", f"{record.sequence}:{kind}")
        parent = by_sequence[record.parent_sequence]
        if parent.kind != kind:
            raise JournalFailure("CAUSAL_PARENT_WRONG", f"{record.sequence}:{kind}")
        return parent

    configuration = singles[JOURNAL_KIND_CONFIGURATION]
    if configuration.payload.get("acceptance_contract_version") != contract.contract_version:
        raise JournalFailure("JOURNAL_CONTRACT_VERSION_MISMATCH")
    if (
        configuration.payload.get("rth_start") != contract.expected_rth_start
        or configuration.payload.get("rth_stop") != contract.expected_rth_stop
        or configuration.payload.get("intake_stop") != contract.expected_intake_stop
    ):
        raise JournalFailure("SESSION_BOUNDARY_INVALID")
    parent_is(configuration, JOURNAL_KIND_SESSION_START)
    parent_is(singles[JOURNAL_KIND_PROVIDER_READY], JOURNAL_KIND_CONFIGURATION)
    parent_is(singles[JOURNAL_KIND_INTAKE_STOPPED], JOURNAL_KIND_CONFIGURATION)
    parent_is(singles[JOURNAL_KIND_PERSISTENCE_DRAINED], JOURNAL_KIND_INTAKE_STOPPED)
    parent_is(singles[JOURNAL_KIND_SESSION_FINALIZED], JOURNAL_KIND_PERSISTENCE_DRAINED)
    shutdown_correlation = singles[JOURNAL_KIND_INTAKE_STOPPED].correlation_id
    if (
        singles[JOURNAL_KIND_PERSISTENCE_DRAINED].correlation_id != shutdown_correlation
        or singles[JOURNAL_KIND_SESSION_FINALIZED].correlation_id != shutdown_correlation
    ):
        raise JournalFailure("SHUTDOWN_CORRELATION_INVALID")

    lifecycle = tuple(record.kind for record in records if record.kind in required_single)
    if lifecycle != contract.allowed_lifecycle_order:
        raise JournalFailure("LIFECYCLE_ORDER_INVALID")

    epochs: dict[int, JournalRecord] = {}
    active: JournalRecord | None = None
    commands: dict[str, JournalRecord] = {}
    acknowledgements: dict[str, JournalRecord] = {}
    event_ids: set[str] = set()
    observed_accepted: list[tuple[str, int, str, str]] = []
    observed_rejected: list[tuple[str, int, str, str]] = []
    observed_boundaries: list[str] = []
    disconnected: JournalRecord | None = None
    disconnected_membership: tuple[str, ...] | None = None
    seen_stop = False
    discovery_count = 0

    for record in records:
        if seen_stop and record.kind not in {
            JOURNAL_KIND_PERSISTENCE_DRAINED,
            JOURNAL_KIND_SESSION_FINALIZED,
        }:
            raise JournalFailure("RECORD_AFTER_INTAKE_STOP", str(record.sequence))
        if record.kind == JOURNAL_KIND_DISCOVERY_START:
            discovery_count += 1
            expected_parent = (
                JOURNAL_KIND_PROVIDER_READY
                if discovery_count == 1
                else JOURNAL_KIND_REEVALUATION_START
            )
            parent_is(record, expected_parent)
        elif record.kind == JOURNAL_KIND_DISCOVERY_COMPLETE:
            parent = parent_is(record, JOURNAL_KIND_DISCOVERY_START)
            if record.correlation_id != parent.correlation_id or (
                record.epoch_sequence != parent.epoch_sequence
            ):
                raise JournalFailure("DISCOVERY_CORRELATION_INVALID", str(record.sequence))
        elif record.kind == JOURNAL_KIND_ENRICHMENT_START:
            parent = parent_is(record, JOURNAL_KIND_DISCOVERY_COMPLETE)
            if record.epoch_sequence != parent.epoch_sequence:
                raise JournalFailure("ENRICHMENT_CORRELATION_INVALID", str(record.sequence))
        elif record.kind == JOURNAL_KIND_ENRICHMENT_COMPLETE:
            parent = parent_is(record, JOURNAL_KIND_ENRICHMENT_START)
            if record.correlation_id != parent.correlation_id or (
                record.epoch_sequence != parent.epoch_sequence
            ):
                raise JournalFailure("ENRICHMENT_CORRELATION_INVALID", str(record.sequence))
        elif record.kind == JOURNAL_KIND_EPOCH_CREATED:
            if record.epoch_sequence is None or record.epoch_sequence in epochs:
                raise JournalFailure("EPOCH_SEQUENCE_INVALID", str(record.sequence))
            parent = parent_is(record, JOURNAL_KIND_ENRICHMENT_COMPLETE)
            if record.epoch_sequence != parent.epoch_sequence:
                raise JournalFailure("EPOCH_CORRELATION_INVALID", str(record.sequence))
            if record.epoch_sequence != len(epochs) + 1 or not record.epoch_id:
                raise JournalFailure("EPOCH_SEQUENCE_INVALID", str(record.sequence))
            membership = tuple(map(str, cast(list[object], record.payload.get("membership", []))))
            if len(membership) != len(set(membership)):
                raise JournalFailure("EPOCH_MEMBERSHIP_INVALID", str(record.sequence))
            if (
                record.epoch_sequence > len(contract.expected_epoch_memberships)
                or membership != (contract.expected_epoch_memberships[record.epoch_sequence - 1])
            ):
                raise JournalFailure("EPOCH_MEMBERSHIP_INVALID", str(record.sequence))
            epochs[record.epoch_sequence] = record
        elif record.kind == JOURNAL_KIND_EPOCH_PERSISTED:
            parent = parent_is(record, JOURNAL_KIND_EPOCH_CREATED)
            if (
                record.correlation_id != parent.correlation_id
                or record.epoch_sequence != parent.epoch_sequence
                or record.epoch_id != parent.epoch_id
                or record.payload.get("content_hash") != parent.payload.get("content_hash")
            ):
                raise JournalFailure("EPOCH_PERSISTENCE_INVALID", str(record.sequence))
        elif record.kind == JOURNAL_KIND_SUBSCRIPTION_COMMAND:
            if not record.command_id or record.command_id in commands:
                raise JournalFailure("COMMAND_ID_INVALID", str(record.sequence))
            created = epochs.get(cast(int, record.epoch_sequence))
            if created is None or not record.contract_identity:
                raise JournalFailure("COMMAND_TARGET_INVALID", str(record.sequence))
            if parent_is(record, JOURNAL_KIND_EPOCH_CREATED) != created:
                raise JournalFailure("COMMAND_EPOCH_INVALID", str(record.sequence))
            if record.correlation_id != record.command_id:
                raise JournalFailure("COMMAND_CORRELATION_INVALID", str(record.sequence))
            if record.payload.get("channel") not in {"TRADE", "QUOTE"} or record.payload.get(
                "action"
            ) not in {"add", "remove"}:
                raise JournalFailure("COMMAND_CHANNEL_INVALID", str(record.sequence))
            commands[record.command_id] = record
        elif record.kind == JOURNAL_KIND_SUBSCRIPTION_ACKNOWLEDGEMENT:
            if not record.command_id or record.command_id not in commands:
                raise JournalFailure("ACK_COMMAND_UNMATCHED", str(record.sequence))
            if record.command_id in acknowledgements:
                raise JournalFailure("ACK_DUPLICATE", record.command_id)
            command = commands[record.command_id]
            if record.parent_sequence != command.sequence:
                raise JournalFailure("CAUSAL_PARENT_WRONG", str(record.sequence))
            if (
                record.correlation_id != command.correlation_id
                or record.epoch_sequence != command.epoch_sequence
                or record.epoch_id != command.epoch_id
                or record.contract_identity != command.contract_identity
                or record.payload.get("channel") != command.payload.get("channel")
                or record.payload.get("action") != command.payload.get("action")
                or record.payload.get("provider_request_id")
                != command.payload.get("provider_request_id")
                or record.payload.get("accepted") is not True
            ):
                raise JournalFailure("ACK_CORRELATION_INVALID", str(record.sequence))
            acknowledgements[record.command_id] = record
        elif record.kind == JOURNAL_KIND_EPOCH_ACTIVATED:
            created = epochs.get(cast(int, record.epoch_sequence))
            if (
                created is None
                or record.epoch_id != created.epoch_id
                or record.correlation_id != created.correlation_id
                or record.payload.get("membership") != created.payload.get("membership")
            ):
                raise JournalFailure("ACTIVATION_EPOCH_INVALID", str(record.sequence))
            activation_membership = set(map(str, cast(list[object], created.payload["membership"])))
            previous = (
                set(map(str, cast(list[object], active.payload["membership"])))
                if active is not None
                else set()
            )
            expected = {
                (contract_identity, channel, action)
                for action, identities in (
                    ("add", activation_membership - previous),
                    ("remove", previous - activation_membership),
                )
                for contract_identity in identities
                for channel in ("TRADE", "QUOTE")
            }
            epoch_commands = [
                item for item in commands.values() if item.epoch_sequence == record.epoch_sequence
            ]
            observed = {
                (
                    cast(str, item.contract_identity),
                    cast(str, item.payload.get("channel")),
                    cast(str, item.payload.get("action")),
                )
                for item in epoch_commands
            }
            if observed != expected:
                raise JournalFailure("SUBSCRIPTION_DIFF_INCOMPLETE", str(record.sequence))
            if any(item.command_id not in acknowledgements for item in epoch_commands):
                raise JournalFailure("ACTIVATION_BEFORE_ACK", str(record.sequence))
            ack_sequences = [
                acknowledgements[cast(str, item.command_id)].sequence for item in epoch_commands
            ]
            if not ack_sequences or record.parent_sequence != max(ack_sequences):
                raise JournalFailure("ACTIVATION_BEFORE_ACK", str(record.sequence))
            active = record
        elif record.kind in {JOURNAL_KIND_EVENT_ACCEPTED, JOURNAL_KIND_EVENT_REJECTED}:
            event_id = str(record.payload.get("event_id", ""))
            channel = str(record.payload.get("event_kind", ""))
            if not event_id or event_id in event_ids:
                raise JournalFailure("DUPLICATE_EVENT", event_id)
            event_ids.add(event_id)
            if active is None or record.epoch_sequence != active.epoch_sequence:
                raise JournalFailure("EVENT_EPOCH_INVALID", str(record.sequence))
            if record.parent_sequence != active.sequence or record.epoch_id != active.epoch_id:
                raise JournalFailure("CAUSAL_PARENT_WRONG", str(record.sequence))
            if (
                channel not in {"TRADE", "QUOTE"}
                or record.payload.get("symbol") != str(record.contract_identity).split(":", 1)[0]
            ):
                raise JournalFailure("EVENT_PAYLOAD_INVALID", str(record.sequence))
            event_membership = set(map(str, cast(list[object], active.payload["membership"])))
            present = record.contract_identity in event_membership
            observation = (
                event_id,
                cast(int, record.epoch_sequence),
                cast(str, record.contract_identity),
                channel,
            )
            if record.kind == JOURNAL_KIND_EVENT_ACCEPTED:
                if not present:
                    raise JournalFailure("ACCEPTED_EVENT_NOT_ACTIVE", str(record.sequence))
                observed_accepted.append(observation)
            else:
                if present:
                    raise JournalFailure("REJECTED_EVENT_ACTIVE", str(record.sequence))
                observed_rejected.append(observation)
        elif record.kind == JOURNAL_KIND_CLOCK_ADVANCED:
            boundary = str(record.payload.get("boundary", ""))
            if not boundary:
                raise JournalFailure("CLOCK_BOUNDARY_MISSING", str(record.sequence))
            if record.timestamp.isoformat() != boundary:
                raise JournalFailure("CLOCK_BOUNDARY_TIMESTAMP_INVALID", str(record.sequence))
            if active is None or record.parent_sequence != active.sequence:
                raise JournalFailure("CLOCK_EPOCH_INVALID", str(record.sequence))
            if record.epoch_sequence != active.epoch_sequence or record.epoch_id != active.epoch_id:
                raise JournalFailure("CLOCK_EPOCH_INVALID", str(record.sequence))
            observed_boundaries.append(boundary)
        elif record.kind == JOURNAL_KIND_REEVALUATION_START:
            parent = parent_is(record, JOURNAL_KIND_CLOCK_ADVANCED)
            if (
                record.correlation_id != parent.correlation_id
                or record.payload.get("boundary") != parent.payload.get("boundary")
                or record.epoch_sequence != parent.epoch_sequence
                or record.epoch_id != parent.epoch_id
            ):
                raise JournalFailure("REEVALUATION_CLOCK_CAUSE_INVALID", str(record.sequence))
        elif record.kind == JOURNAL_KIND_REEVALUATION_NOOP:
            parent_is(record, JOURNAL_KIND_REEVALUATION_START)
        elif record.kind == JOURNAL_KIND_PROVIDER_DISCONNECTED:
            if disconnected is not None:
                raise JournalFailure("RECOVERY_RECORD_COUNT_INVALID", record.kind)
            if active is None or record.epoch_sequence != active.epoch_sequence:
                raise JournalFailure("DISCONNECT_EPOCH_INVALID", str(record.sequence))
            disconnected = record
            disconnected_membership = tuple(
                map(str, cast(list[object], active.payload["membership"]))
            )
        elif record.kind == JOURNAL_KIND_PROVIDER_RECONNECTED:
            parent = parent_is(record, JOURNAL_KIND_PROVIDER_DISCONNECTED)
            if (
                disconnected is None
                or parent != disconnected
                or (record.correlation_id != disconnected.correlation_id)
            ):
                raise JournalFailure("RECONNECT_CYCLE_INVALID", str(record.sequence))
        elif record.kind == JOURNAL_KIND_EPOCH_RESTORED:
            parent = parent_is(record, JOURNAL_KIND_PROVIDER_RECONNECTED)
            membership = tuple(map(str, cast(list[object], record.payload.get("membership", []))))
            if (
                disconnected is None
                or parent.correlation_id != disconnected.correlation_id
                or record.correlation_id != disconnected.correlation_id
                or (record.epoch_sequence, record.epoch_id)
                != (disconnected.epoch_sequence, disconnected.epoch_id)
                or membership != disconnected_membership
                or membership != contract.expected_restoration_membership
            ):
                raise JournalFailure("RESTORATION_MEMBERSHIP_INVALID", str(record.sequence))
        elif record.kind == JOURNAL_KIND_INTAKE_STOPPED:
            seen_stop = True
            if (
                record.timestamp.isoformat() != contract.expected_intake_stop
                or record.payload.get("boundary") != contract.expected_intake_stop
            ):
                raise JournalFailure("INTAKE_STOP_BOUNDARY_INVALID")

    if tuple(observed_boundaries) != contract.expected_clock_boundaries:
        raise JournalFailure("SCHEDULED_BOUNDARY_INVALID")
    if tuple(observed_accepted) != contract.expected_accepted_events:
        raise JournalFailure("UNEXPECTED_ACCEPTED_EVENT")
    if tuple(observed_rejected) != contract.expected_rejected_events:
        raise JournalFailure("UNEXPECTED_REJECTED_EVENT")
    if set(epochs) != set(range(1, len(contract.expected_epoch_memberships) + 1)):
        raise JournalFailure("EPOCH_COUNT_INVALID")
    activated = tuple(
        record.epoch_sequence for record in records if record.kind == JOURNAL_KIND_EPOCH_ACTIVATED
    )
    if activated != tuple(range(1, len(contract.expected_epoch_memberships) + 1)):
        raise JournalFailure("EPOCH_ACTIVATION_MISSING")
    for kind in (
        JOURNAL_KIND_PROVIDER_DISCONNECTED,
        JOURNAL_KIND_PROVIDER_RECONNECTED,
        JOURNAL_KIND_EPOCH_RESTORED,
    ):
        if len(tuple(record for record in records if record.kind == kind)) != 1:
            raise JournalFailure("RECOVERY_RECORD_COUNT_INVALID", kind)


def verify_complete(journal: Journal, contract: AcceptanceContract) -> VerifiedJournal:
    """Low-level structural/semantic verification; never authoritative certification."""
    if journal.run_id != contract.expected_run_id:
        raise JournalFailure("CONTRACT_RUN_BINDING_MISMATCH")
    verify_structural(journal)
    verify_semantics(journal, contract)
    return VerifiedJournal(
        run_id=journal.run_id,
        contract_version=str(
            _one(journal.records, JOURNAL_KIND_CONFIGURATION).payload["acceptance_contract_version"]
        ),
        records=journal.records,
        seal=cast(JournalSeal, journal.seal_value),
        canonical_serialization=journal.serialize(),
    )


def persist_verified_journal(
    contract: AcceptanceContract,
    journal: VerifiedJournal,
    repository: JournalRepository,
) -> PersistenceReceipt:
    """Low-level save/claim helper; its result is not authoritative certification."""
    if journal.contract_version != contract.contract_version:
        raise JournalFailure("CONTRACT_VERSION_MISMATCH")
    if journal.run_id != contract.expected_run_id:
        raise JournalFailure("CONTRACT_RUN_BINDING_MISMATCH")
    identity = repository.save(journal.run_id, journal.canonical_serialization)
    return PersistenceReceipt.create(contract, journal, identity)


def verify_persistence_receipt(
    contract: AcceptanceContract,
    journal: VerifiedJournal,
    receipt: PersistenceReceipt,
) -> None:
    if receipt.receipt_digest != sha256(receipt.commitment()):
        raise JournalFailure("PERSISTENCE_RECEIPT_DIGEST_MISMATCH")
    if receipt.contract_version != contract.contract_version:
        raise JournalFailure("PERSISTENCE_CONTRACT_VERSION_MISMATCH")
    if receipt.run_id != contract.expected_run_id or receipt.run_id != journal.run_id:
        raise JournalFailure("EVIDENCE_RUN_ID_MISMATCH")
    if receipt.persistence_completed is not True:
        raise JournalFailure("PERSISTENCE_INCOMPLETE")
    if receipt.persistence_identity != persistence_object_identity(
        journal.run_id, journal.canonical_serialization
    ):
        raise JournalFailure("PERSISTENCE_OBJECT_IDENTITY_INVALID")
    if (
        receipt.journal_root_digest != journal.root_digest
        or receipt.journal_byte_sha256 != sha256_bytes(journal.canonical_serialization)
        or receipt.journal_record_count != journal.seal.record_count
    ):
        raise JournalFailure("PERSISTENCE_JOURNAL_BINDING_MISMATCH")


def verify_replay_receipt(
    contract: AcceptanceContract,
    journal: VerifiedJournal,
    persistence: PersistenceReceipt,
    replay: ReplayReceipt,
) -> None:
    if replay.receipt_digest != sha256(replay.commitment()):
        raise JournalFailure("REPLAY_RECEIPT_DIGEST_MISMATCH")
    if replay.contract_version != contract.contract_version:
        raise JournalFailure("REPLAY_CONTRACT_VERSION_MISMATCH")
    if replay.run_id != contract.expected_run_id or replay.run_id != journal.run_id:
        raise JournalFailure("EVIDENCE_RUN_ID_MISMATCH")
    if replay.persistence_identity != persistence.persistence_identity:
        raise JournalFailure("REPLAY_PERSISTENCE_BINDING_MISMATCH")
    expected_byte_digest = sha256_bytes(journal.canonical_serialization)
    if (
        replay.original_root_digest != journal.root_digest
        or replay.reconstructed_root_digest != journal.root_digest
        or replay.original_byte_sha256 != expected_byte_digest
        or replay.reconstructed_byte_sha256 != expected_byte_digest
        or replay.original_record_count != journal.seal.record_count
        or replay.reconstructed_record_count != journal.seal.record_count
    ):
        raise JournalFailure("REPLAY_JOURNAL_BINDING_MISMATCH")
    if replay.original_projection_digest != replay.replayed_projection_digest:
        raise JournalFailure("REPLAY_PROJECTION_MISMATCH")
    if replay.exact_equality is not True:
        raise JournalFailure("REPLAY_EXACT_EQUALITY_FAILED")
