"""Tamper-evident causal journal and verified-journal boundary."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from types import MappingProxyType
from typing import Any, cast
from uuid import UUID


def canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def sha256(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


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
JOURNAL_KIND_JOURNAL_PERSISTED = "journal.persisted"
JOURNAL_KIND_REPLAY_VERIFIED = "replay.verified"

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


class FileJournalRepository(JournalRepository):
    def __init__(self, directory: Path = Path("/tmp/institutional-signal-engine-journals")) -> None:
        self._directory = directory

    def save(self, run_id: UUID, serialized: str) -> str:
        self._directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        target = self._directory / f"{run_id}.json"
        temporary = target.with_suffix(".tmp")
        temporary.write_text(serialized, encoding="utf-8")
        os.chmod(temporary, 0o600)
        temporary.replace(target)
        return str(target)

    def load(self, identity: str) -> str:
        return Path(identity).read_text(encoding="utf-8")


class InMemoryJournalRepository(JournalRepository):
    def __init__(self) -> None:
        self._values: dict[str, str] = {}

    def save(self, run_id: UUID, serialized: str) -> str:
        identity = f"memory://journal/{run_id}"
        self._values[identity] = serialized
        return identity

    def load(self, identity: str) -> str:
        try:
            return self._values[identity]
        except KeyError as exc:
            raise JournalFailure("PERSISTED_JOURNAL_MISSING", identity) from exc


@dataclass(frozen=True)
class JournalRecord:
    sequence: int
    kind: str
    timestamp: datetime
    run_id: UUID
    parent_sequence: int | None
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
    """Immutable capability produced only by complete production verification."""

    run_id: UUID
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
        record = JournalRecord(
            sequence=len(self._records),
            kind=kind,
            timestamp=timestamp,
            run_id=self._run_id,
            parent_sequence=parent_sequence,
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
            JOURNAL_KIND_JOURNAL_PERSISTED,
            JOURNAL_KIND_REPLAY_VERIFIED,
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
    for expected_sequence, record in enumerate(records):
        if record.sequence != expected_sequence or record.sequence in seen:
            raise JournalFailure("SEQUENCE_INVALID", str(record.sequence))
        seen.add(record.sequence)
        if record.run_id != journal.run_id:
            raise JournalFailure("RUN_ID_MISMATCH", str(record.sequence))
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


def verify_semantics(journal: Journal) -> None:
    records = journal.records
    by_sequence = {record.sequence: record for record in records}
    required_single = (
        JOURNAL_KIND_SESSION_START,
        JOURNAL_KIND_CONFIGURATION,
        JOURNAL_KIND_PROVIDER_READY,
        JOURNAL_KIND_INTAKE_STOPPED,
        JOURNAL_KIND_PERSISTENCE_DRAINED,
        JOURNAL_KIND_SESSION_FINALIZED,
        JOURNAL_KIND_JOURNAL_PERSISTED,
        JOURNAL_KIND_REPLAY_VERIFIED,
    )
    singles = {kind: _one(records, kind) for kind in required_single}
    if records[0].kind != JOURNAL_KIND_SESSION_START:
        raise JournalFailure("SESSION_START_NOT_FIRST")
    if records[-1].kind != JOURNAL_KIND_REPLAY_VERIFIED:
        raise JournalFailure("RECORD_AFTER_TERMINAL_LIFECYCLE")
    final_sequence = singles[JOURNAL_KIND_SESSION_FINALIZED].sequence
    if any(
        record.sequence > final_sequence
        and record.kind not in {JOURNAL_KIND_JOURNAL_PERSISTED, JOURNAL_KIND_REPLAY_VERIFIED}
        for record in records
    ):
        raise JournalFailure("RECORD_AFTER_FINALIZATION")

    def parent_is(record: JournalRecord, kind: str) -> None:
        if record.parent_sequence is None or by_sequence[record.parent_sequence].kind != kind:
            raise JournalFailure("CAUSAL_PARENT_WRONG", f"{record.sequence}:{kind}")

    parent_is(singles[JOURNAL_KIND_CONFIGURATION], JOURNAL_KIND_SESSION_START)
    parent_is(singles[JOURNAL_KIND_PROVIDER_READY], JOURNAL_KIND_CONFIGURATION)
    parent_is(singles[JOURNAL_KIND_PERSISTENCE_DRAINED], JOURNAL_KIND_INTAKE_STOPPED)
    parent_is(singles[JOURNAL_KIND_SESSION_FINALIZED], JOURNAL_KIND_PERSISTENCE_DRAINED)
    parent_is(singles[JOURNAL_KIND_JOURNAL_PERSISTED], JOURNAL_KIND_SESSION_FINALIZED)
    parent_is(singles[JOURNAL_KIND_REPLAY_VERIFIED], JOURNAL_KIND_JOURNAL_PERSISTED)

    epochs: dict[int, JournalRecord] = {}
    active: JournalRecord | None = None
    commands: dict[str, JournalRecord] = {}
    acknowledgements: dict[str, JournalRecord] = {}
    disconnected_epoch: tuple[int | None, str | None] | None = None
    seen_stop = False
    expected_boundary = str(singles[JOURNAL_KIND_CONFIGURATION].payload.get("intake_stop"))
    discovery_count = 0
    for record in records:
        if seen_stop and record.kind in {
            JOURNAL_KIND_EVENT_ACCEPTED,
            JOURNAL_KIND_EVENT_REJECTED,
            JOURNAL_KIND_CLOCK_ADVANCED,
            JOURNAL_KIND_REEVALUATION_START,
            JOURNAL_KIND_EPOCH_CREATED,
            JOURNAL_KIND_EPOCH_ACTIVATED,
        }:
            raise JournalFailure("RECORD_AFTER_INTAKE_STOP", str(record.sequence))
        if record.kind == JOURNAL_KIND_DISCOVERY_START:
            discovery_count += 1
            parent_is(
                record,
                JOURNAL_KIND_PROVIDER_READY
                if discovery_count == 1
                else JOURNAL_KIND_REEVALUATION_START,
            )
        elif record.kind == JOURNAL_KIND_DISCOVERY_COMPLETE:
            parent_is(record, JOURNAL_KIND_DISCOVERY_START)
        elif record.kind == JOURNAL_KIND_ENRICHMENT_START:
            parent_is(record, JOURNAL_KIND_DISCOVERY_COMPLETE)
        elif record.kind == JOURNAL_KIND_ENRICHMENT_COMPLETE:
            parent_is(record, JOURNAL_KIND_ENRICHMENT_START)
        elif record.kind == JOURNAL_KIND_EPOCH_CREATED:
            if record.epoch_sequence is None or record.epoch_sequence in epochs:
                raise JournalFailure("EPOCH_SEQUENCE_INVALID", str(record.sequence))
            parent_is(record, JOURNAL_KIND_ENRICHMENT_COMPLETE)
            membership = cast(list[object], record.payload.get("membership", []))
            if len(membership) != len(set(map(str, membership))):
                raise JournalFailure("EPOCH_MEMBERSHIP_INVALID", str(record.sequence))
            epochs[record.epoch_sequence] = record
        elif record.kind == JOURNAL_KIND_EPOCH_PERSISTED:
            parent_is(record, JOURNAL_KIND_EPOCH_CREATED)
        elif record.kind == JOURNAL_KIND_SUBSCRIPTION_COMMAND:
            if not record.command_id or record.command_id in commands:
                raise JournalFailure("COMMAND_ID_INVALID", str(record.sequence))
            if record.epoch_sequence not in epochs or not record.contract_identity:
                raise JournalFailure("COMMAND_TARGET_INVALID", str(record.sequence))
            parent_is(record, JOURNAL_KIND_EPOCH_CREATED)
            if record.payload.get("channel") not in {"TRADE", "QUOTE"}:
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
                record.epoch_sequence != command.epoch_sequence
                or record.contract_identity != command.contract_identity
                or record.payload.get("channel") != command.payload.get("channel")
                or record.payload.get("action") != command.payload.get("action")
                or record.payload.get("accepted") is not True
            ):
                raise JournalFailure("ACK_CORRELATION_INVALID", str(record.sequence))
            acknowledgements[record.command_id] = record
        elif record.kind == JOURNAL_KIND_EPOCH_ACTIVATED:
            created = epochs.get(cast(int, record.epoch_sequence))
            if created is None or record.epoch_id != created.epoch_id:
                raise JournalFailure("ACTIVATION_EPOCH_INVALID", str(record.sequence))
            activated_membership = set(
                map(str, cast(list[object], created.payload.get("membership", [])))
            )
            epoch_commands = [
                c for c in commands.values() if c.epoch_sequence == record.epoch_sequence
            ]
            expected = {
                (str(contract), channel, "add")
                for contract in activated_membership
                for channel in ("TRADE", "QUOTE")
                if active is None
                or str(contract)
                not in set(map(str, cast(list[object], active.payload.get("membership", []))))
            }
            if active is not None:
                old = set(map(str, cast(list[object], active.payload.get("membership", []))))
                expected |= {
                    (contract, channel, "remove")
                    for contract in old - activated_membership
                    for channel in ("TRADE", "QUOTE")
                }
            observed = {
                (
                    cast(str, c.contract_identity),
                    cast(str, c.payload.get("channel")),
                    cast(str, c.payload.get("action")),
                )
                for c in epoch_commands
            }
            if observed != expected:
                raise JournalFailure("SUBSCRIPTION_DIFF_INCOMPLETE", str(record.sequence))
            if any(c.command_id not in acknowledgements for c in epoch_commands):
                raise JournalFailure("ACTIVATION_BEFORE_ACK", str(record.sequence))
            epoch_ack_sequences = [
                acknowledgements[cast(str, command.command_id)].sequence
                for command in epoch_commands
            ]
            if epoch_ack_sequences and record.parent_sequence != max(epoch_ack_sequences):
                raise JournalFailure("ACTIVATION_CAUSAL_PARENT_INVALID", str(record.sequence))
            active = record
        elif record.kind in {JOURNAL_KIND_EVENT_ACCEPTED, JOURNAL_KIND_EVENT_REJECTED}:
            if active is None or record.epoch_sequence != active.epoch_sequence:
                raise JournalFailure("EVENT_EPOCH_INVALID", str(record.sequence))
            event_membership = set(
                map(str, cast(list[object], active.payload.get("membership", [])))
            )
            if record.parent_sequence != active.sequence:
                raise JournalFailure("CAUSAL_PARENT_WRONG", str(record.sequence))
            present = record.contract_identity in event_membership
            if record.kind == JOURNAL_KIND_EVENT_ACCEPTED and not present:
                raise JournalFailure("ACCEPTED_EVENT_NOT_ACTIVE", str(record.sequence))
            if record.kind == JOURNAL_KIND_EVENT_REJECTED and present:
                raise JournalFailure("REJECTED_EVENT_ACTIVE", str(record.sequence))
        elif record.kind == JOURNAL_KIND_CLOCK_ADVANCED:
            if record.payload.get("boundary") is None:
                raise JournalFailure("CLOCK_BOUNDARY_MISSING", str(record.sequence))
        elif record.kind == JOURNAL_KIND_REEVALUATION_START:
            parent_is(record, JOURNAL_KIND_CLOCK_ADVANCED)
            parent = by_sequence[cast(int, record.parent_sequence)]
            if record.payload.get("boundary") != parent.payload.get("boundary"):
                raise JournalFailure("REEVALUATION_CLOCK_CAUSE_INVALID", str(record.sequence))
        elif record.kind == JOURNAL_KIND_PROVIDER_DISCONNECTED:
            disconnected_epoch = (record.epoch_sequence, record.epoch_id)
        elif record.kind == JOURNAL_KIND_PROVIDER_RECONNECTED:
            parent_is(record, JOURNAL_KIND_PROVIDER_DISCONNECTED)
        elif record.kind == JOURNAL_KIND_EPOCH_RESTORED:
            parent_is(record, JOURNAL_KIND_PROVIDER_RECONNECTED)
            if disconnected_epoch != (record.epoch_sequence, record.epoch_id):
                raise JournalFailure("RESTORATION_EPOCH_INVALID", str(record.sequence))
        elif record.kind == JOURNAL_KIND_INTAKE_STOPPED:
            seen_stop = True
            if record.timestamp.isoformat() != expected_boundary:
                raise JournalFailure("INTAKE_STOP_BOUNDARY_INVALID")

    activated_epochs = {r.epoch_sequence for r in records if r.kind == JOURNAL_KIND_EPOCH_ACTIVATED}
    if set(epochs) != activated_epochs:
        raise JournalFailure("EPOCH_ACTIVATION_MISSING")
    for kind in (
        JOURNAL_KIND_PROVIDER_DISCONNECTED,
        JOURNAL_KIND_PROVIDER_RECONNECTED,
        JOURNAL_KIND_EPOCH_RESTORED,
    ):
        if len(tuple(record for record in records if record.kind == kind)) != 1:
            raise JournalFailure("RECOVERY_RECORD_COUNT_INVALID", kind)
    if len(epochs) != 3:
        raise JournalFailure("EPOCH_COUNT_INVALID")
    memberships = [
        tuple(cast(list[object], epochs[i].payload.get("membership", []))) for i in sorted(epochs)
    ]
    membership_roots = [
        tuple(str(item).split(":", 1)[0] for item in group) for group in memberships
    ]
    if membership_roots != [("A",), ("A", "B"), ("B",)]:
        raise JournalFailure("EPOCH_MEMBERSHIP_INVALID")
    persisted = singles[JOURNAL_KIND_JOURNAL_PERSISTED]
    replay = singles[JOURNAL_KIND_REPLAY_VERIFIED]
    if (
        replay.payload.get("exact_equal") is not True
        or replay.payload.get("persisted_identity") != persisted.payload.get("persisted_identity")
        or replay.payload.get("original_record_count")
        != replay.payload.get("replayed_record_count")
        or replay.payload.get("original_terminal_digest")
        != replay.payload.get("replayed_terminal_digest")
    ):
        raise JournalFailure("REPLAY_MISMATCH")


def verify_complete(journal: Journal) -> VerifiedJournal:
    verify_structural(journal)
    verify_semantics(journal)
    return VerifiedJournal(
        run_id=journal.run_id,
        records=journal.records,
        seal=cast(JournalSeal, journal.seal_value),
        canonical_serialization=journal.serialize(),
    )
