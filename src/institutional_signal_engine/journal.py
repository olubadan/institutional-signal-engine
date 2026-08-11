"""Causal journal for the assembled composition execution.

Every material action during a session appends exactly one ordered journal
record. The journal is the single source of runtime acceptance evidence. The
certificate is a pure projection of the completed journal — no evidence may
be manufactured outside the composition execution.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def _digest(payload: dict[str, object]) -> str:
    return hashlib.sha256(_canonical(payload)).hexdigest()


# ---------------------------------------------------------------------------
# Journal record kinds — every material action has a kind
# ---------------------------------------------------------------------------

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
JOURNAL_KIND_REPLAY_VERIFIED = "replay.verified"


@dataclass(frozen=True)
class JournalRecord:
    """One ordered, immutable record in the causal journal."""

    sequence: int
    kind: str
    timestamp: datetime
    run_id: UUID
    parent_sequence: int | None
    epoch_sequence: int | None
    epoch_id: str | None
    contract_identity: str | None
    command_id: str | None
    ack_id: str | None
    payload_digest: str
    payload: dict[str, object]

    def as_dict(self) -> dict[str, object]:
        return {
            "sequence": self.sequence,
            "kind": self.kind,
            "timestamp": self.timestamp.isoformat(),
            "run_id": str(self.run_id),
            "parent_sequence": self.parent_sequence,
            "epoch_sequence": self.epoch_sequence,
            "epoch_id": self.epoch_id,
            "contract_identity": self.contract_identity,
            "command_id": self.command_id,
            "ack_id": self.ack_id,
            "payload_digest": self.payload_digest,
            "payload": self.payload,
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> JournalRecord:
        return cls(
            sequence=int(str(data["sequence"])),
            kind=str(data["kind"]),
            timestamp=datetime.fromisoformat(str(data["timestamp"])),
            run_id=UUID(str(data["run_id"])),
            parent_sequence=(
                int(str(data["parent_sequence"]))
                if data.get("parent_sequence") is not None
                else None
            ),
            epoch_sequence=(
                int(str(data["epoch_sequence"])) if data.get("epoch_sequence") is not None else None
            ),
            epoch_id=str(data["epoch_id"]) if data.get("epoch_id") is not None else None,
            contract_identity=(
                str(data["contract_identity"])
                if data.get("contract_identity") is not None
                else None
            ),
            command_id=str(data["command_id"]) if data.get("command_id") is not None else None,
            ack_id=str(data["ack_id"]) if data.get("ack_id") is not None else None,
            payload_digest=str(data["payload_digest"]),
            payload=dict(cast(dict[str, object], data.get("payload", {}))),
        )


class Journal:
    """Append-only causal journal for one composition execution.

    Every material action during a session appends exactly one record.
    After the session completes, the journal is sealed and becomes the
    sole source of acceptance evidence.

    The journal is NOT threadsafe — it is owned by the orchestration shell
    and accessed only from the async event loop.
    """

    def __init__(self, run_id: UUID, clock: Any = None) -> None:
        self._run_id = run_id
        self._records: list[JournalRecord] = []
        self._sequence: int = 0
        self._sealed: bool = False
        self._clock = clock

    # -- properties --

    @property
    def run_id(self) -> UUID:
        return self._run_id

    @property
    def records(self) -> tuple[JournalRecord, ...]:
        return tuple(self._records)

    @property
    def sealed(self) -> bool:
        return self._sealed

    @property
    def sequence(self) -> int:
        return self._sequence

    # -- append --

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
        """Append one ordered journal record. Returns the immutable record."""
        if self._sealed:
            raise RuntimeError("journal_sealed")
        if self._clock is not None:
            ts = self._clock()
        elif timestamp is not None:
            ts = timestamp
        else:
            ts = datetime.now(UTC)
        safe_payload: dict[str, object] = {
            key: value for key, value in payload.items() if value is not None
        }
        record = JournalRecord(
            sequence=self._sequence,
            kind=kind,
            timestamp=ts,
            run_id=self._run_id,
            parent_sequence=parent_sequence,
            epoch_sequence=epoch_sequence,
            epoch_id=epoch_id,
            contract_identity=contract_identity,
            command_id=command_id,
            ack_id=ack_id,
            payload_digest=_digest(safe_payload),
            payload=safe_payload,
        )
        self._records.append(record)
        self._sequence += 1
        return record

    def seal(self) -> None:
        """Seal the journal — no further appends are permitted."""
        self._sealed = True

    # -- serialization for persistence and replay --

    def serialize(self) -> str:
        """Serialize the complete journal as a JSON string."""
        return json.dumps(
            {
                "run_id": str(self._run_id),
                "sealed": self._sealed,
                "records": [record.as_dict() for record in self._records],
            },
            sort_keys=True,
            indent=2,
        )

    @classmethod
    def deserialize(cls, data: str) -> Journal:
        """Reconstruct a journal from its serialized form."""
        raw = json.loads(data)
        journal = cls(run_id=UUID(str(raw["run_id"])))
        journal._sealed = bool(raw.get("sealed", False))
        for item in raw.get("records", []):
            record = JournalRecord.from_dict(item)
            journal._records.append(record)
        journal._sequence = len(journal._records)
        return journal

    # -- projection helpers for the certificate --

    def records_by_kind(self, kind: str) -> tuple[JournalRecord, ...]:
        """Return all records of a given kind in sequence order."""
        return tuple(record for record in self._records if record.kind == kind)

    def epoch_records(self) -> tuple[JournalRecord, ...]:
        """Return all epoch-created records in sequence order."""
        return self.records_by_kind(JOURNAL_KIND_EPOCH_CREATED)

    def subscription_commands(self) -> tuple[JournalRecord, ...]:
        """Return all subscription command records in sequence order."""
        return self.records_by_kind(JOURNAL_KIND_SUBSCRIPTION_COMMAND)

    def subscription_acks(self) -> tuple[JournalRecord, ...]:
        """Return all acknowledgement records in sequence order."""
        return self.records_by_kind(JOURNAL_KIND_SUBSCRIPTION_ACKNOWLEDGEMENT)

    def accepted_events(self) -> tuple[JournalRecord, ...]:
        """Return all accepted event records in sequence order."""
        return self.records_by_kind(JOURNAL_KIND_EVENT_ACCEPTED)

    def rejected_events(self) -> tuple[JournalRecord, ...]:
        """Return all rejected event records in sequence order."""
        return self.records_by_kind(JOURNAL_KIND_EVENT_REJECTED)

    def lifecycle_records(self) -> tuple[JournalRecord, ...]:
        """Return lifecycle records (disconnect, reconnect, stop, drain, finalize)."""
        lifecycle_kinds = {
            JOURNAL_KIND_PROVIDER_DISCONNECTED,
            JOURNAL_KIND_PROVIDER_RECONNECTED,
            JOURNAL_KIND_EPOCH_RESTORED,
            JOURNAL_KIND_INTAKE_STOPPED,
            JOURNAL_KIND_PERSISTENCE_DRAINED,
            JOURNAL_KIND_SESSION_FINALIZED,
            JOURNAL_KIND_REPLAY_VERIFIED,
        }
        return tuple(record for record in self._records if record.kind in lifecycle_kinds)

    def verify_integrity(self) -> tuple[bool, str]:
        """Verify the journal's internal integrity.

        Checks:
        - Monotonic sequence numbers
        - Unique sequence numbers
        - Parent references are valid
        """
        sequences = [record.sequence for record in self._records]
        if sequences != list(range(len(sequences))):
            return False, "non_monotonic_sequence"
        for record in self._records:
            if record.parent_sequence is not None:
                if record.parent_sequence >= record.sequence:
                    return False, f"parent_after_child:{record.sequence}"
                if record.parent_sequence >= len(self._records):
                    return False, f"parent_out_of_range:{record.sequence}"
        return True, "ok"
