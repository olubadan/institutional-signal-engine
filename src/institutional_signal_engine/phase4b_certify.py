"""Provider-free causal-journal certification through the production shell."""

from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, cast
from uuid import UUID

from jsonschema import Draft202012Validator, FormatChecker  # type: ignore[import-untyped]
from pydantic import SecretStr

from .config import Settings
from .dynamic_subscriptions import SubscriptionAcknowledgement
from .impact_coverage import PILOT_COVERAGE_POPULATION_VERSION
from .journal import (
    JOURNAL_KIND_CLOCK_ADVANCED,
    JOURNAL_KIND_CONFIGURATION,
    JOURNAL_KIND_EPOCH_CREATED,
    JOURNAL_KIND_EPOCH_RESTORED,
    JOURNAL_KIND_EVENT_ACCEPTED,
    JOURNAL_KIND_EVENT_REJECTED,
    JOURNAL_KIND_PROVIDER_DISCONNECTED,
    JOURNAL_KIND_PROVIDER_RECONNECTED,
    JOURNAL_KIND_REEVALUATION_START,
    JOURNAL_KIND_SESSION_FINALIZED,
    JOURNAL_KIND_SUBSCRIPTION_ACKNOWLEDGEMENT,
    JOURNAL_KIND_SUBSCRIPTION_COMMAND,
    AcceptanceContract,
    InMemoryJournalRepository,
    Journal,
    JournalFailure,
    JournalRecord,
    JournalRepository,
    PersistenceReceipt,
    ReplayReceipt,
    VerifiedJournal,
    persist_verified_journal,
    sha256,
    sha256_bytes,
    verify_complete,
    verify_persistence_receipt,
    verify_replay_receipt,
)
from .orchestration import (
    DriverSignal,
    OrchestrationConfig,
    OrchestrationShell,
    SessionDriverPort,
)
from .persistence import InMemoryRepository
from .providers.thetadata import ThetaContract
from .schemas import CanonicalEvent, EventKind
from .universe import PlannerEpoch, UniverseSelection

CERTIFICATION_VERSION = "PHASE4B_EVIDENCE_BUNDLE_CERT_V3"
CONTRACT_VERSION = "PHASE4B_ACCEPTANCE_CONTRACT_V3"
SCENARIO_VERSION = "PHASE4B_ACCELERATED_CAUSAL_RTH_V3"
RUN_ID = UUID("4b000000-0000-4000-8000-000000000027")
ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = ROOT / "docs/phase4b/PHASE4B_CERT_V1.schema.json"
CONTRACT_PATH = ROOT / "docs/phase4b/PHASE4B_CERT_V1.contract.json"
DEFAULT_OUTPUT = Path("/tmp/phase4b-certification/CERTIFICATE.json")
DEFAULT_JOURNAL_OUTPUT = Path("/tmp/phase4b-certification/JOURNAL.json")
DEFAULT_PERSISTENCE_OUTPUT = Path("/tmp/phase4b-certification/PERSISTENCE_RECEIPT.json")
DEFAULT_REPLAY_OUTPUT = Path("/tmp/phase4b-certification/REPLAY_RECEIPT.json")


def load_acceptance_contract(path: Path = CONTRACT_PATH) -> AcceptanceContract:
    """Load the immutable expected-value authority for this acceptance execution."""
    try:
        raw = cast(dict[str, object], json.loads(path.read_text(encoding="utf-8")))
        expected = cast(dict[str, object], raw["expected"])
        return AcceptanceContract(
            contract_version=str(raw["contract_version"]),
            certificate_version=str(raw["certificate_version"]),
            expected_run_id=UUID(str(expected["run_id"])),
            allowed_record_kinds=frozenset(
                map(str, cast(list[object], raw["allowed_record_kinds"]))
            ),
            expected_epoch_memberships=tuple(
                tuple(map(str, cast(list[object], membership)))
                for membership in cast(list[object], expected["epoch_memberships"])
            ),
            expected_clock_boundaries=tuple(
                map(str, cast(list[object], expected["clock_boundaries"]))
            ),
            expected_rth_start=str(expected["rth_start"]),
            expected_rth_stop=str(expected["rth_stop"]),
            expected_intake_stop=str(expected["intake_stop"]),
            expected_accepted_events=tuple(
                (
                    str(cast(dict[str, object], item)["event_id"]),
                    int(cast(int, cast(dict[str, object], item)["epoch_sequence"])),
                    str(cast(dict[str, object], item)["contract_identity"]),
                    str(cast(dict[str, object], item)["channel"]),
                )
                for item in cast(list[object], expected["accepted_events"])
            ),
            expected_rejected_events=tuple(
                (
                    str(cast(dict[str, object], item)["event_id"]),
                    int(cast(int, cast(dict[str, object], item)["epoch_sequence"])),
                    str(cast(dict[str, object], item)["contract_identity"]),
                    str(cast(dict[str, object], item)["channel"]),
                )
                for item in cast(list[object], expected["rejected_events"])
            ),
            expected_restoration_membership=tuple(
                map(str, cast(list[object], expected["restoration_membership"]))
            ),
            required_invariants=tuple(map(str, cast(list[object], raw["required_invariants"]))),
            allowed_lifecycle_order=tuple(
                map(str, cast(list[object], raw["allowed_lifecycle_order"]))
            ),
        )
    except JournalFailure:
        raise
    except Exception as exc:
        raise JournalFailure("CONTRACT_DESERIALIZATION_FAILED", type(exc).__name__) from exc


class MutableClock:
    def __init__(self, start: datetime) -> None:
        self._now = start

    def __call__(self) -> datetime:
        return self._now

    def set(self, value: datetime) -> None:
        if value < self._now:
            raise ValueError("clock_cannot_move_backwards")
        self._now = value


CONTRACT_A = ThetaContract("A", 20260821, 10000, "C")
CONTRACT_B = ThetaContract("B", 20260821, 11000, "C")
EPOCH_CONTRACTS = {
    1: (CONTRACT_A,),
    2: (CONTRACT_A, CONTRACT_B),
    3: (CONTRACT_B,),
}


class DeterministicPlanner:
    def plan(
        self,
        selections: tuple[UniverseSelection, ...] = (),
        sequence: int = 1,
        effective_at: datetime | None = None,
        previous_contracts: tuple[ThetaContract, ...] = (),
        baseline_symbols: frozenset[str] = frozenset(),
    ) -> PlannerEpoch:
        del selections, baseline_symbols
        return PlannerEpoch.create(
            sequence=sequence,
            effective_at=effective_at or datetime(2026, 8, 11, tzinfo=UTC),
            candidate_population_version=PILOT_COVERAGE_POPULATION_VERSION,
            selected_contracts=EPOCH_CONTRACTS[sequence],
            previous_contracts=previous_contracts,
            provenance="deterministic-causal-certification-v2",
        )


class DeterministicAdapter:
    request_types = ("TRADE", "QUOTE")

    def __init__(self) -> None:
        self.connected = False
        self.connection_generation = 0
        self._next_id = 1
        self._active: set[ThetaContract] = set()

    async def connect(self) -> None:
        self.connected = True
        self.connection_generation += 1

    async def add_subscriptions(self, contracts: tuple[ThetaContract, ...]) -> tuple[int, ...]:
        result = tuple(range(self._next_id, self._next_id + 2 * len(contracts)))
        self._next_id += len(result)
        self._active.update(contracts)
        return result

    async def remove_subscriptions(self, contracts: tuple[ThetaContract, ...]) -> tuple[int, ...]:
        result = tuple(range(self._next_id, self._next_id + 2 * len(contracts)))
        self._next_id += len(result)
        self._active.difference_update(contracts)
        return result

    async def acknowledge(
        self, expected_request_ids: tuple[int, ...], timeout: float | None = None
    ) -> SubscriptionAcknowledgement:
        del timeout
        return SubscriptionAcknowledgement(
            acknowledged=expected_request_ids,
            rejected=(),
            timed_out=(),
            partially_acknowledged=False,
            accepted=True,
            diagnostic="deterministic_exact_acknowledgement",
        )

    def is_event_accepted(self, contract: ThetaContract) -> bool:
        return contract in self._active

    async def restore_epoch(self, desired: tuple[ThetaContract, ...]) -> None:
        self.connected = True
        self.connection_generation += 1
        self._active = set(desired)

    def diagnostics(self) -> dict[str, object]:
        return {
            "connected": self.connected,
            "connection_generation": self.connection_generation,
            "active_contract_count": len(self._active),
        }


class EmptyEventStream:
    def events(self) -> Any:
        async def values() -> Any:
            if False:
                yield None

        return values()

    async def health(self) -> dict[str, object]:
        return {"status": "healthy"}


class DeterministicDiscovery:
    async def discover(self, symbols: tuple[str, ...], as_of: datetime) -> tuple[object, ...]:
        return tuple(symbols)

    async def prices(self, symbols: tuple[str, ...]) -> dict[str, Decimal]:
        return {symbol: Decimal(100) for symbol in symbols}


class DeterministicEnrichment:
    async def enrich(
        self,
        discovered: tuple[object, ...],
        prices: dict[str, Decimal],
        as_of: datetime,
    ) -> tuple[UniverseSelection, ...]:
        del discovered, prices, as_of
        return ()


def make_event(number: int, contract: ThetaContract, channel: str, at: datetime) -> CanonicalEvent:
    return CanonicalEvent(
        event_id=UUID(f"4b000000-0000-4000-8000-{number:012d}"),
        kind=EventKind.OPTIONS,
        symbol=contract.root,
        source="deterministic-driver",
        source_timestamp=at,
        received_timestamp=at,
        normalized_timestamp=at,
        sequence=number,
        payload={
            "provider_event_kind": channel.lower(),
            "price": 3.1,
            "volume": 100,
            "contract": {
                "root": contract.root,
                "expiration": contract.expiration,
                "strike": contract.strike,
                "right": contract.right,
            },
            "quote_context": {"bid": 3.0, "ask": 3.1},
            "conditions": ("@",),
        },
    )


class DeterministicSessionDriver(SessionDriverPort):
    """Timestamp-ordered inputs; timers are selected independently by the shell."""

    def __init__(self, clock: MutableClock, signals: list[DriverSignal]) -> None:
        self._clock = clock
        self._signals = signals
        self._index = 0

    async def next_signal(
        self, next_reevaluation: datetime | None, intake_stop: datetime
    ) -> DriverSignal:
        next_external = self._signals[self._index] if self._index < len(self._signals) else None
        candidates = [intake_stop]
        if next_reevaluation is not None:
            candidates.append(next_reevaluation)
        boundary = min(candidates)
        if next_external is None or boundary <= next_external.timestamp:
            self._clock.set(boundary)
            return DriverSignal("stop" if boundary == intake_stop else "clock", boundary)
        self._index += 1
        self._clock.set(next_external.timestamp)
        return next_external


@dataclass(frozen=True)
class Composition:
    contract: AcceptanceContract
    journal: VerifiedJournal
    persistence: PersistenceReceipt
    replay: ReplayReceipt
    reconstructed: VerifiedJournal
    journal_repository: InMemoryJournalRepository


def replay_persisted(
    contract: AcceptanceContract,
    original: VerifiedJournal,
    persistence: PersistenceReceipt,
    repository: JournalRepository,
) -> tuple[VerifiedJournal, ReplayReceipt]:
    """Create untrusted replay claim R after a repository reconstruction."""

    def receipt_projection(value: VerifiedJournal) -> dict[str, object]:
        records = value.records

        def by_kind(kind: str) -> tuple[JournalRecord, ...]:
            return tuple(record for record in records if record.kind == kind)

        configuration = by_kind(JOURNAL_KIND_CONFIGURATION)[0]
        epochs = by_kind(JOURNAL_KIND_EPOCH_CREATED)
        commands = by_kind(JOURNAL_KIND_SUBSCRIPTION_COMMAND)
        acknowledgements = {
            record.command_id: record
            for record in by_kind(JOURNAL_KIND_SUBSCRIPTION_ACKNOWLEDGEMENT)
        }
        finalized = by_kind(JOURNAL_KIND_SESSION_FINALIZED)[0]
        return {
            "run_id": str(value.run_id),
            "configuration_versions": {
                "scenario": configuration.payload["scenario_version"],
                "control_model": configuration.payload["control_model_version"],
                "shadow_model": configuration.payload["shadow_model_version"],
                "coverage": configuration.payload["coverage_version"],
            },
            "epochs": [
                {
                    "sequence": record.epoch_sequence,
                    "epoch_id": record.epoch_id,
                    "membership": list(cast(list[object], record.payload["membership"])),
                    "operation_id": record.operation_id,
                }
                for record in epochs
            ],
            "subscriptions": [
                {
                    "command_id": command.command_id,
                    "epoch_sequence": command.epoch_sequence,
                    "contract_identity": command.contract_identity,
                    "action": command.payload["action"],
                    "channel": command.payload["channel"],
                    "ack_id": acknowledgements[command.command_id].ack_id,
                    "acknowledged": acknowledgements[command.command_id].payload["accepted"],
                    "command_sequence": command.sequence,
                    "ack_sequence": acknowledgements[command.command_id].sequence,
                }
                for command in commands
            ],
            "events": {
                kind.split(".", 1)[1]: [
                    {
                        "event_id": record.payload["event_id"],
                        "epoch_sequence": record.epoch_sequence,
                        "contract_identity": record.contract_identity,
                        "channel": record.payload["event_kind"],
                    }
                    for record in by_kind(kind)
                ]
                for kind in (JOURNAL_KIND_EVENT_ACCEPTED, JOURNAL_KIND_EVENT_REJECTED)
            },
            "clock": {
                "boundaries": [
                    record.payload["boundary"] for record in by_kind(JOURNAL_KIND_CLOCK_ADVANCED)
                ],
                "reevaluations": len(by_kind(JOURNAL_KIND_REEVALUATION_START)),
            },
            "recovery": {
                "disconnects": len(by_kind(JOURNAL_KIND_PROVIDER_DISCONNECTED)),
                "reconnects": len(by_kind(JOURNAL_KIND_PROVIDER_RECONNECTED)),
                "restorations": [
                    {
                        "cycle_id": record.correlation_id,
                        "epoch_sequence": record.epoch_sequence,
                        "membership": list(cast(list[object], record.payload["membership"])),
                    }
                    for record in by_kind(JOURNAL_KIND_EPOCH_RESTORED)
                ],
            },
            "lifecycle": [
                record.kind
                for record in records
                if record.kind
                in {
                    "session.start",
                    "configuration.loaded",
                    "provider.ready",
                    "intake.stopped",
                    "persistence.drained",
                    "session.finalized",
                }
            ],
            "orders": {
                "trading_enabled": finalized.payload["trading_enabled"],
                "constructed": finalized.payload["orders_constructed"],
                "submitted": finalized.payload["orders_submitted"],
            },
        }

    _verify_persistence_receipt_integrity(contract, persistence)
    loaded = repository.load(persistence.persistence_identity)
    reconstructed = verify_complete(Journal.deserialize(loaded), contract)
    if loaded != original.canonical_serialization:
        raise JournalFailure("PERSISTED_JOURNAL_BYTES_MISMATCH")
    if reconstructed is original:
        raise JournalFailure("REPLAY_RECONSTRUCTION_NOT_DISTINCT")
    verify_persistence_receipt(contract, original, persistence)
    original_projection_digest = sha256(receipt_projection(original))
    replayed_projection_digest = sha256(receipt_projection(reconstructed))
    original_bytes = sha256_bytes(original.canonical_serialization)
    reconstructed_bytes = sha256_bytes(loaded)
    exact = (
        loaded == original.canonical_serialization
        and reconstructed.canonical_serialization == original.canonical_serialization
        and reconstructed.root_digest == original.root_digest
        and reconstructed.seal.record_count == original.seal.record_count
        and original_projection_digest == replayed_projection_digest
    )
    values: dict[str, object] = {
        "contract_version": contract.contract_version,
        "run_id": original.run_id,
        "persistence_identity": persistence.persistence_identity,
        "original_root_digest": original.root_digest,
        "reconstructed_root_digest": reconstructed.root_digest,
        "original_byte_sha256": original_bytes,
        "reconstructed_byte_sha256": reconstructed_bytes,
        "original_record_count": original.seal.record_count,
        "reconstructed_record_count": reconstructed.seal.record_count,
        "original_projection_digest": original_projection_digest,
        "replayed_projection_digest": replayed_projection_digest,
        "exact_equality": exact,
    }
    replay = ReplayReceipt.from_values(**values)
    return reconstructed, replay


def _verify_persistence_receipt_integrity(
    contract: AcceptanceContract,
    receipt: PersistenceReceipt,
) -> None:
    """Validate P as a claim before consulting its repository object."""
    if receipt.receipt_digest != sha256(receipt.commitment()):
        raise JournalFailure("PERSISTENCE_RECEIPT_DIGEST_MISMATCH")
    if receipt.contract_version != contract.contract_version:
        raise JournalFailure("PERSISTENCE_CONTRACT_VERSION_MISMATCH")
    if receipt.run_id != contract.expected_run_id:
        raise JournalFailure("EVIDENCE_RUN_ID_MISMATCH")
    if receipt.persistence_completed is not True:
        raise JournalFailure("PERSISTENCE_INCOMPLETE")
    expected_identity = f"journal://{receipt.run_id}/{receipt.journal_byte_sha256}"
    if receipt.persistence_identity != expected_identity:
        raise JournalFailure("PERSISTENCE_OBJECT_IDENTITY_INVALID")


def certify_repository_backed(
    contract: AcceptanceContract,
    journal: VerifiedJournal,
    persistence: PersistenceReceipt,
    replay: ReplayReceipt,
    repository: JournalRepository,
) -> dict[str, object]:
    """Verify repository-backed J/P/R and immediately issue the certificate."""
    if not isinstance(contract, AcceptanceContract):
        raise JournalFailure("CERTIFICATION_REQUIRES_ACCEPTANCE_CONTRACT")
    if not isinstance(journal, VerifiedJournal):
        raise JournalFailure("EVIDENCE_REQUIRES_VERIFIED_JOURNAL")
    if not isinstance(persistence, PersistenceReceipt):
        raise JournalFailure("CERTIFICATION_REQUIRES_PERSISTENCE_RECEIPT")
    if not isinstance(replay, ReplayReceipt):
        raise JournalFailure("CERTIFICATION_REQUIRES_REPLAY_RECEIPT")
    if not isinstance(repository, JournalRepository):
        raise JournalFailure("CERTIFICATION_REQUIRES_JOURNAL_REPOSITORY")

    # J is caller-supplied evidence. Reconstruct and verify it before using any
    # of its values, including its declared contract binding.
    independently_verified = verify_complete(
        Journal.deserialize(journal.canonical_serialization), contract
    )
    if independently_verified != journal:
        raise JournalFailure("VERIFIED_JOURNAL_VALUE_MISMATCH")
    if journal.contract_version != contract.contract_version:
        raise JournalFailure("JOURNAL_CONTRACT_VERSION_MISMATCH")

    # P is only a syntactically and cryptographically intact claim until this
    # invocation consults the repository and verifies the named object.
    _verify_persistence_receipt_integrity(contract, persistence)
    loaded = repository.load(persistence.persistence_identity)
    if loaded != independently_verified.canonical_serialization:
        raise JournalFailure("PERSISTED_JOURNAL_BYTES_MISMATCH")
    reconstructed = verify_complete(Journal.deserialize(loaded), contract)
    if reconstructed is independently_verified or reconstructed is journal:
        raise JournalFailure("REPOSITORY_RECONSTRUCTION_NOT_DISTINCT")
    verify_persistence_receipt(contract, independently_verified, persistence)
    locally_derived_persistence = PersistenceReceipt.create(
        contract, reconstructed, persistence.persistence_identity
    )
    if persistence != locally_derived_persistence:
        raise JournalFailure("PERSISTENCE_FACTS_NOT_REPOSITORY_DERIVED")

    # Projection is intentionally local to the fused boundary. It closes over
    # the independently verified repository reconstruction and accepts no
    # caller-created evidence object.
    def repository_observed_projection(value: VerifiedJournal) -> dict[str, object]:
        records = value.records

        def by_kind(kind: str) -> tuple[JournalRecord, ...]:
            return tuple(record for record in records if record.kind == kind)

        configuration = by_kind(JOURNAL_KIND_CONFIGURATION)[0]
        epochs = by_kind(JOURNAL_KIND_EPOCH_CREATED)
        commands = by_kind(JOURNAL_KIND_SUBSCRIPTION_COMMAND)
        acknowledgements = {
            record.command_id: record
            for record in by_kind(JOURNAL_KIND_SUBSCRIPTION_ACKNOWLEDGEMENT)
        }
        finalized = by_kind(JOURNAL_KIND_SESSION_FINALIZED)[0]
        return {
            "run_id": str(value.run_id),
            "configuration_versions": {
                "scenario": configuration.payload["scenario_version"],
                "control_model": configuration.payload["control_model_version"],
                "shadow_model": configuration.payload["shadow_model_version"],
                "coverage": configuration.payload["coverage_version"],
            },
            "epochs": [
                {
                    "sequence": record.epoch_sequence,
                    "epoch_id": record.epoch_id,
                    "membership": list(cast(list[object], record.payload["membership"])),
                    "operation_id": record.operation_id,
                }
                for record in epochs
            ],
            "subscriptions": [
                {
                    "command_id": command.command_id,
                    "epoch_sequence": command.epoch_sequence,
                    "contract_identity": command.contract_identity,
                    "action": command.payload["action"],
                    "channel": command.payload["channel"],
                    "ack_id": acknowledgements[command.command_id].ack_id,
                    "acknowledged": acknowledgements[command.command_id].payload["accepted"],
                    "command_sequence": command.sequence,
                    "ack_sequence": acknowledgements[command.command_id].sequence,
                }
                for command in commands
            ],
            "events": {
                kind.split(".", 1)[1]: [
                    {
                        "event_id": record.payload["event_id"],
                        "epoch_sequence": record.epoch_sequence,
                        "contract_identity": record.contract_identity,
                        "channel": record.payload["event_kind"],
                    }
                    for record in by_kind(kind)
                ]
                for kind in (JOURNAL_KIND_EVENT_ACCEPTED, JOURNAL_KIND_EVENT_REJECTED)
            },
            "clock": {
                "boundaries": [
                    record.payload["boundary"] for record in by_kind(JOURNAL_KIND_CLOCK_ADVANCED)
                ],
                "reevaluations": len(by_kind(JOURNAL_KIND_REEVALUATION_START)),
            },
            "recovery": {
                "disconnects": len(by_kind(JOURNAL_KIND_PROVIDER_DISCONNECTED)),
                "reconnects": len(by_kind(JOURNAL_KIND_PROVIDER_RECONNECTED)),
                "restorations": [
                    {
                        "cycle_id": record.correlation_id,
                        "epoch_sequence": record.epoch_sequence,
                        "membership": list(cast(list[object], record.payload["membership"])),
                    }
                    for record in by_kind(JOURNAL_KIND_EPOCH_RESTORED)
                ],
            },
            "lifecycle": [
                record.kind
                for record in records
                if record.kind
                in {
                    "session.start",
                    "configuration.loaded",
                    "provider.ready",
                    "intake.stopped",
                    "persistence.drained",
                    "session.finalized",
                }
            ],
            "orders": {
                "trading_enabled": finalized.payload["trading_enabled"],
                "constructed": finalized.payload["orders_constructed"],
                "submitted": finalized.payload["orders_submitted"],
            },
        }

    original_observed = repository_observed_projection(independently_verified)
    observed = repository_observed_projection(reconstructed)
    original_projection_digest = sha256(original_observed)
    reconstructed_projection_digest = sha256(observed)
    canonical_byte_digest = sha256_bytes(reconstructed.canonical_serialization)
    locally_derived_replay = ReplayReceipt.from_values(
        contract_version=contract.contract_version,
        run_id=reconstructed.run_id,
        persistence_identity=locally_derived_persistence.persistence_identity,
        original_root_digest=independently_verified.root_digest,
        reconstructed_root_digest=reconstructed.root_digest,
        original_byte_sha256=sha256_bytes(independently_verified.canonical_serialization),
        reconstructed_byte_sha256=canonical_byte_digest,
        original_record_count=independently_verified.seal.record_count,
        reconstructed_record_count=reconstructed.seal.record_count,
        original_projection_digest=original_projection_digest,
        replayed_projection_digest=reconstructed_projection_digest,
        exact_equality=(
            reconstructed.canonical_serialization == independently_verified.canonical_serialization
            and reconstructed.root_digest == independently_verified.root_digest
            and reconstructed.seal.record_count == independently_verified.seal.record_count
            and original_projection_digest == reconstructed_projection_digest
        ),
    )
    verify_replay_receipt(contract, independently_verified, persistence, replay)
    if replay != locally_derived_replay:
        raise JournalFailure("REPLAY_FACTS_NOT_INDEPENDENTLY_RECONSTRUCTED")

    observed_epochs = tuple(
        tuple(map(str, cast(list[object], item["membership"])))
        for item in cast(list[dict[str, object]], observed["epochs"])
    )
    clock = cast(dict[str, object], observed["clock"])
    subscriptions = cast(list[dict[str, object]], observed["subscriptions"])
    recovery = cast(dict[str, object], observed["recovery"])
    orders = cast(dict[str, object], observed["orders"])
    lifecycle = tuple(map(str, cast(list[object], observed["lifecycle"])))
    invariants = {
        "run_binding": observed["run_id"] == str(contract.expected_run_id),
        "epochs_exact": observed_epochs == contract.expected_epoch_memberships,
        "paired_commands_acknowledged": all(
            item["acknowledged"] is True
            and cast(int, item["command_sequence"]) < cast(int, item["ack_sequence"])
            for item in subscriptions
        ),
        "clock_boundaries_exact": tuple(map(str, cast(list[object], clock["boundaries"])))
        == contract.expected_clock_boundaries,
        "disconnect_reconnect_restore": recovery["disconnects"] == 1
        and recovery["reconnects"] == 1
        and len(cast(list[object], recovery["restorations"])) == 1,
        "lifecycle_complete": lifecycle == contract.allowed_lifecycle_order,
        "journal_sealed_finalization_terminal": reconstructed.records[-1].kind
        == JOURNAL_KIND_SESSION_FINALIZED,
        "persistence_complete": locally_derived_persistence.persistence_completed is True,
        "replay_exact": locally_derived_replay.exact_equality is True,
        "trading_disabled": orders["trading_enabled"] is False,
        "orders_zero": orders["constructed"] == 0 and orders["submitted"] == 0,
    }
    unknown = set(contract.required_invariants) - set(invariants)
    if unknown:
        raise JournalFailure("CONTRACT_REQUIRED_INVARIANT_UNKNOWN", min(unknown))
    failures = [name for name in contract.required_invariants if not invariants[name]]
    return {
        "certificate_version": contract.certificate_version,
        "evidence_kind": "RUNTIME_EVIDENCE_BUNDLE_CERTIFICATE",
        "acceptance_contract": {
            "contract_version": contract.contract_version,
            "expected_run_id": str(contract.expected_run_id),
            "expected_epoch_memberships": [
                list(membership) for membership in contract.expected_epoch_memberships
            ],
            "expected_clock_boundaries": list(contract.expected_clock_boundaries),
            "required_invariants": list(contract.required_invariants),
        },
        "observed": {
            **observed,
            "journal": {
                "record_count": reconstructed.seal.record_count,
                "terminal_sequence": reconstructed.seal.terminal_sequence,
                "root_digest": reconstructed.root_digest,
                "seal_digest": reconstructed.seal.seal_digest,
                "canonical_byte_sha256": canonical_byte_digest,
            },
            "persistence": locally_derived_persistence.as_dict(),
            "replay": locally_derived_replay.as_dict(),
        },
        "invariants": invariants,
        "failed_invariants": failures,
        "overall": "PASS" if not failures else "FAIL",
    }


async def execute_composition() -> Composition:
    contract = load_acceptance_contract()
    start = datetime(2026, 8, 11, 13, 30, tzinfo=UTC)
    clock = MutableClock(start)
    signals = [
        DriverSignal(
            "event", start + timedelta(minutes=1), make_event(1, CONTRACT_A, "TRADE", start)
        ),
        DriverSignal(
            "event", start + timedelta(minutes=2), make_event(2, CONTRACT_A, "QUOTE", start)
        ),
        DriverSignal(
            "event", start + timedelta(minutes=6), make_event(3, CONTRACT_A, "TRADE", start)
        ),
        DriverSignal(
            "event",
            start + timedelta(minutes=6, seconds=10),
            make_event(4, CONTRACT_B, "QUOTE", start),
        ),
        DriverSignal("disconnect", start + timedelta(minutes=7)),
        DriverSignal(
            "event", start + timedelta(minutes=8), make_event(5, CONTRACT_A, "QUOTE", start)
        ),
        DriverSignal(
            "event",
            start + timedelta(minutes=8, seconds=10),
            make_event(6, CONTRACT_B, "TRADE", start),
        ),
        DriverSignal(
            "event", start + timedelta(minutes=11), make_event(7, CONTRACT_A, "TRADE", start)
        ),
        DriverSignal(
            "event", start + timedelta(minutes=12), make_event(8, CONTRACT_B, "QUOTE", start)
        ),
    ]
    driver = DeterministicSessionDriver(clock, signals)
    journal_repository = InMemoryJournalRepository()
    event_repository = InMemoryRepository()
    settings = Settings(
        alpaca_key_id=SecretStr("fixture-key"),
        alpaca_secret_key=SecretStr("fixture-secret"),
        theta_api_key=SecretStr("fixture-theta"),
        database_url=None,
    )
    config = OrchestrationConfig(
        run_id=RUN_ID,
        session_date="2026-08-11",
        rth_start=start,
        rth_stop=start + timedelta(minutes=11),
        intake_stop=start + timedelta(minutes=15),
        reevaluation_interval=timedelta(minutes=5),
        scenario_version=SCENARIO_VERSION,
        acceptance_contract_version=contract.contract_version,
    )
    shell = OrchestrationShell(
        config=config,
        settings=settings,
        clock=clock,
        discovery=DeterministicDiscovery(),
        enrichment=DeterministicEnrichment(),
        planner=DeterministicPlanner(),
        subscription_adapter=cast(Any, DeterministicAdapter()),
        event_stream=cast(Any, EmptyEventStream()),
        repository=event_repository,
        session_driver=driver,
    )
    result = await shell.run()
    if result.status != "live_observation_complete":
        raise JournalFailure("COMPOSITION_FAILED", result.status)
    original = verify_complete(shell.journal, contract)
    persistence = persist_verified_journal(contract, original, journal_repository)
    reconstructed, replay = replay_persisted(contract, original, persistence, journal_repository)
    return Composition(contract, original, persistence, replay, reconstructed, journal_repository)


def validate_certificate_schema(value: dict[str, object]) -> list[str]:
    """Validate certificate shape only; this is never operational verification."""
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    return [
        f"{'/'.join(str(item) for item in error.absolute_path) or '<root>'}: {error.message}"
        for error in sorted(validator.iter_errors(value), key=lambda item: list(item.absolute_path))
    ]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--journal-output", type=Path, default=DEFAULT_JOURNAL_OUTPUT)
    parser.add_argument("--persistence-output", type=Path, default=DEFAULT_PERSISTENCE_OUTPUT)
    parser.add_argument("--replay-output", type=Path, default=DEFAULT_REPLAY_OUTPUT)
    arguments = parser.parse_args(argv)
    try:
        composition = asyncio.run(execute_composition())
        certificate = certify_repository_backed(
            composition.contract,
            composition.journal,
            composition.persistence,
            composition.replay,
            composition.journal_repository,
        )
        errors = validate_certificate_schema(certificate)
        if errors:
            raise JournalFailure("CERTIFICATE_SCHEMA_INVALID", ";".join(errors))
        arguments.output.parent.mkdir(parents=True, exist_ok=True)
        arguments.output.write_text(
            json.dumps(certificate, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        arguments.journal_output.parent.mkdir(parents=True, exist_ok=True)
        arguments.journal_output.write_text(
            composition.journal.canonical_serialization, encoding="utf-8"
        )
        arguments.persistence_output.parent.mkdir(parents=True, exist_ok=True)
        arguments.persistence_output.write_text(
            composition.persistence.serialize(), encoding="utf-8"
        )
        arguments.replay_output.parent.mkdir(parents=True, exist_ok=True)
        arguments.replay_output.write_text(composition.replay.serialize(), encoding="utf-8")
    except JournalFailure as exc:
        print(json.dumps({"overall": "FAIL", "failure_code": exc.code}, sort_keys=True))
        return 1
    print(
        json.dumps(
            {
                "overall": certificate["overall"],
                "journal_sha256": sha256_bytes(composition.journal.canonical_serialization),
                "persistence_receipt_sha256": sha256_bytes(composition.persistence.serialize()),
                "replay_receipt_sha256": sha256_bytes(composition.replay.serialize()),
                "certificate_path": str(arguments.output),
                "journal_path": str(arguments.journal_output),
                "persistence_path": str(arguments.persistence_output),
                "replay_path": str(arguments.replay_output),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
