"""Provider-free causal-journal certification through the production shell."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from importlib import resources
from pathlib import Path
from types import MappingProxyType
from typing import Any, Final, cast
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
    ReplayReceipt,
    VerifiedJournal,
    canonical_json,
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

__all__ = [
    "CertificationArtifacts",
    "CertificationOutputPaths",
    "generate_phase4b_certification",
    "main",
    "validate_certificate_schema",
]

CERTIFICATION_VERSION: Final = "PHASE4B_EVIDENCE_BUNDLE_CERT_V3"
CONTRACT_VERSION: Final = "PHASE4B_ACCEPTANCE_CONTRACT_V3"
SCENARIO_VERSION: Final = "PHASE4B_ACCELERATED_CAUSAL_RTH_V3"
RUN_ID: Final = UUID("4b000000-0000-4000-8000-000000000027")
CONTRACT_RESOURCE: Final = "resources/phase4b_acceptance_contract.json"
SCHEMA_RESOURCE: Final = "resources/phase4b_certificate_schema.json"
CONTRACT_CANONICAL_SHA256: Final = (
    "da13d2b3b632913ffefafad278e9f48e6fca8b11e3341b760c001d0f0cea468d"
)
# Updated only when the checked-in scenario definition is intentionally changed.
SCENARIO_CANONICAL_SHA256: Final = (
    "05ac756c035d8aca12fe8ff5d9016024ef814b512166c8ceecaed40a318b593f"
)
DEFAULT_OUTPUT: Final = Path("/tmp/phase4b-certification/CERTIFICATE.json")
DEFAULT_JOURNAL_OUTPUT: Final = Path("/tmp/phase4b-certification/JOURNAL.json")
DEFAULT_PERSISTENCE_OUTPUT: Final = Path("/tmp/phase4b-certification/PERSISTENCE_RECEIPT.json")
DEFAULT_REPLAY_OUTPUT: Final = Path("/tmp/phase4b-certification/REPLAY_RECEIPT.json")


@dataclass(frozen=True)
class CertificationOutputPaths:
    """Presentation-only destinations; these values never select certification facts."""

    certificate: Path
    journal: Path
    persistence_receipt: Path
    replay_receipt: Path


@dataclass(frozen=True)
class CertificationArtifacts:
    """Immutable publication result from the sole authoritative operation."""

    output_paths: CertificationOutputPaths
    journal_sha256: str
    persistence_receipt_sha256: str
    replay_receipt_sha256: str
    certificate_sha256: str
    certificate_commitment_sha256: str


@dataclass(frozen=True)
class _ScenarioSignal:
    kind: str
    timestamp: str
    event_number: int | None = None
    contract_identity: str | None = None
    channel: str | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "kind": self.kind,
            "timestamp": self.timestamp,
            "event_number": self.event_number,
            "contract_identity": self.contract_identity,
            "channel": self.channel,
        }


@dataclass(frozen=True)
class _ScenarioDefinition:
    version: str
    run_id: UUID
    session_date: str
    rth_start: str
    rth_stop: str
    intake_stop: str
    reevaluation_seconds: int
    epoch_memberships: tuple[tuple[str, ...], ...]
    signals: tuple[_ScenarioSignal, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "version": self.version,
            "run_id": str(self.run_id),
            "session_date": self.session_date,
            "rth_start": self.rth_start,
            "rth_stop": self.rth_stop,
            "intake_stop": self.intake_stop,
            "reevaluation_seconds": self.reevaluation_seconds,
            "epoch_memberships": [list(value) for value in self.epoch_memberships],
            "signals": [value.as_dict() for value in self.signals],
        }


def _checked_in_scenario() -> _ScenarioDefinition:
    """Return a fresh immutable instance of the checked-in scenario anchor."""
    return _ScenarioDefinition(
        version=SCENARIO_VERSION,
        run_id=RUN_ID,
        session_date="2026-08-11",
        rth_start="2026-08-11T13:30:00+00:00",
        rth_stop="2026-08-11T13:41:00+00:00",
        intake_stop="2026-08-11T13:45:00+00:00",
        reevaluation_seconds=300,
        epoch_memberships=(
            ("A:20260821:10000:C",),
            ("A:20260821:10000:C", "B:20260821:11000:C"),
            ("B:20260821:11000:C",),
        ),
        signals=(
            _ScenarioSignal("event", "2026-08-11T13:31:00+00:00", 1, "A:20260821:10000:C", "TRADE"),
            _ScenarioSignal("event", "2026-08-11T13:32:00+00:00", 2, "A:20260821:10000:C", "QUOTE"),
            _ScenarioSignal("event", "2026-08-11T13:36:00+00:00", 3, "A:20260821:10000:C", "TRADE"),
            _ScenarioSignal("event", "2026-08-11T13:36:10+00:00", 4, "B:20260821:11000:C", "QUOTE"),
            _ScenarioSignal("disconnect", "2026-08-11T13:37:00+00:00"),
            _ScenarioSignal("event", "2026-08-11T13:38:00+00:00", 5, "A:20260821:10000:C", "QUOTE"),
            _ScenarioSignal("event", "2026-08-11T13:38:10+00:00", 6, "B:20260821:11000:C", "TRADE"),
            _ScenarioSignal("event", "2026-08-11T13:41:00+00:00", 7, "A:20260821:10000:C", "TRADE"),
            _ScenarioSignal("event", "2026-08-11T13:42:00+00:00", 8, "B:20260821:11000:C", "QUOTE"),
        ),
    )


@dataclass(frozen=True)
class _AuthoritativeContract:
    value: AcceptanceContract
    canonical_sha256: str
    scenario_canonical_sha256: str
    configuration_versions: tuple[tuple[str, str], ...]


def _contract_resource_bytes() -> bytes:
    """Read exact bytes from the sole fixed authoritative contract resource."""
    try:
        resource = resources.files("institutional_signal_engine").joinpath(CONTRACT_RESOURCE)
        if isinstance(resource, Path) and (resource.is_symlink() or not resource.is_file()):
            raise JournalFailure("CONTRACT_RESOURCE_INVALID", CONTRACT_RESOURCE)
        return resource.read_bytes()
    except JournalFailure:
        raise
    except FileNotFoundError as exc:
        raise JournalFailure("CONTRACT_RESOURCE_MISSING", CONTRACT_RESOURCE) from exc
    except OSError as exc:
        raise JournalFailure("CONTRACT_RESOURCE_INVALID", type(exc).__name__) from exc


def _schema_resource_bytes() -> bytes:
    """Read exact bytes from the fixed, shape-only certificate schema resource."""
    try:
        resource = resources.files("institutional_signal_engine").joinpath(SCHEMA_RESOURCE)
        if isinstance(resource, Path) and (resource.is_symlink() or not resource.is_file()):
            raise JournalFailure("CERTIFICATE_SCHEMA_RESOURCE_INVALID", SCHEMA_RESOURCE)
        return resource.read_bytes()
    except JournalFailure:
        raise
    except FileNotFoundError as exc:
        raise JournalFailure("CERTIFICATE_SCHEMA_RESOURCE_MISSING", SCHEMA_RESOURCE) from exc
    except OSError as exc:
        raise JournalFailure("CERTIFICATE_SCHEMA_RESOURCE_INVALID", type(exc).__name__) from exc


def _strict_string_list(value: object, field: str) -> list[str]:
    if not isinstance(value, list) or any(type(item) is not str for item in value):
        raise JournalFailure("CONTRACT_VALIDATION_FAILED", field)
    return cast(list[str], value)


def _load_authoritative_contract() -> _AuthoritativeContract:
    """Load, digest-pin, deserialize, and fully validate the sole contract anchor."""
    raw_bytes = _contract_resource_bytes()
    digest = __import__("hashlib").sha256(raw_bytes).hexdigest()
    if digest != CONTRACT_CANONICAL_SHA256:
        raise JournalFailure("CONTRACT_DIGEST_MISMATCH", digest)
    try:
        decoded = raw_bytes.decode("utf-8", errors="strict")
        raw_object = json.loads(decoded)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise JournalFailure("CONTRACT_DESERIALIZATION_FAILED", type(exc).__name__) from exc
    if not isinstance(raw_object, dict):
        raise JournalFailure("CONTRACT_VALIDATION_FAILED", "root")
    raw = cast(dict[str, object], raw_object)
    required_root = {
        "contract_version",
        "certificate_version",
        "purpose",
        "expected",
        "allowed_record_kinds",
        "allowed_lifecycle_order",
        "required_invariants",
        "external_build_envelope_excluded",
        "safety",
    }
    if set(raw) != required_root:
        raise JournalFailure("CONTRACT_VALIDATION_FAILED", "root_fields")
    if (
        raw["contract_version"] != CONTRACT_VERSION
        or raw["certificate_version"] != CERTIFICATION_VERSION
    ):
        raise JournalFailure("CONTRACT_VALIDATION_FAILED", "versions")
    if type(raw["purpose"]) is not str:
        raise JournalFailure("CONTRACT_VALIDATION_FAILED", "purpose")
    expected_object = raw["expected"]
    if not isinstance(expected_object, dict):
        raise JournalFailure("CONTRACT_VALIDATION_FAILED", "expected")
    expected = cast(dict[str, object], expected_object)
    expected_fields = {
        "run_id",
        "scenario_canonical_sha256",
        "configuration_versions",
        "rth_start",
        "rth_stop",
        "intake_stop",
        "epoch_memberships",
        "clock_boundaries",
        "accepted_events",
        "rejected_events",
        "restoration_membership",
    }
    if set(expected) != expected_fields:
        raise JournalFailure("CONTRACT_VALIDATION_FAILED", "expected_fields")
    configuration_object = expected["configuration_versions"]
    expected_configuration_fields = {"scenario", "control_model", "shadow_model", "coverage"}
    if (
        not isinstance(configuration_object, dict)
        or set(configuration_object) != expected_configuration_fields
        or any(type(value) is not str for value in configuration_object.values())
        or type(expected["scenario_canonical_sha256"]) is not str
    ):
        raise JournalFailure("CONTRACT_VALIDATION_FAILED", "configuration_versions")
    configuration_versions = tuple(sorted(cast(dict[str, str], configuration_object).items()))
    event_fields = {"event_id", "epoch_sequence", "contract_identity", "channel"}

    def events(field: str) -> tuple[tuple[str, int, str, str], ...]:
        items = expected[field]
        if not isinstance(items, list):
            raise JournalFailure("CONTRACT_VALIDATION_FAILED", field)
        result: list[tuple[str, int, str, str]] = []
        for item in items:
            if not isinstance(item, dict) or set(item) != event_fields:
                raise JournalFailure("CONTRACT_VALIDATION_FAILED", field)
            typed = cast(dict[str, object], item)
            if (
                type(typed["event_id"]) is not str
                or type(typed["epoch_sequence"]) is not int
                or type(typed["contract_identity"]) is not str
                or type(typed["channel"]) is not str
            ):
                raise JournalFailure("CONTRACT_VALIDATION_FAILED", field)
            try:
                UUID(typed["event_id"])
            except ValueError as exc:
                raise JournalFailure("CONTRACT_VALIDATION_FAILED", field) from exc
            result.append(
                (
                    typed["event_id"],
                    typed["epoch_sequence"],
                    typed["contract_identity"],
                    typed["channel"],
                )
            )
        if len(set(result)) != len(result):
            raise JournalFailure("CONTRACT_VALIDATION_FAILED", f"{field}_duplicate")
        return tuple(result)

    epoch_values = expected["epoch_memberships"]
    if not isinstance(epoch_values, list):
        raise JournalFailure("CONTRACT_VALIDATION_FAILED", "epoch_memberships")
    epoch_memberships = tuple(
        tuple(_strict_string_list(value, "epoch_memberships")) for value in epoch_values
    )
    for field in ("run_id", "rth_start", "rth_stop", "intake_stop"):
        if type(expected[field]) is not str:
            raise JournalFailure("CONTRACT_VALIDATION_FAILED", field)
    try:
        run_id = UUID(cast(str, expected["run_id"]))
        for field in ("rth_start", "rth_stop", "intake_stop"):
            parsed = datetime.fromisoformat(cast(str, expected[field]))
            if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
                raise ValueError(field)
    except ValueError as exc:
        raise JournalFailure("CONTRACT_VALIDATION_FAILED", "identity_or_time") from exc
    safety = raw["safety"]
    if safety != {
        "trading_enabled": False,
        "orders_constructed": 0,
        "orders_submitted": 0,
        "live_providers": False,
    }:
        raise JournalFailure("CONTRACT_VALIDATION_FAILED", "safety")
    required_invariants = _strict_string_list(raw["required_invariants"], "required_invariants")
    lifecycle_order = _strict_string_list(raw["allowed_lifecycle_order"], "allowed_lifecycle_order")
    if not required_invariants or len(set(required_invariants)) != len(required_invariants):
        raise JournalFailure("CONTRACT_VALIDATION_FAILED", "required_invariants")
    if not lifecycle_order or len(set(lifecycle_order)) != len(lifecycle_order):
        raise JournalFailure("CONTRACT_VALIDATION_FAILED", "allowed_lifecycle_order")
    if len(epoch_memberships) != 3:
        raise JournalFailure("CONTRACT_VALIDATION_FAILED", "epoch_memberships")
    clock_boundaries = _strict_string_list(expected["clock_boundaries"], "clock_boundaries")
    if len(clock_boundaries) != 2:
        raise JournalFailure("CONTRACT_VALIDATION_FAILED", "clock_boundaries")
    contract = AcceptanceContract(
        contract_version=raw["contract_version"],
        certificate_version=raw["certificate_version"],
        expected_run_id=run_id,
        allowed_record_kinds=frozenset(
            _strict_string_list(raw["allowed_record_kinds"], "allowed_record_kinds")
        ),
        expected_epoch_memberships=epoch_memberships,
        expected_clock_boundaries=tuple(clock_boundaries),
        expected_rth_start=cast(str, expected["rth_start"]),
        expected_rth_stop=cast(str, expected["rth_stop"]),
        expected_intake_stop=cast(str, expected["intake_stop"]),
        expected_accepted_events=events("accepted_events"),
        expected_rejected_events=events("rejected_events"),
        expected_restoration_membership=tuple(
            _strict_string_list(expected["restoration_membership"], "restoration_membership")
        ),
        required_invariants=tuple(required_invariants),
        allowed_lifecycle_order=tuple(lifecycle_order),
    )
    _strict_string_list(raw["external_build_envelope_excluded"], "external_build_envelope_excluded")
    return _AuthoritativeContract(
        contract,
        digest,
        expected["scenario_canonical_sha256"],
        configuration_versions,
    )


class _MutableClock:
    def __init__(self, start: datetime) -> None:
        self._now = start

    def __call__(self) -> datetime:
        return self._now

    def set(self, value: datetime) -> None:
        if value < self._now:
            raise ValueError("clock_cannot_move_backwards")
        self._now = value


_CONTRACT_A = ThetaContract("A", 20260821, 10000, "C")
_CONTRACT_B = ThetaContract("B", 20260821, 11000, "C")
_CONTRACTS_BY_ID = MappingProxyType(
    {"A:20260821:10000:C": _CONTRACT_A, "B:20260821:11000:C": _CONTRACT_B}
)


class _DeterministicPlanner:
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
            selected_contracts=tuple(
                _CONTRACTS_BY_ID[value]
                for value in _checked_in_scenario().epoch_memberships[sequence - 1]
            ),
            previous_contracts=previous_contracts,
            provenance="deterministic-causal-certification-v2",
        )


class _DeterministicAdapter:
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


class _EmptyEventStream:
    def events(self) -> Any:
        async def values() -> Any:
            if False:
                yield None

        return values()

    async def health(self) -> dict[str, object]:
        return {"status": "healthy"}


class _DeterministicDiscovery:
    async def discover(self, symbols: tuple[str, ...], as_of: datetime) -> tuple[object, ...]:
        return tuple(symbols)

    async def prices(self, symbols: tuple[str, ...]) -> dict[str, Decimal]:
        return {symbol: Decimal(100) for symbol in symbols}


class _DeterministicEnrichment:
    async def enrich(
        self,
        discovered: tuple[object, ...],
        prices: dict[str, Decimal],
        as_of: datetime,
    ) -> tuple[UniverseSelection, ...]:
        del discovered, prices, as_of
        return ()


def _make_event(number: int, contract: ThetaContract, channel: str, at: datetime) -> CanonicalEvent:
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


class _DeterministicSessionDriver(SessionDriverPort):
    """Timestamp-ordered inputs; timers are selected independently by the shell."""

    def __init__(self, clock: _MutableClock, signals: list[DriverSignal]) -> None:
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


def generate_phase4b_certification(
    output_paths: CertificationOutputPaths,
) -> CertificationArtifacts:
    """Run and publish the sole authoritative Phase 4B certification lifecycle."""
    if type(output_paths) is not CertificationOutputPaths:
        raise JournalFailure("OUTPUT_PATHS_INVALID")
    requested_destinations = (
        output_paths.journal,
        output_paths.persistence_receipt,
        output_paths.replay_receipt,
        output_paths.certificate,
    )
    if any(not isinstance(path, Path) for path in requested_destinations):
        raise JournalFailure("OUTPUT_PATHS_INVALID")
    destinations = tuple(
        path.parent.resolve(strict=False) / path.name for path in requested_destinations
    )
    if len(set(destinations)) != len(destinations):
        raise JournalFailure("OUTPUT_PATHS_NOT_DISTINCT")
    parents = {path.parent for path in destinations}
    if len(parents) != 1:
        raise JournalFailure("OUTPUT_PATHS_INVALID", "common_parent_required")
    output_parent = parents.pop()
    output_parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if output_parent.is_symlink() or not output_parent.is_dir():
        raise JournalFailure("OUTPUT_PATHS_INVALID", "parent")
    if any(path.exists() or path.is_symlink() for path in destinations):
        raise JournalFailure("OUTPUT_TARGET_EXISTS")

    authority = _load_authoritative_contract()
    contract = authority.value
    scenario = _checked_in_scenario()
    scenario_digest = sha256(scenario.as_dict())
    if scenario_digest != SCENARIO_CANONICAL_SHA256:
        raise JournalFailure("SCENARIO_DIGEST_MISMATCH", scenario_digest)
    if (
        scenario.version != SCENARIO_VERSION
        or scenario_digest != authority.scenario_canonical_sha256
        or scenario.run_id != contract.expected_run_id
        or scenario.rth_start != contract.expected_rth_start
        or scenario.rth_stop != contract.expected_rth_stop
        or scenario.intake_stop != contract.expected_intake_stop
        or scenario.epoch_memberships != contract.expected_epoch_memberships
    ):
        raise JournalFailure("SCENARIO_CONTRACT_MISMATCH")

    async def compose_owned_scenario() -> VerifiedJournal:
        start = datetime.fromisoformat(scenario.rth_start)
        clock = _MutableClock(start)
        signals: list[DriverSignal] = []
        for signal in scenario.signals:
            timestamp = datetime.fromisoformat(signal.timestamp)
            if signal.kind == "disconnect":
                signals.append(DriverSignal("disconnect", timestamp))
                continue
            if (
                signal.kind != "event"
                or signal.event_number is None
                or signal.contract_identity is None
                or signal.channel is None
            ):
                raise JournalFailure("SCENARIO_DEFINITION_INVALID")
            signals.append(
                DriverSignal(
                    "event",
                    timestamp,
                    _make_event(
                        signal.event_number,
                        _CONTRACTS_BY_ID[signal.contract_identity],
                        signal.channel,
                        start,
                    ),
                )
            )
        shell = OrchestrationShell(
            config=OrchestrationConfig(
                run_id=scenario.run_id,
                session_date=scenario.session_date,
                rth_start=start,
                rth_stop=datetime.fromisoformat(scenario.rth_stop),
                intake_stop=datetime.fromisoformat(scenario.intake_stop),
                reevaluation_interval=timedelta(seconds=scenario.reevaluation_seconds),
                scenario_version=scenario.version,
                acceptance_contract_version=contract.contract_version,
            ),
            settings=Settings(
                alpaca_key_id=SecretStr("fixture-key"),
                alpaca_secret_key=SecretStr("fixture-secret"),
                theta_api_key=SecretStr("fixture-theta"),
                database_url=None,
            ),
            clock=clock,
            discovery=_DeterministicDiscovery(),
            enrichment=_DeterministicEnrichment(),
            planner=_DeterministicPlanner(),
            subscription_adapter=cast(Any, _DeterministicAdapter()),
            event_stream=cast(Any, _EmptyEventStream()),
            repository=InMemoryRepository(),
            session_driver=_DeterministicSessionDriver(clock, signals),
        )
        result = await shell.run()
        if result.status != "live_observation_complete":
            raise JournalFailure("COMPOSITION_FAILED", result.status)
        return verify_complete(shell.journal, contract)

    try:
        original = asyncio.run(compose_owned_scenario())
    except RuntimeError as exc:
        if "asyncio.run() cannot be called" in str(exc):
            raise JournalFailure("CERTIFICATION_EVENT_LOOP_ACTIVE") from exc
        raise
    journal_repository = InMemoryJournalRepository()
    persistence = persist_verified_journal(contract, original, journal_repository)
    try:
        loaded = journal_repository.load(persistence.persistence_identity)
    except JournalFailure:
        raise
    except Exception as exc:
        raise JournalFailure("PERSISTED_JOURNAL_MISSING", type(exc).__name__) from exc
    if loaded != original.canonical_serialization:
        raise JournalFailure("PERSISTED_JOURNAL_BYTES_MISMATCH")
    reconstructed = verify_complete(Journal.deserialize(loaded), contract)
    if reconstructed is original:
        raise JournalFailure("REPLAY_RECONSTRUCTION_NOT_DISTINCT")
    verify_persistence_receipt(contract, original, persistence)

    def observed_projection(value: VerifiedJournal) -> dict[str, object]:
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

    original_observed = observed_projection(original)
    observed = observed_projection(reconstructed)
    original_projection_digest = sha256(original_observed)
    replayed_projection_digest = sha256(observed)
    original_byte_digest = sha256_bytes(original.canonical_serialization)
    reconstructed_byte_digest = sha256_bytes(reconstructed.canonical_serialization)
    replay = ReplayReceipt.from_values(
        contract_version=contract.contract_version,
        run_id=reconstructed.run_id,
        persistence_identity=persistence.persistence_identity,
        original_root_digest=original.root_digest,
        reconstructed_root_digest=reconstructed.root_digest,
        original_byte_sha256=original_byte_digest,
        reconstructed_byte_sha256=reconstructed_byte_digest,
        original_record_count=original.seal.record_count,
        reconstructed_record_count=reconstructed.seal.record_count,
        original_projection_digest=original_projection_digest,
        replayed_projection_digest=replayed_projection_digest,
        exact_equality=(
            loaded == original.canonical_serialization
            and reconstructed.canonical_serialization == original.canonical_serialization
            and reconstructed.root_digest == original.root_digest
            and reconstructed.seal.record_count == original.seal.record_count
            and original_projection_digest == replayed_projection_digest
        ),
    )
    verify_replay_receipt(contract, original, persistence, replay)

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
        "scenario_binding": scenario_digest == authority.scenario_canonical_sha256,
        "configuration_versions_exact": cast(dict[str, object], observed["configuration_versions"])
        == dict(authority.configuration_versions),
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
        "persistence_complete": persistence.persistence_completed is True,
        "replay_exact": replay.exact_equality is True,
        "trading_disabled": orders["trading_enabled"] is False,
        "orders_zero": orders["constructed"] == 0 and orders["submitted"] == 0,
    }
    unknown = set(contract.required_invariants) - set(invariants)
    if unknown:
        raise JournalFailure("CONTRACT_REQUIRED_INVARIANT_UNKNOWN", min(unknown))
    failures = [name for name in contract.required_invariants if not invariants[name]]
    if failures:
        raise JournalFailure("CERTIFICATION_INVARIANT_FAILED", failures[0])
    certificate: dict[str, object] = {
        "certificate_version": contract.certificate_version,
        "evidence_kind": "RUNTIME_EVIDENCE_BUNDLE_CERTIFICATE",
        "acceptance_contract": {
            "contract_version": contract.contract_version,
            "canonical_sha256": authority.canonical_sha256,
            "expected_run_id": str(contract.expected_run_id),
            "expected_epoch_memberships": [
                list(membership) for membership in contract.expected_epoch_memberships
            ],
            "expected_clock_boundaries": list(contract.expected_clock_boundaries),
            "required_invariants": list(contract.required_invariants),
        },
        "deterministic_scenario": {
            "version": scenario.version,
            "canonical_sha256": scenario_digest,
        },
        "observed_projection_sha256": replayed_projection_digest,
        "observed": {
            **observed,
            "journal": {
                "record_count": reconstructed.seal.record_count,
                "terminal_sequence": reconstructed.seal.terminal_sequence,
                "root_digest": reconstructed.root_digest,
                "seal_digest": reconstructed.seal.seal_digest,
                "canonical_byte_sha256": reconstructed_byte_digest,
            },
            "persistence": persistence.as_dict(),
            "replay": replay.as_dict(),
        },
        "invariants": invariants,
        "failed_invariants": [],
        "overall": "PASS",
    }
    certificate_commitment_sha256 = sha256(certificate)
    certificate["certificate_canonical_sha256"] = certificate_commitment_sha256
    schema_errors = validate_certificate_schema(certificate)
    if schema_errors:
        raise JournalFailure("CERTIFICATE_SCHEMA_INVALID", ";".join(schema_errors))

    serialized = (
        original.canonical_serialization,
        persistence.serialize(),
        replay.serialize(),
        canonical_json(certificate),
    )
    temporary_paths: list[Path] = []
    published_paths: list[Path] = []
    try:
        for path, content in zip(destinations, serialized, strict=True):
            descriptor, temporary_name = tempfile.mkstemp(
                prefix=f".{path.name}.", suffix=".tmp", dir=output_parent
            )
            temporary = Path(temporary_name)
            temporary_paths.append(temporary)
            with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(temporary, 0o600)
        for path, temporary in zip(destinations, temporary_paths, strict=True):
            if path.exists() or path.is_symlink():
                raise JournalFailure("OUTPUT_TARGET_EXISTS", str(path))
            temporary.replace(path)
            published_paths.append(path)
        temporary_paths.clear()
    except JournalFailure:
        for path in temporary_paths:
            path.unlink(missing_ok=True)
        for path in published_paths:
            path.unlink(missing_ok=True)
        raise
    except OSError as exc:
        for path in temporary_paths:
            path.unlink(missing_ok=True)
        for path in published_paths:
            path.unlink(missing_ok=True)
        raise JournalFailure("ARTIFACT_PUBLICATION_FAILED", type(exc).__name__) from exc
    return CertificationArtifacts(
        output_paths=output_paths,
        journal_sha256=sha256_bytes(serialized[0]),
        persistence_receipt_sha256=sha256_bytes(serialized[1]),
        replay_receipt_sha256=sha256_bytes(serialized[2]),
        certificate_sha256=sha256_bytes(serialized[3]),
        certificate_commitment_sha256=certificate_commitment_sha256,
    )


def validate_certificate_schema(value: dict[str, object]) -> list[str]:
    """Validate certificate shape only; this is never operational verification."""
    raw_schema = _schema_resource_bytes()
    try:
        schema = json.loads(raw_schema.decode("utf-8", errors="strict"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise JournalFailure("CERTIFICATE_SCHEMA_DESERIALIZATION_FAILED") from exc
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
        artifacts = generate_phase4b_certification(
            CertificationOutputPaths(
                certificate=arguments.output,
                journal=arguments.journal_output,
                persistence_receipt=arguments.persistence_output,
                replay_receipt=arguments.replay_output,
            )
        )
    except JournalFailure as exc:
        print(
            json.dumps({"certification_status": "FAILED", "failure_code": exc.code}, sort_keys=True)
        )
        return 1
    print(
        json.dumps(
            {
                "certification_status": "AUTHORITATIVE_ARTIFACTS_PUBLISHED",
                "journal_sha256": artifacts.journal_sha256,
                "persistence_receipt_sha256": artifacts.persistence_receipt_sha256,
                "replay_receipt_sha256": artifacts.replay_receipt_sha256,
                "certificate_sha256": artifacts.certificate_sha256,
                "certificate_path": str(artifacts.output_paths.certificate),
                "journal_path": str(artifacts.output_paths.journal),
                "persistence_path": str(artifacts.output_paths.persistence_receipt),
                "replay_path": str(artifacts.output_paths.replay_receipt),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
