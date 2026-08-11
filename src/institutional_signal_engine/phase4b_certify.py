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
    JOURNAL_KIND_INTAKE_STOPPED,
    JOURNAL_KIND_JOURNAL_PERSISTED,
    JOURNAL_KIND_PERSISTENCE_DRAINED,
    JOURNAL_KIND_PROVIDER_DISCONNECTED,
    JOURNAL_KIND_PROVIDER_RECONNECTED,
    JOURNAL_KIND_REEVALUATION_START,
    JOURNAL_KIND_REPLAY_VERIFIED,
    JOURNAL_KIND_SESSION_FINALIZED,
    JOURNAL_KIND_SUBSCRIPTION_ACKNOWLEDGEMENT,
    JOURNAL_KIND_SUBSCRIPTION_COMMAND,
    InMemoryJournalRepository,
    Journal,
    JournalFailure,
    JournalRecord,
    VerifiedJournal,
    sha256,
    verify_complete,
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

CERTIFICATION_VERSION = "PHASE4B_CAUSAL_CERT_V2"
SCENARIO_VERSION = "PHASE4B_ACCELERATED_CAUSAL_RTH_V2"
RUN_ID = UUID("4b000000-0000-4000-8000-000000000027")
ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = ROOT / "docs/phase4b/PHASE4B_CERT_V1.schema.json"
DEFAULT_OUTPUT = Path("/tmp/phase4b-certification/CERTIFICATE.json")
DEFAULT_JOURNAL_OUTPUT = Path("/tmp/phase4b-certification/JOURNAL.json")


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
    original: VerifiedJournal
    replayed: VerifiedJournal
    journal_repository: InMemoryJournalRepository
    journal_identity: str


async def execute_composition() -> Composition:
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
        journal_repository=journal_repository,
    )
    result = await shell.run()
    if result.status != "live_observation_complete":
        raise JournalFailure("COMPOSITION_FAILED", result.status)
    original = verify_complete(shell.journal)
    identity = f"memory://journal/{RUN_ID}"
    replayed = verify_complete(Journal.deserialize(journal_repository.load(identity)))
    if original.canonical_serialization != replayed.canonical_serialization:
        raise JournalFailure("PERSISTED_REPLAY_MISMATCH")
    if acceptance_projection(original) != acceptance_projection(replayed):
        raise JournalFailure("REPLAY_PROJECTION_MISMATCH")
    return Composition(original, replayed, journal_repository, identity)


def _require_verified(value: object) -> VerifiedJournal:
    if not isinstance(value, VerifiedJournal):
        raise JournalFailure("PROJECTION_REQUIRES_VERIFIED_JOURNAL")
    reconstructed = Journal.deserialize(value.canonical_serialization)
    independently_verified = verify_complete(reconstructed)
    if independently_verified != value:
        raise JournalFailure("VERIFIED_JOURNAL_VALUE_MISMATCH")
    return independently_verified


def acceptance_projection(journal: VerifiedJournal) -> dict[str, object]:
    """Project acceptance from exactly one sealed, independently verified input."""
    verified = _require_verified(journal)
    records = verified.records

    def by_kind(kind: str) -> tuple[JournalRecord, ...]:
        return tuple(record for record in records if record.kind == kind)

    configuration = by_kind(JOURNAL_KIND_CONFIGURATION)[0]
    epochs = by_kind(JOURNAL_KIND_EPOCH_CREATED)
    commands = by_kind(JOURNAL_KIND_SUBSCRIPTION_COMMAND)
    acknowledgements = {
        record.command_id: record for record in by_kind(JOURNAL_KIND_SUBSCRIPTION_ACKNOWLEDGEMENT)
    }
    accepted = by_kind(JOURNAL_KIND_EVENT_ACCEPTED)
    rejected = by_kind(JOURNAL_KIND_EVENT_REJECTED)
    persisted = by_kind(JOURNAL_KIND_JOURNAL_PERSISTED)[0]
    replayed = by_kind(JOURNAL_KIND_REPLAY_VERIFIED)[0]
    finalized = by_kind(JOURNAL_KIND_SESSION_FINALIZED)[0]

    epoch_projection: list[dict[str, object]] = []
    previous: set[str] = set()
    for epoch in epochs:
        membership = set(map(str, cast(list[object], epoch.payload["membership"])))
        epoch_projection.append(
            {
                "sequence": epoch.epoch_sequence,
                "epoch_id": epoch.epoch_id,
                "membership": sorted(item.split(":", 1)[0] for item in membership),
                "admissions": sorted(item.split(":", 1)[0] for item in membership - previous),
                "removals": sorted(item.split(":", 1)[0] for item in previous - membership),
                "provenance": epoch.payload["provenance"],
            }
        )
        previous = membership

    subscription_projection: list[dict[str, object]] = [
        {
            "command_id": command.command_id,
            "epoch_sequence": command.epoch_sequence,
            "contract": cast(str, command.contract_identity).split(":", 1)[0],
            "action": command.payload["action"],
            "channel": command.payload["channel"],
            "ack_id": acknowledgements[command.command_id].ack_id,
            "acknowledged": acknowledgements[command.command_id].payload["accepted"],
            "command_sequence": command.sequence,
            "ack_sequence": acknowledgements[command.command_id].sequence,
        }
        for command in commands
    ]

    def event_projection(values: tuple[JournalRecord, ...]) -> list[dict[str, object]]:
        return [
            {
                "event_id": record.payload["event_id"],
                "epoch_sequence": record.epoch_sequence,
                "contract": cast(str, record.contract_identity).split(":", 1)[0],
                "channel": record.payload["event_kind"],
            }
            for record in values
        ]

    invariants = {
        "epochs_exact": [item["membership"] for item in epoch_projection]
        == [["A"], ["A", "B"], ["B"]],
        "paired_commands_acknowledged": all(
            item["acknowledged"] is True
            and cast(int, item["command_sequence"]) < cast(int, item["ack_sequence"])
            for item in subscription_projection
        ),
        "clock_driven_reevaluations": len(by_kind(JOURNAL_KIND_REEVALUATION_START)) == 2
        and len(by_kind(JOURNAL_KIND_CLOCK_ADVANCED)) == 2,
        "disconnect_reconnect_restore": len(by_kind(JOURNAL_KIND_PROVIDER_DISCONNECTED)) == 1
        and len(by_kind(JOURNAL_KIND_PROVIDER_RECONNECTED)) == 1
        and len(by_kind(JOURNAL_KIND_EPOCH_RESTORED)) == 1,
        "removed_a_rejected": any(
            record.epoch_sequence == 3 and cast(str, record.contract_identity).startswith("A:")
            for record in rejected
        ),
        "active_b_accepted": any(
            record.epoch_sequence == 3 and cast(str, record.contract_identity).startswith("B:")
            for record in accepted
        ),
        "lifecycle_complete": len(by_kind(JOURNAL_KIND_INTAKE_STOPPED)) == 1
        and len(by_kind(JOURNAL_KIND_PERSISTENCE_DRAINED)) == 1
        and len(by_kind(JOURNAL_KIND_SESSION_FINALIZED)) == 1,
        "persistence_and_replay": replayed.payload["exact_equal"] is True,
        "trading_disabled": configuration.payload["trading_enabled"] is False
        and finalized.payload["trading_enabled"] is False,
        "orders_zero": configuration.payload["orders_constructed"] == 0
        and configuration.payload["orders_submitted"] == 0
        and finalized.payload["orders_constructed"] == 0
        and finalized.payload["orders_submitted"] == 0,
    }
    failures = [name for name, passed in invariants.items() if not passed]
    return {
        "certification_version": CERTIFICATION_VERSION,
        "evidence_kind": "RUNTIME_CERTIFICATE",
        "run_id": str(verified.run_id),
        "journal": {
            "record_count": verified.seal.record_count,
            "terminal_sequence": verified.seal.terminal_sequence,
            "terminal_digest": verified.seal.terminal_digest,
            "seal_digest": verified.seal.seal_digest,
        },
        "configuration_versions": {
            "scenario": configuration.payload["scenario_version"],
            "control_model": configuration.payload["control_model_version"],
            "shadow_model": configuration.payload["shadow_model_version"],
            "coverage": configuration.payload["coverage_version"],
        },
        "epochs": epoch_projection,
        "subscriptions": subscription_projection,
        "events": {
            "accepted": event_projection(accepted),
            "rejected": event_projection(rejected),
        },
        "clock": {
            "advancements": len(by_kind(JOURNAL_KIND_CLOCK_ADVANCED)),
            "reevaluations": len(by_kind(JOURNAL_KIND_REEVALUATION_START)),
        },
        "recovery": {
            "disconnects": len(by_kind(JOURNAL_KIND_PROVIDER_DISCONNECTED)),
            "reconnects": len(by_kind(JOURNAL_KIND_PROVIDER_RECONNECTED)),
            "restorations": len(by_kind(JOURNAL_KIND_EPOCH_RESTORED)),
        },
        "persistence": {
            "identity": persisted.payload["persisted_identity"],
            "digest": persisted.payload["persisted_digest"],
            "queue_depth_final": persisted.payload["queue_depth_final"],
        },
        "replay": {
            "exact_equal": replayed.payload["exact_equal"],
            "record_count": replayed.payload["replayed_record_count"],
            "terminal_digest": replayed.payload["replayed_terminal_digest"],
        },
        "orders": {
            "trading_enabled": finalized.payload["trading_enabled"],
            "constructed": finalized.payload["orders_constructed"],
            "submitted": finalized.payload["orders_submitted"],
        },
        "invariants": invariants,
        "failed_invariants": failures,
        "overall": "PASS" if not failures else "FAIL",
    }


# Compatibility name intentionally retains the now-pure, one-input boundary.
_project_certificate = acceptance_projection


def validate_certificate(value: dict[str, object]) -> list[str]:
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    return [
        f"{'/'.join(str(item) for item in error.absolute_path) or '<root>'}: {error.message}"
        for error in sorted(validator.iter_errors(value), key=lambda item: list(item.absolute_path))
    ]


def write_artifacts(
    composition: Composition, output: Path, journal_output: Path
) -> dict[str, object]:
    certificate = acceptance_projection(composition.replayed)
    errors = validate_certificate(certificate)
    if errors:
        raise JournalFailure("CERTIFICATE_SCHEMA_INVALID", ";".join(errors))
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(certificate, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    journal_output.parent.mkdir(parents=True, exist_ok=True)
    journal_output.write_text(composition.replayed.canonical_serialization + "\n", encoding="utf-8")
    return certificate


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--journal-output", type=Path, default=DEFAULT_JOURNAL_OUTPUT)
    arguments = parser.parse_args(argv)
    try:
        composition = asyncio.run(execute_composition())
        certificate = write_artifacts(composition, arguments.output, arguments.journal_output)
    except JournalFailure as exc:
        print(json.dumps({"overall": "FAIL", "failure_code": exc.code}, sort_keys=True))
        return 1
    print(
        json.dumps(
            {
                "overall": certificate["overall"],
                "journal_sha256": sha256(json.loads(composition.replayed.canonical_serialization)),
                "certificate_path": str(arguments.output),
                "journal_path": str(arguments.journal_output),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
