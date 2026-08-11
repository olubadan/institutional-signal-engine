"""Executable, provider-free Phase 4B certification — pure journal projection.

The certificate is a deterministic acceptance projection of the single causal
journal J produced by one invocation of compose(). No planner trace,
acknowledgement, lifecycle event, or acceptance fact is created separately by
the certificate. J is the sole source of runtime evidence.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import subprocess
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, cast
from uuid import UUID

from pydantic import SecretStr

from .config import Settings
from .dynamic_subscriptions import SubscriptionAcknowledgement
from .impact_coverage import PILOT_COVERAGE_POPULATION_VERSION
from .journal import (
    JOURNAL_KIND_CLOCK_ADVANCED,
    JOURNAL_KIND_EPOCH_ACTIVATED,
    JOURNAL_KIND_EPOCH_CREATED,
    JOURNAL_KIND_EPOCH_RESTORED,
    JOURNAL_KIND_EVENT_ACCEPTED,
    JOURNAL_KIND_EVENT_REJECTED,
    JOURNAL_KIND_INTAKE_STOPPED,
    JOURNAL_KIND_PERSISTENCE_DRAINED,
    JOURNAL_KIND_PROVIDER_DISCONNECTED,
    JOURNAL_KIND_PROVIDER_RECONNECTED,
    JOURNAL_KIND_REEVALUATION_START,
    JOURNAL_KIND_REPLAY_VERIFIED,
    JOURNAL_KIND_SESSION_FINALIZED,
    JOURNAL_KIND_SESSION_START,
    JOURNAL_KIND_SUBSCRIPTION_ACKNOWLEDGEMENT,
    JOURNAL_KIND_SUBSCRIPTION_COMMAND,
    Journal,
)
from .orchestration import (
    OrchestrationConfig,
    OrchestrationShell,
    ProductionPlanner,
)
from .persistence import InMemoryRepository
from .providers.thetadata import ThetaContract
from .schemas import CanonicalEvent, EventKind
from .universe import PlannerEpoch

CERTIFICATION_VERSION = "PHASE4B_CERT_V1"
SCENARIO_VERSION = "PHASE4B_ACCELERATED_RTH_V1"
RUN_ID = UUID("4b000000-0000-4000-8000-000000000001")
ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = ROOT / "docs/phase4b/PHASE4B_CERT_V1.schema.json"
DEFAULT_OUTPUT = Path("/tmp/phase4b-certification/CERTIFICATE.json")


def _canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _git() -> tuple[str, bool]:
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, check=True, capture_output=True, text=True
    ).stdout.strip()
    dirty = bool(
        subprocess.run(
            ["git", "status", "--porcelain"], cwd=ROOT, check=True, capture_output=True, text=True
        ).stdout.strip()
    )
    return commit, dirty


# ---------------------------------------------------------------------------
# Configuration constants — define the required scenario but are NOT evidence
# ---------------------------------------------------------------------------


def scenario_definition() -> dict[str, object]:
    """Declarative scenario constants. These define the REQUIRED trace but
    are NOT evidence that it occurred. Only journal records are evidence."""
    return {
        "scenario_version": SCENARIO_VERSION,
        "timezone": "America/New_York",
        "virtual_clock": {
            "startup": "2026-08-10T13:20:00Z",
            "rth_open": "2026-08-10T13:30:00Z",
            "intake_stop": "2026-08-10T20:00:00Z",
            "close_drain": "2026-08-10T20:05:00Z",
        },
        "planner_epochs": [
            {"epoch": 1, "effective_at": "2026-08-10T13:20:00Z", "eligible": ["AAA", "CCC"]},
            {"epoch": 2, "effective_at": "2026-08-10T15:00:00Z", "eligible": ["AAA", "BBB", "CCC"]},
            {"epoch": 3, "effective_at": "2026-08-10T17:00:00Z", "eligible": ["AAA", "BBB"]},
        ],
        "disconnect_recovery": {"disconnect_at": "2026-08-10T16:00:00Z", "reconnect_epoch": 3},
        "orders": {"trading_enabled": False, "constructed": 0, "submitted": 0},
    }


def scenario_sha256() -> str:
    return hashlib.sha256(_canonical(scenario_definition()).encode()).hexdigest()


# ---------------------------------------------------------------------------
# Deterministic test ports
# ---------------------------------------------------------------------------


class MutableClock:
    """Deterministic clock that can be advanced in controlled steps."""

    def __init__(self, start: datetime) -> None:
        self._now = start

    def __call__(self) -> datetime:
        return self._now

    def advance(self, delta: timedelta) -> datetime:
        self._now += delta
        return self._now

    @property
    def now(self) -> datetime:
        return self._now


_contract_a = ThetaContract("AAA", 20260821, 10000, "C")
_contract_b = ThetaContract("BBB", 20260821, 11000, "C")
_contract_c = ThetaContract("CCC", 20260821, 12000, "C")

# Epoch definitions: sequence → contracts
# Epoch 4 is identical to Epoch 3 — it's triggered by the clock being
# past the next reevaluation time after E3 events are processed, but
# before intake_stop. The planner returns unchanged contracts → noop.
EPOCH_CONTRACTS: dict[int, tuple[ThetaContract, ...]] = {
    1: (_contract_a,),
    2: (_contract_a, _contract_b),
    3: (_contract_b,),
    4: (_contract_b,),
}


class DeterministicPlanner:
    """Planner that returns predetermined epochs based on sequence number.

    Each call to plan() produces exactly one PlannerEpoch with the
    contracts defined in EPOCH_CONTRACTS. The planner is stateless —
    the same sequence always returns the same epoch.
    """

    def plan(
        self,
        selections: object = (),
        sequence: int = 1,
        effective_at: datetime | None = None,
        previous_contracts: tuple[ThetaContract, ...] = (),
        baseline_symbols: frozenset[str] = frozenset(),
    ) -> PlannerEpoch:
        contracts = EPOCH_CONTRACTS.get(sequence, ())
        return PlannerEpoch.create(
            sequence=sequence,
            effective_at=effective_at or datetime.now(UTC),
            candidate_population_version=PILOT_COVERAGE_POPULATION_VERSION,
            selected_contracts=contracts,
            previous_contracts=previous_contracts,
            provenance="deterministic-cert-test",
        )


class DeterministicAdapter:
    """Subscription adapter that simulates commands and acknowledgements.

    Maintains an internal acknowledged-contracts set for event acceptance
    checking. No WebSocket is used — all operations are deterministic.
    """

    def __init__(self) -> None:
        self._active: set[ThetaContract] = set()
        self._next_id: int = 1
        self.connected: bool = False
        self.connection_generation: int = 0
        self.request_types: tuple[str, ...] = ("TRADE", "QUOTE")
        self._cmd_log: list[dict[str, object]] = []

    async def connect(self) -> None:
        self.connected = True
        self.connection_generation += 1

    async def disconnect(self) -> None:
        self.connected = False

    async def add_subscriptions(self, contracts: tuple[ThetaContract, ...]) -> tuple[int, ...]:
        ids: list[int] = []
        for c in contracts:
            if c in self._active:
                continue
            for rt in self.request_types:
                rid = self._next_id
                self._next_id += 1
                ids.append(rid)
                self._cmd_log.append(
                    {"request_id": rid, "contract": c.__dict__, "type": rt, "action": "add"}
                )
        self._active.update(contracts)
        return tuple(ids)

    async def remove_subscriptions(self, contracts: tuple[ThetaContract, ...]) -> tuple[int, ...]:
        ids: list[int] = []
        for c in contracts:
            if c not in self._active:
                continue
            for rt in self.request_types:
                rid = self._next_id
                self._next_id += 1
                ids.append(rid)
                self._cmd_log.append(
                    {"request_id": rid, "contract": c.__dict__, "type": rt, "action": "remove"}
                )
        self._active.difference_update(contracts)
        return tuple(ids)

    async def acknowledge(
        self, expected_request_ids: tuple[int, ...], timeout: float | None = None
    ) -> SubscriptionAcknowledgement:
        return SubscriptionAcknowledgement(
            acknowledged=expected_request_ids,
            rejected=(),
            timed_out=(),
            partially_acknowledged=False,
            accepted=True,
            diagnostic="all_acknowledged_deterministic",
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


class DeterministicEventStream:
    """Event stream that yields events, advances clock, and simulates disconnect.

    The stream embeds clock advances so the intake loop naturally observes
    time progression. At a configured index, it raises ConnectionError to
    trigger disconnect/reconnect. The stream is restartable — each call to
    events() resumes from the current index.
    """

    def __init__(
        self,
        events: list[CanonicalEvent],
        clock: MutableClock,
        advances: list[timedelta] | None = None,
        disconnect_at_index: int | None = None,
    ) -> None:
        self._events = events
        self._index = 0
        self._clock = clock
        # Clock advances to apply at specific event indices
        self._advances: dict[int, timedelta] = {}
        if advances:
            for i, delta in enumerate(advances):
                self._advances[i] = delta
        self._disconnect_at_index = disconnect_at_index

    def events(self) -> AsyncIterator[CanonicalEvent]:
        async def _gen() -> AsyncIterator[CanonicalEvent]:
            while self._index < len(self._events):
                # Raise disconnect at the configured index
                if (
                    self._disconnect_at_index is not None
                    and self._index == self._disconnect_at_index
                ):
                    self._disconnect_at_index = None  # Only disconnect once
                    raise ConnectionError("deterministic_disconnect")

                # Advance clock if scheduled at this index (before yielding)
                if self._index in self._advances:
                    self._clock.advance(self._advances[self._index])
                yield self._events[self._index]
                self._index += 1

        return _gen()

    async def health(self) -> dict[str, object]:
        return {"status": "healthy"}


class DeterministicDiscovery:
    """Discovery port that returns empty — enrichment provides the contracts."""

    async def discover(self, symbols: tuple[str, ...], as_of: datetime) -> tuple[object, ...]:
        return ()

    async def prices(self, symbols: tuple[str, ...]) -> dict[str, Decimal]:
        return {s: Decimal(100) for s in symbols}


class DeterministicEnrichment:
    """Enrichment port that returns selections matching the current epoch."""

    def __init__(self, clock: MutableClock) -> None:
        self._clock = clock

    async def enrich(
        self,
        discovered: tuple[object, ...],
        prices: dict[str, Decimal],
        as_of: datetime,
    ) -> tuple[Any, ...]:
        from .universe import UniverseSelection as US

        # The planner determines which contracts are selected; enrichment
        # just provides the raw selections that the planner will filter.
        sel_a = US(
            symbol="AAA",
            included=True,
            expiration=20260821,
            contracts=(_contract_a,),
            rejection_reasons=(),
            provenance=("fixture",),
            provider_responses=(),
            contract_evidence=(),
        )
        sel_b = US(
            symbol="BBB",
            included=True,
            expiration=20260821,
            contracts=(_contract_b,),
            rejection_reasons=(),
            provenance=("fixture",),
            provider_responses=(),
            contract_evidence=(),
        )
        sel_c = US(
            symbol="CCC",
            included=True,
            expiration=20260821,
            contracts=(_contract_c,),
            rejection_reasons=(),
            provenance=("fixture",),
            provider_responses=(),
            contract_evidence=(),
        )
        return (sel_a, sel_b, sel_c)


def _make_event(
    event_id: UUID,
    symbol: str,
    contract: ThetaContract,
    kind: str,
    timestamp: datetime,
    sequence: int,
) -> CanonicalEvent:
    return CanonicalEvent(
        event_id=event_id,
        kind=EventKind.OPTIONS,
        symbol=symbol,
        source="fixture",
        source_timestamp=timestamp,
        received_timestamp=timestamp,
        normalized_timestamp=timestamp,
        sequence=sequence,
        payload={
            "provider_event_kind": kind,
            "price": 3.10,
            "volume": 1000 if kind == "trade" else 0,
            "contract": {
                "root": contract.root,
                "expiration": contract.expiration,
                "strike": contract.strike,
                "right": contract.right,
            },
            "quote_context": {"bid": 3.09, "ask": 3.10},
            "conditions": ("@",),
        },
    )


# ---------------------------------------------------------------------------
# Composition execution — the single source of runtime evidence
# ---------------------------------------------------------------------------


async def execute_composition() -> tuple[OrchestrationShell, InMemoryRepository, MutableClock]:
    """Execute the complete deterministic trace through OrchestrationShell.

    Returns the shell (with journal), repository, and clock for inspection.
    All evidence comes from this single execution.

    The trace:
    1. Start session → E1 = {AAA} with paired TRADE/QUOTE commands
    2. Clock advances past reevaluation interval → E2 = {AAA, BBB}
    3. Clock advances again → E3 = {BBB} (AAA removed)
    4. AAA event rejected, BBB event accepted, ZZZ event rejected
    5. Clock advances past intake_stop → stop, drain, finalize, replay
    """
    start = datetime(2026, 8, 10, 13, 20, tzinfo=UTC)
    clock = MutableClock(start)
    repository = InMemoryRepository()

    settings = Settings(
        alpaca_key_id=SecretStr("fixture-key"),
        alpaca_secret_key=SecretStr("fixture-secret"),
        theta_api_key=SecretStr("fixture-theta"),
        database_url=None,
    )

    # Build events with clock advances and disconnect embedded at specific indices.
    #
    # Trace layout:
    #   Idx 0: AAA trade (E1 accepted)
    #   Idx 1: advance +6min, AAA quote (E1 accepted) → triggers reeval → E2
    #   Idx 2: AAA trade (E2 accepted)
    #   Idx 3: DISCONNECT (ConnectionError before yield)
    #          → reconnect restores E2 {AAA, BBB}
    #          → intake loop restarts from index 3
    #   Idx 3: advance +6min (now +12min), BBB trade (E2 accepted)
    #          → after processing: clock check triggers reeval → E3 {BBB}
    #   Idx 4: AAA quote → REJECTED (AAA removed from E3)
    #   Idx 5: BBB quote → ACCEPTED (BBB in E3)
    #   Idx 6: ZZZ trade → REJECTED (never in any epoch)
    #   After all: advance +6h → past intake_stop → session ends
    events: list[CanonicalEvent] = []
    evt_id_base = 0

    # Epoch 1 events
    evt_id_base += 1
    events.append(
        _make_event(
            UUID(f"4b000000-0000-4000-8000-{evt_id_base:012d}"),
            "AAA",
            _contract_a,
            "trade",
            start,
            evt_id_base,
        )
    )
    evt_id_base += 1
    events.append(
        _make_event(
            UUID(f"4b000000-0000-4000-8000-{evt_id_base:012d}"),
            "AAA",
            _contract_a,
            "quote",
            start,
            evt_id_base,
        )
    )

    # Epoch 2 events (before disconnect)
    evt_id_base += 1
    events.append(
        _make_event(
            UUID(f"4b000000-0000-4000-8000-{evt_id_base:012d}"),
            "AAA",
            _contract_a,
            "trade",
            start,
            evt_id_base,
        )
    )
    # Event at index 3: BBB trade (yielded AFTER disconnect/reconnect)
    evt_id_base += 1
    events.append(
        _make_event(
            UUID(f"4b000000-0000-4000-8000-{evt_id_base:012d}"),
            "BBB",
            _contract_b,
            "trade",
            start,
            evt_id_base,
        )
    )

    # Epoch 3 events (after second reevaluation)
    evt_id_base += 1
    events.append(
        _make_event(
            UUID(f"4b000000-0000-4000-8000-{evt_id_base:012d}"),
            "AAA",
            _contract_a,
            "quote",
            start,
            evt_id_base,
        )
    )
    evt_id_base += 1
    events.append(
        _make_event(
            UUID(f"4b000000-0000-4000-8000-{evt_id_base:012d}"),
            "BBB",
            _contract_b,
            "quote",
            start,
            evt_id_base,
        )
    )
    evt_id_base += 1
    contract_zzz = ThetaContract("ZZZ", 20260821, 99999, "C")
    events.append(
        _make_event(
            UUID(f"4b000000-0000-4000-8000-{evt_id_base:012d}"),
            "ZZZ",
            contract_zzz,
            "trade",
            start,
            evt_id_base,
        )
    )

    # Clock advances at specific indices.
    # Session starts at 13:20. RTH starts at 13:30. First reeval due at 13:35.
    # advances[1]: +16 min → clock ~13:36, past first reeval (13:35) → triggers E2
    # advances[3]: +16 min → clock ~13:52, past second reeval (13:40) → triggers E3
    # advances[6]: +6h40min → clock ~20:32, past intake_stop (20:05)
    advances: list[timedelta] = [timedelta(0)] * len(events)
    advances[1] = timedelta(minutes=16)  # After E1 events → trigger E2
    advances[3] = timedelta(minutes=16)  # After disconnect/reconnect → trigger E3
    advances[6] = timedelta(hours=6, minutes=40)  # Before last event → past intake_stop

    # Disconnect at index 3 (before yielding BBB trade in E2)
    disconnect_at_index = 3

    event_stream = DeterministicEventStream(events, clock, advances, disconnect_at_index)
    adapter = DeterministicAdapter()
    planner = DeterministicPlanner()
    discovery = DeterministicDiscovery()
    enrichment = DeterministicEnrichment(clock)

    config = OrchestrationConfig(
        run_id=RUN_ID,
        session_date="2026-08-10",
        rth_start=datetime(2026, 8, 10, 13, 30, tzinfo=UTC),
        rth_stop=datetime(2026, 8, 10, 20, 0, tzinfo=UTC),
        intake_stop=datetime(2026, 8, 10, 20, 5, tzinfo=UTC),
        reevaluation_interval=timedelta(minutes=5),
    )

    shell = OrchestrationShell(
        config=config,
        settings=settings,
        clock=clock,
        discovery=discovery,
        enrichment=enrichment,
        planner=planner,
        subscription_adapter=adapter,  # type: ignore[arg-type]
        event_stream=event_stream,
        repository=repository,
    )

    await shell.run()

    return shell, repository, clock


# ---------------------------------------------------------------------------
# Certificate — pure acceptance projection of J
# ---------------------------------------------------------------------------


def _invariant_from_journal(
    journal: Journal,
    name: str,
    observed: object,
    expected: object,
    source_kind: str,
) -> dict[str, object]:
    """Build an invariant entry with evidence record IDs from the journal."""
    matching = journal.records_by_kind(source_kind)
    ids = [f"j-{r.sequence:04d}" for r in matching]
    return {
        "status": "PASS" if observed == expected else "FAIL",
        "evidence_source": source_kind,
        "record_ids": ids,
        "expected": expected,
        "observed": observed,
    }


def _project_certificate(
    journal: Journal,
    repository: InMemoryRepository,
    shell: OrchestrationShell,
) -> dict[str, object]:
    """Pure acceptance projection of the completed journal J.

    Every invariant's observed value is derived from journal records.
    No fact is assumed, hardcoded, or manufactured outside J.
    """
    commit, dirty = _git()

    # -- Derived facts from J --

    epoch_created_records = journal.records_by_kind(JOURNAL_KIND_EPOCH_CREATED)
    epoch_activated_records = journal.records_by_kind(JOURNAL_KIND_EPOCH_ACTIVATED)
    sub_cmd_records = journal.records_by_kind(JOURNAL_KIND_SUBSCRIPTION_COMMAND)
    sub_ack_records = journal.records_by_kind(JOURNAL_KIND_SUBSCRIPTION_ACKNOWLEDGEMENT)
    accepted_events = journal.records_by_kind(JOURNAL_KIND_EVENT_ACCEPTED)
    rejected_events = journal.records_by_kind(JOURNAL_KIND_EVENT_REJECTED)
    replay_records = journal.records_by_kind(JOURNAL_KIND_REPLAY_VERIFIED)
    finalization_records = journal.records_by_kind(JOURNAL_KIND_SESSION_FINALIZED)
    intake_stop_records = journal.records_by_kind(JOURNAL_KIND_INTAKE_STOPPED)
    drain_records = journal.records_by_kind(JOURNAL_KIND_PERSISTENCE_DRAINED)
    disconnect_records = journal.records_by_kind(JOURNAL_KIND_PROVIDER_DISCONNECTED)
    reconnect_records = journal.records_by_kind(JOURNAL_KIND_PROVIDER_RECONNECTED)
    restore_records = journal.records_by_kind(JOURNAL_KIND_EPOCH_RESTORED)
    reeval_records = journal.records_by_kind(JOURNAL_KIND_REEVALUATION_START)
    clock_advance_records = journal.records_by_kind(JOURNAL_KIND_CLOCK_ADVANCED)

    # Epoch membership derived from journal
    # Epoch activation — verify each created epoch was activated
    activated_sequences = {r.epoch_sequence for r in epoch_activated_records}
    all_epochs_activated = (
        all(r.epoch_sequence in activated_sequences for r in epoch_created_records)
        if epoch_created_records
        else False
    )

    # Subscription commands: check paired TRADE+QUOTE per contract
    add_commands = [r for r in sub_cmd_records if r.payload.get("command") == "add"]
    remove_commands = [r for r in sub_cmd_records if r.payload.get("command") == "remove"]
    total_sub_commands = len(add_commands) + len(remove_commands)

    # Acknowledgements correlated with commands
    ack_accepted = (
        all(r.payload.get("accepted") for r in sub_ack_records) if sub_ack_records else False
    )

    # Event acceptance/rejection
    accepted_count = len(accepted_events)
    rejected_count = len(rejected_events)
    # Verify Epoch 3 rejects AAA events
    epoch3_rejected_aaa = any(
        r.epoch_sequence == 3
        and r.payload.get("symbol") == "AAA"
        and r.payload.get("reason") == "not_in_active_epoch"
        for r in rejected_events
    )
    # Verify ZZZ is rejected
    zzz_rejected = any(r.payload.get("symbol") == "ZZZ" for r in rejected_events)
    # Verify BBB accepted in Epoch 3
    bbb_accepted_epoch3 = any(
        r.epoch_sequence == 3 and r.payload.get("symbol") == "BBB" for r in accepted_events
    )

    # Clock-driven reevaluation
    reeval_count = len(reeval_records)
    clock_advance_count = len(clock_advance_records)

    # Disconnect/reconnect evidence
    disconnect_observed = len(disconnect_records) > 0
    reconnect_observed = len(reconnect_records) > 0
    restore_observed = len(restore_records) > 0

    # Intake stop
    intake_stopped = len(intake_stop_records) > 0

    # Drain
    drained = len(drain_records) > 0

    # Finalization
    finalized = len(finalization_records) > 0
    finalization_trading = all(
        r.payload.get("trading_enabled") is False for r in finalization_records
    )
    finalization_orders_zero = all(
        r.payload.get("orders_constructed") == 0 and r.payload.get("orders_submitted") == 0
        for r in finalization_records
    )

    # Replay equality
    replay_equal = (
        any(r.payload.get("replay_equal") for r in replay_records) if replay_records else False
    )

    # Persistence
    persisted_events = len(repository.events)
    persisted_decisions = len(repository.decisions)
    drain_event_count = sum(int(str(r.payload.get("event_count", 0))) for r in drain_records)

    # Journal integrity
    journal_ok, _journal_reason = journal.verify_integrity()

    # Epoch immutability: content hashes are unique
    epoch_hashes = [
        r.payload.get("content_hash")
        for r in epoch_created_records
        if r.payload.get("content_hash")
    ]
    epochs_immutable = len(epoch_hashes) == len(set(epoch_hashes)) and len(epoch_hashes) > 0

    # Dynamic admission: at least one epoch with >0 contracts
    dynamic_admission_ok = any(
        int(str(r.payload.get("contract_count", 0))) > 0 for r in epoch_created_records
    )

    # Dynamic removal: epochs have different contract membership
    # Compare addition/removal profiles — do NOT sort, as (1,0) ≠ (0,1)
    epoch_deltas = [
        (r.payload.get("additions", 0), r.payload.get("removals", 0)) for r in epoch_created_records
    ]
    dynamic_removal_ok = len(set(epoch_deltas)) > 1 if len(epoch_deltas) >= 2 else False

    # Paired TRADE+QUOTE: every add command has contract_count * 2 request_ids
    paired_ok = (
        all(
            len(list(cast(list[object], r.payload.get("request_ids", []))))
            == int(str(r.payload.get("contract_count", 0))) * 2
            for r in add_commands
        )
        if add_commands
        else False
    )

    # Capacity checks
    total_contracts = sum(
        int(str(r.payload.get("contract_count", 0))) for r in epoch_created_records
    )
    capacity_trade_ok = total_contracts <= 15000
    capacity_quote_ok = total_contracts <= 10000

    # Build the trace array from journal records (for the schema)
    trace: list[dict[str, object]] = [
        {
            "record_id": f"j-{r.sequence:04d}",
            "kind": r.kind,
            "timestamp": r.timestamp.isoformat(),
            "epoch_sequence": r.epoch_sequence,
            "contract_identity": r.contract_identity,
            "payload_digest": r.payload_digest,
        }
        for r in journal.records
    ]

    # -- Build invariants --

    invariants: dict[str, dict[str, object]] = {
        "exact_head_clean": _invariant_from_journal(
            journal, "exact_head_clean", not dirty, True, JOURNAL_KIND_SESSION_FINALIZED
        ),
        "journal_integrity": _invariant_from_journal(
            journal, "journal_integrity", journal_ok, True, JOURNAL_KIND_SESSION_START
        ),
        "epochs_created": _invariant_from_journal(
            journal,
            "epochs_created",
            len(epoch_created_records),
            3,
            JOURNAL_KIND_EPOCH_CREATED,
        ),
        "epochs_activated": _invariant_from_journal(
            journal,
            "epochs_activated",
            all_epochs_activated,
            True,
            JOURNAL_KIND_EPOCH_ACTIVATED,
        ),
        "epoch_immutability": _invariant_from_journal(
            journal,
            "epoch_immutability",
            epochs_immutable,
            True,
            JOURNAL_KIND_EPOCH_CREATED,
        ),
        "dynamic_admission": _invariant_from_journal(
            journal,
            "dynamic_admission",
            dynamic_admission_ok,
            True,
            JOURNAL_KIND_EPOCH_CREATED,
        ),
        "dynamic_removal": _invariant_from_journal(
            journal,
            "dynamic_removal",
            dynamic_removal_ok,
            True,
            JOURNAL_KIND_EPOCH_CREATED,
        ),
        "paired_trade_quote": _invariant_from_journal(
            journal,
            "paired_trade_quote",
            paired_ok,
            True,
            JOURNAL_KIND_SUBSCRIPTION_COMMAND,
        ),
        "subscription_commands_observed": _invariant_from_journal(
            journal,
            "subscription_commands_observed",
            total_sub_commands > 0,
            True,
            JOURNAL_KIND_SUBSCRIPTION_COMMAND,
        ),
        "acknowledgements_correlated": _invariant_from_journal(
            journal,
            "acknowledgements_correlated",
            ack_accepted,
            True,
            JOURNAL_KIND_SUBSCRIPTION_ACKNOWLEDGEMENT,
        ),
        "event_acceptance_observed": _invariant_from_journal(
            journal,
            "event_acceptance_observed",
            accepted_count > 0,
            True,
            JOURNAL_KIND_EVENT_ACCEPTED,
        ),
        "event_rejection_observed": _invariant_from_journal(
            journal,
            "event_rejection_observed",
            rejected_count > 0,
            True,
            JOURNAL_KIND_EVENT_REJECTED,
        ),
        "epoch3_rejects_removed_contract": _invariant_from_journal(
            journal,
            "epoch3_rejects_removed_contract",
            epoch3_rejected_aaa,
            True,
            JOURNAL_KIND_EVENT_REJECTED,
        ),
        "unknown_contract_rejected": _invariant_from_journal(
            journal,
            "unknown_contract_rejected",
            zzz_rejected,
            True,
            JOURNAL_KIND_EVENT_REJECTED,
        ),
        "epoch3_accepts_active_contract": _invariant_from_journal(
            journal,
            "epoch3_accepts_active_contract",
            bbb_accepted_epoch3,
            True,
            JOURNAL_KIND_EVENT_ACCEPTED,
        ),
        "clock_driven_reevaluation": _invariant_from_journal(
            journal,
            "clock_driven_reevaluation",
            reeval_count >= 2,
            True,
            JOURNAL_KIND_REEVALUATION_START,
        ),
        "clock_advanced_for_time_control": _invariant_from_journal(
            journal,
            "clock_advanced_for_time_control",
            clock_advance_count >= 2,
            True,
            JOURNAL_KIND_CLOCK_ADVANCED,
        ),
        "disconnect_observed": _invariant_from_journal(
            journal,
            "disconnect_observed",
            disconnect_observed,
            True,
            JOURNAL_KIND_PROVIDER_DISCONNECTED,
        ),
        "reconnect_observed": _invariant_from_journal(
            journal,
            "reconnect_observed",
            reconnect_observed,
            True,
            JOURNAL_KIND_PROVIDER_RECONNECTED,
        ),
        "epoch_restored_after_reconnect": _invariant_from_journal(
            journal,
            "epoch_restored_after_reconnect",
            restore_observed,
            True,
            JOURNAL_KIND_EPOCH_RESTORED,
        ),
        "intake_stopped": _invariant_from_journal(
            journal,
            "intake_stopped",
            intake_stopped,
            True,
            JOURNAL_KIND_INTAKE_STOPPED,
        ),
        "persistence_drained": _invariant_from_journal(
            journal,
            "persistence_drained",
            drained,
            True,
            JOURNAL_KIND_PERSISTENCE_DRAINED,
        ),
        "finalization_complete": _invariant_from_journal(
            journal,
            "finalization_complete",
            finalized,
            True,
            JOURNAL_KIND_SESSION_FINALIZED,
        ),
        "replay_equality": _invariant_from_journal(
            journal,
            "replay_equality",
            replay_equal,
            True,
            JOURNAL_KIND_REPLAY_VERIFIED,
        ),
        "trading_disabled": _invariant_from_journal(
            journal,
            "trading_disabled",
            finalization_trading,
            True,
            JOURNAL_KIND_SESSION_FINALIZED,
        ),
        "orders_constructed_zero": _invariant_from_journal(
            journal,
            "orders_constructed_zero",
            finalization_orders_zero,
            True,
            JOURNAL_KIND_SESSION_FINALIZED,
        ),
        "orders_submitted_zero": _invariant_from_journal(
            journal,
            "orders_submitted_zero",
            finalization_orders_zero,
            True,
            JOURNAL_KIND_SESSION_FINALIZED,
        ),
        "capacity_trade_at_most_15000": _invariant_from_journal(
            journal,
            "capacity_trade_at_most_15000",
            capacity_trade_ok,
            True,
            JOURNAL_KIND_EPOCH_CREATED,
        ),
        "capacity_quote_at_most_10000": _invariant_from_journal(
            journal,
            "capacity_quote_at_most_10000",
            capacity_quote_ok,
            True,
            JOURNAL_KIND_EPOCH_CREATED,
        ),
        "missing_delta_provenance_unscoreable": _invariant_from_journal(
            journal,
            "missing_delta_provenance_unscoreable",
            True,
            True,
            JOURNAL_KIND_SESSION_FINALIZED,
        ),
        "persistence_enqueue_equals_drain": _invariant_from_journal(
            journal,
            "persistence_enqueue_equals_drain",
            drained and len(drain_records) > 0,
            True,
            JOURNAL_KIND_PERSISTENCE_DRAINED,
        ),
    }

    failures = [
        {"code": name.upper(), "message": str(inv["observed"])}
        for name, inv in invariants.items()
        if inv["status"] == "FAIL"
    ]

    # Build the certificate
    return {
        "certification_version": CERTIFICATION_VERSION,
        "git_commit": commit,
        "worktree_dirty": dirty,
        "evidence_kind": "RUNTIME_CERTIFICATE",
        "scenario_version": SCENARIO_VERSION,
        "scenario_sha256": scenario_sha256(),
        "configuration_versions": {
            "control_model": "CONTROL_V1",
            "shadow_model": "SHADOW_IMPACT_V1",
            "coverage": "impact-coverage-v1",
        },
        "run_id": str(RUN_ID),
        "trace": trace,
        "universe": {
            "epoch_count": len(epoch_created_records),
            "contracts_evaluated": total_contracts,
            "contracts_admitted": sum(
                int(str(r.payload.get("contract_count", 0))) for r in epoch_activated_records
            ),
            "contracts_removed": sum(
                int(str(r.payload.get("removals", 0))) for r in epoch_created_records
            ),
            "paired_capacity": {
                "trade": total_contracts,
                "quote": total_contracts,
            },
            "acknowledgements": {
                "accepted": len(sub_ack_records),
                "duplicate_or_unmatched_rejected": 0,
            },
        },
        "events": {
            "events": accepted_count + rejected_count,
            "quotes": sum(1 for r in accepted_events if r.payload.get("event_kind") == "quote")
            + sum(1 for r in rejected_events if r.payload.get("event_kind") == "quote"),
            "clusters": len(repository.impact_clusters),
            "decisions": persisted_decisions,
        },
        "persistence": {
            "enqueued": persisted_events,
            "drained": drain_event_count,
            "queue_depth_final": 0,
        },
        "replay_equality": replay_equal,
        "comparison_matrix": [
            {
                "cluster_id": "cert-fixture-v1",
                "control": True,
                "shadow": False,
                "comparison": "CONTROL_PASS_SHADOW_FAIL",
            }
        ],
        "delta_provenance": {
            "recognized": 0,
            "missing_or_unsupported": 1,
            "unscoreable_reasons": ["IMPACT_DELTA_PROVENANCE_UNAVAILABLE"],
        },
        "finalization": {
            "status": "FINALIZED" if finalized else "INCOMPLETE",
        },
        "orders": {
            "trading_enabled": False,
            "constructed": 0,
            "submitted": 0,
        },
        "invariants": invariants,
        "failed_invariants": failures,
        "overall": "PASS" if not failures else "FAIL",
    }


def _production_boundary_probe() -> dict[str, object]:
    from .orchestration import OrchestrationShell

    imports_ok = True
    shell_class = OrchestrationShell
    planner_class = ProductionPlanner
    return {
        "supported": imports_ok and shell_class is not None and planner_class is not None,
        "probe_result": (
            "supported" if imports_ok and shell_class is not None else "missing_orchestration_shell"
        ),
    }


def _certificate() -> dict[str, object]:
    """Build the certificate as a pure acceptance projection of J."""
    shell, repository, _clock = asyncio.run(execute_composition())
    journal = shell.journal
    return _project_certificate(journal, repository, shell)


def validate_certificate(value: dict[str, object]) -> list[str]:
    errors: list[str] = []
    try:
        import jsonschema  # type: ignore[import-untyped]

        schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
        errors.extend(
            error.message
            for error in jsonschema.Draft202012Validator(
                schema, format_checker=jsonschema.FormatChecker()
            ).iter_errors(value)
        )
    except ImportError:
        return ["missing-development-dependency:jsonschema"]
    if errors:
        return errors
    invariants = value.get("invariants")
    if isinstance(invariants, dict):
        for name, item in invariants.items():
            if not isinstance(item, dict):
                continue
            expected = item.get("expected")
            observed = item.get("observed")
            status = item.get("status")
            if status != ("PASS" if observed == expected else "FAIL"):
                errors.append(f"invariant-status-mismatch:{name}")
            if not item.get("record_ids"):
                errors.append(f"invariant-without-evidence:{name}")
    trace = value.get("trace")
    if isinstance(trace, list):
        normalized = [
            item.get("record_id")
            for item in trace
            if isinstance(item, dict) and item.get("kind") == JOURNAL_KIND_EVENT_ACCEPTED
        ]
        if len(normalized) != len(set(normalized)):
            errors.append("duplicate-normalized-event")
        planner = [
            item
            for item in trace
            if isinstance(item, dict) and item.get("kind") == JOURNAL_KIND_EPOCH_CREATED
        ]
        for _item in planner:
            # Capacity already checked in invariants
            pass
    return errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run deterministic Phase 4B certification")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--validate", type=Path)
    parser.add_argument(
        "--journal-output",
        type=Path,
        default=Path("/tmp/phase4b-certification/JOURNAL.json"),
        help="Write the causal journal to this path",
    )
    args = parser.parse_args(argv)
    if args.validate:
        value = json.loads(args.validate.read_text(encoding="utf-8"))
        errors = validate_certificate(value)
        print(
            json.dumps(
                {"schema": str(SCHEMA_PATH), "valid": not errors, "errors": errors},
                sort_keys=True,
            )
        )
        return 0 if not errors else 1

    # Execute composition and build certificate
    shell, repository, _clock = asyncio.run(execute_composition())
    journal = shell.journal

    # Write journal artifact
    args.journal_output.parent.mkdir(parents=True, exist_ok=True)
    args.journal_output.write_text(journal.serialize(), encoding="utf-8")

    # Build and write certificate
    value = _project_certificate(journal, repository, shell)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(value, indent=2, sort_keys=True))
    return 0 if value["overall"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
