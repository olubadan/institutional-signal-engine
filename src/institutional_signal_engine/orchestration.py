"""Phase 4B RTH orchestration shell with continuous candidate reevaluation.

This module replaces the frozen-universe startup in phase4_live_smoke.py with
a thin orchestration shell that:

- Creates PlannerEpoch instances for every universe transition
- Reevaluates candidates on a configurable schedule during RTH
- Manages incremental subscription add/remove through DynamicSubscriptionAdapter
- Handles disconnect/reconnect with idempotent epoch restoration
- Maintains a single authority for the active epoch
- Persists enough state for deterministic multi-epoch replay

CONTROL_V1 and SHADOW_IMPACT_V1 are preserved unchanged. Trading remains
disabled. Zero orders are constructed or submitted.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Literal, Protocol, cast
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

from .config import Settings
from .dynamic_subscriptions import (
    DEFAULT_ACK_TIMEOUT,
    DynamicSubscriptionAdapter,
    SubscriptionAcknowledgement,
)
from .impact import ImpactBaseline
from .impact_coverage import (
    PILOT_COVERAGE_POPULATION_VERSION,
    build_coverage_plan,
    build_pilot_coverage_candidates,
)
from .journal import (
    JOURNAL_KIND_CLOCK_ADVANCED,
    JOURNAL_KIND_CONFIGURATION,
    JOURNAL_KIND_DISCOVERY_COMPLETE,
    JOURNAL_KIND_DISCOVERY_START,
    JOURNAL_KIND_ENRICHMENT_COMPLETE,
    JOURNAL_KIND_ENRICHMENT_START,
    JOURNAL_KIND_EPOCH_ACTIVATED,
    JOURNAL_KIND_EPOCH_CREATED,
    JOURNAL_KIND_EPOCH_PERSISTED,
    JOURNAL_KIND_EPOCH_RESTORED,
    JOURNAL_KIND_EVENT_ACCEPTED,
    JOURNAL_KIND_EVENT_REJECTED,
    JOURNAL_KIND_INTAKE_STOPPED,
    JOURNAL_KIND_JOURNAL_PERSISTED,
    JOURNAL_KIND_PERSISTENCE_DRAINED,
    JOURNAL_KIND_PROVIDER_DISCONNECTED,
    JOURNAL_KIND_PROVIDER_READY,
    JOURNAL_KIND_PROVIDER_RECONNECTED,
    JOURNAL_KIND_REEVALUATION_START,
    JOURNAL_KIND_REPLAY_VERIFIED,
    JOURNAL_KIND_SESSION_FINALIZED,
    JOURNAL_KIND_SESSION_START,
    JOURNAL_KIND_SUBSCRIPTION_ACKNOWLEDGEMENT,
    JOURNAL_KIND_SUBSCRIPTION_COMMAND,
    FileJournalRepository,
    Journal,
    JournalRepository,
    sha256,
)
from .persistence import InMemoryRepository
from .persistence_async import AsyncAuditWriter
from .pipeline import SignalPipeline
from .ports import EventRepository
from .providers.thetadata import ThetaContract
from .schemas import CanonicalEvent
from .startup import StageCallback, StageRecorder
from .universe import (
    PILOT_SYMBOLS,
    PlannerEpoch,
    UniverseSelection,
)

logger = logging.getLogger(__name__)

# Default RTH window (ET).
RTH_START_HOUR = 9
RTH_START_MINUTE = 30
RTH_END_HOUR = 16
RTH_END_MINUTE = 0

# Default interval between candidate reevaluations during RTH.
DEFAULT_REEVALUATION_INTERVAL_SECONDS = 300  # 5 minutes

# Maximum allowed drift between scheduled and actual reevaluation time.
MAX_SCHEDULE_DRIFT_SECONDS = 2.0

ET = ZoneInfo("America/New_York")


def contract_identity(contract: ThetaContract) -> str:
    return f"{contract.root}:{contract.expiration}:{contract.strike}:{contract.right}"


# ---------------------------------------------------------------------------
# Port interfaces for deterministic composition
# ---------------------------------------------------------------------------


class DiscoveryPort(Protocol):
    """Discovers option contracts for the pilot symbols."""

    async def discover(self, symbols: tuple[str, ...], as_of: datetime) -> tuple[object, ...]: ...

    async def prices(self, symbols: tuple[str, ...]) -> dict[str, Decimal]: ...


class EnrichmentPort(Protocol):
    """Enriches discovered contracts with quote and OI evidence."""

    async def enrich(
        self,
        discovered: tuple[object, ...],
        prices: dict[str, Decimal],
        as_of: datetime,
    ) -> tuple[UniverseSelection, ...]: ...


class PlannerPort(Protocol):
    """Produces PlannerEpoch instances from enriched selections."""

    def plan(
        self,
        selections: tuple[UniverseSelection, ...],
        sequence: int,
        effective_at: datetime,
        previous_contracts: tuple[ThetaContract, ...],
        baseline_symbols: frozenset[str],
    ) -> PlannerEpoch: ...


class EventStreamPort(Protocol):
    """Provides canonical events from provider(s)."""

    def events(self) -> AsyncIterator[CanonicalEvent]: ...

    async def health(self) -> dict[str, object]: ...


@dataclass(frozen=True)
class DriverSignal:
    """One independently selected clock, event, or connection input."""

    kind: Literal["event", "clock", "disconnect", "stop", "end"]
    timestamp: datetime
    event: CanonicalEvent | None = None


class SessionDriverPort(Protocol):
    async def next_signal(
        self, next_reevaluation: datetime | None, intake_stop: datetime
    ) -> DriverSignal: ...


class AsyncSessionDriver:
    """Production driver racing provider arrival against independent timers."""

    def __init__(
        self,
        event_stream: EventStreamPort,
        clock: Callable[[], datetime],
    ) -> None:
        self._event_stream = event_stream
        self._iterator = event_stream.events().__aiter__()
        self._clock = clock
        self._event_task: asyncio.Future[CanonicalEvent] | None = None
        self._ended = False

    async def next_signal(
        self, next_reevaluation: datetime | None, intake_stop: datetime
    ) -> DriverSignal:
        now = self._clock()
        boundary = min(value for value in (next_reevaluation, intake_stop) if value is not None)
        if now >= boundary:
            return DriverSignal("stop" if boundary == intake_stop else "clock", boundary)
        if self._event_task is None and not self._ended:
            self._event_task = asyncio.ensure_future(self._iterator.__anext__())
        timer = asyncio.create_task(asyncio.sleep(max(0.0, (boundary - now).total_seconds())))
        if self._event_task is None:
            await timer
            return DriverSignal("stop" if boundary == intake_stop else "clock", boundary)
        waiters = {
            cast(asyncio.Future[object], self._event_task),
            cast(asyncio.Future[object], timer),
        }
        done, _ = await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
        if timer in done:
            return DriverSignal("stop" if boundary == intake_stop else "clock", boundary)
        timer.cancel()
        try:
            event = self._event_task.result()
        except StopAsyncIteration:
            self._event_task = None
            self._ended = True
            return DriverSignal("end", self._clock())
        except (OSError, RuntimeError, ValueError, ConnectionError):
            self._event_task = None
            self._iterator = self._event_stream.events().__aiter__()
            return DriverSignal("disconnect", self._clock())
        self._event_task = None
        return DriverSignal("event", self._clock(), event)


class PersistencePort(Protocol):
    """Persists and replays session state."""

    def record_epoch(self, epoch: PlannerEpoch) -> None: ...

    def record_event(self, event: CanonicalEvent) -> None: ...

    def flush(self) -> None: ...

    def replay_epochs(self, run_id: UUID) -> tuple[PlannerEpoch, ...]: ...

    def replay_events(self, run_id: UUID | None = None) -> tuple[CanonicalEvent, ...]: ...


class LifecyclePort(Protocol):
    """Session lifecycle and finalization hooks."""

    def is_rth(self, now: datetime) -> bool: ...

    def is_past_boundary(self, now: datetime, boundary: datetime) -> bool: ...

    def should_stop_intake(self, now: datetime, stop_at: datetime) -> bool: ...


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class OrchestrationConfig:
    """Immutable configuration for an orchestrated RTH session."""

    run_id: UUID
    session_date: str
    rth_start: datetime
    rth_stop: datetime
    intake_stop: datetime
    reevaluation_interval: timedelta = timedelta(seconds=DEFAULT_REEVALUATION_INTERVAL_SECONDS)
    ack_timeout: float = DEFAULT_ACK_TIMEOUT
    trading_enabled: bool = False
    scenario_version: str = "PRODUCTION_CONTINUOUS_RTH_V1"
    control_model_version: str = "CONTROL_V1"
    shadow_model_version: str = "SHADOW_IMPACT_V1"
    coverage_version: str = PILOT_COVERAGE_POPULATION_VERSION

    @classmethod
    def from_settings(cls, settings: Settings, run_id: UUID | None = None) -> OrchestrationConfig:
        now = datetime.now(ET)
        session_date = now.date()
        rth_start = datetime(
            session_date.year,
            session_date.month,
            session_date.day,
            RTH_START_HOUR,
            RTH_START_MINUTE,
            tzinfo=ET,
        )
        rth_stop = datetime(
            session_date.year,
            session_date.month,
            session_date.day,
            RTH_END_HOUR,
            RTH_END_MINUTE,
            tzinfo=ET,
        )
        intake_stop = rth_stop + timedelta(minutes=5)
        return cls(
            run_id=run_id or uuid4(),
            session_date=session_date.isoformat(),
            rth_start=rth_start,
            rth_stop=rth_stop,
            intake_stop=intake_stop,
            trading_enabled=False,
        )


# ---------------------------------------------------------------------------
# Session result
# ---------------------------------------------------------------------------


@dataclass
class SessionResult:
    """Immutable result of an orchestrated session."""

    run_id: UUID
    status: str
    epochs: tuple[PlannerEpoch, ...]
    event_count: int
    decision_count: int
    trading_enabled: bool
    orders_constructed: int
    orders_submitted: int
    replay_equal: bool
    diagnostics: dict[str, object]
    error: str | None = None


# ---------------------------------------------------------------------------
# Production planner (implements PlannerPort using existing components)
# ---------------------------------------------------------------------------


class ProductionPlanner:
    """Planner that uses the existing coverage and allocation components.

    Every call to plan() produces exactly one PlannerEpoch. The same planner
    instance is used for startup (Epoch 1) and all subsequent reevaluations.
    """

    def __init__(
        self,
        trade_limit: int = 15000,
        quote_limit: int = 10000,
        max_contracts_per_symbol: int = 1000,
    ) -> None:
        self.trade_limit = trade_limit
        self.quote_limit = quote_limit
        self.max_contracts_per_symbol = max_contracts_per_symbol

    def plan(
        self,
        selections: tuple[UniverseSelection, ...],
        sequence: int,
        effective_at: datetime,
        previous_contracts: tuple[ThetaContract, ...] = (),
        baseline_symbols: frozenset[str] = frozenset(),
    ) -> PlannerEpoch:
        """Produce a PlannerEpoch from universe selections.

        Uses the existing build_pilot_coverage_candidates → build_coverage_plan
        pipeline. Every call is deterministic given the same inputs.
        """
        coverage_candidates = build_pilot_coverage_candidates(selections, baseline_symbols)
        coverage_plan = build_coverage_plan(
            coverage_candidates,
            trade_limit=self.trade_limit,
            quote_limit=self.quote_limit,
            max_contracts_per_symbol=self.max_contracts_per_symbol,
        )
        selected = tuple(
            ThetaContract(
                candidate.symbol,
                candidate.expiration,
                candidate.strike,
                candidate.right,
            )
            for candidate in coverage_plan.selected
        )
        selection_records: tuple[dict[str, object], ...] = tuple(
            {
                "symbol": selection.symbol,
                "included": selection.included,
                "expiration": selection.expiration,
                "contract_count": len(selection.contracts),
                "rejection_reasons": list(selection.rejection_reasons),
                "provenance": list(selection.provenance),
            }
            for selection in selections
        )
        return PlannerEpoch.create(
            sequence=sequence,
            effective_at=effective_at,
            candidate_population_version=PILOT_COVERAGE_POPULATION_VERSION,
            selected_contracts=selected,
            previous_contracts=previous_contracts,
            provenance="production-planner-v1",
            coverage_plan=coverage_plan.as_dict(),
            selection_records=selection_records,
        )


# ---------------------------------------------------------------------------
# Candidate reevaluation scheduler
# ---------------------------------------------------------------------------


@dataclass
class ReevaluationScheduler:
    """Deterministic RTH reevaluation policy with injected clock.

    Uses a bounded scheduled design. Reevaluation times are computed from the
    RTH start and the configured interval. The scheduler does not busy-poll;
    callers await next_reevaluation() which returns the scheduled time or None
    if RTH has ended.
    """

    rth_start: datetime
    rth_stop: datetime
    interval: timedelta = timedelta(seconds=DEFAULT_REEVALUATION_INTERVAL_SECONDS)
    _next_due: datetime | None = field(default=None, init=False)
    _last_effective: datetime | None = field(default=None, init=False)
    _evaluations_completed: int = field(default=0, init=False)

    def _compute_next_due(self) -> datetime | None:
        """Compute the next due time based on completed evaluations."""
        candidate = self.rth_start + self.interval * (self._evaluations_completed + 1)
        if candidate >= self.rth_stop:
            return None
        return candidate

    def next_reevaluation(self, now: datetime) -> datetime | None:
        """Return the next scheduled reevaluation time, or None if past RTH stop.

        Does NOT mutate state. Idempotent — only record_reevaluation() advances.
        """
        if self._next_due is None:
            self._next_due = self._compute_next_due()
        return self._next_due

    def is_due(self, now: datetime) -> bool:
        """Check if a reevaluation is due at `now`. Does not mutate state."""
        due = self.next_reevaluation(now)
        if due is None:
            return False
        return now >= due

    def record_reevaluation(self, effective_at: datetime) -> None:
        """Record that a reevaluation occurred and advance to next interval."""
        self._last_effective = effective_at
        self._evaluations_completed += 1
        self._next_due = self._compute_next_due()

    @property
    def evaluations_completed(self) -> int:
        return self._evaluations_completed


# ---------------------------------------------------------------------------
# Orchestration Shell
# ---------------------------------------------------------------------------


class OrchestrationShell:
    """Thin orchestration shell for a complete 09:30–16:00 ET RTH session.

    Owns the session lifecycle:
    1. Pre-open initialization
    2. Authentication and provider readiness
    3. Initial discovery and enrichment → Epoch 1
    4. Epoch 1 persistence and activation
    5. Paired subscription request + exact acknowledgement
    6. Event intake loop with:
       a. Event processing through pipeline
       b. Scheduled RTH reevaluation → epoch diff → activation
       c. Disconnect detection → reconnect → idempotent restoration
    7. Intake stop at configured boundary
    8. Persistence drain → finalization
    9. Post-session replay and evidence

    The shell accepts deterministic ports for testing. The same composition
    root is used by the production command and by hermetic tests.
    """

    def __init__(
        self,
        config: OrchestrationConfig,
        settings: Settings,
        *,
        clock: Callable[[], datetime] | None = None,
        discovery: DiscoveryPort | None = None,
        enrichment: EnrichmentPort | None = None,
        planner: PlannerPort | None = None,
        subscription_adapter: DynamicSubscriptionAdapter | None = None,
        event_stream: EventStreamPort | None = None,
        repository: EventRepository | None = None,
        historical_baselines: Mapping[tuple[str, int], ImpactBaseline] | None = None,
        stage_callback: StageCallback | None = None,
        journal: Journal | None = None,
        session_driver: SessionDriverPort | None = None,
        journal_repository: JournalRepository | None = None,
    ) -> None:
        self.config = config
        self.settings = settings
        self._clock = clock or (lambda: datetime.now(UTC))
        self._discovery = discovery
        self._enrichment = enrichment
        self._planner = planner or ProductionPlanner(
            trade_limit=settings.phase4_trade_subscription_limit,
            quote_limit=settings.phase4_quote_subscription_limit,
            max_contracts_per_symbol=settings.phase4_max_contracts_per_symbol,
        )
        self._subscription_adapter = subscription_adapter
        self._event_stream = event_stream
        self._repository = repository or InMemoryRepository()
        self._historical_baselines = historical_baselines or {}
        self._stage_callback = stage_callback
        self._journal = journal or Journal(config.run_id, clock=self._clock)
        self._session_driver = session_driver
        self._journal_repository = journal_repository or FileJournalRepository()
        self._journal_identity: str | None = None

        # Mutable session state
        self._active_epoch: PlannerEpoch | None = None
        self._active_epoch_record_sequence: int | None = None
        self._epochs: list[PlannerEpoch] = []
        self._scheduler = ReevaluationScheduler(
            rth_start=config.rth_start,
            rth_stop=config.rth_stop,
            interval=config.reevaluation_interval,
        )
        self._pipeline: SignalPipeline | None = None
        self._writer: AsyncAuditWriter | None = None
        self._event_count: int = 0
        self._intake_stopped: bool = False
        self._drained: bool = False
        self._finalized: bool = False
        self._error: str | None = None
        self._recorder = StageRecorder(stage_callback)

    # -- Read-only properties --

    @property
    def active_epoch(self) -> PlannerEpoch | None:
        return self._active_epoch

    @property
    def epochs(self) -> tuple[PlannerEpoch, ...]:
        return tuple(self._epochs)

    @property
    def journal(self) -> Journal:
        return self._journal

    @property
    def event_count(self) -> int:
        return self._event_count

    @property
    def intake_stopped(self) -> bool:
        return self._intake_stopped

    @property
    def drained(self) -> bool:
        return self._drained

    @property
    def finalized(self) -> bool:
        return self._finalized

    # -- Lifecycle --

    async def run(self) -> SessionResult:
        """Execute the complete orchestrated session."""
        try:
            # 1. Pre-open initialization
            await self._initialize()

            # 2. Authentication and provider readiness
            await self._verify_provider_readiness()

            # 3. Initial discovery and enrichment → Epoch 1
            # Fail closed: production requires discovery, enrichment, and event stream
            if self._discovery is None or self._enrichment is None:
                return SessionResult(
                    run_id=self.config.run_id,
                    status="blocked_missing_production_ports",
                    epochs=(),
                    event_count=0,
                    decision_count=0,
                    trading_enabled=False,
                    orders_constructed=0,
                    orders_submitted=0,
                    replay_equal=False,
                    diagnostics={"reason": "discovery_and_enrichment_ports_required"},
                )
            if self._event_stream is None:
                return SessionResult(
                    run_id=self.config.run_id,
                    status="blocked_missing_event_stream",
                    epochs=(),
                    event_count=0,
                    decision_count=0,
                    trading_enabled=False,
                    orders_constructed=0,
                    orders_submitted=0,
                    replay_equal=False,
                    diagnostics={"reason": "event_stream_port_required"},
                )

            epoch1 = await self._create_initial_epoch()
            if epoch1 is None or epoch1.contract_count == 0:
                return SessionResult(
                    run_id=self.config.run_id,
                    status="blocked_no_contracts_selected",
                    epochs=(),
                    event_count=0,
                    decision_count=0,
                    trading_enabled=False,
                    orders_constructed=0,
                    orders_submitted=0,
                    replay_equal=False,
                    diagnostics={"reason": "no_contracts_selected_at_startup"},
                )

            # 4. Epoch 1 persistence
            self._persist_epoch(epoch1)

            # 5. Paired subscription request + exact acknowledgement
            ack = await self._request_and_acknowledge_subscriptions(epoch1)
            if not ack.accepted:
                return SessionResult(
                    run_id=self.config.run_id,
                    status="blocked_subscription_acknowledgement_failed",
                    epochs=(epoch1,),
                    event_count=0,
                    decision_count=0,
                    trading_enabled=False,
                    orders_constructed=0,
                    orders_submitted=0,
                    replay_equal=True,
                    diagnostics={"acknowledgement": ack.diagnostic},
                )

            # Activate Epoch 1
            epoch1 = epoch1.activate()
            self._active_epoch = epoch1
            self._epochs.append(epoch1)
            activation = self._journal.append(
                JOURNAL_KIND_EPOCH_ACTIVATED,
                timestamp=self._clock(),
                parent_sequence=self._journal.sequence - 1,
                epoch_sequence=epoch1.sequence,
                epoch_id=epoch1.epoch_id,
                content_hash=epoch1.content_hash,
                contract_count=epoch1.contract_count,
                membership=[contract_identity(item) for item in epoch1.selected_contracts],
                lifecycle=epoch1.lifecycle,
            )
            self._active_epoch_record_sequence = activation.sequence
            self._recorder.emit("epoch_activated", epoch=1, contract_count=epoch1.contract_count)

            # 6. Event intake loop
            await self._run_intake_loop()

            # 7. Intake stop at boundary
            await self._stop_intake()

            # 8. Persistence drain
            await self._drain_persistence()

            # 9. Finalization
            await self._finalize()

            # 10. Persist, reconstruct, and verify the causal journal through its boundary.
            replay_equal = self._persist_and_verify_journal()

            # The terminal replay record is now present; seal and persist the canonical value.
            self._journal.seal()
            assert self._journal_identity is not None
            self._journal_repository.save(self.config.run_id, self._journal.serialize())

            return SessionResult(
                run_id=self.config.run_id,
                status="live_observation_complete",
                epochs=tuple(self._epochs),
                event_count=self._event_count,
                decision_count=len(self._pipeline.decisions) if self._pipeline else 0,
                trading_enabled=False,
                orders_constructed=0,
                orders_submitted=0,
                replay_equal=replay_equal,
                diagnostics=self._build_diagnostics(),
            )
        except Exception as exc:
            logger.exception("orchestration_failure")
            self._error = str(exc)
            return SessionResult(
                run_id=self.config.run_id,
                status="orchestration_failure",
                epochs=tuple(self._epochs),
                event_count=self._event_count,
                decision_count=0,
                trading_enabled=False,
                orders_constructed=0,
                orders_submitted=0,
                replay_equal=False,
                diagnostics={"error": str(exc)},
                error=str(exc),
            )

    async def _initialize(self) -> None:
        """Pre-open initialization."""
        if self.settings.trading_enabled:
            raise RuntimeError("orchestration_shell_refuses_trading_enabled")
        now = self._clock()
        self._journal.append(
            JOURNAL_KIND_SESSION_START,
            timestamp=now,
            session_date=self.config.session_date,
            rth_start=self.config.rth_start.isoformat(),
            rth_stop=self.config.rth_stop.isoformat(),
            intake_stop=self.config.intake_stop.isoformat(),
            trading_enabled=False,
            orders_constructed=0,
            orders_submitted=0,
            scenario_version=self.config.scenario_version,
        )
        self._journal.append(
            JOURNAL_KIND_CONFIGURATION,
            timestamp=now,
            parent_sequence=0,
            reevaluation_interval_seconds=self.config.reevaluation_interval.total_seconds(),
            ack_timeout=self.config.ack_timeout,
            intake_stop=self.config.intake_stop.isoformat(),
            trading_enabled=False,
            orders_constructed=0,
            orders_submitted=0,
            scenario_version=self.config.scenario_version,
            control_model_version=self.config.control_model_version,
            shadow_model_version=self.config.shadow_model_version,
            coverage_version=self.config.coverage_version,
        )
        self._recorder.emit("configuration_loaded")
        self._recorder.emit("pre_open_initialization_complete")

    async def _verify_provider_readiness(self) -> None:
        """Verify providers are ready before proceeding."""
        if self._subscription_adapter is not None:
            await self._subscription_adapter.connect()
        if self._event_stream is not None:
            health = await self._event_stream.health()
            if health.get("status") == "unavailable":
                raise RuntimeError("provider_not_ready")
        self._journal.append(
            JOURNAL_KIND_PROVIDER_READY,
            timestamp=self._clock(),
            parent_sequence=self._journal.sequence - 1,
            adapter_connected=(
                self._subscription_adapter.connected
                if self._subscription_adapter is not None
                else False
            ),
        )
        self._recorder.emit("providers_authenticated")

    async def _create_initial_epoch(self) -> PlannerEpoch | None:
        """Run initial discovery and enrichment, produce Epoch 1."""
        assert self._discovery is not None, "discovery port required"
        assert self._enrichment is not None, "enrichment port required"

        symbolic_now = self._clock()
        parent_seq = self._journal.sequence - 1

        self._journal.append(
            JOURNAL_KIND_DISCOVERY_START,
            timestamp=symbolic_now,
            parent_sequence=parent_seq,
            symbols=list(PILOT_SYMBOLS),
        )
        self._recorder.emit("universe_discovery_started")
        prices = await self._discovery.prices(PILOT_SYMBOLS)
        discovered = await self._discovery.discover(PILOT_SYMBOLS, symbolic_now)
        disc_seq = self._journal.append(
            JOURNAL_KIND_DISCOVERY_COMPLETE,
            timestamp=self._clock(),
            parent_sequence=parent_seq + 1,
            completed_items=len(discovered),
            remaining_items=0,
        ).sequence
        self._recorder.emit(
            "universe_discovery_completed",
            completed_items=len(discovered),
            remaining_items=0,
        )

        self._journal.append(
            JOURNAL_KIND_ENRICHMENT_START,
            timestamp=self._clock(),
            parent_sequence=disc_seq,
            symbol_count=len(discovered),
        )
        self._recorder.emit("enrichment_started")
        selections = await self._enrichment.enrich(discovered, prices, symbolic_now)
        enrich_seq = self._journal.append(
            JOURNAL_KIND_ENRICHMENT_COMPLETE,
            timestamp=self._clock(),
            parent_sequence=disc_seq + 1,
            completed_items=len(selections),
            remaining_items=0,
        ).sequence
        self._recorder.emit(
            "enrichment_completed",
            completed_items=len(selections),
            remaining_items=0,
        )

        baseline_symbols = frozenset(
            str(key[0]).upper()
            for key in self._historical_baselines
            if isinstance(key, tuple) and len(key) == 2
        )

        epoch = self._planner.plan(
            selections=selections,
            sequence=1,
            effective_at=symbolic_now,
            previous_contracts=(),
            baseline_symbols=baseline_symbols,
        )
        if epoch.contract_count == 0:
            self._journal.append(
                JOURNAL_KIND_EPOCH_CREATED,
                timestamp=self._clock(),
                parent_sequence=enrich_seq,
                epoch_sequence=1,
                contract_count=0,
                lifecycle="empty",
            )
            return None
        self._journal.append(
            JOURNAL_KIND_EPOCH_CREATED,
            timestamp=self._clock(),
            parent_sequence=enrich_seq,
            epoch_sequence=epoch.sequence,
            epoch_id=epoch.epoch_id,
            content_hash=epoch.content_hash,
            contract_count=epoch.contract_count,
            additions=len(epoch.additions),
            removals=len(epoch.removals),
            membership=[contract_identity(item) for item in epoch.selected_contracts],
            provenance=epoch.provenance,
            lifecycle=epoch.lifecycle,
        )
        return epoch

    async def _request_and_acknowledge_subscriptions(
        self, epoch: PlannerEpoch
    ) -> SubscriptionAcknowledgement:
        """Request paired TRADE/QUOTE subscriptions and wait for acknowledgement."""
        if self._subscription_adapter is None:
            raise RuntimeError("subscription_adapter_required")

        self._recorder.emit(
            "subscription_planning_completed",
            completed_items=epoch.contract_count,
            remaining_items=0,
        )
        # Request subscriptions for all contracts in the epoch
        add_ids = await self._subscription_adapter.add_subscriptions(epoch.trade_subscriptions)
        ack = await self._subscription_adapter.acknowledge(add_ids, timeout=self.config.ack_timeout)
        epoch_created = self._journal.records_by_kind(JOURNAL_KIND_EPOCH_CREATED)[-1]
        pairs = [
            (contract, channel)
            for contract in epoch.additions
            for channel in self._subscription_adapter.request_types
        ]
        if len(pairs) != len(add_ids):
            raise RuntimeError("subscription_command_cardinality_mismatch")
        for request_id, (contract, channel) in zip(add_ids, pairs):
            command_id = f"e{epoch.sequence}-add-{channel}-{request_id}"
            command = self._journal.append(
                JOURNAL_KIND_SUBSCRIPTION_COMMAND,
                timestamp=self._clock(),
                parent_sequence=epoch_created.sequence,
                epoch_sequence=epoch.sequence,
                epoch_id=epoch.epoch_id,
                contract_identity=contract_identity(contract),
                command_id=command_id,
                action="add",
                channel=channel,
                provider_request_id=request_id,
            )
            accepted = request_id in ack.acknowledged and ack.accepted
            self._journal.append(
                JOURNAL_KIND_SUBSCRIPTION_ACKNOWLEDGEMENT,
                timestamp=self._clock(),
                parent_sequence=command.sequence,
                epoch_sequence=epoch.sequence,
                epoch_id=epoch.epoch_id,
                contract_identity=contract_identity(contract),
                command_id=command_id,
                ack_id=f"ack-{request_id}",
                action="add",
                channel=channel,
                provider_request_id=request_id,
                accepted=accepted,
                diagnostic=ack.diagnostic,
            )
        self._recorder.emit(
            "subscriptions_acknowledged",
            acknowledged=len(ack.acknowledged),
            rejected=len(ack.rejected),
            timed_out=len(ack.timed_out),
        )
        return ack

    async def _run_intake_loop(self) -> None:
        """Select independently between event arrival, clock, connection, and stop."""
        if self._event_stream is None:
            return
        self._pipeline = SignalPipeline(
            self.settings,
            repository=self._repository,
            now=self._clock,
            run_id=self.config.run_id,
            symbols=PILOT_SYMBOLS,
        )
        if self._writer is not None:
            self._writer.start()

        self._recorder.emit("observation_started")
        driver = self._session_driver or AsyncSessionDriver(self._event_stream, self._clock)
        reconnect_attempts = 0
        while not self._intake_stopped:
            due = self._scheduler.next_reevaluation(self._clock())
            signal = await driver.next_signal(due, self.config.intake_stop)
            if signal.kind == "stop":
                self._intake_stopped = True
                break
            if signal.kind == "clock":
                clock_record = self._journal.append(
                    JOURNAL_KIND_CLOCK_ADVANCED,
                    timestamp=signal.timestamp,
                    parent_sequence=self._active_epoch_record_sequence,
                    epoch_sequence=(self._active_epoch.sequence if self._active_epoch else None),
                    epoch_id=(self._active_epoch.epoch_id if self._active_epoch else None),
                    boundary=signal.timestamp.isoformat(),
                    reason="scheduled_reevaluation",
                )
                await self._perform_reevaluation(signal.timestamp, clock_record.sequence)
                continue
            if signal.kind == "disconnect":
                reconnect_attempts += 1
                if reconnect_attempts > 5:
                    raise RuntimeError("provider_reconnect_limit_exceeded")
                await self._handle_disconnect()
                continue
            if signal.kind == "end":
                continue
            if signal.event is None:
                raise RuntimeError("driver_event_missing")
            self._record_event(signal.event, signal.timestamp)

    def _record_event(self, event: CanonicalEvent, now: datetime) -> None:
        if self._active_epoch is None or self._active_epoch_record_sequence is None:
            raise RuntimeError("event_without_active_epoch")
        raw = event.payload.get("contract")
        if not isinstance(raw, dict):
            raise TypeError("option_event_contract_missing")
        contract = ThetaContract(
            str(raw.get("root", "")).upper(),
            int(raw.get("expiration", 0)),
            int(raw.get("strike", 0)),
            str(raw.get("right", "")),
        )
        identity = contract_identity(contract)
        event_id = str(event.event_id)
        event_kind = str(event.payload.get("provider_event_kind", "unknown")).upper()
        if not self._is_event_accepted(event):
            self._journal.append(
                JOURNAL_KIND_EVENT_REJECTED,
                timestamp=now,
                parent_sequence=self._active_epoch_record_sequence,
                epoch_sequence=self._active_epoch.sequence,
                epoch_id=self._active_epoch.epoch_id,
                contract_identity=identity,
                reason="not_in_active_acknowledged_epoch",
                event_id=event_id,
                event_kind=event_kind,
                symbol=event.symbol,
            )
            return
        assert self._pipeline is not None
        self._pipeline.process(event)
        self._event_count += 1
        self._journal.append(
            JOURNAL_KIND_EVENT_ACCEPTED,
            timestamp=now,
            parent_sequence=self._active_epoch_record_sequence,
            epoch_sequence=self._active_epoch.sequence,
            epoch_id=self._active_epoch.epoch_id,
            contract_identity=identity,
            event_count=self._event_count,
            event_id=event_id,
            event_kind=event_kind,
            symbol=event.symbol,
        )

    async def _perform_reevaluation(self, now: datetime, clock_sequence: int) -> None:
        """Run a scheduled candidate reevaluation and epoch transition."""
        if self._active_epoch is None:
            return

        self._scheduler.record_reevaluation(now)
        parent_seq = self._journal.append(
            JOURNAL_KIND_REEVALUATION_START,
            timestamp=now,
            parent_sequence=clock_sequence,
            epoch_sequence=self._active_epoch.sequence,
            epoch_id=self._active_epoch.epoch_id,
            boundary=now.isoformat(),
            evaluations_completed=self._scheduler.evaluations_completed,
        ).sequence
        self._recorder.emit("reevaluation_started", epoch=self._active_epoch.sequence)

        assert self._discovery is not None
        assert self._enrichment is not None
        discovery_start = self._journal.append(
            JOURNAL_KIND_DISCOVERY_START,
            timestamp=now,
            parent_sequence=parent_seq,
            epoch_sequence=self._active_epoch.sequence + 1,
            symbols=list(PILOT_SYMBOLS),
        )
        prices = await self._discovery.prices(PILOT_SYMBOLS)
        discovered = await self._discovery.discover(PILOT_SYMBOLS, now)
        discovery_complete = self._journal.append(
            JOURNAL_KIND_DISCOVERY_COMPLETE,
            timestamp=self._clock(),
            parent_sequence=discovery_start.sequence,
            epoch_sequence=self._active_epoch.sequence + 1,
            completed_items=len(discovered),
        )
        enrichment_start = self._journal.append(
            JOURNAL_KIND_ENRICHMENT_START,
            timestamp=self._clock(),
            parent_sequence=discovery_complete.sequence,
            epoch_sequence=self._active_epoch.sequence + 1,
            discovered_count=len(discovered),
        )
        selections = await self._enrichment.enrich(discovered, prices, now)
        enrichment_complete = self._journal.append(
            JOURNAL_KIND_ENRICHMENT_COMPLETE,
            timestamp=self._clock(),
            parent_sequence=enrichment_start.sequence,
            epoch_sequence=self._active_epoch.sequence + 1,
            completed_items=len(selections),
        )
        new_epoch = self._planner.plan(
            selections=selections,
            sequence=self._active_epoch.sequence + 1,
            effective_at=now,
            previous_contracts=self._active_epoch.selected_contracts,
            baseline_symbols=frozenset(),
        )

        # No-op detection: if the selected set is unchanged, skip
        if new_epoch.is_noop:
            self._journal.append(
                "reevaluation.noop",
                timestamp=self._clock(),
                parent_sequence=parent_seq,
                epoch_sequence=new_epoch.sequence,
            )
            self._recorder.emit("reevaluation_noop", sequence=new_epoch.sequence)
            return

        # Journal the new epoch creation
        self._journal.append(
            JOURNAL_KIND_EPOCH_CREATED,
            timestamp=self._clock(),
            parent_sequence=enrichment_complete.sequence,
            epoch_sequence=new_epoch.sequence,
            epoch_id=new_epoch.epoch_id,
            content_hash=new_epoch.content_hash,
            contract_count=new_epoch.contract_count,
            additions=len(new_epoch.additions),
            removals=len(new_epoch.removals),
            membership=[contract_identity(item) for item in new_epoch.selected_contracts],
            provenance=new_epoch.provenance,
            lifecycle=new_epoch.lifecycle,
        )

        # Persist the new epoch before activation
        self._persist_epoch(new_epoch)

        if not await self._apply_subscription_diff(new_epoch):
            return

        # Supersede the previous epoch and activate the new one
        superseded = self._active_epoch.supersede()
        self._epochs[-1] = superseded  # Replace in-place for history
        new_epoch = new_epoch.activate()
        self._active_epoch = new_epoch
        self._epochs.append(new_epoch)
        activation = self._journal.append(
            JOURNAL_KIND_EPOCH_ACTIVATED,
            timestamp=self._clock(),
            parent_sequence=self._journal.sequence - 1,
            epoch_sequence=new_epoch.sequence,
            epoch_id=new_epoch.epoch_id,
            content_hash=new_epoch.content_hash,
            contract_count=new_epoch.contract_count,
            additions=len(new_epoch.additions),
            removals=len(new_epoch.removals),
            membership=[contract_identity(item) for item in new_epoch.selected_contracts],
            lifecycle=new_epoch.lifecycle,
        )
        self._active_epoch_record_sequence = activation.sequence

        self._recorder.emit(
            "epoch_activated",
            epoch=new_epoch.sequence,
            contract_count=new_epoch.contract_count,
            additions=len(new_epoch.additions),
            removals=len(new_epoch.removals),
        )

    async def _apply_subscription_diff(self, epoch: PlannerEpoch) -> bool:
        if self._subscription_adapter is None:
            raise RuntimeError("subscription_adapter_required")
        created = next(
            record
            for record in reversed(self._journal.records)
            if record.kind == JOURNAL_KIND_EPOCH_CREATED and record.epoch_sequence == epoch.sequence
        )
        for action, contracts in (("add", epoch.additions), ("remove", epoch.removals)):
            if not contracts:
                continue
            if action == "add":
                request_ids = await self._subscription_adapter.add_subscriptions(contracts)
            else:
                request_ids = await self._subscription_adapter.remove_subscriptions(contracts)
            pairs = [
                (contract, channel)
                for contract in contracts
                for channel in self._subscription_adapter.request_types
            ]
            if len(pairs) != len(request_ids):
                raise RuntimeError("subscription_command_cardinality_mismatch")
            acknowledgement = await self._subscription_adapter.acknowledge(
                request_ids, timeout=self.config.ack_timeout
            )
            for request_id, (contract, channel) in zip(request_ids, pairs):
                command_id = f"e{epoch.sequence}-{action}-{channel}-{request_id}"
                command = self._journal.append(
                    JOURNAL_KIND_SUBSCRIPTION_COMMAND,
                    timestamp=self._clock(),
                    parent_sequence=created.sequence,
                    epoch_sequence=epoch.sequence,
                    epoch_id=epoch.epoch_id,
                    contract_identity=contract_identity(contract),
                    command_id=command_id,
                    action=action,
                    channel=channel,
                    provider_request_id=request_id,
                )
                accepted = request_id in acknowledgement.acknowledged and acknowledgement.accepted
                self._journal.append(
                    JOURNAL_KIND_SUBSCRIPTION_ACKNOWLEDGEMENT,
                    timestamp=self._clock(),
                    parent_sequence=command.sequence,
                    epoch_sequence=epoch.sequence,
                    epoch_id=epoch.epoch_id,
                    contract_identity=contract_identity(contract),
                    command_id=command_id,
                    ack_id=f"ack-{request_id}",
                    action=action,
                    channel=channel,
                    provider_request_id=request_id,
                    accepted=accepted,
                    diagnostic=acknowledgement.diagnostic,
                )
                if not accepted:
                    return False
        return True

    async def _handle_disconnect(self) -> None:
        """Handle provider disconnect with idempotent epoch restoration."""
        if self._subscription_adapter is None or self._active_epoch is None:
            return

        disc_seq = self._journal.append(
            JOURNAL_KIND_PROVIDER_DISCONNECTED,
            timestamp=self._clock(),
            parent_sequence=self._journal.sequence - 1,
            epoch_sequence=self._active_epoch.sequence,
            epoch_id=self._active_epoch.epoch_id,
            adapter_connected=(
                self._subscription_adapter.connected
                if self._subscription_adapter is not None
                else False
            ),
        ).sequence
        self._recorder.emit("provider_disconnected")
        # The underlying provider's reconnecting_stream handles reconnection.
        # After reconnect, restore the current epoch's subscriptions.
        await self._subscription_adapter.restore_epoch(self._active_epoch.selected_contracts)
        reconnect = self._journal.append(
            JOURNAL_KIND_PROVIDER_RECONNECTED,
            timestamp=self._clock(),
            parent_sequence=disc_seq,
            epoch_sequence=self._active_epoch.sequence,
            epoch_id=self._active_epoch.epoch_id,
            adapter_connected=self._subscription_adapter.connected,
            connection_generation=self._subscription_adapter.connection_generation,
        )
        self._journal.append(
            JOURNAL_KIND_EPOCH_RESTORED,
            timestamp=self._clock(),
            parent_sequence=reconnect.sequence,
            epoch_sequence=self._active_epoch.sequence,
            epoch_id=self._active_epoch.epoch_id,
            contract_count=len(self._active_epoch.selected_contracts),
            membership=[contract_identity(item) for item in self._active_epoch.selected_contracts],
        )
        self._recorder.emit("epoch_restored_after_reconnect", epoch=self._active_epoch.sequence)

    def _should_reevaluate(self, now: datetime) -> bool:
        """Check if a scheduled reevaluation is due."""
        return self._scheduler.is_due(now)

    def _is_event_accepted(self, event: CanonicalEvent) -> bool:
        """Check whether an event should be accepted under the active epoch."""
        if self._active_epoch is None:
            return False
        # Check if the event contract is in the active epoch's selected set
        contract_raw = event.payload.get("contract")
        if not isinstance(contract_raw, dict):
            return True  # Non-option events are always accepted
        contract = ThetaContract(
            str(contract_raw.get("root", "")).upper(),
            int(contract_raw.get("expiration", 0)),
            int(contract_raw.get("strike", 0)),
            str(contract_raw.get("right", "")),
        )
        # Check against subscription adapter if available
        if self._subscription_adapter is not None:
            return self._subscription_adapter.is_event_accepted(contract)
        # Fall back to active epoch check
        return contract in set(self._active_epoch.selected_contracts)

    def _persist_epoch(self, epoch: PlannerEpoch) -> None:
        """Persist an epoch before or atomically with activation."""
        if isinstance(self._repository, InMemoryRepository):
            self._repository.record_universe(epoch.record())
        self._journal.append(
            JOURNAL_KIND_EPOCH_PERSISTED,
            timestamp=self._clock(),
            parent_sequence=self._journal.sequence - 1,
            epoch_sequence=epoch.sequence,
            epoch_id=epoch.epoch_id,
            content_hash=epoch.content_hash,
        )

    async def _stop_intake(self) -> None:
        """Stop event intake at the configured boundary."""
        self._intake_stopped = True
        self._journal.append(
            JOURNAL_KIND_INTAKE_STOPPED,
            timestamp=self._clock(),
            parent_sequence=self._journal.sequence - 1,
            event_count=self._event_count,
            boundary=self.config.intake_stop.isoformat(),
        )
        self._recorder.emit("intake_stopped")

    async def _drain_persistence(self) -> None:
        """Drain the persistence queue."""
        if self._writer is not None:
            await self._writer.close()
        self._drained = True
        self._journal.append(
            JOURNAL_KIND_PERSISTENCE_DRAINED,
            timestamp=self._clock(),
            parent_sequence=self._journal.sequence - 1,
            event_count=self._event_count,
            repository_events=(
                len(getattr(self._repository, "events", []))
                if isinstance(self._repository, InMemoryRepository)
                else 0
            ),
            queue_depth_final=(self._writer.queue.qsize() if self._writer is not None else 0),
        )
        self._recorder.emit("persistence_drained")

    async def _finalize(self) -> None:
        """Finalize the session and write completion records."""
        if self._active_epoch is not None:
            finalized = self._active_epoch.finalize()
            self._epochs[-1] = finalized
            self._active_epoch = finalized
        self._finalized = True
        self._journal.append(
            JOURNAL_KIND_SESSION_FINALIZED,
            timestamp=self._clock(),
            parent_sequence=self._journal.sequence - 1,
            epoch_count=len(self._epochs),
            event_count=self._event_count,
            trading_enabled=False,
            orders_constructed=0,
            orders_submitted=0,
        )
        self._recorder.emit("observation_completed")

    def _persist_and_verify_journal(self) -> bool:
        """Persist and reconstruct the journal through its repository boundary."""
        finalized_sequence = self._journal.sequence - 1
        drain = self._journal.records_by_kind(JOURNAL_KIND_PERSISTENCE_DRAINED)[0]
        prefix = self._journal.serialize()
        identity = self._journal_repository.save(self.config.run_id, prefix)
        self._journal_identity = identity
        persisted = self._journal.append(
            JOURNAL_KIND_JOURNAL_PERSISTED,
            timestamp=self._clock(),
            parent_sequence=finalized_sequence,
            persisted_identity=identity,
            persisted_digest=sha256(prefix),
            persisted_record_count=len(self._journal.records),
            queue_depth_final=drain.payload["queue_depth_final"],
        )
        persisted_value = self._journal.serialize()
        self._journal_repository.save(self.config.run_id, persisted_value)
        loaded = self._journal_repository.load(identity)
        reconstructed = Journal.deserialize(loaded)
        exact_equal = (
            loaded == persisted_value
            and reconstructed.serialize() == persisted_value
            and reconstructed.records == self._journal.records
        )
        self._journal.append(
            JOURNAL_KIND_REPLAY_VERIFIED,
            timestamp=self._clock(),
            parent_sequence=persisted.sequence,
            persisted_identity=identity,
            persisted_digest=sha256(persisted_value),
            original_record_count=len(self._journal.records),
            replayed_record_count=len(reconstructed.records),
            original_terminal_digest=self._journal.records[-1].record_digest,
            replayed_terminal_digest=reconstructed.records[-1].record_digest,
            exact_equal=exact_equal,
        )
        return exact_equal

    def _build_diagnostics(self) -> dict[str, object]:
        """Build diagnostic evidence for the completed session."""
        adapter_diag = (
            self._subscription_adapter.diagnostics()
            if self._subscription_adapter is not None
            else {}
        )
        return {
            "run_id": str(self.config.run_id),
            "session_date": self.config.session_date,
            "epoch_count": len(self._epochs),
            "event_count": self._event_count,
            "decision_count": len(self._pipeline.decisions) if self._pipeline else 0,
            "trading_enabled": False,
            "orders_constructed": 0,
            "orders_submitted": 0,
            "epochs": [epoch.record() for epoch in self._epochs],
            "subscription_diagnostics": adapter_diag,
            "reevaluations_completed": self._scheduler.evaluations_completed,
            "intake_stopped": self._intake_stopped,
            "drained": self._drained,
            "finalized": self._finalized,
        }

    def result(self) -> dict[str, object]:
        """Return a JSON-serialisable result compatible with existing reporting."""
        return {
            "run_id": str(self.config.run_id),
            "trading_enabled": False,
            "status": "live_observation_complete" if self._finalized else "incomplete",
            "epoch_count": len(self._epochs),
            "event_count": self._event_count,
            "decisions_persisted": len(self._pipeline.decisions) if self._pipeline else 0,
            "orders_constructed": 0,
            "orders_submitted": 0,
            "finalized": self._finalized,
            **self._build_diagnostics(),
        }
