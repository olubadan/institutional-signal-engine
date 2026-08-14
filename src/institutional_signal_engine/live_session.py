"""Durable production and offline evidence path for one observational session.

This subsystem is deliberately separate from the deterministic Phase 4B
acceptance certificate.  It certifies only evidence from one completed
observational session and has no execution or order route.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import hmac
import importlib.resources
import json
import os
import subprocess
import tempfile
import threading
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, ClassVar, Final, Literal, Protocol, cast
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

import websockets

from .config import Settings
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
    JOURNAL_KIND_PERSISTENCE_DRAINED,
    JOURNAL_KIND_PROVIDER_DISCONNECTED,
    JOURNAL_KIND_PROVIDER_READY,
    JOURNAL_KIND_PROVIDER_RECONNECTED,
    JOURNAL_KIND_REEVALUATION_NOOP,
    JOURNAL_KIND_REEVALUATION_START,
    JOURNAL_KIND_SCIENTIFIC_EVIDENCE_BATCH,
    JOURNAL_KIND_SESSION_FINALIZED,
    JOURNAL_KIND_SESSION_START,
    JOURNAL_KIND_SUBSCRIPTION_ACKNOWLEDGEMENT,
    JOURNAL_KIND_SUBSCRIPTION_COMMAND,
    FileJournalRepository,
    Journal,
    JournalFailure,
    canonical_json,
    persistence_object_identity,
    sha256,
    sha256_bytes,
    verify_structural,
)
from .live_smoke import _secret
from .mathematical_pipeline import MathematicalPipeline, replay_semantics
from .orchestration import DiscoveryPort, EnrichmentPort, PlannerPort, ProductionPlanner
from .persistence import PostgresRepository
from .providers.alpaca import AlpacaEquitiesProvider
from .providers.common import ProviderError
from .providers.thetadata import SubscriptionRequest, ThetaContract, ThetaDataOptionsProvider
from .schemas import CanonicalEvent
from .side_b import ForwardOutcomeTracker, OutcomeAnchor, SideBJournal
from .universe import PILOT_SYMBOLS, PlannerEpoch

ET: Final = ZoneInfo("America/New_York")
RULES_RESOURCE: Final = "resources/live_session_validation_rules.json"
MANIFEST_NAME: Final = "MANIFEST.json"
JOURNAL_NAME: Final = "JOURNAL.json"
FINAL_STATE_NAME: Final = "FINAL_STATE.json"
CONFIGURATION_NAME: Final = "CONFIGURATION.json"
PERSISTENCE_RECEIPT_NAME: Final = "PERSISTENCE_RECEIPT.json"
REPLAY_RECEIPT_NAME: Final = "REPLAY_RECEIPT.json"
CHECKPOINT_NAME: Final = "JOURNAL.checkpoint.json"
INCOMPLETE_NAME: Final = "INCOMPLETE"
CERTIFICATE_VERSION: Final = "LIVE_OBSERVATIONAL_SESSION_CERTIFICATE_V1"
MANIFEST_VERSION: Final = "LIVE_EVIDENCE_MANIFEST_V1"
JOURNAL_SCHEMA_VERSION: Final = "LIVE_CAUSAL_JOURNAL_V1"
IMPACT_MODE: Final = "APPROVED_OFFLINE_FALLBACK"
REQUIRED_CHANNELS: Final = ("TRADE", "QUOTE")
# The provider transition path requires a short quiet interval after each
# correlated acknowledgement. This bounds request-transition pressure without
# changing the selected contract population or acknowledgement semantics.
SUBSCRIPTION_TRANSITION_PACING_SECONDS: Final = 0.25


class LiveEvidenceFailure(ValueError):
    """Fail-closed live-evidence error with a stable machine code."""

    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(f"{code}:{detail}" if detail else code)
        self.code = code
        self.detail = detail


def _rules_bytes() -> bytes:
    return (
        importlib.resources.files("institutional_signal_engine")
        .joinpath(RULES_RESOURCE)
        .read_bytes()
    )


def _digest_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _atomic_write(path: Path, content: str) -> None:
    if path.exists() or path.is_symlink():
        raise LiveEvidenceFailure("OUTPUT_TARGET_EXISTS", str(path))
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600)
        temporary.replace(path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _replace_checkpoint(path: Path, content: str) -> None:
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600)
        temporary.replace(path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _validate_fresh_directory(path: Path) -> Path:
    if not path.is_absolute():
        raise LiveEvidenceFailure("OUTPUT_DIRECTORY_NOT_ABSOLUTE")
    if path.exists() or path.is_symlink():
        raise LiveEvidenceFailure("OUTPUT_DIRECTORY_EXISTS", str(path))
    parent = path.parent
    if not parent.exists() or not parent.is_dir():
        raise LiveEvidenceFailure("OUTPUT_PARENT_INVALID", str(parent))
    cursor = parent
    while cursor != cursor.parent:
        # macOS exposes /var and /tmp as stable OS aliases; reject all
        # user-created aliases beneath them.
        if cursor.is_symlink() and cursor not in {Path("/var"), Path("/tmp")}:
            raise LiveEvidenceFailure("OUTPUT_DIRECTORY_ALIASED", str(cursor))
        cursor = cursor.parent
    if path != Path(os.path.abspath(path)):
        raise LiveEvidenceFailure("OUTPUT_DIRECTORY_ALIASED", str(path))
    return path


def _load_authority_key(path: Path | None) -> bytes:
    if path is None:
        raise LiveEvidenceFailure("AUTHORITY_KEY_CONFIGURATION_REQUIRED")
    if path.is_symlink() or not path.is_file():
        raise LiveEvidenceFailure("AUTHORITY_KEY_UNAVAILABLE")
    if path.stat().st_mode & 0o077:
        raise LiveEvidenceFailure("AUTHORITY_KEY_PERMISSIONS_UNSAFE")
    value = path.read_bytes()
    if len(value) < 32:
        raise LiveEvidenceFailure("AUTHORITY_KEY_TOO_SHORT")
    return value


def _validate_fresh_file(path: Path) -> None:
    if not path.is_absolute():
        raise LiveEvidenceFailure("OUTPUT_FILE_NOT_ABSOLUTE")
    if path.exists() or path.is_symlink():
        raise LiveEvidenceFailure("OUTPUT_TARGET_EXISTS", str(path))
    if not path.parent.exists() or not path.parent.is_dir():
        raise LiveEvidenceFailure("OUTPUT_PARENT_INVALID", str(path.parent))
    cursor = path.parent
    while cursor != cursor.parent:
        if cursor.is_symlink() and cursor not in {Path("/var"), Path("/tmp")}:
            raise LiveEvidenceFailure("OUTPUT_DIRECTORY_ALIASED", str(cursor))
        cursor = cursor.parent


def _authority_tag(key: bytes, commitment: Mapping[str, object]) -> str:
    return hmac.new(key, canonical_json(dict(commitment)).encode(), hashlib.sha256).hexdigest()


def _git_head(repository: Path) -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


@dataclass(frozen=True)
class SessionIdentity:
    git_commit: str
    application_version: str
    config_version: str
    journal_schema_version: str
    rules_sha256: str
    configuration_sha256: str

    def as_dict(self) -> dict[str, object]:
        return {
            "git_commit": self.git_commit,
            "application_version": self.application_version,
            "config_version": self.config_version,
            "journal_schema_version": self.journal_schema_version,
            "rules_sha256": self.rules_sha256,
            "configuration_sha256": self.configuration_sha256,
        }


@dataclass(frozen=True)
class SessionBoundaries:
    market_date: date
    opens_at: datetime
    closes_at: datetime
    timezone: str = "America/New_York"

    @classmethod
    def for_market_date(cls, market_date: date) -> SessionBoundaries:
        if market_date.weekday() >= 5:
            raise LiveEvidenceFailure("MARKET_DATE_NOT_WEEKDAY")
        return cls(
            market_date=market_date,
            opens_at=datetime.combine(market_date, time(9, 30), ET),
            closes_at=datetime.combine(market_date, time(16, 0), ET),
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "market_date": self.market_date.isoformat(),
            "timezone": self.timezone,
            "opens_at": self.opens_at.isoformat(),
            "closes_at": self.closes_at.isoformat(),
        }


class SessionClock(Protocol):
    def now(self) -> datetime: ...

    async def wait_until(self, boundary: datetime) -> None: ...


class RealSessionClock:
    def now(self) -> datetime:
        return datetime.now(ET)

    async def wait_until(self, boundary: datetime) -> None:
        delay = (boundary - self.now()).total_seconds()
        if delay > 0:
            await asyncio.sleep(delay)


@dataclass(frozen=True)
class InboundFrame:
    kind: Literal["ack", "event", "clock", "disconnect", "terminated", "malformed"]
    observed_at: datetime
    request_id: int | None = None
    response: str | None = None
    event: CanonicalEvent | None = None
    detail: str = ""


class UnifiedSessionPort(Protocol):
    request_types: tuple[str, ...]
    connection_generation: int

    async def connect(self) -> None: ...

    async def close(self) -> None: ...

    async def reconnect(self) -> None: ...

    async def health(self) -> dict[str, object]: ...

    def prepare_request(
        self, action: Literal["add", "remove"], contract: ThetaContract, channel: str
    ) -> SubscriptionRequest: ...

    async def transmit(self, request: SubscriptionRequest) -> None: ...

    async def receive_until(self, boundary: datetime) -> InboundFrame: ...


class ObservationRepository(Protocol):
    def record_event(self, event: CanonicalEvent) -> None: ...

    def replay_events(self, run_id: UUID | None = None) -> tuple[CanonicalEvent, ...]: ...

    def flush(self) -> None: ...

    def durable_identity(self, run_id: UUID) -> str: ...


class PostgresObservationRepository:
    """Durable production observation store using the configured application DSN."""

    def __init__(self, repository: PostgresRepository) -> None:
        self._repository = repository

    def preflight(self) -> None:
        self._repository.initialize()
        if not self._repository.healthcheck() or not self._repository.write_drain_probe(
            f"live-evidence-{uuid4()}"
        ):
            raise LiveEvidenceFailure("DURABLE_REPOSITORY_UNAVAILABLE")

    def record_event(self, event: CanonicalEvent) -> None:
        self._repository.record_event(event)

    def flush(self) -> None:
        self._repository.flush()

    def durable_identity(self, run_id: UUID) -> str:
        return f"postgresql-run://{run_id}"

    def record_decision(self, value: Any) -> None:
        self._repository.record_decision(value)

    def record_quote_consumption(self, value: Any) -> None:
        self._repository.record_quote_consumption(value)

    def record_sweep(self, value: dict[str, object]) -> None:
        self._repository.record_sweep(value)

    def record_impact_cluster(self, value: dict[str, object]) -> None:
        self._repository.record_impact_cluster(value)

    def record_impact_session(self, value: dict[str, object]) -> None:
        self._repository.record_impact_session(value)

    def record_shared_feature_vector(self, value: dict[str, object]) -> None:
        self._repository.record_shared_feature_vector(value)

    def record_model_comparison(self, value: dict[str, object]) -> None:
        self._repository.record_model_comparison(value)

    def record_control_evaluation(self, value: dict[str, object]) -> None:
        self._repository.record_control_evaluation(value)

    def record_shadow_work_item(self, value: dict[str, object]) -> bool:
        self._repository.record_shadow_work_item(value)
        return True

    def replay_events(self, run_id: UUID | None = None) -> tuple[CanonicalEvent, ...]:
        return tuple(self._repository.replay_events(run_id))


class BundleWriter:
    """Crash-distinguishable staged publication with manifest-last completion."""

    def __init__(self, output_directory: Path) -> None:
        self.directory = _validate_fresh_directory(output_directory)
        self.directory.mkdir(mode=0o700)
        _atomic_write(
            self.directory / INCOMPLETE_NAME,
            canonical_json({"status": "incomplete", "created_at": datetime.now(UTC).isoformat()}),
        )

    def checkpoint(self, journal: Journal) -> None:
        _replace_checkpoint(self.directory / CHECKPOINT_NAME, journal.serialize())

    def publish_component(self, name: str, value: Mapping[str, object] | str) -> str:
        serialized = value if isinstance(value, str) else canonical_json(dict(value))
        _atomic_write(self.directory / name, serialized)
        return sha256_bytes(serialized)

    def publish_manifest(self, manifest: Mapping[str, object]) -> None:
        _atomic_write(self.directory / MANIFEST_NAME, canonical_json(dict(manifest)))
        (self.directory / INCOMPLETE_NAME).unlink(missing_ok=True)
        (self.directory / CHECKPOINT_NAME).unlink(missing_ok=True)


@dataclass
class LiveSessionEngine:
    """Single-authority observational session state machine."""

    run_id: UUID
    identity: SessionIdentity
    boundaries: SessionBoundaries
    settings: Settings
    clock: SessionClock
    discovery: DiscoveryPort
    enrichment: EnrichmentPort
    planner: PlannerPort
    provider: UnifiedSessionPort
    repository: ObservationRepository
    writer: BundleWriter
    journal_repository: FileJournalRepository
    authority_key: bytes
    reevaluation_interval: timedelta = timedelta(minutes=5)
    acknowledgement_timeout: timedelta = timedelta(seconds=30)
    provider_identity: Mapping[str, object] = field(default_factory=dict)
    baseline_symbols: frozenset[str] = frozenset()
    mathematical_pipeline: MathematicalPipeline | None = None
    equity_provider: AlpacaEquitiesProvider | None = None
    equity_symbols: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.settings.trading_enabled:
            raise LiveEvidenceFailure("TRADING_ENABLED")
        if self.boundaries.closes_at <= self.boundaries.opens_at:
            raise LiveEvidenceFailure("SESSION_BOUNDARIES_INVALID")
        self.journal = Journal(self.run_id)
        self.active_epoch: PlannerEpoch | None = None
        self.active_channels: dict[ThetaContract, set[str]] = {}
        self.seen_events: set[UUID] = set()
        self.event_count = 0
        self.processed_count = 0
        self.epoch_count = 0
        self.disconnect_count = 0
        self.side_b = ForwardOutcomeTracker(SideBJournal(self.writer.directory / "SIDE_B.jsonl"))
        self._side_b_anchors: set[str] = set()
        self._processing_lock = threading.RLock()
        self._pending_scientific_evidence: list[dict[str, object]] = []

    def _append(self, kind: str, *, checkpoint: bool = True, **values: Any) -> int:
        record = self.journal.append(kind, timestamp=self.clock.now(), **values)
        if checkpoint:
            self.writer.checkpoint(self.journal)
        return record.sequence

    def _record_math_evidence(self, kind: str, payload: Mapping[str, object]) -> None:
        cluster_id = str(payload.get("cluster_id", ""))
        with self._processing_lock:
            self._pending_scientific_evidence.append(
                {"kind": kind, "cluster_id": cluster_id, "payload": dict(payload)}
            )

    def _take_math_evidence(self) -> list[dict[str, object]]:
        pending = self._pending_scientific_evidence
        self._pending_scientific_evidence = []
        return pending

    def _flush_math_evidence(self, *, checkpoint: bool = False) -> None:
        pending = self._take_math_evidence()
        if not pending:
            return
        self._append(
            JOURNAL_KIND_SCIENTIFIC_EVIDENCE_BATCH,
            operation_id=f"scientific-batch:{self.journal.sequence}",
            correlation_id=str(pending[0].get("cluster_id") or "scientific"),
            scientific_evidence={
                "schema_version": "MATHEMATICAL_EVIDENCE_BATCH_V1",
                "records": pending,
            },
            checkpoint=checkpoint,
        )

    async def _receive_acknowledgements(
        self, requests: tuple[SubscriptionRequest, ...], epoch: PlannerEpoch
    ) -> None:
        pending = {request.request_id: request for request in requests}
        observed: set[int] = set()
        deadline = self.clock.now() + self.acknowledgement_timeout
        while pending:
            frame = await self.provider.receive_until(deadline)
            if frame.kind == "clock":
                raise LiveEvidenceFailure("ACKNOWLEDGEMENT_MISSING")
            if frame.kind == "disconnect":
                raise LiveEvidenceFailure("DISCONNECT_DURING_TRANSITION")
            if frame.kind == "terminated":
                raise LiveEvidenceFailure("PROVIDER_TERMINATED")
            if frame.kind == "malformed":
                raise LiveEvidenceFailure("MALFORMED_PROVIDER_RESPONSE", frame.detail)
            if frame.kind == "event":
                # Preserve active-channel market frames while a later epoch
                # transition waits for a provider control response. Frames
                # before activation remain outside the active session.
                if frame.event is not None and self.active_epoch is not None:
                    await asyncio.to_thread(self._process_theta_event, frame.event, True)
                continue
            request_id = frame.request_id
            if request_id is None or request_id not in pending:
                if request_id in observed:
                    raise LiveEvidenceFailure("DUPLICATE_ACKNOWLEDGEMENT")
                raise LiveEvidenceFailure("UNMATCHED_ACKNOWLEDGEMENT")
            request = pending.pop(request_id)
            observed.add(request_id)
            expected = "SUBSCRIBED" if request.add else "UNSUBSCRIBED"
            accepted = frame.response == expected
            command_id = f"g{request.generation}-r{request.request_id}"
            self._append(
                JOURNAL_KIND_SUBSCRIPTION_ACKNOWLEDGEMENT,
                parent_sequence=next(
                    item.sequence
                    for item in reversed(self.journal.records)
                    if item.command_id == command_id
                ),
                operation_id=f"ack-{command_id}",
                correlation_id=command_id,
                epoch_sequence=epoch.sequence,
                epoch_id=epoch.epoch_id,
                contract_identity=_contract_identity(request.contract),
                command_id=command_id,
                ack_id=f"ack-{command_id}",
                provider_request_id=request.request_id,
                connection_generation=request.generation,
                action="add" if request.add else "remove",
                channel=request.req_type,
                response=frame.response or "",
                accepted=accepted,
            )
            if not accepted:
                raise LiveEvidenceFailure("ACKNOWLEDGEMENT_REJECTED", frame.response or "")
            channels = self.active_channels.setdefault(request.contract, set())
            if request.add:
                channels.add(request.req_type)
            else:
                channels.discard(request.req_type)
                if not channels:
                    self.active_channels.pop(request.contract, None)

    async def _transition(
        self,
        epoch: PlannerEpoch,
        action: Literal["add", "remove"],
        contracts: tuple[ThetaContract, ...],
    ) -> None:
        # The local Theta service accepts REMOVE_TRADE but emits no correlated
        # REQ_RESPONSE. Retain provider subscriptions and enforce epoch
        # membership at ingestion; additions remain ACK-gated.
        if action == "remove" and not getattr(
            self.provider, "supports_removal_acknowledgements", False
        ):
            return
        for contract in contracts:
            for channel in REQUIRED_CHANNELS:
                request = self.provider.prepare_request(action, contract, channel)
                command_id = f"g{request.generation}-r{request.request_id}"
                self._append(
                    JOURNAL_KIND_SUBSCRIPTION_COMMAND,
                    parent_sequence=self.journal.sequence - 1,
                    operation_id=command_id,
                    correlation_id=command_id,
                    epoch_sequence=epoch.sequence,
                    epoch_id=epoch.epoch_id,
                    contract_identity=_contract_identity(contract),
                    command_id=command_id,
                    provider_request_id=request.request_id,
                    connection_generation=request.generation,
                    action=action,
                    channel=channel,
                )
                await self.provider.transmit(request)
                await self._receive_acknowledgements((request,), epoch)
                await asyncio.sleep(SUBSCRIPTION_TRANSITION_PACING_SECONDS)

    def _journal_event(
        self, event: CanonicalEvent, *, accepted: bool, reason: str = "", checkpoint: bool = True
    ) -> None:
        raw_contract = event.payload.get("contract")
        if not isinstance(raw_contract, dict):
            raise LiveEvidenceFailure("EVENT_CONTRACT_MALFORMED")
        contract = ThetaContract(
            str(raw_contract.get("root", "")).upper(),
            int(raw_contract.get("expiration", 0)),
            int(raw_contract.get("strike", 0)),
            str(raw_contract.get("right", "")).upper(),
        )
        if event.event_id in self.seen_events:
            accepted = False
            reason = reason or "duplicate_event"
        if self.active_epoch is None:
            accepted = False
            reason = reason or "no_active_epoch"
        channel = str(event.payload.get("provider_event_kind", "")).upper()
        if (
            self.active_epoch is None
            or contract not in self.active_epoch.selected_contracts
            or contract not in self.active_channels
            or channel not in self.active_channels[contract]
        ):
            accepted = False
            reason = reason or "not_active_acknowledged"
        if accepted:
            self.seen_events.add(event.event_id)
        kind = JOURNAL_KIND_EVENT_ACCEPTED if accepted else JOURNAL_KIND_EVENT_REJECTED
        if accepted:
            persisted = event.model_copy(
                update={"run_id": self.run_id, "ingest_order": self.event_count + 1}
            )
            self.repository.record_event(persisted)
            self.event_count += 1
            self.processed_count += 1
            if self.mathematical_pipeline is not None:
                self.mathematical_pipeline.process_accepted(event)
        elif self.mathematical_pipeline is not None:
            self.mathematical_pipeline.record_rejected(event, reason)
        scientific_evidence = self._take_math_evidence()
        self._append(
            kind,
            parent_sequence=self.journal.sequence - 1,
            operation_id=f"event-{event.event_id}",
            correlation_id=str(event.event_id),
            epoch_sequence=self.active_epoch.sequence if self.active_epoch else None,
            epoch_id=self.active_epoch.epoch_id if self.active_epoch else None,
            contract_identity=_contract_identity(contract),
            event_id=str(event.event_id),
            event_kind=channel,
            symbol=event.symbol,
            accepted=accepted,
            reason=reason,
            canonical_event=event.model_dump(mode="json"),
            processed=accepted,
            connection_generation=self.provider.connection_generation,
            checkpoint=checkpoint,
            scientific_evidence=scientific_evidence,
        )

    def _process_theta_event(self, event: CanonicalEvent, accepted: bool, reason: str = "") -> None:
        with self._processing_lock:
            self._journal_event(event, accepted=accepted, reason=reason)

    def _journal_equity_event(self, event: CanonicalEvent) -> None:
        persisted = event.model_copy(
            update={"run_id": self.run_id, "ingest_order": self.event_count + 1}
        )
        self.repository.record_event(persisted)
        self.event_count += 1
        self.processed_count += 1
        self.side_b.observe(persisted)
        if self.mathematical_pipeline is not None:
            before = len(self.mathematical_pipeline.feature_vectors)
            self.mathematical_pipeline.process_accepted(persisted)
            self._record_side_b_math_outputs()
            for decision in self.mathematical_pipeline.pipeline.decisions:
                self.side_b.record_filter_decision(decision)
            for vector in self.mathematical_pipeline.feature_vectors[before:]:
                cluster_id = str(vector.get("cluster_id", ""))
                inputs = vector.get("impact_inputs", {})
                if not isinstance(inputs, dict) or cluster_id in self._side_b_anchors:
                    continue
                symbol = str(inputs.get("symbol", "")).upper()
                timestamp = inputs.get("last_timestamp")
                if not isinstance(timestamp, str):
                    continue
                cluster_time = datetime.fromisoformat(timestamp)
                observed = self.side_b.price_at_or_before(symbol, cluster_time)
                price_t0 = observed[1] if observed is not None else None
                if price_t0 is None:
                    continue
                signed = inputs.get("signed_delta_demand")
                signed_direction = (
                    (
                        "UP"
                        if Decimal(str(signed)) > 0
                        else "DOWN"
                        if Decimal(str(signed)) < 0
                        else "FLAT"
                    )
                    if signed is not None
                    else None
                )
                self.side_b.register_anchor(
                    OutcomeAnchor(
                        cluster_id,
                        symbol,
                        cluster_time,
                        price_t0,
                        bool(
                            cast(Mapping[str, object], vector.get("control_inputs", {})).get(
                                "control_qualified"
                            )
                        ),
                        bool(inputs.get("shadow_qualified")),
                        signed_direction,
                        Decimal(str(inputs["z_score"]))
                        if inputs.get("z_score") is not None
                        else None,
                        str(self.run_id),
                    )
                )
                self._side_b_anchors.add(cluster_id)
        scientific_evidence = self._take_math_evidence()
        self._append(
            JOURNAL_KIND_EVENT_ACCEPTED,
            parent_sequence=self.journal.sequence - 1,
            operation_id=f"equity-event-{event.event_id}",
            correlation_id=str(event.event_id),
            event_id=str(event.event_id),
            event_kind=str(event.payload.get("provider_event_kind", "equity")),
            symbol=event.symbol,
            accepted=True,
            reason="",
            canonical_event=persisted.model_dump(mode="json"),
            processed=True,
            connection_generation=self.equity_provider.connection_generation
            if self.equity_provider is not None
            else 0,
            scientific_evidence=scientific_evidence,
        )

    def _process_equity_event(self, event: CanonicalEvent) -> None:
        with self._processing_lock:
            self._journal_equity_event(event)

    def _record_side_b_math_outputs(self) -> None:
        """Attach asynchronously completed mathematical vectors to Side-B."""
        if self.mathematical_pipeline is None:
            return
        for decision in self.mathematical_pipeline.pipeline.decisions:
            self.side_b.record_filter_decision(decision)
        for vector in self.mathematical_pipeline.feature_vectors:
            cluster_id = str(vector.get("cluster_id", ""))
            if cluster_id in self._side_b_anchors:
                continue
            inputs = vector.get("impact_inputs", {})
            if not isinstance(inputs, dict):
                continue
            timestamp = inputs.get("last_timestamp")
            if not isinstance(timestamp, str):
                continue
            observed = self.side_b.price_at_or_before(
                str(inputs.get("symbol", "")).upper(), datetime.fromisoformat(timestamp)
            )
            if observed is None:
                continue
            signed = inputs.get("signed_delta_demand")
            signed_direction = (
                (
                    "UP"
                    if Decimal(str(signed)) > 0
                    else "DOWN"
                    if Decimal(str(signed)) < 0
                    else "FLAT"
                )
                if signed is not None
                else None
            )
            control_inputs = cast(Mapping[str, object], vector.get("control_inputs", {}))
            self.side_b.register_anchor(
                OutcomeAnchor(
                    cluster_id,
                    str(inputs.get("symbol", "")).upper(),
                    datetime.fromisoformat(timestamp),
                    observed[1],
                    bool(control_inputs.get("control_qualified")),
                    bool(inputs.get("shadow_qualified")),
                    signed_direction,
                    Decimal(str(inputs["z_score"])) if inputs.get("z_score") is not None else None,
                    str(self.run_id),
                )
            )
            self._side_b_anchors.add(cluster_id)

    async def _observe_equities(self) -> None:
        if self.equity_provider is None or not self.equity_symbols:
            return
        self._append(
            JOURNAL_KIND_PROVIDER_READY,
            parent_sequence=self.journal.sequence - 1,
            operation_id="equity-provider-ready",
            correlation_id="equity-stream",
            provider_identity={"provider": "alpaca", "symbol_count": len(self.equity_symbols)},
            health=await self.equity_provider.health(),
            connection_generation=self.equity_provider.connection_generation,
        )
        try:
            async for event in self.equity_provider.events(self.equity_symbols):
                if self.clock.now() >= self.boundaries.closes_at:
                    break
                await asyncio.to_thread(self._process_equity_event, event)
        except ProviderError as exc:
            self._append(
                JOURNAL_KIND_EVENT_REJECTED,
                parent_sequence=self.journal.sequence - 1,
                operation_id="equity-provider-error",
                correlation_id="equity-stream",
                event_kind="equity",
                accepted=False,
                reason=f"provider:{exc.category}",
                processed=False,
            )

    async def _discover_epoch(
        self, sequence: int, previous: tuple[ThetaContract, ...]
    ) -> PlannerEpoch:
        now = self.clock.now()
        start = self._append(
            JOURNAL_KIND_DISCOVERY_START,
            parent_sequence=self.journal.sequence - 1,
            operation_id=f"discovery-e{sequence}-start",
            correlation_id=f"discovery-e{sequence}",
            epoch_sequence=sequence,
            symbols=list(PILOT_SYMBOLS),
        )
        prices = await self.discovery.prices(PILOT_SYMBOLS)
        discovered = await self.discovery.discover(PILOT_SYMBOLS, now)
        complete = self._append(
            JOURNAL_KIND_DISCOVERY_COMPLETE,
            parent_sequence=start,
            operation_id=f"discovery-e{sequence}-complete",
            correlation_id=f"discovery-e{sequence}",
            epoch_sequence=sequence,
            completed_items=len(discovered),
            discovered_sha256=sha256([str(item) for item in discovered]),
        )
        enrichment_start = self._append(
            JOURNAL_KIND_ENRICHMENT_START,
            parent_sequence=complete,
            operation_id=f"enrichment-e{sequence}-start",
            correlation_id=f"enrichment-e{sequence}",
            epoch_sequence=sequence,
            discovered_count=len(discovered),
        )
        selections = await self.enrichment.enrich(discovered, prices, now)
        self._append(
            JOURNAL_KIND_ENRICHMENT_COMPLETE,
            parent_sequence=enrichment_start,
            operation_id=f"enrichment-e{sequence}-complete",
            correlation_id=f"enrichment-e{sequence}",
            epoch_sequence=sequence,
            completed_items=len(selections),
            enrichment_sha256=sha256([str(item) for item in selections]),
        )
        epoch = self.planner.plan(
            selections,
            sequence,
            now,
            previous,
            self.baseline_symbols,
        )
        self._append(
            JOURNAL_KIND_EPOCH_CREATED,
            parent_sequence=self.journal.sequence - 1,
            operation_id=f"epoch-e{sequence}-created",
            correlation_id=f"epoch-e{sequence}",
            epoch_sequence=sequence,
            epoch_id=epoch.epoch_id,
            content_hash=epoch.content_hash,
            membership=[_contract_identity(item) for item in epoch.selected_contracts],
            additions=[_contract_identity(item) for item in epoch.additions],
            removals=[_contract_identity(item) for item in epoch.removals],
        )
        self._append(
            JOURNAL_KIND_EPOCH_PERSISTED,
            parent_sequence=self.journal.sequence - 1,
            operation_id=f"epoch-e{sequence}-persisted",
            correlation_id=f"epoch-e{sequence}",
            epoch_sequence=sequence,
            epoch_id=epoch.epoch_id,
            content_hash=epoch.content_hash,
        )
        return epoch

    async def _activate(self, epoch: PlannerEpoch) -> None:
        await self._transition(epoch, "add", epoch.additions)
        await self._transition(epoch, "remove", epoch.removals)
        expected = {contract: set(REQUIRED_CHANNELS) for contract in epoch.selected_contracts}
        if self.active_channels != expected:
            raise LiveEvidenceFailure("ACTIVE_SUBSCRIPTION_STATE_MISMATCH")
        activated = epoch.activate()
        self.active_epoch = activated
        self.epoch_count += 1
        self._append(
            JOURNAL_KIND_EPOCH_ACTIVATED,
            parent_sequence=self.journal.sequence - 1,
            operation_id=f"epoch-e{epoch.sequence}-activated",
            correlation_id=f"epoch-e{epoch.sequence}",
            epoch_sequence=epoch.sequence,
            epoch_id=epoch.epoch_id,
            membership=[_contract_identity(item) for item in epoch.selected_contracts],
            content_hash=epoch.content_hash,
        )

    async def _recover(self) -> None:
        if self.active_epoch is None:
            raise LiveEvidenceFailure("DISCONNECT_WITHOUT_ACTIVE_EPOCH")
        self.disconnect_count += 1
        cycle = f"recovery-{self.disconnect_count}"
        disconnected = self._append(
            JOURNAL_KIND_PROVIDER_DISCONNECTED,
            parent_sequence=self.journal.sequence - 1,
            operation_id=f"{cycle}-disconnected",
            correlation_id=cycle,
            epoch_sequence=self.active_epoch.sequence,
            epoch_id=self.active_epoch.epoch_id,
            connection_generation=self.provider.connection_generation,
        )
        self.active_channels.clear()
        await self.provider.reconnect()
        reconnected = self._append(
            JOURNAL_KIND_PROVIDER_RECONNECTED,
            parent_sequence=disconnected,
            operation_id=f"{cycle}-reconnected",
            correlation_id=cycle,
            epoch_sequence=self.active_epoch.sequence,
            epoch_id=self.active_epoch.epoch_id,
            connection_generation=self.provider.connection_generation,
        )
        await self._transition(self.active_epoch, "add", self.active_epoch.selected_contracts)
        expected = {
            contract: set(REQUIRED_CHANNELS) for contract in self.active_epoch.selected_contracts
        }
        if self.active_channels != expected:
            raise LiveEvidenceFailure("RESTORATION_INCOMPLETE")
        self._append(
            JOURNAL_KIND_EPOCH_RESTORED,
            parent_sequence=reconnected,
            operation_id=f"{cycle}-restored",
            correlation_id=cycle,
            epoch_sequence=self.active_epoch.sequence,
            epoch_id=self.active_epoch.epoch_id,
            membership=[_contract_identity(item) for item in self.active_epoch.selected_contracts],
            connection_generation=self.provider.connection_generation,
        )

    async def execute(self) -> dict[str, object]:
        session_metadata: dict[str, Any] = {
            **self.boundaries.as_dict(),
            **self.identity.as_dict(),
        }
        self._append(
            JOURNAL_KIND_SESSION_START,
            operation_id="session-start",
            correlation_id="session-lifecycle",
            session_id=str(self.run_id),
            **session_metadata,
            trading_enabled=False,
            orders_constructed=0,
            orders_submitted=0,
            impact_mode=IMPACT_MODE,
        )
        configuration_metadata: dict[str, Any] = {
            **self.identity.as_dict(),
        }
        self._append(
            JOURNAL_KIND_CONFIGURATION,
            parent_sequence=0,
            operation_id="configuration-load",
            correlation_id="session-lifecycle",
            **configuration_metadata,
            reevaluation_interval_seconds=self.reevaluation_interval.total_seconds(),
            acknowledgement_timeout_seconds=self.acknowledgement_timeout.total_seconds(),
            opens_at=self.boundaries.opens_at.isoformat(),
            closes_at=self.boundaries.closes_at.isoformat(),
            trading_enabled=False,
            orders_constructed=0,
            orders_submitted=0,
        )
        await self.clock.wait_until(self.boundaries.opens_at)
        if self.clock.now() > self.boundaries.closes_at:
            raise LiveEvidenceFailure("SESSION_ALREADY_CLOSED")
        await self.provider.connect()
        health = await self.provider.health()
        if health.get("status") in {"unavailable", "terminated"}:
            raise LiveEvidenceFailure("PROVIDER_UNAVAILABLE")
        self._append(
            JOURNAL_KIND_PROVIDER_READY,
            parent_sequence=self.journal.sequence - 1,
            operation_id="provider-ready",
            correlation_id="session-lifecycle",
            provider_identity=dict(self.provider_identity),
            health=health,
            connection_generation=self.provider.connection_generation,
        )
        epoch = await self._discover_epoch(1, ())
        if not epoch.selected_contracts:
            raise LiveEvidenceFailure("NO_CONTRACTS_SELECTED")
        await self._activate(epoch)
        equity_task = (
            asyncio.create_task(self._observe_equities())
            if self.equity_provider is not None and self.equity_symbols
            else None
        )
        next_reevaluation = self.boundaries.opens_at + self.reevaluation_interval
        while self.clock.now() < self.boundaries.closes_at:
            boundary = min(next_reevaluation, self.boundaries.closes_at)
            frame = await self.provider.receive_until(boundary)
            if frame.kind == "event":
                assert frame.event is not None
                await asyncio.to_thread(self._process_theta_event, frame.event, True)
            elif frame.kind == "disconnect":
                await self._recover()
            elif frame.kind == "terminated":
                raise LiveEvidenceFailure("PROVIDER_TERMINATED")
            elif frame.kind == "malformed":
                raise LiveEvidenceFailure("MALFORMED_PROVIDER_RESPONSE", frame.detail)
            elif frame.kind == "ack":
                raise LiveEvidenceFailure("UNMATCHED_ACKNOWLEDGEMENT")
            elif frame.kind == "clock" and boundary < self.boundaries.closes_at:
                assert self.active_epoch is not None
                clock_sequence = self._append(
                    JOURNAL_KIND_CLOCK_ADVANCED,
                    parent_sequence=self.journal.sequence - 1,
                    operation_id=f"clock-{boundary.isoformat()}",
                    correlation_id=f"reevaluation-{boundary.isoformat()}",
                    epoch_sequence=self.active_epoch.sequence,
                    epoch_id=self.active_epoch.epoch_id,
                    boundary=boundary.isoformat(),
                    host_observed_at=self.clock.now().isoformat(),
                )
                self._append(
                    JOURNAL_KIND_REEVALUATION_START,
                    parent_sequence=clock_sequence,
                    operation_id=f"reevaluation-{boundary.isoformat()}",
                    correlation_id=f"reevaluation-{boundary.isoformat()}",
                    epoch_sequence=self.active_epoch.sequence,
                    epoch_id=self.active_epoch.epoch_id,
                    boundary=boundary.isoformat(),
                )
                new_epoch = await self._discover_epoch(
                    self.active_epoch.sequence + 1, self.active_epoch.selected_contracts
                )
                if new_epoch.is_noop:
                    self._append(
                        JOURNAL_KIND_REEVALUATION_NOOP,
                        parent_sequence=self.journal.sequence - 1,
                        operation_id=f"reevaluation-noop-{boundary.isoformat()}",
                        correlation_id=f"reevaluation-{boundary.isoformat()}",
                        epoch_sequence=new_epoch.sequence,
                    )
                else:
                    await self._activate(new_epoch)
                next_reevaluation += self.reevaluation_interval
        if self.event_count == 0:
            raise LiveEvidenceFailure("NO_MARKET_EVENTS_RECEIVED")
        self._append(
            JOURNAL_KIND_INTAKE_STOPPED,
            parent_sequence=self.journal.sequence - 1,
            operation_id="intake-stop",
            correlation_id="shutdown-chain",
            boundary=self.boundaries.closes_at.isoformat(),
            event_count=self.event_count,
            host_observed_at=self.clock.now().isoformat(),
        )
        if equity_task is not None:
            equity_task.cancel()
            await asyncio.gather(equity_task, return_exceptions=True)
        await self.provider.close()
        if self.mathematical_pipeline is not None:
            await self.mathematical_pipeline.drain_shadow()
        with self._processing_lock:
            self._flush_math_evidence()
        self.repository.flush()
        self._append(
            JOURNAL_KIND_PERSISTENCE_DRAINED,
            parent_sequence=self.journal.sequence - 1,
            operation_id="persistence-drain",
            correlation_id="shutdown-chain",
            accepted_events=self.event_count,
            processed_events=self.processed_count,
            durable_repository_identity=self.repository.durable_identity(self.run_id),
            queue_depth_final=0,
        )
        self._append(
            JOURNAL_KIND_SESSION_FINALIZED,
            parent_sequence=self.journal.sequence - 1,
            operation_id="session-finalize",
            correlation_id="shutdown-chain",
            epoch_count=self.epoch_count,
            event_count=self.event_count,
            processed_event_count=self.processed_count,
            disconnect_count=self.disconnect_count,
            trading_enabled=False,
            orders_constructed=0,
            orders_submitted=0,
            impact_mode=IMPACT_MODE,
        )
        self.journal.seal()
        self.writer.checkpoint(self.journal)
        return _publish_completed_bundle(self)


def _contract_identity(contract: ThetaContract) -> str:
    return f"{contract.root}:{contract.expiration}:{contract.strike}:{contract.right}"


def _project_state(journal: Journal) -> dict[str, object]:
    active_membership: list[str] = []
    epochs: list[dict[str, object]] = []
    accepted = 0
    rejected = 0
    processed = 0
    disconnects = 0
    reconnects = 0
    restorations = 0
    additions = 0
    removals = 0
    for record in journal.records:
        if record.kind == JOURNAL_KIND_EPOCH_ACTIVATED:
            active_membership = list(cast(list[str], record.payload.get("membership", [])))
            epochs.append(
                {
                    "sequence": record.epoch_sequence,
                    "epoch_id": record.epoch_id,
                    "membership": active_membership,
                    "content_hash": record.payload.get("content_hash"),
                }
            )
        elif record.kind == JOURNAL_KIND_SUBSCRIPTION_COMMAND:
            additions += int(record.payload.get("action") == "add")
            removals += int(record.payload.get("action") == "remove")
        elif record.kind == JOURNAL_KIND_EVENT_ACCEPTED:
            accepted += 1
            processed += int(record.payload.get("processed") is True)
        elif record.kind == JOURNAL_KIND_EVENT_REJECTED:
            rejected += 1
        elif record.kind == JOURNAL_KIND_PROVIDER_DISCONNECTED:
            disconnects += 1
        elif record.kind == JOURNAL_KIND_PROVIDER_RECONNECTED:
            reconnects += 1
        elif record.kind == JOURNAL_KIND_EPOCH_RESTORED:
            restorations += 1
    final = journal.records[-1].payload
    return {
        "run_id": str(journal.run_id),
        "epochs": epochs,
        "active_membership": active_membership,
        "accepted_events": accepted,
        "rejected_events": rejected,
        "processed_events": processed,
        "subscription_add_commands": additions,
        "subscription_remove_commands": removals,
        "disconnects": disconnects,
        "reconnects": reconnects,
        "restorations": restorations,
        "trading_enabled": final.get("trading_enabled"),
        "orders_constructed": final.get("orders_constructed"),
        "orders_submitted": final.get("orders_submitted"),
        "impact_mode": final.get("impact_mode"),
        "finalized": journal.records[-1].kind == JOURNAL_KIND_SESSION_FINALIZED,
    }


def _verify_live_journal(journal: Journal, identity: SessionIdentity) -> dict[str, object]:
    try:
        verify_structural(journal)
    except JournalFailure as exc:
        raise LiveEvidenceFailure(exc.code, exc.detail) from exc
    if journal.records[-1].kind != JOURNAL_KIND_SESSION_FINALIZED:
        raise LiveEvidenceFailure("INCOMPLETE_SHUTDOWN")
    required = (
        JOURNAL_KIND_SESSION_START,
        JOURNAL_KIND_CONFIGURATION,
        JOURNAL_KIND_PROVIDER_READY,
        JOURNAL_KIND_INTAKE_STOPPED,
        JOURNAL_KIND_PERSISTENCE_DRAINED,
        JOURNAL_KIND_SESSION_FINALIZED,
    )
    for kind in required:
        if len(journal.records_by_kind(kind)) != 1:
            raise LiveEvidenceFailure("REQUIRED_RECORD_CARDINALITY", kind)
    start = journal.records_by_kind(JOURNAL_KIND_SESSION_START)[0]
    configuration = journal.records_by_kind(JOURNAL_KIND_CONFIGURATION)[0]
    for key, value in identity.as_dict().items():
        if start.payload.get(key) != value or configuration.payload.get(key) != value:
            raise LiveEvidenceFailure("CODE_CONFIG_SCHEMA_IDENTITY_MISMATCH", key)
    stop = journal.records_by_kind(JOURNAL_KIND_INTAKE_STOPPED)[0]
    if stop.payload.get("boundary") != start.payload.get("closes_at"):
        raise LiveEvidenceFailure("INTAKE_STOP_BOUNDARY_INVALID")
    commands = {record.command_id: record for record in journal.subscription_commands()}
    acknowledgements = journal.subscription_acks()
    if not commands or len(acknowledgements) != len(commands):
        raise LiveEvidenceFailure("ACKNOWLEDGEMENT_HISTORY_INCOMPLETE")
    seen_ack: set[str] = set()
    for acknowledgement in acknowledgements:
        if acknowledgement.command_id not in commands:
            raise LiveEvidenceFailure("UNMATCHED_ACKNOWLEDGEMENT")
        if cast(str, acknowledgement.command_id) in seen_ack:
            raise LiveEvidenceFailure("DUPLICATE_ACKNOWLEDGEMENT")
        seen_ack.add(cast(str, acknowledgement.command_id))
        if acknowledgement.payload.get("accepted") is not True:
            raise LiveEvidenceFailure("ACKNOWLEDGEMENT_REJECTED")
        if acknowledgement.parent_sequence != commands[acknowledgement.command_id].sequence:
            raise LiveEvidenceFailure("ACKNOWLEDGEMENT_CAUSAL_PARENT_INVALID")
    disconnects = len(journal.records_by_kind(JOURNAL_KIND_PROVIDER_DISCONNECTED))
    if disconnects != len(
        journal.records_by_kind(JOURNAL_KIND_PROVIDER_RECONNECTED)
    ) or disconnects != len(journal.records_by_kind(JOURNAL_KIND_EPOCH_RESTORED)):
        raise LiveEvidenceFailure("RECOVERY_EVIDENCE_INCOMPLETE")
    projection = _project_state(journal)
    if (
        projection["accepted_events"] == 0
        or projection["processed_events"] != projection["accepted_events"]
    ):
        raise LiveEvidenceFailure("EVENT_PROCESSING_INCOMPLETE")
    if (
        projection["trading_enabled"] is not False
        or projection["orders_constructed"] != 0
        or projection["orders_submitted"] != 0
    ):
        raise LiveEvidenceFailure("ORDER_SAFETY_VIOLATION")
    if projection["impact_mode"] != IMPACT_MODE:
        raise LiveEvidenceFailure("IMPACT_MODE_INVALID")
    return projection


def _persistence_receipt(
    journal: Journal, persistence_identity: str, repository_identity: str
) -> dict[str, object]:
    assert journal.seal_value is not None
    commitment: dict[str, object] = {
        "receipt_version": "LIVE_PERSISTENCE_RECEIPT_V1",
        "run_id": str(journal.run_id),
        "journal_record_count": journal.seal_value.record_count,
        "journal_root_hash": journal.seal_value.terminal_digest,
        "journal_seal_hash": journal.seal_value.seal_digest,
        "journal_byte_sha256": sha256_bytes(journal.serialize()),
        "persistence_identity": persistence_identity,
        "observation_repository_identity": repository_identity,
        "persistence_completed": True,
    }
    return {**commitment, "receipt_sha256": sha256(commitment)}


def _replay_receipt(
    journal: Journal, persistence: Mapping[str, object], recorded: Mapping[str, object]
) -> dict[str, object]:
    replayed = _project_state(journal)
    commitment: dict[str, object] = {
        "receipt_version": "LIVE_REPLAY_RECEIPT_V1",
        "run_id": str(journal.run_id),
        "persistence_receipt_sha256": persistence["receipt_sha256"],
        "journal_byte_sha256": sha256_bytes(journal.serialize()),
        "recorded_state_sha256": sha256(dict(recorded)),
        "replayed_state_sha256": sha256(replayed),
        "exact_equality": replayed == dict(recorded),
    }
    return {**commitment, "receipt_sha256": sha256(commitment)}


def _publish_completed_bundle(engine: LiveSessionEngine) -> dict[str, object]:
    projection = _verify_live_journal(engine.journal, engine.identity)
    serialized = engine.journal.serialize()
    persistence_identity = engine.journal_repository.save(engine.run_id, serialized)
    reloaded = engine.journal_repository.load(persistence_identity)
    if reloaded != serialized:
        raise LiveEvidenceFailure("PERSISTENCE_RELOAD_MISMATCH")
    persistence = _persistence_receipt(
        engine.journal, persistence_identity, engine.repository.durable_identity(engine.run_id)
    )
    replay = _replay_receipt(engine.journal, persistence, projection)
    if replay["exact_equality"] is not True:
        raise LiveEvidenceFailure("REPLAY_EQUALITY_FAILED")
    components = {
        JOURNAL_NAME: engine.writer.publish_component(JOURNAL_NAME, serialized),
        CONFIGURATION_NAME: engine.writer.publish_component(
            CONFIGURATION_NAME, engine.settings.public_snapshot()
        ),
        FINAL_STATE_NAME: engine.writer.publish_component(FINAL_STATE_NAME, projection),
        PERSISTENCE_RECEIPT_NAME: engine.writer.publish_component(
            PERSISTENCE_RECEIPT_NAME, persistence
        ),
        REPLAY_RECEIPT_NAME: engine.writer.publish_component(REPLAY_RECEIPT_NAME, replay),
    }
    if engine.mathematical_pipeline is not None:
        mathematical = engine.mathematical_pipeline.snapshot()
        persisted_events = engine.repository.replay_events(engine.run_id)
        replayed = replay_semantics(
            persisted_events,
            engine.settings,
            engine.mathematical_pipeline.symbols,
            engine.run_id,
        )
        replay_fields = (
            "decisions",
            "feature_vectors",
            "comparisons",
            "control_evaluation_records",
        )
        replay_equal = all(
            mathematical.get(field) == replayed.get(field) for field in replay_fields
        )
        mathematical["fresh_process_replay_equal"] = replay_equal
        mathematical["fresh_process_replay"] = replayed
        if not replay_equal:
            raise LiveEvidenceFailure("MATHEMATICAL_REPLAY_EQUALITY_FAILED")
        scientific_records: list[dict[str, object]] = []
        for record in engine.journal.records:
            direct = record.as_dict().get("scientific_evidence")
            if isinstance(direct, list):
                for entry in direct:
                    if isinstance(entry, dict):
                        scientific_records.append(
                            {
                                "kind": entry.get("kind"),
                                "scientific_evidence": entry.get("payload", {}),
                                "event_sequence": record.sequence,
                            }
                        )
            if record.kind == JOURNAL_KIND_SCIENTIFIC_EVIDENCE_BATCH:
                batch = record.as_dict().get("scientific_evidence", {})
                entries = batch.get("records", []) if isinstance(batch, dict) else []
                if isinstance(entries, list):
                    for entry in entries:
                        if not isinstance(entry, dict):
                            continue
                        scientific_records.append(
                            {
                                "kind": entry.get("kind"),
                                "scientific_evidence": entry.get("payload", {}),
                                "batch_sequence": record.sequence,
                            }
                        )
            elif record.kind in {
                "cluster.completed",
                "control.evaluated",
                "feature_vector.persisted",
                "shadow.enqueued",
                "shadow.completed",
                "comparison.completed",
            }:
                scientific_records.append(record.as_dict())
        scientific_identity_fields = {
            "cluster.completed": "cluster_evidence_id",
            "control.evaluated": "evaluation_id",
            "feature_vector.persisted": "feature_vector_id",
            "shadow.enqueued": "shadow_evaluation_id",
            "shadow.completed": "shadow_evaluation_id",
            "comparison.completed": "comparison_id",
        }
        scientific_ids = [
            cast(Mapping[str, object], record.get("scientific_evidence", {})).get(
                scientific_identity_fields[str(record.get("kind"))]
            )
            for record in scientific_records
            if str(record.get("kind")) in scientific_identity_fields
        ]
        scientific_ids = [str(value) for value in scientific_ids if value is not None]
        scientific_replay = {
            "schema_version": "SCIENTIFIC_REPLAY_V1",
            "run_id": str(engine.run_id),
            "accepted_input_event_ids": [str(event.event_id) for event in persisted_events],
            "scientific_record_count": len(scientific_records),
            "scientific_records": scientific_records,
            "fresh_process_replay_equal": replay_equal,
            "model_replay_sha256": sha256({field: replayed[field] for field in replay_fields}),
            "live_model_sha256": sha256({field: mathematical[field] for field in replay_fields}),
            "duplicate_scientific_records": len(scientific_ids) != len(set(scientific_ids)),
            "scientific_identity_count": len(scientific_ids),
            "scientific_identity_unique_count": len(set(scientific_ids)),
        }
        components["SCIENTIFIC_REPLAY.json"] = engine.writer.publish_component(
            "SCIENTIFIC_REPLAY.json", scientific_replay
        )
        components["MATHEMATICAL_PIPELINE.json"] = engine.writer.publish_component(
            "MATHEMATICAL_PIPELINE.json", mathematical
        )
        components["SHARED_FEATURE_VECTORS.json"] = engine.writer.publish_component(
            "SHARED_FEATURE_VECTORS.json",
            {"version": "SHARED_FEATURE_VECTOR_V1", "vectors": mathematical["feature_vectors"]},
        )
        components["MODEL_COMPARISON.json"] = engine.writer.publish_component(
            "MODEL_COMPARISON.json",
            {"version": "CONTROL_SHADOW_COMPARISON_V1", "comparisons": mathematical["comparisons"]},
        )
    assert engine.journal.seal_value is not None
    commitment: dict[str, object] = {
        "manifest_version": MANIFEST_VERSION,
        "evidence_kind": "LIVE_OBSERVATIONAL_SESSION_EVIDENCE_BUNDLE",
        "status": "complete",
        "session_id": str(engine.run_id),
        "identity": engine.identity.as_dict(),
        "session": engine.boundaries.as_dict(),
        "provider_identity": dict(engine.provider_identity),
        "journal_record_count": engine.journal.seal_value.record_count,
        "journal_root_hash": engine.journal.seal_value.terminal_digest,
        "journal_seal_hash": engine.journal.seal_value.seal_digest,
        "components": components,
        "trading_enabled": False,
        "orders_constructed": 0,
        "orders_submitted": 0,
        "impact_mode": IMPACT_MODE,
    }
    manifest = {
        **commitment,
        "manifest_sha256": sha256(commitment),
        "authority_tag": _authority_tag(engine.authority_key, commitment),
    }
    engine.writer.publish_manifest(manifest)
    return manifest


@dataclass(frozen=True)
class LoadedBundle:
    directory: Path
    manifest: dict[str, object]
    journal: Journal
    configuration: dict[str, object]
    final_state: dict[str, object]
    persistence_receipt: dict[str, object]
    replay_receipt: dict[str, object]


def load_and_verify_bundle(
    bundle_directory: Path,
    *,
    authority_key: bytes,
    repository_root: Path,
) -> LoadedBundle:
    if bundle_directory.is_symlink() or not bundle_directory.is_dir():
        raise LiveEvidenceFailure("BUNDLE_DIRECTORY_INVALID")
    if (bundle_directory / INCOMPLETE_NAME).exists():
        raise LiveEvidenceFailure("BUNDLE_INCOMPLETE")
    try:
        manifest = cast(
            dict[str, object], json.loads((bundle_directory / MANIFEST_NAME).read_text())
        )
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        raise LiveEvidenceFailure("MANIFEST_MISSING_OR_INVALID") from exc
    authority_tag = manifest.pop("authority_tag", None)
    manifest_sha256 = manifest.pop("manifest_sha256", None)
    commitment = dict(manifest)
    if manifest_sha256 != sha256(commitment):
        raise LiveEvidenceFailure("MANIFEST_DIGEST_MISMATCH")
    if not isinstance(authority_tag, str) or not hmac.compare_digest(
        authority_tag, _authority_tag(authority_key, commitment)
    ):
        raise LiveEvidenceFailure("TRUSTED_BINDING_MISMATCH")
    manifest["manifest_sha256"] = manifest_sha256
    manifest["authority_tag"] = authority_tag
    if manifest.get("status") != "complete" or manifest.get("evidence_kind") != (
        "LIVE_OBSERVATIONAL_SESSION_EVIDENCE_BUNDLE"
    ):
        raise LiveEvidenceFailure("MANIFEST_NOT_COMPLETE")
    identity_raw = cast(dict[str, object], manifest.get("identity"))
    identity = SessionIdentity(
        git_commit=str(identity_raw["git_commit"]),
        application_version=str(identity_raw["application_version"]),
        config_version=str(identity_raw["config_version"]),
        journal_schema_version=str(identity_raw["journal_schema_version"]),
        rules_sha256=str(identity_raw["rules_sha256"]),
        configuration_sha256=str(identity_raw["configuration_sha256"]),
    )
    if identity.git_commit != _git_head(repository_root):
        raise LiveEvidenceFailure("GIT_COMMIT_MISMATCH")
    if identity.rules_sha256 != _digest_bytes(_rules_bytes()):
        raise LiveEvidenceFailure("VALIDATION_RULES_MISMATCH")
    components = cast(dict[str, str], manifest.get("components"))

    def component(name: str) -> str:
        try:
            content = (bundle_directory / name).read_text(encoding="utf-8")
        except FileNotFoundError as exc:
            raise LiveEvidenceFailure("BUNDLE_COMPONENT_MISSING", name) from exc
        if components.get(name) != sha256_bytes(content):
            raise LiveEvidenceFailure("BUNDLE_COMPONENT_DIGEST_MISMATCH", name)
        return content

    journal = Journal.deserialize(component(JOURNAL_NAME))
    configuration = cast(dict[str, object], json.loads(component(CONFIGURATION_NAME)))
    final_state = cast(dict[str, object], json.loads(component(FINAL_STATE_NAME)))
    persistence = cast(dict[str, object], json.loads(component(PERSISTENCE_RECEIPT_NAME)))
    replay = cast(dict[str, object], json.loads(component(REPLAY_RECEIPT_NAME)))
    projection = _verify_live_journal(journal, identity)
    rules = cast(dict[str, object], json.loads(_rules_bytes()))
    if (
        identity.application_version != rules.get("application_version")
        or identity.journal_schema_version != rules.get("journal_schema_version")
        or identity.configuration_sha256 != sha256(configuration)
    ):
        raise LiveEvidenceFailure("CODE_CONFIG_SCHEMA_IDENTITY_MISMATCH")
    if projection != final_state:
        raise LiveEvidenceFailure("RECORDED_FINAL_STATE_MISMATCH")
    seal = journal.seal_value
    if seal is None or manifest.get("journal_record_count") != seal.record_count:
        raise LiveEvidenceFailure("MANIFEST_JOURNAL_COUNT_MISMATCH")
    if (
        manifest.get("journal_root_hash") != seal.terminal_digest
        or manifest.get("journal_seal_hash") != seal.seal_digest
    ):
        raise LiveEvidenceFailure("MANIFEST_JOURNAL_SEAL_MISMATCH")
    provider_ready = journal.records_by_kind(JOURNAL_KIND_PROVIDER_READY)[0]
    if provider_ready.payload.get("provider_identity") != manifest.get("provider_identity"):
        raise LiveEvidenceFailure("PROVIDER_IDENTITY_MISMATCH")
    clock_records = journal.records_by_kind(JOURNAL_KIND_CLOCK_ADVANCED)
    if not clock_records or any(
        not isinstance(record.payload.get("host_observed_at"), str) for record in clock_records
    ):
        raise LiveEvidenceFailure("HOST_CLOCK_EVIDENCE_MISSING")
    if persistence.get("receipt_sha256") != sha256(
        {key: value for key, value in persistence.items() if key != "receipt_sha256"}
    ):
        raise LiveEvidenceFailure("PERSISTENCE_RECEIPT_INVALID")
    expected_identity = persistence_object_identity(journal.run_id, journal.serialize())
    if persistence.get("persistence_identity") != expected_identity:
        raise LiveEvidenceFailure("PERSISTENCE_RECEIPT_BINDING_MISMATCH")
    reproduced = _replay_receipt(journal, persistence, final_state)
    if replay != reproduced or replay.get("exact_equality") is not True:
        raise LiveEvidenceFailure("REPLAY_RECEIPT_INVALID")
    if str(journal.run_id) != manifest.get("session_id"):
        raise LiveEvidenceFailure("MIXED_SESSION_ARTIFACTS")
    if (
        manifest.get("trading_enabled") is not False
        or manifest.get("orders_constructed") != 0
        or manifest.get("orders_submitted") != 0
    ):
        raise LiveEvidenceFailure("ORDER_SAFETY_VIOLATION")
    return LoadedBundle(
        bundle_directory,
        manifest,
        journal,
        configuration,
        final_state,
        persistence,
        replay,
    )


def replay_live_bundle(
    bundle_directory: Path,
    output: Path,
    *,
    authority_key: bytes,
    repository_root: Path,
) -> dict[str, object]:
    _validate_fresh_file(output)
    loaded = load_and_verify_bundle(
        bundle_directory, authority_key=authority_key, repository_root=repository_root
    )
    receipt = _replay_receipt(loaded.journal, loaded.persistence_receipt, loaded.final_state) | {
        "source_manifest_sha256": loaded.manifest["manifest_sha256"],
        "verification_status": "OFFLINE_REPLAY_VERIFIED",
    }
    _atomic_write(output, canonical_json(receipt))
    return receipt


def certify_live_bundle(
    bundle_directory: Path,
    certificate_output: Path,
    replay_output: Path,
    *,
    authority_key: bytes,
    repository_root: Path,
) -> dict[str, object]:
    if certificate_output == replay_output:
        raise LiveEvidenceFailure("OUTPUT_TARGET_COLLISION")
    _validate_fresh_file(certificate_output)
    replay = replay_live_bundle(
        bundle_directory,
        replay_output,
        authority_key=authority_key,
        repository_root=repository_root,
    )
    loaded = load_and_verify_bundle(
        bundle_directory, authority_key=authority_key, repository_root=repository_root
    )
    certificate: dict[str, object] = {
        "certificate_version": CERTIFICATE_VERSION,
        "evidence_kind": "LIVE_OBSERVATIONAL_SESSION_CERTIFICATE",
        "statement": "Validated one persisted observational session offline.",
        "deterministic_test_scenario_ran": False,
        "session_id": str(loaded.journal.run_id),
        "identity": loaded.manifest["identity"],
        "session": loaded.manifest["session"],
        "provider_identity": loaded.manifest["provider_identity"],
        "journal_record_count": loaded.manifest["journal_record_count"],
        "journal_root_hash": loaded.manifest["journal_root_hash"],
        "journal_seal_hash": loaded.manifest["journal_seal_hash"],
        "persistence_receipt_sha256": loaded.persistence_receipt["receipt_sha256"],
        "replay_receipt_sha256": replay["receipt_sha256"],
        "replay_exact_equality": True,
        "final_state": loaded.final_state,
        "trading_enabled": False,
        "orders_constructed": 0,
        "orders_submitted": 0,
        "impact_mode": IMPACT_MODE,
        "overall": "VALIDATED",
    }
    certificate["certificate_sha256"] = sha256(certificate)
    _atomic_write(certificate_output, canonical_json(certificate))
    return certificate


MAX_PROVIDER_MESSAGE_BYTES = 8 * 1024 * 1024
MESSAGE_PARSE_TIMEOUT_SECONDS = 5.0


class UnifiedThetaSession:
    """One Theta WebSocket for commands, acknowledgements, and market events."""

    request_types: tuple[str, ...] = REQUIRED_CHANNELS
    _lifecycle_lock: ClassVar[asyncio.Lock | None] = None
    _active_owner: ClassVar[UnifiedThetaSession | None] = None

    def __init__(self, events_url: str, api_key: str, timeout: float = 10.0) -> None:
        self.events_url = events_url
        self.api_key = api_key
        self.timeout = timeout
        self.connection_generation = 0
        self._next_request_id = 1
        self._ws: Any = None
        self._reader_task: asyncio.Task[None] | None = None
        self._decoder_task: asyncio.Task[None] | None = None
        # The reader must never stop calling recv because downstream durable
        # processing is slower than the provider.  This lossless queue lets
        # the single socket drain continuously; frames are processed in order
        # by the serialized event-processing path.
        self._raw_inbound: asyncio.Queue[str | bytes | None] = asyncio.Queue()
        self._inbound: asyncio.Queue[InboundFrame] = asyncio.Queue()
        self._requests: dict[int, SubscriptionRequest] = {}
        self._normalizer = ThetaDataOptionsProvider(events_url, api_key, contracts=())

    @classmethod
    def _owner_lock(cls) -> asyncio.Lock:
        if cls._lifecycle_lock is None:
            cls._lifecycle_lock = asyncio.Lock()
        return cls._lifecycle_lock

    async def _connect_unlocked(self) -> None:
        if self._ws is not None:
            raise LiveEvidenceFailure("PROVIDER_ALREADY_CONNECTED")
        if self._active_owner is not None and self._active_owner is not self:
            raise LiveEvidenceFailure("THETA_EVENTS_CONNECTION_ALREADY_OWNED")
        try:
            websocket = await websockets.connect(
                self.events_url,
                open_timeout=self.timeout,
                ping_interval=20,
                ping_timeout=10,
            )
        except (OSError, TimeoutError) as exc:
            raise ProviderError("thetadata", "connection_failed", True) from exc
        self._ws = websocket
        type(self)._active_owner = self
        self.connection_generation += 1
        self._requests.clear()
        self._normalizer.connected = False
        self._reader_task = asyncio.create_task(self._read_loop(websocket))
        self._decoder_task = asyncio.create_task(self._decode_loop())
        # Theta keeps stream subscriptions across its FPSS reconnects.  A new
        # local consumer must reset any streams left by an interrupted prior
        # run before rebuilding its deterministic subscription set.
        await websocket.send(json.dumps({"msg_type": "STOP"}))

    async def connect(self) -> None:
        async with self._owner_lock():
            await self._connect_unlocked()

    async def _close_unlocked(self) -> None:
        websocket = self._ws
        if websocket is None:
            return
        reader = self._reader_task
        decoder = self._decoder_task
        try:
            try:
                await websocket.send(json.dumps({"msg_type": "STOP"}))
            except (OSError, RuntimeError, websockets.ConnectionClosed):
                pass
            await websocket.close()
            await websocket.wait_closed()
        finally:
            if reader is not None and not reader.done():
                reader.cancel()
                with suppress(asyncio.CancelledError):
                    await reader
            if decoder is not None and not decoder.done():
                decoder.cancel()
                with suppress(asyncio.CancelledError):
                    await decoder
            self._reader_task = None
            self._decoder_task = None
            while not self._raw_inbound.empty():
                with suppress(asyncio.QueueEmpty):
                    self._raw_inbound.get_nowait()
            while not self._inbound.empty():
                with suppress(asyncio.QueueEmpty):
                    self._inbound.get_nowait()
            self._ws = None
            if type(self)._active_owner is self:
                type(self)._active_owner = None

    async def close(self) -> None:
        async with self._owner_lock():
            await self._close_unlocked()

    async def reconnect(self) -> None:
        """Replace the local socket without overlapping its successor."""
        async with self._owner_lock():
            await self._close_unlocked()
            await self._connect_unlocked()

    async def health(self) -> dict[str, object]:
        return {
            "status": (
                "healthy"
                if self._ws is not None
                and self._reader_task is not None
                and not self._reader_task.done()
                and self._decoder_task is not None
                and not self._decoder_task.done()
                else "unavailable"
            ),
            "provider": "thetadata",
            "events_url": self.events_url,
            "configured": bool(self.api_key),
        }

    def prepare_request(
        self, action: Literal["add", "remove"], contract: ThetaContract, channel: str
    ) -> SubscriptionRequest:
        if self._ws is None:
            raise LiveEvidenceFailure("PROVIDER_NOT_CONNECTED")
        if channel not in REQUIRED_CHANNELS:
            raise LiveEvidenceFailure("SUBSCRIPTION_CHANNEL_INVALID")
        request = SubscriptionRequest(
            self._next_request_id,
            contract,
            channel,
            action == "add",
            self.connection_generation,
        )
        self._next_request_id += 1
        self._requests[request.request_id] = request
        return request

    async def transmit(self, request: SubscriptionRequest) -> None:
        if self._ws is None or request.generation != self.connection_generation:
            raise LiveEvidenceFailure("SUBSCRIPTION_REQUEST_STALE_GENERATION")
        await self._ws.send(
            json.dumps(
                request.contract.payload(
                    request.request_id, add=request.add, req_type=request.req_type
                )
            )
        )

    async def _decode_message(self, raw: str | bytes) -> InboundFrame | None:
        if isinstance(raw, (str, bytes)) and len(raw) > MAX_PROVIDER_MESSAGE_BYTES:
            return InboundFrame("malformed", datetime.now(ET), detail="message_too_large")
        try:
            message = cast(
                dict[str, Any],
                await asyncio.wait_for(
                    asyncio.to_thread(json.loads, raw),
                    timeout=MESSAGE_PARSE_TIMEOUT_SECONDS,
                ),
            )
            header = cast(dict[str, Any], message.get("header", {}))
        except TimeoutError:
            return InboundFrame("malformed", datetime.now(ET), detail="json_parse_timeout")
        except (json.JSONDecodeError, TypeError):
            return InboundFrame("malformed", datetime.now(ET), detail="invalid_json")
        message_type = header.get("type")
        if message_type == "REQ_RESPONSE":
            request_id = header.get("req_id")
            if not isinstance(request_id, int):
                return InboundFrame("malformed", datetime.now(ET), detail="missing_request_id")
            return InboundFrame(
                "ack",
                datetime.now(ET),
                request_id=request_id,
                response=str(header.get("response", "")).upper(),
            )
        if message_type in {"ERROR"} or header.get("status") in {
            "ERROR",
            "UNAUTHORIZED",
            "DENIED",
        }:
            return InboundFrame("terminated", datetime.now(ET), detail="provider_error")
        if message_type in {"STATUS", "OHLC"} or (
            header.get("status") == "CONNECTED" and message_type not in {"TRADE", "QUOTE"}
        ):
            return None
        try:
            # Preserve valid market frames that arrive before their correlated
            # subscription acknowledgement; the engine journals them as rejected
            # until activation. The provider normalizer otherwise filters them.
            acknowledged = self._normalizer.acknowledged_contracts
            self._normalizer.acknowledged_contracts = set()
            try:
                event = self._normalizer._normalize(message)
            finally:
                self._normalizer.acknowledged_contracts = acknowledged
        except (KeyError, TypeError, ValueError) as exc:
            return InboundFrame("malformed", datetime.now(ET), detail=type(exc).__name__)
        if event is None:
            return InboundFrame(
                "malformed",
                datetime.now(ET),
                detail=f"unknown_message:{message_type}:{header.get('status', '')}",
            )
        return InboundFrame("event", datetime.now(ET), event=event)

    async def _read_loop(self, websocket: Any) -> None:
        """Continuously drain the one owned socket into a raw lossless queue."""
        while self._ws is websocket:
            try:
                raw = await websocket.recv()
            except websockets.ConnectionClosed:
                await self._raw_inbound.put(None)
                return
            except (OSError, RuntimeError):
                await self._raw_inbound.put(None)
                return
            await self._raw_inbound.put(raw)

    async def _decode_loop(self) -> None:
        """Decode raw frames in order without delaying WebSocket recv."""
        while True:
            raw = await self._raw_inbound.get()
            if raw is None:
                await self._inbound.put(
                    InboundFrame("disconnect", datetime.now(ET), detail="socket_closed")
                )
                return
            frame = await self._decode_message(raw)
            if frame is not None:
                await self._inbound.put(frame)

    async def receive_until(self, boundary: datetime) -> InboundFrame:
        if self._ws is None:
            return InboundFrame("disconnect", datetime.now(ET), detail="socket_not_connected")
        timeout = max(0.0, (boundary - datetime.now(ET)).total_seconds())
        try:
            return await asyncio.wait_for(self._inbound.get(), timeout=timeout)
        except TimeoutError:
            return InboundFrame("clock", boundary)


def build_identity(
    settings: Settings, expected_git_commit: str, repository_root: Path
) -> SessionIdentity:
    actual = _git_head(repository_root)
    if actual != expected_git_commit:
        raise LiveEvidenceFailure("EXPECTED_GIT_COMMIT_MISMATCH")
    public_configuration = settings.public_snapshot()
    return SessionIdentity(
        git_commit=actual,
        application_version=settings.engine_version,
        config_version=settings.config_version,
        journal_schema_version=JOURNAL_SCHEMA_VERSION,
        rules_sha256=_digest_bytes(_rules_bytes()),
        configuration_sha256=sha256(public_configuration),
    )


def validate_environment(
    settings: Settings,
    output_directory: Path,
    expected_git_commit: str,
    repository_root: Path,
) -> tuple[SessionIdentity, bytes, PostgresObservationRepository, FileJournalRepository]:
    if settings.trading_enabled:
        raise LiveEvidenceFailure("TRADING_ENABLED")
    _validate_fresh_directory(output_directory)
    if settings.database_url is None:
        raise LiveEvidenceFailure("DURABLE_REPOSITORY_CONFIGURATION_REQUIRED")
    if settings.live_journal_directory is None:
        raise LiveEvidenceFailure("DURABLE_JOURNAL_CONFIGURATION_REQUIRED")
    journal_directory = settings.live_journal_directory
    if journal_directory.is_symlink():
        raise LiveEvidenceFailure("DURABLE_JOURNAL_DIRECTORY_INVALID")
    journal_directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    probe = journal_directory / f".preflight-{uuid4()}"
    _atomic_write(probe, "preflight")
    probe.unlink()
    key = _load_authority_key(settings.live_evidence_authority_key_file)
    repository = PostgresObservationRepository(PostgresRepository(settings.database_url))
    repository.preflight()
    identity = build_identity(settings, expected_git_commit, repository_root)
    return identity, key, repository, FileJournalRepository(journal_directory)


def _load_settings() -> Settings:
    runtime_file = os.environ.get(
        "RUNTIME_ENV_FILE", "/etc/institutional-signal-engine/runtime.env"
    )
    return (
        Settings.from_env_file(runtime_file) if Path(runtime_file).exists() else Settings.from_env()
    )


def _production_ports(settings: Settings) -> tuple[DiscoveryPort, EnrichmentPort]:
    from .phase4_live_smoke import _DiscoveryAdapter, _EnrichmentAdapter

    return _DiscoveryAdapter(settings), cast(EnrichmentPort, _EnrichmentAdapter(settings))


async def _run_production(arguments: argparse.Namespace) -> dict[str, object]:
    settings = _load_settings()
    repository_root = arguments.repository_root.resolve()
    identity, authority_key, repository, journal_repository = await asyncio.to_thread(
        validate_environment,
        settings,
        arguments.session_output,
        arguments.expected_git_commit,
        repository_root,
    )
    boundaries = SessionBoundaries.for_market_date(date.fromisoformat(arguments.market_date))
    alpaca = AlpacaEquitiesProvider(
        settings.alpaca_data_url,
        _secret(settings.alpaca_key_id),
        _secret(settings.alpaca_secret_key),
        timeout=30.0,
    )
    bootstrap_started = datetime.now(UTC)
    bootstrap_task = asyncio.create_task(
        asyncio.to_thread(
            lambda: asyncio.run(
                alpaca.historical_bootstrap(
                    (*PILOT_SYMBOLS, "SPY", "XLK"),
                    date.fromisoformat(arguments.market_date),
                )
            )
        )
    )
    discovery, enrichment = _production_ports(settings)
    provider = UnifiedThetaSession(
        settings.theta_events_url,
        settings.theta_api_key.get_secret_value() if settings.theta_api_key else "",
    )
    writer = BundleWriter(arguments.session_output)
    engine = LiveSessionEngine(
        run_id=uuid4(),
        identity=identity,
        boundaries=boundaries,
        settings=settings,
        clock=RealSessionClock(),
        discovery=discovery,
        enrichment=enrichment,
        planner=ProductionPlanner(
            trade_limit=settings.phase4_trade_subscription_limit,
            quote_limit=settings.phase4_quote_subscription_limit,
            max_contracts_per_symbol=settings.phase4_max_contracts_per_symbol,
        ),
        provider=provider,
        repository=repository,
        writer=writer,
        journal_repository=journal_repository,
        authority_key=authority_key,
        # The 501-symbol historical bootstrap runs concurrently. The live
        # observer may activate with an empty initial baseline set; once the
        # immutable bootstrap completes, later epochs receive its symbols and
        # the persisted status binds offline shadow scoring to this run.
        baseline_symbols=frozenset(),
        acknowledgement_timeout=timedelta(seconds=900),
        provider_identity={
            "discovery": "alpaca-options-contracts",
            "enrichment": "alpaca-option-snapshots",
            "events": "thetadata-terminal-standard",
        },
        equity_provider=alpaca,
        equity_symbols=tuple(PILOT_SYMBOLS),
    )
    engine.mathematical_pipeline = MathematicalPipeline(
        settings,
        repository,
        engine.run_id,
        PILOT_SYMBOLS,
        async_shadow=True,
        shadow_work_item_sink=repository.record_shadow_work_item,
        result_observer=engine._record_side_b_math_outputs,
        evidence_sink=engine._record_math_evidence,
    )

    async def finish_bootstrap() -> object | None:
        try:
            historical = await bootstrap_task
            baseline_symbols = frozenset(
                str(key[0]).upper()
                for key in historical.impact_baselines
                if isinstance(key, tuple) and len(key) == 2
            )
            engine.baseline_symbols = baseline_symbols
            if engine.mathematical_pipeline is not None:
                for key, baseline in historical.impact_baselines.items():
                    engine.mathematical_pipeline.impact_engine.baselines[key] = baseline
            status = {
                "status": "COMPLETE"
                if len(baseline_symbols) == len(PILOT_SYMBOLS)
                else "INCOMPLETE_BASELINE_COVERAGE",
                "started_at": bootstrap_started.isoformat(),
                "finished_at": datetime.now(UTC).isoformat(),
                "candidate_symbol_count": len(PILOT_SYMBOLS),
                "baseline_symbol_count": len(baseline_symbols),
                "coverage_complete": len(baseline_symbols) == len(PILOT_SYMBOLS),
                "source": historical.source_provenance,
            }
            (arguments.session_output / "HISTORICAL_BOOTSTRAP_STATUS.json").write_text(
                json.dumps(status, sort_keys=True) + "\n"
            )
            return historical
        except Exception as exc:  # noqa: BLE001 - bootstrap status must capture sanitized failure
            status = {
                "status": "FAILED",
                "started_at": bootstrap_started.isoformat(),
                "finished_at": datetime.now(UTC).isoformat(),
                "candidate_symbol_count": len(PILOT_SYMBOLS),
                "error_type": type(exc).__name__,
            }
            (arguments.session_output / "HISTORICAL_BOOTSTRAP_STATUS.json").write_text(
                json.dumps(status, sort_keys=True) + "\n"
            )
            return None

    bootstrap_monitor = asyncio.create_task(finish_bootstrap())
    try:
        return await engine.execute()
    finally:
        if not bootstrap_monitor.done():
            await bootstrap_monitor


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    run = subparsers.add_parser("run", help="run one full 09:30-16:00 ET session")
    run.add_argument("--session-output", type=Path, required=True)
    run.add_argument("--market-date", required=True)
    run.add_argument("--expected-git-commit", required=True)
    run.add_argument("--repository-root", type=Path, default=Path.cwd())
    validate = subparsers.add_parser("validate-environment")
    validate.add_argument("--session-output", type=Path, required=True)
    validate.add_argument("--expected-git-commit", required=True)
    validate.add_argument("--repository-root", type=Path, default=Path.cwd())
    inspect = subparsers.add_parser("inspect")
    inspect.add_argument("--bundle", type=Path, required=True)
    inspect.add_argument("--repository-root", type=Path, default=Path.cwd())
    replay = subparsers.add_parser("replay")
    replay.add_argument("--bundle", type=Path, required=True)
    replay.add_argument("--output", type=Path, required=True)
    replay.add_argument("--repository-root", type=Path, default=Path.cwd())
    certify = subparsers.add_parser("certify")
    certify.add_argument("--bundle", type=Path, required=True)
    certify.add_argument("--replay-output", type=Path, required=True)
    certify.add_argument("--certificate-output", type=Path, required=True)
    certify.add_argument("--repository-root", type=Path, default=Path.cwd())
    return parser


def main(argv: list[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    try:
        if arguments.command == "run":
            result = asyncio.run(_run_production(arguments))
        elif arguments.command == "validate-environment":
            settings = _load_settings()
            validate_environment(
                settings,
                arguments.session_output,
                arguments.expected_git_commit,
                arguments.repository_root.resolve(),
            )
            result = {"status": "environment_valid", "provider_contacted": False}
        else:
            settings = _load_settings()
            key = _load_authority_key(settings.live_evidence_authority_key_file)
            if arguments.command == "inspect":
                bundle = load_and_verify_bundle(
                    arguments.bundle,
                    authority_key=key,
                    repository_root=arguments.repository_root.resolve(),
                )
                result = {
                    "status": "bundle_verified",
                    "session_id": str(bundle.journal.run_id),
                    "final_state": bundle.final_state,
                }
            elif arguments.command == "replay":
                result = replay_live_bundle(
                    arguments.bundle,
                    arguments.output,
                    authority_key=key,
                    repository_root=arguments.repository_root.resolve(),
                )
            else:
                result = certify_live_bundle(
                    arguments.bundle,
                    arguments.certificate_output,
                    arguments.replay_output,
                    authority_key=key,
                    repository_root=arguments.repository_root.resolve(),
                )
    except (LiveEvidenceFailure, ProviderError, OSError, ValueError) as exc:
        code = exc.code if isinstance(exc, LiveEvidenceFailure) else type(exc).__name__
        print(json.dumps({"status": "failed", "failure_code": code}, sort_keys=True))
        return 2
    print(json.dumps(result, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
