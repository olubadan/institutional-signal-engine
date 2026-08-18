"""Provider-free accelerated full-session harness for the production evidence path."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Literal
from uuid import NAMESPACE_URL, UUID, uuid5

from .config import Settings
from .journal import FileJournalRepository, canonical_json, sha256
from .live_session import (
    BundleWriter,
    InboundFrame,
    LiveSessionEngine,
    ObservationRepository,
    SessionBoundaries,
    SessionClock,
    SessionIdentity,
    UnifiedSessionPort,
    _digest_bytes,
    _git_head,
    _rules_bytes,
    certify_live_bundle,
)
from .orchestration import DiscoveryPort, EnrichmentPort, PlannerPort
from .providers.thetadata import SubscriptionRequest, ThetaContract
from .schemas import CanonicalEvent, EventKind
from .universe import PlannerEpoch, UniverseSelection

CONTRACT_A = ThetaContract("AAPL", 20260821, 310000, "C")
CONTRACT_B = ThetaContract("MSFT", 20260821, 500000, "C")
MEMBERSHIPS = ((CONTRACT_A,), (CONTRACT_A, CONTRACT_B), (CONTRACT_B,))
HARNESS_AUTHORITY_KEY = hashlib.sha256(b"live-session-hermetic-authority-v1").digest()
HARNESS_RUN_ID = UUID("12345678-1234-5678-9234-567812345678")


class VirtualClock(SessionClock):
    def __init__(self, now: datetime) -> None:
        self._now = now

    def now(self) -> datetime:
        return self._now

    async def wait_until(self, boundary: datetime) -> None:
        if boundary < self._now:
            return
        self._now = boundary

    def advance(self, boundary: datetime) -> None:
        if boundary < self._now:
            raise ValueError("virtual_clock_cannot_reverse")
        self._now = boundary


class HarnessDiscovery(DiscoveryPort):
    async def discover(self, symbols: tuple[str, ...], as_of: datetime) -> tuple[object, ...]:
        return (as_of.isoformat(), *symbols)

    async def prices(self, symbols: tuple[str, ...]) -> dict[str, Decimal]:
        return {symbol: Decimal(100) for symbol in symbols}


class HarnessEnrichment(EnrichmentPort):
    async def enrich(
        self,
        discovered: tuple[object, ...],
        prices: dict[str, Decimal],
        as_of: datetime,
    ) -> tuple[UniverseSelection, ...]:
        del discovered, prices, as_of
        return ()


class HarnessPlanner(PlannerPort):
    def plan(
        self,
        selections: tuple[UniverseSelection, ...],
        sequence: int,
        effective_at: datetime,
        previous_contracts: tuple[ThetaContract, ...],
        baseline_symbols: frozenset[str],
    ) -> PlannerEpoch:
        del selections, baseline_symbols
        membership = MEMBERSHIPS[min(sequence - 1, len(MEMBERSHIPS) - 1)]
        return PlannerEpoch.create(
            sequence=sequence,
            effective_at=effective_at,
            candidate_population_version="LIVE_HARNESS_POPULATION_V1",
            selected_contracts=membership,
            previous_contracts=previous_contracts,
            provenance="live-session-production-path-harness-v1",
        )


def _event(contract: ThetaContract, timestamp: datetime, sequence: int) -> CanonicalEvent:
    return CanonicalEvent(
        event_id=uuid5(
            NAMESPACE_URL,
            f"live-harness:{contract.root}:{timestamp.isoformat()}:{sequence}",
        ),
        kind=EventKind.OPTIONS,
        symbol=contract.root,
        source="thetadata-harness",
        source_timestamp=timestamp.astimezone(UTC),
        received_timestamp=timestamp.astimezone(UTC),
        normalized_timestamp=timestamp.astimezone(UTC),
        sequence=sequence,
        payload={
            "provider_event_kind": "trade",
            "contract": {
                "root": contract.root,
                "expiration": contract.expiration,
                "strike": contract.strike,
                "right": contract.right,
            },
            "trade_size": 10,
            "trade_price": "1.25",
        },
    )


@dataclass(frozen=True)
class ScheduledFrame:
    timestamp: datetime
    kind: Literal["event", "disconnect"]
    event: CanonicalEvent | None = None


class HarnessUnifiedSession(UnifiedSessionPort):
    request_types: tuple[str, ...] = ("TRADE", "QUOTE")
    supports_removal_acknowledgements = True

    def __init__(
        self,
        clock: VirtualClock,
        frames: tuple[ScheduledFrame, ...],
        drop_first_ack: bool = False,
    ) -> None:
        self.clock = clock
        self.connection_generation = 0
        self._next_request_id = 1
        self._connected = False
        self._acknowledgements: list[InboundFrame] = []
        self._frames = list(frames)
        self._drop_first_ack = drop_first_ack

    async def connect(self) -> None:
        if self._connected:
            raise ValueError("already_connected")
        self._connected = True
        self.connection_generation += 1

    async def close(self) -> None:
        self._connected = False

    async def reconnect(self) -> None:
        await self.close()
        await self.connect()

    async def health(self) -> dict[str, object]:
        return {
            "status": "healthy" if self._connected else "unavailable",
            "provider": "deterministic-unified-harness",
        }

    def prepare_request(
        self, action: Literal["add", "remove"], contract: ThetaContract, channel: str
    ) -> SubscriptionRequest:
        request = SubscriptionRequest(
            self._next_request_id,
            contract,
            channel,
            action == "add",
            self.connection_generation,
        )
        self._next_request_id += 1
        return request

    async def transmit(self, request: SubscriptionRequest) -> None:
        if self._drop_first_ack:
            self._drop_first_ack = False
            return
        response = "SUBSCRIBED" if request.add else "UNSUBSCRIBED"
        self._acknowledgements.append(
            InboundFrame(
                "ack",
                self.clock.now(),
                request_id=request.request_id,
                response=response,
            )
        )

    async def receive_until(
        self, boundary: datetime, *, acknowledgements_only: bool = False
    ) -> InboundFrame:
        del acknowledgements_only
        if self._acknowledgements:
            return self._acknowledgements.pop(0)
        if not self._connected:
            return InboundFrame("disconnect", self.clock.now(), detail="fixture_disconnected")
        if self._frames and self._frames[0].timestamp <= boundary:
            frame = self._frames.pop(0)
            self.clock.advance(frame.timestamp)
            if frame.kind == "disconnect":
                self._connected = False
                return InboundFrame("disconnect", frame.timestamp, detail="controlled_disconnect")
            return InboundFrame("event", frame.timestamp, event=frame.event)
        self.clock.advance(boundary)
        return InboundFrame("clock", boundary)


class HarnessObservationRepository(ObservationRepository):
    def __init__(self, path: Path) -> None:
        self.path = path
        self.events: list[dict[str, object]] = []

    def record_event(self, event: CanonicalEvent) -> None:
        self.events.append(event.model_dump(mode="json"))

    def flush(self) -> None:
        if self.path.exists():
            raise ValueError("harness_repository_exists")
        self.path.write_text(canonical_json(self.events), encoding="utf-8")

    def replay_events(self, run_id: UUID | None = None) -> tuple[CanonicalEvent, ...]:
        """Reconstruct canonical events in the exact persisted ingest order."""
        events = tuple(CanonicalEvent.model_validate(raw) for raw in self.events)
        if run_id is not None:
            events = tuple(event for event in events if event.run_id == run_id)
        return tuple(sorted(events, key=lambda event: (event.ingest_order, event.event_id)))

    def durable_identity(self, run_id: UUID) -> str:
        return f"harness-observation-repository://{run_id}/{sha256(self.events)}"


async def run_harness(
    bundle_directory: Path,
    journal_repository_directory: Path,
    observation_repository: Path,
    repository_root: Path,
    drop_first_ack: bool = False,
) -> dict[str, object]:
    settings = Settings()
    market_date = date(2026, 8, 11)
    boundaries = SessionBoundaries.for_market_date(market_date)
    clock = VirtualClock(boundaries.opens_at - timedelta(minutes=5))
    frames = (
        ScheduledFrame(
            boundaries.opens_at + timedelta(minutes=30),
            "event",
            _event(CONTRACT_A, boundaries.opens_at + timedelta(minutes=30), 1),
        ),
        ScheduledFrame(boundaries.opens_at + timedelta(hours=2, minutes=30), "disconnect"),
        ScheduledFrame(
            boundaries.opens_at + timedelta(hours=4, minutes=30),
            "event",
            _event(CONTRACT_B, boundaries.opens_at + timedelta(hours=4, minutes=30), 2),
        ),
    )
    identity = SessionIdentity(
        git_commit=_git_head(repository_root),
        application_version=settings.engine_version,
        config_version=settings.config_version,
        journal_schema_version="LIVE_CAUSAL_JOURNAL_V1",
        rules_sha256=_digest_bytes(_rules_bytes()),
        configuration_sha256=sha256(settings.public_snapshot()),
    )
    engine = LiveSessionEngine(
        run_id=HARNESS_RUN_ID,
        identity=identity,
        boundaries=boundaries,
        settings=settings,
        clock=clock,
        discovery=HarnessDiscovery(),
        enrichment=HarnessEnrichment(),
        planner=HarnessPlanner(),
        provider=HarnessUnifiedSession(clock, frames, drop_first_ack=drop_first_ack),
        repository=HarnessObservationRepository(observation_repository),
        writer=BundleWriter(bundle_directory),
        journal_repository=FileJournalRepository(journal_repository_directory),
        authority_key=HARNESS_AUTHORITY_KEY,
        reevaluation_interval=timedelta(hours=2),
        provider_identity={
            "discovery": "deterministic-alpaca-port",
            "enrichment": "deterministic-enrichment-port",
            "events": "deterministic-thetadata-unified-port",
        },
    )
    return await engine.execute()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--journal-repository", type=Path, required=True)
    parser.add_argument("--observation-repository", type=Path, required=True)
    parser.add_argument("--replay-output", type=Path, required=True)
    parser.add_argument("--certificate-output", type=Path, required=True)
    parser.add_argument("--repository-root", type=Path, default=Path.cwd())
    arguments = parser.parse_args(argv)
    manifest = asyncio.run(
        run_harness(
            arguments.bundle,
            arguments.journal_repository,
            arguments.observation_repository,
            arguments.repository_root.resolve(),
        )
    )
    certificate = certify_live_bundle(
        arguments.bundle,
        arguments.certificate_output,
        arguments.replay_output,
        authority_key=HARNESS_AUTHORITY_KEY,
        repository_root=arguments.repository_root.resolve(),
    )
    print(
        json.dumps(
            {
                "status": "accelerated_live_session_complete",
                "manifest_sha256": manifest["manifest_sha256"],
                "certificate_sha256": certificate["certificate_sha256"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
