"""Append-only PostgreSQL audit repository plus deterministic in-memory fixture."""

import json
from collections.abc import Iterable
from typing import Any

from .schemas import CanonicalEvent, Decision

SCHEMA = """
CREATE TABLE IF NOT EXISTS canonical_events (event_id uuid PRIMARY KEY, kind text NOT NULL, symbol text NOT NULL, source text NOT NULL, source_timestamp timestamptz NOT NULL, received_timestamp timestamptz NOT NULL, normalized_timestamp timestamptz NOT NULL, sequence bigint NOT NULL, payload jsonb NOT NULL);
CREATE TABLE IF NOT EXISTS decisions (decision_id uuid PRIMARY KEY, decided_at timestamptz NOT NULL, selected_symbol text, fire boolean NOT NULL, candidates jsonb NOT NULL, rejection_reasons jsonb NOT NULL, input_event_ids jsonb NOT NULL, config_version text NOT NULL, engine_version text NOT NULL, counters jsonb NOT NULL);
"""


class InMemoryRepository:
    def __init__(self) -> None:
        self.events: list[CanonicalEvent] = []
        self.decisions: list[Decision] = []

    def record_event(self, event: CanonicalEvent) -> None:
        if event.event_id not in {existing.event_id for existing in self.events}:
            self.events.append(event)

    def record_decision(self, decision: Decision) -> None:
        if decision.decision_id not in {existing.decision_id for existing in self.decisions}:
            self.decisions.append(decision)

    def replay_events(self) -> Iterable[CanonicalEvent]:
        return tuple(self.events)


class PostgresRepository:
    def __init__(self, database_url: str) -> None:
        self.database_url = database_url
        self._connection: Any | None = None
        self._pending_events: list[CanonicalEvent] = []

    def _connect(self) -> Any:
        import psycopg

        return psycopg.connect(self.database_url, autocommit=True)

    def _session(self) -> Any:
        if self._connection is None or self._connection.closed:
            self._connection = self._connect()
        return self._connection

    def initialize(self) -> None:
        connection = self._session()
        connection.execute(SCHEMA)
        connection.execute(
            "ALTER TABLE decisions ADD COLUMN IF NOT EXISTS counters jsonb NOT NULL DEFAULT '{}'::jsonb"
        )

    def record_event(self, event: CanonicalEvent) -> None:
        self._pending_events.append(event)
        if len(self._pending_events) >= 100_000:
            self.flush()

    def flush(self) -> None:
        if not self._pending_events:
            return
        self._session().cursor().executemany(
            "INSERT INTO canonical_events VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
            [
                (
                    event.event_id,
                    event.kind.value,
                    event.symbol,
                    event.source,
                    event.source_timestamp,
                    event.received_timestamp,
                    event.normalized_timestamp,
                    event.sequence,
                    json.dumps(event.payload, default=str),
                )
                for event in self._pending_events
            ],
        )
        self._pending_events.clear()

    def record_decision(self, decision: Decision) -> None:
        self.flush()
        self._session().execute(
            "INSERT INTO decisions VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
            (
                decision.decision_id,
                decision.decided_at,
                decision.selected_symbol,
                decision.fire,
                json.dumps(
                    [candidate.model_dump(mode="json") for candidate in decision.candidates]
                ),
                json.dumps(decision.rejection_reasons),
                json.dumps([str(value) for value in decision.input_event_ids]),
                decision.config_version,
                decision.engine_version,
                json.dumps(decision.counters.model_dump()),
            ),
        )

    def replay_events(self) -> Iterable[CanonicalEvent]:
        self.flush()
        rows = (
            self._session()
            .execute(
                "SELECT event_id,kind,symbol,source,source_timestamp,received_timestamp,"
                "normalized_timestamp,sequence,payload FROM canonical_events ORDER BY "
                "normalized_timestamp,event_id"
            )
            .fetchall()
        )
        from .schemas import EventKind

        return tuple(
            CanonicalEvent(
                event_id=row[0],
                kind=EventKind(row[1]),
                symbol=row[2],
                source=row[3],
                source_timestamp=row[4],
                received_timestamp=row[5],
                normalized_timestamp=row[6],
                sequence=row[7],
                payload=row[8],
            )
            for row in rows
        )

    def decision_count(self) -> int:
        self.flush()
        return int(self._session().execute("SELECT count(*) FROM decisions").fetchone()[0])

    def event_count(self) -> int:
        self.flush()
        return int(self._session().execute("SELECT count(*) FROM canonical_events").fetchone()[0])

    def replay_decisions(self) -> tuple[Decision, ...]:
        """Read decisions as typed models for field-by-field replay comparison."""
        self.flush()
        rows = (
            self._session()
            .execute(
                "SELECT decision_id,decided_at,selected_symbol,fire,candidates,"
                "rejection_reasons,input_event_ids,config_version,engine_version,counters "
                "FROM decisions ORDER BY decided_at,decision_id"
            )
            .fetchall()
        )
        return tuple(
            Decision(
                decision_id=row[0],
                decided_at=row[1],
                selected_symbol=row[2],
                fire=row[3],
                candidates=tuple(row[4]),
                rejection_reasons=tuple(row[5]),
                input_event_ids=tuple(row[6]),
                config_version=row[7],
                engine_version=row[8],
                counters=row[9],
            )
            for row in rows
        )
