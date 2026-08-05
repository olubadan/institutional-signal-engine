"""Append-only PostgreSQL audit repository plus deterministic in-memory fixture."""

import json
from collections.abc import Iterable
from typing import Any
from uuid import UUID

from .schemas import CanonicalEvent, Decision

SCHEMA = """
CREATE TABLE IF NOT EXISTS canonical_events (event_id uuid PRIMARY KEY, run_id uuid NOT NULL, kind text NOT NULL, symbol text NOT NULL, source text NOT NULL, source_timestamp timestamptz NOT NULL, received_timestamp timestamptz NOT NULL, normalized_timestamp timestamptz NOT NULL, sequence bigint NOT NULL, payload jsonb NOT NULL);
CREATE TABLE IF NOT EXISTS decisions (decision_id uuid PRIMARY KEY, run_id uuid NOT NULL, decided_at timestamptz NOT NULL, selected_symbol text, fire boolean NOT NULL, candidates jsonb NOT NULL, rejection_reasons jsonb NOT NULL, input_event_ids jsonb NOT NULL, config_version text NOT NULL, engine_version text NOT NULL, counters jsonb NOT NULL);
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

    def replay_events(self, run_id: UUID | None = None) -> Iterable[CanonicalEvent]:
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
        connection.execute(
            "ALTER TABLE canonical_events ADD COLUMN IF NOT EXISTS run_id uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000'"
        )
        connection.execute(
            "ALTER TABLE decisions ADD COLUMN IF NOT EXISTS run_id uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000'"
        )

    def record_event(self, event: CanonicalEvent) -> None:
        self._pending_events.append(event)
        if len(self._pending_events) >= 100_000:
            self.flush()

    def flush(self) -> None:
        if not self._pending_events:
            return
        connection = self._session()
        connection.execute(
            "CREATE TEMP TABLE IF NOT EXISTS canonical_events_stage "
            "(event_id uuid, run_id uuid, kind text, symbol text, source text, "
            "source_timestamp timestamptz, received_timestamp timestamptz, "
            "normalized_timestamp timestamptz, sequence bigint, payload jsonb)"
        )
        connection.execute("TRUNCATE canonical_events_stage")
        with connection.cursor().copy("COPY canonical_events_stage FROM STDIN") as copy:
            for event in self._pending_events:
                copy.write_row(
                    (
                        event.event_id,
                        event.run_id,
                        event.kind.value,
                        event.symbol,
                        event.source,
                        event.source_timestamp,
                        event.received_timestamp,
                        event.normalized_timestamp,
                        event.sequence,
                        json.dumps(event.payload, default=str),
                    )
                )
        connection.execute(
            "INSERT INTO canonical_events "
            "(event_id,run_id,kind,symbol,source,source_timestamp,received_timestamp,"
            "normalized_timestamp,sequence,payload) "
            "SELECT event_id,run_id,kind,symbol,source,source_timestamp,received_timestamp,"
            "normalized_timestamp,sequence,payload FROM canonical_events_stage "
            "ON CONFLICT DO NOTHING"
        )
        self._pending_events.clear()

    def record_decision(self, decision: Decision) -> None:
        self._session().execute(
            "INSERT INTO decisions (decision_id,run_id,decided_at,selected_symbol,fire,candidates,rejection_reasons,input_event_ids,config_version,engine_version,counters) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
            (
                decision.decision_id,
                decision.run_id,
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

    def replay_events(self, run_id: UUID | None = None) -> Iterable[CanonicalEvent]:
        self.flush()
        query = "SELECT event_id,run_id,kind,symbol,source,source_timestamp,received_timestamp,normalized_timestamp,sequence,payload FROM canonical_events"
        params: tuple[UUID, ...] = ()
        if run_id is not None:
            query += " WHERE run_id = %s"
            params = (run_id,)
        query += " ORDER BY normalized_timestamp,event_id"
        rows = self._session().execute(query, params).fetchall()
        from .schemas import EventKind

        return tuple(
            CanonicalEvent(
                event_id=row[0],
                run_id=row[1],
                kind=EventKind(row[2]),
                symbol=row[3],
                source=row[4],
                source_timestamp=row[5],
                received_timestamp=row[6],
                normalized_timestamp=row[7],
                sequence=row[8],
                payload=row[9],
            )
            for row in rows
        )

    def decision_count(self, run_id: UUID | None = None) -> int:
        self.flush()
        if run_id is None:
            return int(self._session().execute("SELECT count(*) FROM decisions").fetchone()[0])
        return int(
            self._session()
            .execute("SELECT count(*) FROM decisions WHERE run_id=%s", (run_id,))
            .fetchone()[0]
        )

    def event_count(self, run_id: UUID | None = None) -> int:
        self.flush()
        if run_id is None:
            return int(
                self._session().execute("SELECT count(*) FROM canonical_events").fetchone()[0]
            )
        return int(
            self._session()
            .execute("SELECT count(*) FROM canonical_events WHERE run_id=%s", (run_id,))
            .fetchone()[0]
        )

    def replay_decisions(self, run_id: UUID | None = None) -> tuple[Decision, ...]:
        """Read decisions as typed models for field-by-field replay comparison."""
        self.flush()
        query = "SELECT decision_id,run_id,decided_at,selected_symbol,fire,candidates,rejection_reasons,input_event_ids,config_version,engine_version,counters FROM decisions"
        params: tuple[UUID, ...] = ()
        if run_id is not None:
            query += " WHERE run_id = %s"
            params = (run_id,)
        query += " ORDER BY decided_at,decision_id"
        rows = self._session().execute(query, params).fetchall()
        return tuple(
            Decision(
                decision_id=row[0],
                run_id=row[1],
                decided_at=row[2],
                selected_symbol=row[3],
                fire=row[4],
                candidates=tuple(row[5]),
                rejection_reasons=tuple(row[6]),
                input_event_ids=tuple(row[7]),
                config_version=row[8],
                engine_version=row[9],
                counters=row[10],
            )
            for row in rows
        )
