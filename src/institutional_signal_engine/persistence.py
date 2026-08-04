"""Append-only PostgreSQL audit repository plus deterministic in-memory fixture."""

import json
from collections.abc import Iterable
from typing import Any

from .schemas import CanonicalEvent, Decision

SCHEMA = """
CREATE TABLE IF NOT EXISTS canonical_events (event_id uuid PRIMARY KEY, kind text NOT NULL, symbol text NOT NULL, source text NOT NULL, source_timestamp timestamptz NOT NULL, received_timestamp timestamptz NOT NULL, normalized_timestamp timestamptz NOT NULL, sequence bigint NOT NULL, payload jsonb NOT NULL);
CREATE TABLE IF NOT EXISTS decisions (decision_id uuid PRIMARY KEY, decided_at timestamptz NOT NULL, selected_symbol text, fire boolean NOT NULL, candidates jsonb NOT NULL, rejection_reasons jsonb NOT NULL, input_event_ids jsonb NOT NULL, config_version text NOT NULL, engine_version text NOT NULL);
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

    def _connect(self) -> Any:
        import psycopg

        return psycopg.connect(self.database_url)

    def initialize(self) -> None:
        with self._connect() as connection:
            connection.execute(SCHEMA)

    def record_event(self, event: CanonicalEvent) -> None:
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO canonical_events VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
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
                ),
            )

    def record_decision(self, decision: Decision) -> None:
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO decisions VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
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
                ),
            )

    def replay_events(self) -> Iterable[CanonicalEvent]:
        raise NotImplementedError(
            "Use a typed query/replay export in the next persistence increment"
        )
