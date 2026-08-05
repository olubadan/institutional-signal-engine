"""Append-only PostgreSQL audit repository plus deterministic in-memory fixture."""

import json
from collections.abc import Iterable
from typing import Any
from uuid import UUID

from .quote_book import QuoteConsumption
from .schemas import CanonicalEvent, Decision

SCHEMA = """
CREATE TABLE IF NOT EXISTS canonical_events (event_id uuid PRIMARY KEY, run_id uuid NOT NULL, ingest_order bigint NOT NULL, kind text NOT NULL, symbol text NOT NULL, source text NOT NULL, source_timestamp timestamptz NOT NULL, received_timestamp timestamptz NOT NULL, normalized_timestamp timestamptz NOT NULL, sequence bigint NOT NULL, payload jsonb NOT NULL);
CREATE TABLE IF NOT EXISTS decisions (decision_id uuid PRIMARY KEY, run_id uuid NOT NULL, decision_order bigint NOT NULL, decided_at timestamptz NOT NULL, selected_symbol text, fire boolean NOT NULL, candidates jsonb NOT NULL, rejection_reasons jsonb NOT NULL, input_event_ids jsonb NOT NULL, config_version text NOT NULL, engine_version text NOT NULL, condition_mapping_version text NOT NULL, triggering_change_reasons jsonb NOT NULL DEFAULT '[]'::jsonb, synchronized_state_identity text NOT NULL DEFAULT '', counters jsonb NOT NULL, indicator_provenance jsonb NOT NULL DEFAULT '{}'::jsonb, sweep_state jsonb NOT NULL DEFAULT '{}'::jsonb);
CREATE TABLE IF NOT EXISTS quote_consumptions (run_id uuid NOT NULL, consumption_order bigint NOT NULL, quote_event_id uuid NOT NULL, trade_event_id uuid, quote_role text NOT NULL, kind text NOT NULL, symbol text NOT NULL, source text NOT NULL, source_timestamp timestamptz NOT NULL, received_timestamp timestamptz NOT NULL, normalized_timestamp timestamptz NOT NULL, sequence bigint NOT NULL, payload jsonb NOT NULL, PRIMARY KEY (run_id, consumption_order));
CREATE TABLE IF NOT EXISTS sweep_clusters (run_id uuid NOT NULL, cluster_id uuid NOT NULL, payload jsonb NOT NULL, PRIMARY KEY (run_id, cluster_id));
"""


class InMemoryRepository:
    def __init__(self) -> None:
        self.events: list[CanonicalEvent] = []
        self.decisions: list[Decision] = []
        self.quote_consumptions: list[QuoteConsumption] = []
        self.sweeps: list[dict[str, object]] = []

    def record_event(self, event: CanonicalEvent) -> None:
        if event.event_id not in {existing.event_id for existing in self.events}:
            self.events.append(event)

    def record_decision(self, decision: Decision) -> None:
        if decision.decision_id not in {existing.decision_id for existing in self.decisions}:
            self.decisions.append(decision)

    def replay_events(self, run_id: UUID | None = None) -> Iterable[CanonicalEvent]:
        return tuple(self.events)

    def record_quote_consumption(self, consumption: QuoteConsumption) -> None:
        self.quote_consumptions.append(consumption)

    def record_sweep(self, sweep: dict[str, object]) -> None:
        self.sweeps.append(sweep)

    def replay_quote_consumptions(self, run_id: UUID | None = None) -> tuple[QuoteConsumption, ...]:
        if run_id is None:
            return tuple(self.quote_consumptions)
        return tuple(value for value in self.quote_consumptions if value.quote.run_id == run_id)


class PostgresRepository:
    def __init__(self, database_url: str) -> None:
        self.database_url = database_url
        self._connection: Any | None = None
        self._pending_events: list[CanonicalEvent] = []
        self._pending_decisions: list[Decision] = []
        self._pending_quote_consumptions: list[QuoteConsumption] = []
        self._pending_sweeps: list[dict[str, object]] = []

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
            "ALTER TABLE canonical_events ADD COLUMN IF NOT EXISTS ingest_order bigint NOT NULL DEFAULT 0"
        )
        connection.execute(
            "ALTER TABLE decisions ADD COLUMN IF NOT EXISTS run_id uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000'"
        )
        connection.execute(
            "ALTER TABLE decisions ADD COLUMN IF NOT EXISTS decision_order bigint NOT NULL DEFAULT 0"
        )
        connection.execute(
            "ALTER TABLE decisions ADD COLUMN IF NOT EXISTS condition_mapping_version text NOT NULL DEFAULT 'legacy-unknown'"
        )
        connection.execute(
            "ALTER TABLE decisions ADD COLUMN IF NOT EXISTS triggering_change_reasons jsonb NOT NULL DEFAULT '[]'::jsonb"
        )
        connection.execute(
            "ALTER TABLE decisions ADD COLUMN IF NOT EXISTS synchronized_state_identity text NOT NULL DEFAULT ''"
        )
        connection.execute(
            "ALTER TABLE decisions ADD COLUMN IF NOT EXISTS indicator_provenance jsonb NOT NULL DEFAULT '{}'::jsonb"
        )
        connection.execute(
            "ALTER TABLE decisions ADD COLUMN IF NOT EXISTS sweep_state jsonb NOT NULL DEFAULT '{}'::jsonb"
        )
        for statement in (
            "ALTER TABLE quote_consumptions ADD COLUMN IF NOT EXISTS kind text NOT NULL DEFAULT 'options'",
            "ALTER TABLE quote_consumptions ADD COLUMN IF NOT EXISTS symbol text NOT NULL DEFAULT ''",
            "ALTER TABLE quote_consumptions ADD COLUMN IF NOT EXISTS normalized_timestamp timestamptz NOT NULL DEFAULT now()",
            "ALTER TABLE quote_consumptions ADD COLUMN IF NOT EXISTS sequence bigint NOT NULL DEFAULT 0",
        ):
            connection.execute(statement)

    def record_event(self, event: CanonicalEvent) -> None:
        self._pending_events.append(event)
        if len(self._pending_events) >= 100_000:
            self.flush()

    def flush(self) -> None:
        if (
            not self._pending_events
            and not self._pending_decisions
            and not self._pending_quote_consumptions
            and not self._pending_sweeps
        ):
            return
        connection = self._session()
        if self._pending_events:
            connection.execute(
                "CREATE TEMP TABLE IF NOT EXISTS canonical_events_stage "
                "(event_id uuid, run_id uuid, ingest_order bigint, kind text, symbol text, source text, "
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
                            event.ingest_order,
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
                "(event_id,run_id,ingest_order,kind,symbol,source,source_timestamp,received_timestamp,"
                "normalized_timestamp,sequence,payload) "
                "SELECT event_id,run_id,ingest_order,kind,symbol,source,source_timestamp,received_timestamp,"
                "normalized_timestamp,sequence,payload FROM canonical_events_stage "
                "ON CONFLICT DO NOTHING"
            )
            self._pending_events.clear()
        if self._pending_decisions:
            with connection.cursor() as cursor:
                cursor.executemany(
                    "INSERT INTO decisions (decision_id,run_id,decision_order,decided_at,selected_symbol,fire,candidates,rejection_reasons,input_event_ids,config_version,engine_version,condition_mapping_version,triggering_change_reasons,synchronized_state_identity,counters,indicator_provenance,sweep_state) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                    [self._decision_parameters(decision) for decision in self._pending_decisions],
                )
            self._pending_decisions.clear()
        for consumption in self._pending_quote_consumptions:
            self._record_quote_consumption_now(connection, consumption)
        self._pending_quote_consumptions.clear()
        for sweep in self._pending_sweeps:
            connection.execute(
                "INSERT INTO sweep_clusters (run_id,cluster_id,payload) VALUES (%s,%s,%s) ON CONFLICT (run_id,cluster_id) DO UPDATE SET payload=EXCLUDED.payload",
                (sweep["run_id"], sweep["cluster_id"], json.dumps(sweep, default=str)),
            )
        self._pending_sweeps.clear()

    @staticmethod
    def _decision_parameters(decision: Decision) -> tuple[object, ...]:
        return (
            decision.decision_id,
            decision.run_id,
            decision.decision_order,
            decision.decided_at,
            decision.selected_symbol,
            decision.fire,
            json.dumps([candidate.model_dump(mode="json") for candidate in decision.candidates]),
            json.dumps(decision.rejection_reasons),
            json.dumps([str(value) for value in decision.input_event_ids]),
            decision.config_version,
            decision.engine_version,
            decision.condition_mapping_version,
            json.dumps(decision.triggering_change_reasons),
            decision.synchronized_state_identity,
            json.dumps(decision.counters.model_dump()),
            json.dumps(decision.indicator_provenance),
            json.dumps(decision.sweep_state),
        )

    def record_decision(self, decision: Decision) -> None:
        self._pending_decisions.append(decision)
        if len(self._pending_decisions) >= 1000:
            self.flush()

    def record_quote_consumption(self, consumption: QuoteConsumption) -> None:
        self._pending_quote_consumptions.append(consumption)
        if len(self._pending_quote_consumptions) >= 1000:
            self.flush()

    def record_sweep(self, sweep: dict[str, object]) -> None:
        self._pending_sweeps.append(sweep)
        if len(self._pending_sweeps) >= 100:
            self.flush()

    def _record_quote_consumption_now(self, connection: Any, consumption: QuoteConsumption) -> None:
        quote = consumption.quote
        connection.execute(
            "INSERT INTO quote_consumptions (run_id,consumption_order,quote_event_id,trade_event_id,quote_role,kind,symbol,source,source_timestamp,received_timestamp,normalized_timestamp,sequence,payload) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
            (
                quote.run_id,
                consumption.consumption_order,
                quote.event_id,
                consumption.trade_event_id,
                consumption.quote_role,
                quote.kind.value,
                quote.symbol,
                quote.source,
                quote.source_timestamp,
                quote.received_timestamp,
                quote.normalized_timestamp,
                quote.sequence,
                json.dumps(quote.payload, default=str),
            ),
        )

    def replay_quote_consumptions(self, run_id: UUID | None = None) -> tuple[QuoteConsumption, ...]:
        from .schemas import EventKind

        query = "SELECT run_id,consumption_order,quote_event_id,trade_event_id,quote_role,kind,symbol,source,source_timestamp,received_timestamp,normalized_timestamp,sequence,payload FROM quote_consumptions"
        params: tuple[UUID, ...] = ()
        if run_id is not None:
            query += " WHERE run_id=%s"
            params = (run_id,)
        query += " ORDER BY consumption_order"
        rows = self._session().execute(query, params).fetchall()
        return tuple(
            QuoteConsumption(
                row[1],
                row[2],
                row[3],
                row[4],
                CanonicalEvent(
                    event_id=row[2],
                    run_id=row[0],
                    kind=EventKind(row[5]),
                    symbol=row[6],
                    source=row[7],
                    source_timestamp=row[8],
                    received_timestamp=row[9],
                    normalized_timestamp=row[10],
                    sequence=row[11],
                    payload=row[12],
                ),
            )
            for row in rows
        )

    def replay_events(self, run_id: UUID | None = None) -> Iterable[CanonicalEvent]:
        self.flush()
        query = "SELECT event_id,run_id,ingest_order,kind,symbol,source,source_timestamp,received_timestamp,normalized_timestamp,sequence,payload FROM canonical_events"
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
                ingest_order=row[2],
                kind=EventKind(row[3]),
                symbol=row[4],
                source=row[5],
                source_timestamp=row[6],
                received_timestamp=row[7],
                normalized_timestamp=row[8],
                sequence=row[9],
                payload=row[10],
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
        query = "SELECT decision_id,run_id,decision_order,decided_at,selected_symbol,fire,candidates,rejection_reasons,input_event_ids,config_version,engine_version,condition_mapping_version,triggering_change_reasons,synchronized_state_identity,counters,indicator_provenance,sweep_state FROM decisions"
        params: tuple[UUID, ...] = ()
        if run_id is not None:
            query += " WHERE run_id = %s"
            params = (run_id,)
        query += " ORDER BY decision_order,decision_id"
        rows = self._session().execute(query, params).fetchall()
        return tuple(
            Decision(
                decision_id=row[0],
                run_id=row[1],
                decision_order=row[2],
                decided_at=row[3],
                selected_symbol=row[4],
                fire=row[5],
                candidates=tuple(row[6]),
                rejection_reasons=tuple(row[7]),
                input_event_ids=tuple(row[8]),
                config_version=row[9],
                engine_version=row[10],
                condition_mapping_version=row[11],
                triggering_change_reasons=tuple(row[12]),
                synchronized_state_identity=row[13],
                counters=row[14],
                indicator_provenance=row[15],
                sweep_state=row[16],
            )
            for row in rows
        )
