"""Append-only PostgreSQL audit repository plus deterministic in-memory fixture."""

import json
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from .quote_book import QuoteConsumption
from .schemas import CanonicalEvent, Decision

SCHEMA = """
CREATE TABLE IF NOT EXISTS canonical_events (event_id uuid PRIMARY KEY, run_id uuid NOT NULL, ingest_order bigint NOT NULL, kind text NOT NULL, symbol text NOT NULL, source text NOT NULL, source_timestamp timestamptz NOT NULL, received_timestamp timestamptz NOT NULL, normalized_timestamp timestamptz NOT NULL, sequence bigint NOT NULL, payload jsonb NOT NULL);
CREATE TABLE IF NOT EXISTS canonical_event_receipts (event_id uuid PRIMARY KEY, accepted_at timestamptz NOT NULL, clock_domain text NOT NULL, acceptance_stage text NOT NULL);
CREATE TABLE IF NOT EXISTS decisions (decision_id uuid PRIMARY KEY, run_id uuid NOT NULL, decision_order bigint NOT NULL, decided_at timestamptz NOT NULL, selected_symbol text, fire boolean NOT NULL, candidates jsonb NOT NULL, rejection_reasons jsonb NOT NULL, input_event_ids jsonb NOT NULL, config_version text NOT NULL, engine_version text NOT NULL, condition_mapping_version text NOT NULL, triggering_change_reasons jsonb NOT NULL DEFAULT '[]'::jsonb, synchronized_state_identity text NOT NULL DEFAULT '', counters jsonb NOT NULL, indicator_provenance jsonb NOT NULL DEFAULT '{}'::jsonb, sweep_state jsonb NOT NULL DEFAULT '{}'::jsonb);
CREATE TABLE IF NOT EXISTS quote_consumptions (run_id uuid NOT NULL, consumption_order bigint NOT NULL, quote_event_id uuid NOT NULL, trade_event_id uuid, quote_role text NOT NULL, kind text NOT NULL, symbol text NOT NULL, source text NOT NULL, source_timestamp timestamptz NOT NULL, received_timestamp timestamptz NOT NULL, normalized_timestamp timestamptz NOT NULL, sequence bigint NOT NULL, quote_ingest_order bigint NOT NULL DEFAULT 0, payload jsonb NOT NULL, PRIMARY KEY (run_id, consumption_order));
CREATE TABLE IF NOT EXISTS sweep_clusters (run_id uuid NOT NULL, cluster_id uuid NOT NULL, payload jsonb NOT NULL, PRIMARY KEY (run_id, cluster_id));
CREATE TABLE IF NOT EXISTS sweep_transitions (run_id uuid NOT NULL, cluster_id uuid NOT NULL, transition_order bigint NOT NULL, transition text NOT NULL, payload jsonb NOT NULL, PRIMARY KEY (run_id, cluster_id, transition_order));
CREATE TABLE IF NOT EXISTS universe_audits (run_id uuid NOT NULL, audit_order bigint GENERATED ALWAYS AS IDENTITY, symbol text NOT NULL, included boolean NOT NULL, payload jsonb NOT NULL, PRIMARY KEY (run_id, audit_order));
CREATE TABLE IF NOT EXISTS impact_clusters (run_id uuid NOT NULL, cluster_id text NOT NULL, payload jsonb NOT NULL, PRIMARY KEY (run_id, cluster_id));
CREATE TABLE IF NOT EXISTS impact_sessions (run_id uuid NOT NULL, symbol text NOT NULL, as_of timestamptz NOT NULL, payload jsonb NOT NULL, PRIMARY KEY (run_id, symbol, as_of));
CREATE TABLE IF NOT EXISTS shared_feature_vectors (run_id uuid NOT NULL, cluster_id text NOT NULL, version text NOT NULL, payload jsonb NOT NULL, PRIMARY KEY (run_id, cluster_id));
CREATE TABLE IF NOT EXISTS model_comparisons (run_id uuid NOT NULL, cluster_id text NOT NULL, version text NOT NULL, payload jsonb NOT NULL, PRIMARY KEY (run_id, cluster_id));
CREATE TABLE IF NOT EXISTS control_evaluations (run_id uuid NOT NULL, decision_id uuid NOT NULL, version text NOT NULL, payload jsonb NOT NULL, PRIMARY KEY (run_id, decision_id));
CREATE TABLE IF NOT EXISTS shadow_work_items (run_id uuid NOT NULL, work_item_id text PRIMARY KEY, cluster_id text NOT NULL, payload jsonb NOT NULL);
"""


class InMemoryRepository:
    def __init__(self) -> None:
        self.events: list[CanonicalEvent] = []
        self.journal_receipts: list[dict[str, object]] = []
        self.decisions: list[Decision] = []
        self.quote_consumptions: list[QuoteConsumption] = []
        self.sweeps: list[dict[str, object]] = []
        self.sweep_transitions: list[dict[str, object]] = []
        self.universe_audits: list[dict[str, object]] = []
        self.impact_clusters: list[dict[str, object]] = []
        self.impact_sessions: list[dict[str, object]] = []
        self.shared_feature_vectors: list[dict[str, object]] = []
        self.model_comparisons: list[dict[str, object]] = []
        self.control_evaluations: list[dict[str, object]] = []
        self.shadow_work_items: list[dict[str, object]] = []
        self.probes: list[str] = []

    def write_drain_probe(self, probe_id: str) -> bool:
        self.probes.append(probe_id)
        return True

    def record_event(self, event: CanonicalEvent) -> None:
        if event.event_id not in {existing.event_id for existing in self.events}:
            self.events.append(event)
            self.journal_receipts.append(
                {
                    "event_id": str(event.event_id),
                    "accepted_at": datetime.now(UTC),
                    "clock_domain": "local_wall_clock",
                    "acceptance_stage": "in_memory_repository_record_event",
                }
            )

    def replay_journal_receipts(self) -> tuple[dict[str, object], ...]:
        return tuple(self.journal_receipts)

    def record_decision(self, decision: Decision) -> None:
        if decision.decision_id not in {existing.decision_id for existing in self.decisions}:
            self.decisions.append(decision)

    def replay_events(self, run_id: UUID | None = None) -> Iterable[CanonicalEvent]:
        if run_id is None:
            return tuple(self.events)
        return tuple(event for event in self.events if event.run_id == run_id)

    def replay_decisions(self, run_id: UUID | None = None) -> tuple[Decision, ...]:
        if run_id is None:
            return tuple(self.decisions)
        return tuple(decision for decision in self.decisions if decision.run_id == run_id)

    def record_quote_consumption(self, consumption: QuoteConsumption) -> None:
        self.quote_consumptions.append(consumption)

    def record_sweep(self, sweep: dict[str, object]) -> None:
        self.sweeps.append(sweep)
        if sweep.get("transition") is not None:
            self.sweep_transitions.append(sweep)

    def replay_sweep_transitions(self, run_id: UUID | None = None) -> Iterable[dict[str, object]]:
        values = self.sweep_transitions
        if run_id is not None:
            values = [value for value in values if value.get("run_id") == str(run_id)]
        return tuple(sorted(values, key=lambda value: int(str(value["transition_order"]))))

    def record_universe(self, audit: dict[str, object]) -> None:
        self.universe_audits.append(audit)

    def record_impact_cluster(self, cluster: dict[str, object]) -> None:
        if cluster.get("cluster_id") not in {
            value.get("cluster_id") for value in self.impact_clusters
        }:
            self.impact_clusters.append(cluster)

    def record_impact_session(self, session: dict[str, object]) -> None:
        self.impact_sessions.append(session)

    def record_shared_feature_vector(self, vector: dict[str, object]) -> None:
        if vector.get("cluster_id") not in {
            item.get("cluster_id") for item in self.shared_feature_vectors
        }:
            self.shared_feature_vectors.append(vector)

    def record_model_comparison(self, comparison: dict[str, object]) -> None:
        if comparison.get("cluster_id") not in {
            item.get("cluster_id") for item in self.model_comparisons
        }:
            self.model_comparisons.append(comparison)

    def record_control_evaluation(self, evaluation: dict[str, object]) -> None:
        if evaluation.get("decision_id") not in {
            item.get("decision_id") for item in self.control_evaluations
        }:
            self.control_evaluations.append(evaluation)

    def record_shadow_work_item(self, item: dict[str, object]) -> None:
        if item.get("work_item_id") not in {
            value.get("work_item_id") for value in self.shadow_work_items
        }:
            self.shadow_work_items.append(item)

    def replay_shadow_work_items(self, run_id: UUID | None = None) -> Iterable[dict[str, object]]:
        values = self.shadow_work_items
        if run_id is not None:
            values = [value for value in values if value.get("run_id") == str(run_id)]
        return tuple(values)

    def replay_impact_clusters(self, run_id: UUID | None = None) -> Iterable[dict[str, object]]:
        values = self.impact_clusters
        if run_id is not None:
            values = [value for value in values if value.get("run_id") == str(run_id)]
        return tuple(values)

    def replay_impact_sessions(self, run_id: UUID | None = None) -> Iterable[dict[str, object]]:
        values = self.impact_sessions
        if run_id is not None:
            values = [value for value in values if value.get("run_id") == str(run_id)]
        return tuple(values)

    def replay_universe(self, run_id: UUID | None = None) -> Iterable[dict[str, object]]:
        values = self.universe_audits
        if run_id is not None:
            values = [value for value in values if value.get("run_id") == str(run_id)]
        return tuple(values)

    def replay_universe_finalization(self, run_id: UUID) -> dict[str, object] | None:
        values = [value for value in self.universe_audits if value.get("run_id") == str(run_id)]
        for value in reversed(values):
            finalization = value.get("finalization")
            if isinstance(finalization, dict) and finalization:
                return finalization
        return None

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
        self._pending_universe: list[dict[str, object]] = []
        self._pending_impact_clusters: list[dict[str, object]] = []
        self._pending_impact_sessions: list[dict[str, object]] = []
        self._pending_shared_feature_vectors: list[dict[str, object]] = []
        self._pending_model_comparisons: list[dict[str, object]] = []
        self._pending_control_evaluations: list[dict[str, object]] = []
        self._pending_shadow_work_items: list[dict[str, object]] = []

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
            "ALTER TABLE quote_consumptions ADD COLUMN IF NOT EXISTS quote_ingest_order bigint NOT NULL DEFAULT 0",
        ):
            connection.execute(statement)

    def healthcheck(self) -> bool:
        """Verify the same configured connection path used by live writes."""
        try:
            self._session().execute("SELECT 1").fetchone()
            return True
        except Exception:  # noqa: BLE001 - health probes return sanitized booleans
            return False

    def write_drain_probe(self, probe_id: str) -> bool:
        """Exercise a reversible write/read/delete probe on the application connection."""
        try:
            connection = self._session()
            connection.execute(
                "CREATE TEMP TABLE IF NOT EXISTS ignition_write_probe "
                "(probe_id text PRIMARY KEY, value integer NOT NULL)"
            )
            connection.execute("DELETE FROM ignition_write_probe WHERE probe_id=%s", (probe_id,))
            connection.execute(
                "INSERT INTO ignition_write_probe (probe_id,value) VALUES (%s,%s)",
                (probe_id, 1),
            )
            row = connection.execute(
                "SELECT value FROM ignition_write_probe WHERE probe_id=%s", (probe_id,)
            ).fetchone()
            connection.execute("DELETE FROM ignition_write_probe WHERE probe_id=%s", (probe_id,))
            return row is not None and int(row[0]) == 1
        except Exception:  # noqa: BLE001 - health probes return sanitized booleans
            return False

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
            and not self._pending_universe
            and not self._pending_impact_clusters
            and not self._pending_impact_sessions
            and not self._pending_shared_feature_vectors
            and not self._pending_model_comparisons
            and not self._pending_control_evaluations
            and not self._pending_shadow_work_items
        ):
            return
        connection = self._session()
        if self._pending_events:
            pending_events = tuple(self._pending_events)
            connection.execute(
                "CREATE TEMP TABLE IF NOT EXISTS canonical_events_stage "
                "(event_id uuid, run_id uuid, ingest_order bigint, kind text, symbol text, source text, "
                "source_timestamp timestamptz, received_timestamp timestamptz, "
                "normalized_timestamp timestamptz, sequence bigint, payload jsonb)"
            )
            connection.execute("TRUNCATE canonical_events_stage")
            with connection.cursor().copy("COPY canonical_events_stage FROM STDIN") as copy:
                for event in pending_events:
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
            accepted_at = datetime.now(UTC)
            with connection.cursor() as cursor:
                cursor.executemany(
                    "INSERT INTO canonical_event_receipts "
                    "(event_id,accepted_at,clock_domain,acceptance_stage) VALUES (%s,%s,%s,%s) "
                    "ON CONFLICT DO NOTHING",
                    [
                        (
                            event.event_id,
                            accepted_at,
                            "local_wall_clock",
                            "postgres_flush_after_canonical_insert",
                        )
                        for event in pending_events
                    ],
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
            if sweep.get("transition") is not None:
                connection.execute(
                    "INSERT INTO sweep_transitions (run_id,cluster_id,transition_order,transition,payload) VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                    (
                        sweep["run_id"],
                        sweep["cluster_id"],
                        sweep["transition_order"],
                        sweep["transition"],
                        json.dumps(sweep, default=str),
                    ),
                )
        self._pending_sweeps.clear()
        for audit in self._pending_universe:
            connection.execute(
                "INSERT INTO universe_audits (run_id,symbol,included,payload) VALUES (%s,%s,%s,%s)",
                (
                    audit["run_id"],
                    audit.get("symbol", "__MANIFEST__"),
                    audit.get("included", bool(audit.get("subscription_plan"))),
                    json.dumps(audit, default=str),
                ),
            )
        self._pending_universe.clear()
        for cluster in self._pending_impact_clusters:
            connection.execute(
                "INSERT INTO impact_clusters (run_id,cluster_id,payload) VALUES (%s,%s,%s) ON CONFLICT (run_id,cluster_id) DO UPDATE SET payload=EXCLUDED.payload",
                (cluster["run_id"], cluster["cluster_id"], json.dumps(cluster, default=str)),
            )
        self._pending_impact_clusters.clear()
        for session in self._pending_impact_sessions:
            connection.execute(
                "INSERT INTO impact_sessions (run_id,symbol,as_of,payload) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (
                    session["run_id"],
                    session["symbol"],
                    session["as_of"],
                    json.dumps(session, default=str),
                ),
            )
        self._pending_impact_sessions.clear()
        for vector in self._pending_shared_feature_vectors:
            connection.execute(
                "INSERT INTO shared_feature_vectors (run_id,cluster_id,version,payload) VALUES (%s,%s,%s,%s) ON CONFLICT (run_id,cluster_id) DO NOTHING",
                (
                    vector["run_id"],
                    vector["cluster_id"],
                    vector["version"],
                    json.dumps(vector, default=str),
                ),
            )
        self._pending_shared_feature_vectors.clear()
        for comparison in self._pending_model_comparisons:
            connection.execute(
                "INSERT INTO model_comparisons (run_id,cluster_id,version,payload) VALUES (%s,%s,%s,%s) ON CONFLICT (run_id,cluster_id) DO NOTHING",
                (
                    comparison["run_id"],
                    comparison["cluster_id"],
                    comparison["version"],
                    json.dumps(comparison, default=str),
                ),
            )
        self._pending_model_comparisons.clear()
        for evaluation in self._pending_control_evaluations:
            connection.execute(
                "INSERT INTO control_evaluations (run_id,decision_id,version,payload) VALUES (%s,%s,%s,%s) ON CONFLICT (run_id,decision_id) DO NOTHING",
                (
                    evaluation["run_id"],
                    evaluation["decision_id"],
                    evaluation["version"],
                    json.dumps(evaluation, default=str),
                ),
            )
        self._pending_control_evaluations.clear()
        for item in self._pending_shadow_work_items:
            connection.execute(
                "INSERT INTO shadow_work_items (run_id,work_item_id,cluster_id,payload) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (
                    item["run_id"],
                    item["work_item_id"],
                    item["cluster_id"],
                    json.dumps(item, default=str),
                ),
            )
        self._pending_shadow_work_items.clear()

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

    def record_universe(self, audit: dict[str, object]) -> None:
        self._pending_universe.append(audit)
        if len(self._pending_universe) >= 100:
            self.flush()

    def record_impact_cluster(self, cluster: dict[str, object]) -> None:
        self._pending_impact_clusters.append(cluster)
        if len(self._pending_impact_clusters) >= 100:
            self.flush()

    def record_impact_session(self, session: dict[str, object]) -> None:
        self._pending_impact_sessions.append(session)
        if len(self._pending_impact_sessions) >= 100:
            self.flush()

    def record_shared_feature_vector(self, vector: dict[str, object]) -> None:
        self._pending_shared_feature_vectors.append(vector)
        if len(self._pending_shared_feature_vectors) >= 100:
            self.flush()

    def record_model_comparison(self, comparison: dict[str, object]) -> None:
        self._pending_model_comparisons.append(comparison)
        if len(self._pending_model_comparisons) >= 100:
            self.flush()

    def record_control_evaluation(self, evaluation: dict[str, object]) -> None:
        self._pending_control_evaluations.append(evaluation)
        if len(self._pending_control_evaluations) >= 100:
            self.flush()

    def record_shadow_work_item(self, item: dict[str, object]) -> None:
        self._pending_shadow_work_items.append(item)
        if len(self._pending_shadow_work_items) >= 100:
            self.flush()

    def replay_shadow_work_items(self, run_id: UUID | None = None) -> Iterable[dict[str, object]]:
        self.flush()
        query = "SELECT payload FROM shadow_work_items"
        params: tuple[UUID, ...] = ()
        if run_id is not None:
            query += " WHERE run_id=%s"
            params = (run_id,)
        query += " ORDER BY work_item_id"
        return tuple(row[0] for row in self._session().execute(query, params).fetchall())

    def replay_impact_clusters(self, run_id: UUID | None = None) -> Iterable[dict[str, object]]:
        self.flush()
        query = "SELECT payload FROM impact_clusters"
        params: tuple[UUID, ...] = ()
        if run_id is not None:
            query += " WHERE run_id=%s"
            params = (run_id,)
        query += " ORDER BY cluster_id"
        return tuple(row[0] for row in self._session().execute(query, params).fetchall())

    def replay_impact_sessions(self, run_id: UUID | None = None) -> Iterable[dict[str, object]]:
        self.flush()
        query = "SELECT payload FROM impact_sessions"
        params: tuple[UUID, ...] = ()
        if run_id is not None:
            query += " WHERE run_id=%s"
            params = (run_id,)
        query += " ORDER BY as_of,symbol"
        return tuple(row[0] for row in self._session().execute(query, params).fetchall())

    def replay_universe(self, run_id: UUID | None = None) -> Iterable[dict[str, object]]:
        self.flush()
        query = "SELECT payload FROM universe_audits"
        params: tuple[UUID, ...] = ()
        if run_id is not None:
            query += " WHERE run_id=%s"
            params = (run_id,)
        query += " ORDER BY audit_order"
        return tuple(row[0] for row in self._session().execute(query, params).fetchall())

    def replay_universe_finalization(self, run_id: UUID) -> dict[str, object] | None:
        values = tuple(self.replay_universe(run_id))
        for value in reversed(values):
            finalization = value.get("finalization")
            if isinstance(finalization, dict) and finalization:
                return finalization
        return None

    def replay_sweep_transitions(self, run_id: UUID | None = None) -> Iterable[dict[str, object]]:
        self.flush()
        query = "SELECT payload FROM sweep_transitions"
        params: tuple[UUID, ...] = ()
        if run_id is not None:
            query += " WHERE run_id=%s"
            params = (run_id,)
        query += " ORDER BY transition_order"
        return tuple(row[0] for row in self._session().execute(query, params).fetchall())

    def _record_quote_consumption_now(self, connection: Any, consumption: QuoteConsumption) -> None:
        quote = consumption.quote
        connection.execute(
            "INSERT INTO quote_consumptions (run_id,consumption_order,quote_event_id,trade_event_id,quote_role,kind,symbol,source,source_timestamp,received_timestamp,normalized_timestamp,sequence,quote_ingest_order,payload) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
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
                quote.ingest_order,
                json.dumps(quote.payload, default=str),
            ),
        )

    def replay_quote_consumptions(self, run_id: UUID | None = None) -> tuple[QuoteConsumption, ...]:
        from .schemas import EventKind

        query = "SELECT run_id,consumption_order,quote_event_id,trade_event_id,quote_role,kind,symbol,source,source_timestamp,received_timestamp,normalized_timestamp,sequence,quote_ingest_order,payload FROM quote_consumptions"
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
                    ingest_order=row[12],
                    payload=row[13],
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
        # Replay must reconstruct live ingress order.  Event time is retained
        # for analysis, but ordering by it can move late-arriving events ahead
        # of inputs that the live pipeline already consumed.
        query += " ORDER BY ingest_order,event_id"
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
