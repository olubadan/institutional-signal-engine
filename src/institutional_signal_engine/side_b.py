"""Durable Side-B equity observations and options-to-equity outcomes.

This module records raw canonical equity observations separately from model
decisions. Future prices are outcome labels only; they are never supplied to
CONTROL or SHADOW evaluation.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from uuid import UUID

FORWARD_HORIZONS_SECONDS: tuple[int, ...] = (5, 15, 30, 60, 120, 300)
SIDE_B_SCHEMA_VERSION = "SIDE_B_EQUITY_OUTCOMES_V1"


def _json_value(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def _decimal(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


class SideBJournal:
    """Append-only JSONL evidence writer with a durable receipt hash."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        self._sequence = 0
        self._digest = hashlib.sha256()

    def append(self, kind: str, payload: Mapping[str, object]) -> dict[str, object]:
        record: dict[str, object] = {
            "schema_version": SIDE_B_SCHEMA_VERSION,
            "sequence": self._sequence,
            "kind": kind,
            "recorded_at": datetime.now(UTC).isoformat(),
            "payload": _json_value(payload),
        }
        encoded = (json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n").encode()
        with self.path.open("ab") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        self._digest.update(encoded)
        self._sequence += 1
        return record

    def receipt(self) -> dict[str, object]:
        return {
            "schema_version": SIDE_B_SCHEMA_VERSION,
            "path": str(self.path),
            "record_count": self._sequence,
            "sha256": self._digest.hexdigest(),
        }


@dataclass(frozen=True)
class OutcomeAnchor:
    cluster_id: str
    symbol: str
    t0: datetime
    price_t0: Decimal
    control_result: bool | None = None
    shadow_result: bool | None = None
    signed_delta_direction: str | None = None
    z_score: Decimal | None = None


@dataclass
class _PendingAnchor:
    anchor: OutcomeAnchor
    completed: set[int] = field(default_factory=set)


class ForwardOutcomeTracker:
    """Join option-time anchors to later equity observations."""

    def __init__(self, journal: SideBJournal) -> None:
        self.journal = journal
        self._pending: dict[str, _PendingAnchor] = {}
        self._latest_prices: dict[str, tuple[datetime, Decimal]] = {}

    def observe(self, event: Any) -> None:
        timestamp = event.normalized_timestamp.astimezone(UTC)
        price = _decimal(event.payload.get("price"))
        if price is None or price <= 0:
            return
        symbol = event.symbol.upper()
        previous = self._latest_prices.get(symbol)
        if previous is None or timestamp >= previous[0]:
            self._latest_prices[symbol] = (timestamp, price)
        self.journal.append(
            "equity.observation",
            {
                "event_id": str(event.event_id),
                "symbol": symbol,
                "source": event.source,
                "source_timestamp": event.source_timestamp,
                "received_timestamp": event.received_timestamp,
                "normalized_timestamp": timestamp,
                "sequence": event.sequence,
                "price": price,
                "volume": event.payload.get("volume"),
                "quote_context": event.payload.get("quote_context"),
                "provider_event_kind": event.payload.get("provider_event_kind"),
            },
        )
        for pending in self._pending.values():
            if pending.anchor.symbol != symbol or timestamp < pending.anchor.t0:
                continue
            for horizon in FORWARD_HORIZONS_SECONDS:
                if horizon in pending.completed:
                    continue
                target = pending.anchor.t0 + timedelta(seconds=horizon)
                if timestamp < target:
                    continue
                forward_return = (price - pending.anchor.price_t0) / pending.anchor.price_t0
                self.journal.append(
                    "equity.forward_outcome",
                    {
                        "cluster_id": pending.anchor.cluster_id,
                        "symbol": pending.anchor.symbol,
                        "t0": pending.anchor.t0,
                        "price_t0": pending.anchor.price_t0,
                        "target_horizon_seconds": horizon,
                        "actual_observation_timestamp": timestamp,
                        "price_th": price,
                        "forward_return": forward_return,
                        "price_direction": "UP"
                        if forward_return > 0
                        else "DOWN"
                        if forward_return < 0
                        else "FLAT",
                        "source": event.source,
                        "timestamp_provenance": "canonical.normalized_timestamp",
                        "control_result": pending.anchor.control_result,
                        "shadow_result": pending.anchor.shadow_result,
                        "signed_delta_direction": pending.anchor.signed_delta_direction,
                        "z_score": pending.anchor.z_score,
                    },
                )
                pending.completed.add(horizon)

    def register_anchor(self, anchor: OutcomeAnchor) -> None:
        self._pending[anchor.cluster_id] = _PendingAnchor(anchor)
        self.journal.append(
            "equity.outcome_anchor",
            {
                "cluster_id": anchor.cluster_id,
                "symbol": anchor.symbol,
                "t0": anchor.t0,
                "price_t0": anchor.price_t0,
                "control_result": anchor.control_result,
                "shadow_result": anchor.shadow_result,
                "signed_delta_direction": anchor.signed_delta_direction,
                "z_score": anchor.z_score,
                "horizons_seconds": FORWARD_HORIZONS_SECONDS,
            },
        )

    def latest_price(self, symbol: str) -> tuple[datetime, Decimal] | None:
        return self._latest_prices.get(symbol.upper())

    def record_filter_decision(self, decision: Any) -> None:
        self.journal.append(
            "equity.filter_ranking",
            {
                "decision_id": decision.decision_id,
                "decided_at": decision.decided_at,
                "candidates": [
                    candidate.model_dump(mode="json") for candidate in decision.candidates
                ],
                "counters": decision.counters.model_dump(mode="json"),
                "rejection_reasons": decision.rejection_reasons,
                "selected_symbol": decision.selected_symbol,
                "config_version": decision.config_version,
            },
        )

    def close(self) -> dict[str, object]:
        return self.journal.receipt()


def replay_side_b(path: Path) -> dict[str, object]:
    """Replay the append-only Side-B journal and return deterministic counts."""
    records = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    return {
        "schema_version": SIDE_B_SCHEMA_VERSION,
        "record_count": len(records),
        "observation_count": sum(record.get("kind") == "equity.observation" for record in records),
        "outcome_count": sum(record.get("kind") == "equity.forward_outcome" for record in records),
        "filter_decision_count": sum(
            record.get("kind") == "equity.filter_ranking" for record in records
        ),
        "anchor_count": sum(record.get("kind") == "equity.outcome_anchor" for record in records),
    }
