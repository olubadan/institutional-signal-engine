"""Deterministic, event-time option sweep clustering."""

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

from .indicators import regular_session
from .schemas import CanonicalEvent

SWEEP_WINDOW = timedelta(milliseconds=1000)
SWEEP_MIN_PREMIUM = Decimal(150000)
SWEEP_MIN_EXCHANGES = 3
SWEEP_MIN_ASK_PERCENTAGE = Decimal(65)
SWEEP_MAX_UNKNOWN_PERCENTAGE = Decimal(50)
SESSION_MIN_SWEEPS = 3
SESSION_MIN_PREMIUM = Decimal(500000)
SWEEP_FRESHNESS = timedelta(minutes=30)


@dataclass(frozen=True)
class SweepKey:
    root: str
    expiration: int
    strike: int
    right: str


@dataclass
class SweepCluster:
    cluster_id: UUID
    run_id: UUID
    key: SweepKey
    open_timestamp: datetime
    close_timestamp: datetime
    trades: dict[UUID, CanonicalEvent] = field(default_factory=dict)
    qualifying: bool = False
    final: bool = False
    expired: bool = False
    last_transition: str = "OPENED"

    @property
    def last_timestamp(self) -> datetime:
        return max(
            (event.source_timestamp for event in self.trades.values()), default=self.open_timestamp
        )

    def recompute(self) -> dict[str, Any]:
        eligible = tuple(self.trades.values())
        exchanges = {
            str(event.payload.get("exchange", "")).strip().upper()
            for event in eligible
            if _valid_exchange(event.payload.get("exchange"))
        }
        eligible_volume = sum(int(event.payload.get("trade_size", 0)) for event in eligible)
        premiums = [
            Decimal(str(event.payload.get("trade_price", 0)))
            * Decimal(int(event.payload.get("trade_size", 0)))
            * Decimal(100)
            for event in eligible
        ]
        aggregate = sum(premiums, Decimal(0))
        ask = sum(
            (
                premium
                for event, premium in zip(eligible, premiums, strict=True)
                if event.payload.get("trade_classification") == "ask"
            ),
            Decimal(0),
        )
        bid = sum(
            (
                premium
                for event, premium in zip(eligible, premiums, strict=True)
                if event.payload.get("trade_classification") == "bid"
            ),
            Decimal(0),
        )
        unknown = aggregate - ask - bid
        classified = ask + bid
        ask_percentage = (ask / classified * Decimal(100)) if classified else None
        unknown_percentage = (unknown / aggregate * Decimal(100)) if aggregate else None
        thresholds = {
            "identity_call": self.key.right == "C",
            "window": True,
            "three_exchanges": len(exchanges) >= SWEEP_MIN_EXCHANGES,
            "ask_side_percentage": ask_percentage is not None
            and ask_percentage >= SWEEP_MIN_ASK_PERCENTAGE,
            "unknown_premium_percentage": unknown_percentage is not None
            and unknown_percentage <= SWEEP_MAX_UNKNOWN_PERCENTAGE,
            "cluster_premium": aggregate >= SWEEP_MIN_PREMIUM,
        }
        return {
            "cluster_id": str(self.cluster_id),
            "run_id": str(self.run_id),
            "root": self.key.root,
            "expiration": self.key.expiration,
            "strike": self.key.strike,
            "right": self.key.right,
            "constituent_trade_ids": [str(value) for value in sorted(self.trades)],
            "open_timestamp": self.open_timestamp.isoformat(),
            "close_timestamp": self.close_timestamp.isoformat(),
            "first_constituent_timestamp": self.open_timestamp.isoformat(),
            "last_constituent_timestamp": self.last_timestamp.isoformat(),
            "exchange_set": sorted(exchanges),
            "eligible_volume": eligible_volume,
            "aggregate_eligible_premium": str(aggregate),
            "ask_side_premium": str(ask),
            "bid_side_premium": str(bid),
            "unknown_side_premium": str(unknown),
            "classified_premium": str(classified),
            "ask_side_percentage": str(ask_percentage) if ask_percentage is not None else None,
            "unknown_premium_percentage": (
                str(unknown_percentage) if unknown_percentage is not None else None
            ),
            "thresholds": thresholds,
            "qualification_state": self.qualifying,
            "final": self.final,
            "freshness_expiry_timestamp": (self.last_timestamp + SWEEP_FRESHNESS).isoformat(),
            "condition_mapping_version": str(
                next(iter(self.trades.values())).payload.get("condition_mapping_version", "unknown")
                if self.trades
                else "unknown"
            ),
            "engine_version": "0.1.0",
            "provenance": "event-time:sweep-v1",
        }


def _valid_exchange(value: object) -> bool:
    text = str(value or "").strip().upper()
    return bool(text) and text not in {"UNKNOWN", "N/A", "NONE", "NULL", "0"}


def _key(event: CanonicalEvent) -> SweepKey | None:
    contract = event.payload.get("contract")
    if not isinstance(contract, dict):
        return None
    return SweepKey(
        str(contract.get("root", "")).upper(),
        int(contract.get("expiration", 0)),
        int(contract.get("strike", 0)),
        str(contract.get("right", "")).upper(),
    )


@dataclass
class SweepUpdate:
    cluster: SweepCluster | None
    reason: str | None
    audit: dict[str, Any] | None
    session_count: int
    session_premium: Decimal
    most_recent_qualifying_timestamp: datetime | None
    closed_audits: tuple[dict[str, Any], ...] = ()


class SweepEngine:
    def __init__(self, run_id: UUID) -> None:
        self.run_id = run_id
        self.active: dict[SweepKey, SweepCluster] = {}
        self.closed: dict[UUID, SweepCluster] = {}
        self.session_qualifying: dict[UUID, Decimal] = {}
        self.expired: set[UUID] = set()
        self.session_date: str | None = None

    def reset_session(self, session_date: str) -> None:
        self.active.clear()
        self.session_qualifying.clear()
        self.expired.clear()
        self.session_date = session_date

    def process(self, event: CanonicalEvent) -> SweepUpdate:
        correction_target = event.payload.get("correction_of") or event.payload.get("cancel_of")
        if correction_target is not None:
            return self._apply_correction(event, correction_target)
        if (
            not event.payload.get("eligible_trade")
            or event.payload.get("contract", {}).get("right") != "C"
        ):
            return self._update(None, None)
        key = _key(event)
        if key is None or not regular_session(event.source_timestamp):
            return self._update(None, "SWEEP_INVALID_IDENTITY")
        closed_audits: list[dict[str, Any]] = []
        cluster = self.active.get(key)
        if cluster is None or event.source_timestamp > cluster.close_timestamp:
            if cluster is not None:
                cluster.final = True
                self.closed[cluster.cluster_id] = cluster
                closed_audits.append(cluster.recompute())
            cluster_id = uuid5(
                NAMESPACE_URL,
                f"sweep:{self.run_id}:{key.root}:{key.expiration}:{key.strike}:{key.right}:{event.source_timestamp.isoformat()}",
            )
            cluster = SweepCluster(
                cluster_id,
                self.run_id,
                key,
                event.source_timestamp,
                event.source_timestamp + SWEEP_WINDOW,
            )
            self.active[key] = cluster
        cluster.trades[event.event_id] = event
        before = cluster.qualifying
        metrics = cluster.recompute()
        cluster.qualifying = all(metrics["thresholds"].values())
        if cluster.qualifying:
            self.session_qualifying[cluster.cluster_id] = Decimal(
                metrics["aggregate_eligible_premium"]
            )
        else:
            self.session_qualifying.pop(cluster.cluster_id, None)
        if before != cluster.qualifying:
            cluster.last_transition = "QUALIFIED" if cluster.qualifying else "REVOKED"
            reason = "NEW_QUALIFYING_SWEEP" if cluster.qualifying else "SWEEP_QUALIFICATION_REVOKED"
        else:
            reason = None
        return self._update(cluster, reason, metrics, tuple(closed_audits))

    def tick(self, now: datetime) -> SweepUpdate:
        now = now.astimezone(UTC)
        qualifying = [
            cluster
            for cluster in (*self.active.values(), *self.closed.values())
            if cluster.qualifying and not cluster.expired
        ]
        latest = max((cluster.last_timestamp for cluster in qualifying), default=None)
        if latest is not None and now - latest >= SWEEP_FRESHNESS:
            newly_expired = False
            for cluster in qualifying:
                if (
                    cluster.cluster_id not in self.expired
                    and now - cluster.last_timestamp >= SWEEP_FRESHNESS
                ):
                    cluster.expired = True
                    self.expired.add(cluster.cluster_id)
                    newly_expired = True
            if newly_expired:
                return self._update(None, "SWEEP_FRESHNESS_EXPIRED")
        return self._update(None, None)

    def close_session(self) -> tuple[dict[str, Any], ...]:
        audits: list[dict[str, Any]] = []
        for cluster in self.active.values():
            cluster.final = True
            self.closed[cluster.cluster_id] = cluster
            audits.append(cluster.recompute())
        return tuple(audits)

    def _apply_correction(self, event: CanonicalEvent, target: object) -> SweepUpdate:
        try:
            target_id = UUID(str(target))
        except ValueError:
            return self._update(None, "SWEEP_UNCORRELATED_CORRECTION")
        for cluster in tuple(self.active.values()) + tuple(self.closed.values()):
            if target_id not in cluster.trades:
                continue
            before = cluster.qualifying
            del cluster.trades[target_id]
            metrics = cluster.recompute()
            cluster.qualifying = all(metrics["thresholds"].values())
            if cluster.qualifying:
                self.session_qualifying[cluster.cluster_id] = Decimal(
                    metrics["aggregate_eligible_premium"]
                )
            else:
                self.session_qualifying.pop(cluster.cluster_id, None)
            reason = None
            if before and not cluster.qualifying:
                reason = "SWEEP_QUALIFICATION_REVOKED"
            elif not before and cluster.qualifying:
                reason = "NEW_QUALIFYING_SWEEP"
            return self._update(cluster, reason, metrics)
        return self._update(None, "SWEEP_UNCORRELATED_CORRECTION")

    def snapshot(self) -> dict[str, object]:
        premium = sum(self.session_qualifying.values(), Decimal(0))
        latest = max(
            (
                cluster.last_timestamp
                for cluster in (*self.active.values(), *self.closed.values())
                if cluster.qualifying and not cluster.expired
            ),
            default=None,
        )
        return {
            "qualifying_sweep_count": len(self.session_qualifying),
            "qualifying_sweep_premium": str(premium),
            "most_recent_qualifying_sweep_timestamp": latest.isoformat() if latest else None,
            "session_sweep_gate": len(self.session_qualifying) >= SESSION_MIN_SWEEPS
            and premium >= SESSION_MIN_PREMIUM,
        }

    def _update(
        self,
        cluster: SweepCluster | None,
        reason: str | None,
        audit: dict[str, Any] | None = None,
        closed_audits: tuple[dict[str, Any], ...] = (),
    ) -> SweepUpdate:
        latest = max(
            (value.last_timestamp for value in self.active.values() if value.qualifying),
            default=None,
        )
        premium = sum(self.session_qualifying.values(), Decimal(0))
        return SweepUpdate(
            cluster,
            reason,
            audit if audit is not None else (cluster.recompute() if cluster else None),
            len(self.session_qualifying),
            premium,
            latest,
            closed_audits,
        )
