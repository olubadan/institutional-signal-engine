"""Deterministic, event-time option sweep clustering and audit state."""

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
EXCHANGE_MAPPING_VERSION = "thetadata-opra-exchanges-v1"

# ThetaData's published exchange enum is numeric.  The letter aliases are
# retained only for sanitized historical fixtures; live adapter messages use
# the numeric values.  A value absent from this versioned map is unknown and
# cannot satisfy participation.
THETADATA_EXCHANGE_CODES: dict[str, str] = {
    **{
        str(code): name
        for code, name in {
            1: "NASDAQ",
            2: "NASDAQ_ADF",
            3: "NYSE",
            4: "AMEX",
            5: "CBOE",
            6: "ISE",
            7: "NYSE_ARCA_PACIFIC",
            8: "CINCINNATI",
            9: "PHILADELPHIA",
            10: "OPRA",
            11: "BOSTON",
            12: "NASDAQ_NMS",
            13: "NASDAQ_SMALLCAP",
            14: "NASDAQ_BULLETIN",
            15: "NASDAQ_OTC",
            16: "NASDAQ_INDEX",
            17: "CHICAGO",
            18: "TORONTO",
            19: "CDNX",
            20: "CME",
            21: "NYBOT",
            22: "ISE_MERCURY",
            23: "COMEX",
            24: "CBOT",
            25: "NYMEX",
            26: "KCBT",
            27: "MGEX",
            28: "NYSE_ARCA_BONDS",
            29: "NASDAQ_BASIC",
            30: "DOW_JONES",
            31: "ISE_GEMINI",
            32: "SIMEX",
            33: "LONDON",
            34: "EUREX",
            35: "IMPLIED_PRICE",
            36: "DTN",
            37: "LME_MATCHED",
            38: "LME",
            39: "IPEX",
            40: "NASDAQ_MUTUAL_FUNDS",
            41: "COMEX_CLEARPORT",
            42: "C2",
            43: "MIAX",
            44: "NYMEX_CLEARPORT",
            45: "BARCLAYS",
            46: "MIAX_EMERALD",
            47: "NASDAQ_BOSTON",
            48: "HOTSPOT_EUREX",
            49: "EUREX_US",
            50: "EUREX_EU",
            51: "EURONEXT_COMMODITIES",
            52: "EURONEXT_INDEX_DERIVATIVES",
            53: "EURONEXT_INTEREST_RATES",
            54: "CFE",
            55: "PBOT",
            56: "FCME",
            57: "FINRA_NASDAQ_TRF",
            58: "BSE_TRF",
            59: "NYSE_TRF",
            60: "BATS",
            61: "CBOT_FLOOR",
            62: "PINK_SHEETS",
            63: "BATY",
            64: "EDGE_A",
            65: "EDGX",
            66: "RUSSELL",
            67: "CME_INDEXES",
            68: "IEX",
            69: "MIAX_PEARL",
            70: "LONDON_STOCK_EXCHANGE",
            71: "NYSE_GLOBAL_INDEX",
            72: "TSX_INDEXES",
            73: "MEMX",
            74: "EMPTY",
            75: "LTSE",
            76: "EMPTY_RESERVED",
            77: "24X",
        }.items()
    },
    "A": "NYSE_AMERICAN_FIXTURE_ALIAS",
    "B": "BOX_FIXTURE_ALIAS",
    "C": "CBOE_FIXTURE_ALIAS",
}


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
    correction_records: list[dict[str, object]] = field(default_factory=list)
    cancellation_records: list[dict[str, object]] = field(default_factory=list)
    qualifying: bool = False
    final: bool = False
    expired: bool = False
    last_transition: str = "OPENED"
    transition_order: int = 0

    @property
    def last_timestamp(self) -> datetime:
        return max(
            (event.source_timestamp for event in self.trades.values()), default=self.open_timestamp
        )

    def recompute(self) -> dict[str, Any]:
        eligible = tuple(self.trades.values())
        raw_exchanges = sorted(
            {str(event.payload.get("exchange", "")).strip().upper() for event in eligible}
        )
        exchanges = sorted(
            {
                THETADATA_EXCHANGE_CODES[value]
                for value in raw_exchanges
                if value in THETADATA_EXCHANGE_CODES
            }
        )
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
            "exchange_identifiers": raw_exchanges,
            "exchange_set": exchanges,
            "exchange_mapping_version": EXCHANGE_MAPPING_VERSION,
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
            "expired": self.expired,
            "freshness_expiry_timestamp": (self.last_timestamp + SWEEP_FRESHNESS).isoformat(),
            "condition_mapping_version": str(
                next(iter(self.trades.values())).payload.get("condition_mapping_version", "unknown")
                if self.trades
                else "unknown"
            ),
            "engine_version": "0.1.0",
            "provenance": "event-time:sweep-v2",
            "correction_records": self.correction_records,
            "cancellation_records": self.cancellation_records,
        }


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


def _record(event: CanonicalEvent) -> dict[str, object]:
    return event.model_dump(mode="json")


@dataclass
class SweepUpdate:
    cluster: SweepCluster | None
    reason: str | None
    audit: dict[str, Any] | None
    session_count: int
    session_premium: Decimal
    most_recent_qualifying_timestamp: datetime | None
    closed_audits: tuple[dict[str, Any], ...] = ()
    transition_audits: tuple[dict[str, Any], ...] = ()
    expired_timestamps: tuple[datetime, ...] = ()


class SweepEngine:
    def __init__(self, run_id: UUID) -> None:
        self.run_id = run_id
        self.active: dict[SweepKey, SweepCluster] = {}
        self.closed: dict[UUID, SweepCluster] = {}
        self.session_qualifying: dict[UUID, Decimal] = {}
        self.expired: set[UUID] = set()
        self.session_date: str | None = None
        self._transition_order = 0

    def reset_session(self, session_date: str) -> None:
        self.active.clear()
        self.closed.clear()
        self.session_qualifying.clear()
        self.expired.clear()
        self.session_date = session_date

    def process(self, event: CanonicalEvent) -> SweepUpdate:
        correction_target = event.payload.get("correction_of")
        cancellation_target = event.payload.get("cancel_of")
        if correction_target is not None:
            return self._apply_correction(event, correction_target, False)
        if cancellation_target is not None:
            return self._apply_correction(event, cancellation_target, True)
        contract = event.payload.get("contract")
        if not event.payload.get("eligible_trade") or not isinstance(contract, dict):
            return self._update(None, None)
        if str(contract.get("right", "")).upper() != "C":
            return self._update(None, None)
        key = _key(event)
        if key is None or not regular_session(event.source_timestamp):
            return self._update(None, "SWEEP_INVALID_IDENTITY")
        closed_audits: list[dict[str, Any]] = []
        cluster = self.active.get(key)
        if cluster is not None and event.source_timestamp < cluster.open_timestamp:
            return self._update(None, "SWEEP_OUT_OF_ORDER_BEFORE_OPEN")
        if cluster is None or event.source_timestamp > cluster.close_timestamp:
            if cluster is not None:
                cluster.final = True
                self.closed[cluster.cluster_id] = cluster
                closed_audits.append(self._audit(cluster, "CLUSTER_CLOSED"))
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
        self._update_session(cluster, metrics)
        reason = None
        transition_audits: list[dict[str, Any]] = []
        if before != cluster.qualifying:
            reason = "NEW_QUALIFYING_SWEEP" if cluster.qualifying else "SWEEP_QUALIFICATION_REVOKED"
            cluster.last_transition = reason
            transition_audits.append(self._audit(cluster, reason))
        audit = self._audit(cluster, None)
        return self._update(
            cluster,
            reason,
            audit,
            tuple(closed_audits),
            tuple(transition_audits),
        )

    def tick(self, now: datetime) -> SweepUpdate:
        now = now.astimezone(UTC)
        qualifying = [
            cluster
            for cluster in (*self.active.values(), *self.closed.values())
            if cluster.qualifying and not cluster.expired
        ]
        expired_timestamps: list[datetime] = []
        transition_audits: list[dict[str, Any]] = []
        for cluster in qualifying:
            if now - cluster.last_timestamp >= SWEEP_FRESHNESS:
                cluster.expired = True
                self.expired.add(cluster.cluster_id)
                expired_timestamps.append(cluster.last_timestamp + SWEEP_FRESHNESS)
                transition_audits.append(self._audit(cluster, "SWEEP_FRESHNESS_EXPIRED"))
        if transition_audits:
            return self._update(
                None,
                "SWEEP_FRESHNESS_EXPIRED",
                transition_audits=tuple(transition_audits),
                expired_timestamps=tuple(sorted(expired_timestamps)),
            )
        return self._update(None, None)

    def close_session(self) -> tuple[dict[str, Any], ...]:
        audits: list[dict[str, Any]] = []
        for cluster in self.active.values():
            cluster.final = True
            self.closed[cluster.cluster_id] = cluster
            audits.append(self._audit(cluster, "CLUSTER_CLOSED"))
        return tuple(audits)

    def _apply_correction(
        self, event: CanonicalEvent, target: object, cancellation: bool
    ) -> SweepUpdate:
        try:
            target_id = UUID(str(target))
        except ValueError:
            return self._update(None, "SWEEP_UNCORRELATED_CORRECTION")
        for cluster in tuple(self.active.values()) + tuple(self.closed.values()):
            original = cluster.trades.get(target_id)
            if original is None:
                continue
            before = cluster.qualifying
            if cancellation:
                del cluster.trades[target_id]
                cluster.cancellation_records.append(
                    {"original_record": _record(original), "corrective_record": _record(event)}
                )
                transition_name = "SWEEP_CANCELLATION"
            else:
                corrected_payload = {
                    **original.payload,
                    **event.payload,
                    "correction_of": None,
                    "original_trade_id": str(target_id),
                    "corrective_record_id": str(event.event_id),
                    "correction_provenance": {
                        "original_event_id": str(original.event_id),
                        "corrective_event_id": str(event.event_id),
                    },
                }
                corrected = event.model_copy(
                    update={"event_id": target_id, "payload": corrected_payload}
                )
                cluster.trades[target_id] = corrected
                cluster.correction_records.append(
                    {"original_record": _record(original), "corrective_record": _record(event)}
                )
                transition_name = "SWEEP_CORRECTION"
            metrics = cluster.recompute()
            cluster.qualifying = all(metrics["thresholds"].values())
            self._update_session(cluster, metrics)
            transition_audits = [self._audit(cluster, transition_name)]
            reason = None
            if before != cluster.qualifying:
                reason = (
                    "NEW_QUALIFYING_SWEEP" if cluster.qualifying else "SWEEP_QUALIFICATION_REVOKED"
                )
                cluster.last_transition = reason
                transition_audits.append(self._audit(cluster, reason))
            return self._update(
                cluster,
                reason,
                self._audit(cluster, None),
                transition_audits=tuple(transition_audits),
            )
        return self._update(None, "SWEEP_UNCORRELATED_CORRECTION")

    def _update_session(self, cluster: SweepCluster, metrics: dict[str, Any]) -> None:
        if cluster.qualifying:
            self.session_qualifying[cluster.cluster_id] = Decimal(
                metrics["aggregate_eligible_premium"]
            )
        else:
            self.session_qualifying.pop(cluster.cluster_id, None)

    def _audit(self, cluster: SweepCluster, transition: str | None) -> dict[str, Any]:
        self._transition_order += 1 if transition is not None else 0
        if transition is not None:
            cluster.transition_order = self._transition_order
            cluster.last_transition = transition
        audit = cluster.recompute()
        audit["transition"] = transition
        audit["transition_order"] = cluster.transition_order if transition is not None else None
        return audit

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
        transition_audits: tuple[dict[str, Any], ...] = (),
        expired_timestamps: tuple[datetime, ...] = (),
    ) -> SweepUpdate:
        latest = max(
            (
                value.last_timestamp
                for value in (*self.active.values(), *self.closed.values())
                if value.qualifying and not value.expired
            ),
            default=None,
        )
        premium = sum(self.session_qualifying.values(), Decimal(0))
        return SweepUpdate(
            cluster,
            reason,
            audit if audit is not None else (self._audit(cluster, None) if cluster else None),
            len(self.session_qualifying),
            premium,
            latest,
            closed_audits,
            transition_audits,
            expired_timestamps,
        )
