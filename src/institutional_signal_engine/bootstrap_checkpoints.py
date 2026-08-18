"""Load validated historical bootstrap checkpoints into the live data port."""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from .historical import HistoricalBootstrap
from .impact import ImpactBaseline
from .indicators import PreviousClose


class BootstrapCheckpointError(ValueError):
    """Raised when checkpoint evidence cannot prove complete coverage."""


def _read_checkpoint(path: Path, expected_symbol: str, expected_session: str) -> dict[str, Any]:
    if not path.is_file():
        raise BootstrapCheckpointError(f"missing checkpoint: {path.name}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise BootstrapCheckpointError(f"invalid checkpoint JSON: {path.name}") from exc
    if not isinstance(payload, dict):
        raise BootstrapCheckpointError(f"checkpoint is not an object: {path.name}")
    recorded = payload.get("sha256")
    if not isinstance(recorded, str) or not recorded:
        raise BootstrapCheckpointError(f"checkpoint hash missing: {path.name}")
    unsigned = dict(payload)
    unsigned.pop("sha256", None)
    canonical = json.dumps(unsigned, sort_keys=True, separators=(",", ":"))
    actual = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    if actual != recorded:
        raise BootstrapCheckpointError(f"checkpoint hash mismatch: {path.name}")
    if payload.get("schema") != "stol-bootstrap-symbol-v1":
        raise BootstrapCheckpointError(f"checkpoint schema mismatch: {path.name}")
    if payload.get("symbol") != expected_symbol:
        raise BootstrapCheckpointError(f"checkpoint symbol mismatch: {path.name}")
    if payload.get("session") != expected_session:
        raise BootstrapCheckpointError(f"checkpoint session mismatch: {path.name}")
    return payload


def _impact_baselines(payload: dict[str, Any], symbol: str) -> dict[tuple[str, int], ImpactBaseline]:
    raw = payload.get("impact_baselines")
    if not isinstance(raw, dict) or not raw:
        raise BootstrapCheckpointError(f"impact baselines missing: {symbol}")
    result: dict[tuple[str, int], ImpactBaseline] = {}
    for raw_key, raw_value in raw.items():
        if not isinstance(raw_key, str) or "|" not in raw_key or not isinstance(raw_value, dict):
            raise BootstrapCheckpointError(f"invalid impact baseline key: {symbol}")
        baseline_symbol, raw_minute = raw_key.split("|", 1)
        if baseline_symbol != symbol:
            raise BootstrapCheckpointError(f"impact baseline symbol mismatch: {symbol}")
        try:
            minute = int(raw_minute)
            baseline = ImpactBaseline(
                symbol=str(raw_value["symbol"]),
                horizon_minutes=int(raw_value["horizon_minutes"]),
                expected_volume=Decimal(str(raw_value["expected_volume"])),
                expected_volatility=Decimal(str(raw_value["expected_volatility"])),
                effective_date=date.fromisoformat(str(raw_value["effective_date"])),
                sample_size=int(raw_value["sample_size"]),
                provenance=str(raw_value["provenance"]),
                adjustment_metadata=str(raw_value["adjustment_metadata"]),
                source=str(raw_value.get("source", "ALPACA_HISTORICAL_BARS")),
                version=str(raw_value.get("version", "impact-baseline-5m-v1")),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise BootstrapCheckpointError(f"invalid impact baseline: {symbol}|{raw_minute}") from exc
        if baseline.symbol != symbol:
            raise BootstrapCheckpointError(f"impact baseline payload symbol mismatch: {symbol}")
        result[(symbol, minute)] = baseline
    return result


def load_historical_bootstrap(
    checkpoint_dir: Path | str,
    required_symbols: list[str] | tuple[str, ...],
    session: date,
) -> HistoricalBootstrap:
    """Reconstruct a complete HistoricalBootstrap from validated checkpoints."""
    root = Path(checkpoint_dir)
    symbols_dir = root / "symbols"
    if not symbols_dir.is_dir():
        raise BootstrapCheckpointError(f"checkpoint directory missing: {symbols_dir}")
    requested = tuple(sorted({str(symbol).upper() for symbol in required_symbols}))
    session_key = session.isoformat()
    previous_closes: dict[str, PreviousClose] = {}
    cumulative_profiles: dict[str, dict[int, tuple[Decimal, ...]]] = {}
    completed_highs: dict[str, dict[str, Decimal]] = {}
    impact_baselines: dict[tuple[str, int], ImpactBaseline] = {}
    source_provenance: str | None = None
    adjustment: str | None = None

    for symbol in requested:
        payload = _read_checkpoint(symbols_dir / f"{symbol}.json", symbol, session_key)
        try:
            previous = payload["previous_close"]
            previous_close = PreviousClose(
                symbol=symbol,
                close=Decimal(str(previous["close"])),
                as_of=datetime.fromisoformat(f"{previous['day']}T00:00:00+00:00"),
                provenance=str(previous["provenance"]),
            )
            profile_payload = payload["cumulative_profile"]
            highs_payload = payload["completed_highs"]
            if not isinstance(profile_payload, dict) or not isinstance(highs_payload, dict):
                raise TypeError("profile or high payload is not an object")
            profiles = {
                int(minute): tuple(Decimal(str(value)) for value in values)
                for minute, values in profile_payload.items()
            }
            highs = {str(day): Decimal(str(value)) for day, value in highs_payload.items()}
            symbol_impact = _impact_baselines(payload, symbol)
            symbol_source = str(payload["source_provenance"])
            symbol_adjustment = str(payload["adjustment"])
        except (KeyError, TypeError, ValueError) as exc:
            raise BootstrapCheckpointError(f"invalid checkpoint payload: {symbol}") from exc
        if not profiles or not highs:
            raise BootstrapCheckpointError(f"incomplete checkpoint payload: {symbol}")
        if source_provenance is None:
            source_provenance = symbol_source
            adjustment = symbol_adjustment
        elif source_provenance != symbol_source or adjustment != symbol_adjustment:
            raise BootstrapCheckpointError(f"checkpoint provenance mismatch: {symbol}")
        previous_closes[symbol] = previous_close
        cumulative_profiles[symbol] = profiles
        completed_highs[symbol] = highs
        impact_baselines.update(symbol_impact)

    if not requested or len(previous_closes) != len(requested):
        raise BootstrapCheckpointError("required symbol coverage is incomplete")
    return HistoricalBootstrap(
        session=session_key,
        previous_closes=previous_closes,
        cumulative_profiles=cumulative_profiles,
        completed_highs=completed_highs,
        adjustment=adjustment or "split",
        source_provenance=source_provenance or "alpaca:stocks/bars:completed-regular-sessions",
        impact_baselines=impact_baselines,
    )
