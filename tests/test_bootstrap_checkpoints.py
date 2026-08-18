from __future__ import annotations

import hashlib
import json
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from institutional_signal_engine.bootstrap_checkpoints import (
    BootstrapCheckpointError,
    load_historical_bootstrap,
)

SESSION = date(2026, 8, 18)


def _checkpoint(symbol: str) -> dict[str, object]:
    return {
        "schema": "stol-bootstrap-symbol-v1",
        "symbol": symbol,
        "session": SESSION.isoformat(),
        "adjustment": "split",
        "source_provenance": "alpaca:stocks/bars:completed-regular-sessions",
        "previous_close": {
            "close": "100.25",
            "day": "2026-08-17",
            "provenance": "alpaca:stocks/bars:adjustment=split",
        },
        "completed_highs": {"2026-08-17": "101.00"},
        "cumulative_profile": {"0": ["10", "20"]},
        "impact_baselines": {
            f"{symbol}|0": {
                "symbol": symbol,
                "horizon_minutes": 5,
                "expected_volume": "1000",
                "expected_volatility": "0.01",
                "effective_date": SESSION.isoformat(),
                "sample_size": 20,
                "provenance": "alpaca:stocks/bars:completed-regular-sessions",
                "adjustment_metadata": "split",
                "source": "ALPACA_HISTORICAL_BARS",
                "version": "impact-baseline-5m-v1",
            }
        },
    }


def _write_fixture(root: Path, symbols: list[str]) -> None:
    symbols_dir = root / "symbols"
    symbols_dir.mkdir()
    for symbol in symbols:
        body = _checkpoint(symbol)
        canonical = json.dumps(body, sort_keys=True, separators=(",", ":"))
        payload = {**body, "sha256": hashlib.sha256(canonical.encode()).hexdigest()}
        (symbols_dir / f"{symbol}.json").write_text(json.dumps(payload))


def test_loader_reconstructs_typed_historical_bootstrap(tmp_path) -> None:
    _write_fixture(tmp_path, ["AAPL"])

    historical = load_historical_bootstrap(tmp_path, ("AAPL",), SESSION)

    assert historical.previous_close("AAPL", SESSION.isoformat()).close == Decimal("100.25")
    assert historical.cumulative_volume_baseline("AAPL", 0) == (Decimal(10), Decimal(20))
    assert historical.completed_session_highs("AAPL", SESSION.isoformat())["2026-08-17"] == Decimal(
        "101.00"
    )
    assert historical.impact_baselines[("AAPL", 0)].sample_size == 20


def test_loader_reconstructs_full_candidate_coverage(tmp_path) -> None:
    symbols = [f"S{index:03d}" for index in range(501)]
    _write_fixture(tmp_path, symbols)

    historical = load_historical_bootstrap(tmp_path, tuple(symbols), SESSION)

    assert len(historical.previous_closes) == 501
    assert len(historical.impact_baselines) == 501


def test_loader_rejects_tampered_checkpoint(tmp_path) -> None:
    _write_fixture(tmp_path, ["AAPL"])
    path = tmp_path / "symbols" / "AAPL.json"
    payload = json.loads(path.read_text())
    payload["previous_close"]["close"] = "999"
    path.write_text(json.dumps(payload))

    with pytest.raises(BootstrapCheckpointError, match="hash mismatch"):
        load_historical_bootstrap(tmp_path, ("AAPL",), SESSION)


def test_loader_rejects_missing_required_symbol(tmp_path) -> None:
    _write_fixture(tmp_path, ["AAPL"])

    with pytest.raises(BootstrapCheckpointError, match="missing checkpoint"):
        load_historical_bootstrap(tmp_path, ("AAPL", "MSFT"), SESSION)
