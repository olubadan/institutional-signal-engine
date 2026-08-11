from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

import pytest
from pydantic import SecretStr

from institutional_signal_engine.config import Settings
from institutional_signal_engine.contract_mapping import CanonicalOptionIdentity
from institutional_signal_engine.phase4_live_smoke import PILOT_SYMBOLS
from institutional_signal_engine.providers.alpaca_option_snapshots import OptionQuoteEvidence


@pytest.mark.asyncio
async def test_assembled_phase4_runner_forwards_each_stage_once(monkeypatch: pytest.MonkeyPatch):
    import institutional_signal_engine.phase4_live_smoke as runner

    now = datetime.now(UTC)
    expiration = (now.date() + timedelta(days=10)).strftime("%Y-%m-%d")
    provider_symbol = f"AAPL{expiration.replace('-', '')[2:]}C00310000"
    source_payload = {
        "id": "fixture-contract",
        "symbol": provider_symbol,
        "underlying_symbol": "AAPL",
        "expiration_date": expiration,
        "strike_price": "310",
        "type": "call",
        "status": "active",
        "tradable": True,
        "open_interest": 100,
    }
    prices = {symbol: Decimal(310) for symbol in PILOT_SYMBOLS}
    settings = Settings(
        alpaca_key_id=SecretStr("fixture-key"),
        alpaca_secret_key=SecretStr("fixture-secret"),
        theta_api_key=SecretStr("fixture-theta"),
        database_url=None,
    )
    result_run_id = uuid4()

    class FakeEquities:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        async def current_prices(self, _symbols: tuple[str, ...]) -> dict[str, Decimal]:
            return prices

        async def historical_bootstrap(self, _symbols: tuple[str, ...], _session: object):
            return SimpleNamespace(impact_baselines={})

    class FakeCatalog:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        async def discover_active_calls(self, *_args: object, **_kwargs: object):
            yield runner.AlpacaOptionContract.from_payload(source_payload, now)

    class FakeSnapshots:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        async def snapshots(
            self, records: tuple[tuple[str, CanonicalOptionIdentity], ...], **_kwargs: object
        ):
            identity = records[0][1]
            return (
                OptionQuoteEvidence(
                    identity,
                    provider_symbol,
                    "OPRA",
                    now,
                    Decimal("3.09"),
                    10,
                    Decimal("3.10"),
                    10,
                    Decimal("3.10"),
                    now,
                    "fixture",
                ),
            )

    class FakeOI:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        async def mdss_status(self) -> dict[str, str]:
            return {"status": "CONNECTED"}

    async def fake_signal_smoke(*_args: object, stage_callback=None, **_kwargs: object):
        assert stage_callback is not None
        assert _kwargs["impact_baselines"] is None
        assert _kwargs["run_id"] == result_run_id
        for stage in (
            "websocket_connected",
            "subscriptions_acknowledged",
            "observation_started",
            "observation_completed",
            "persistence_drained",
            "report_emitted",
        ):
            stage_callback({"record_type": "phase4_stage", "stage": stage})
        return {
            "subscription_acknowledgement": {
                "request_registry": [],
                "rejected_event_diagnostics": [],
                "requests_by_type": {
                    "TRADE": {"acknowledged": 1, "rejected_or_unmatched": 0},
                    "QUOTE": {"acknowledged": 1, "rejected_or_unmatched": 0},
                },
                "rejected_or_unmatched": [],
            },
            "synchronized_symbols": ["AAPL"],
            "incomplete_state_reasons": {},
            "synchronized_input_count": 1,
            "candidates_evaluated": 1,
            "decisions_persisted": 1,
            "orders_constructed": 0,
            "orders_submitted": 0,
        }

    monkeypatch.setattr(runner, "_load_settings", lambda: settings)
    monkeypatch.setattr(runner, "AlpacaEquitiesProvider", FakeEquities)
    monkeypatch.setattr(runner, "AlpacaOptionsContractProvider", FakeCatalog)
    monkeypatch.setattr(runner, "AlpacaOptionSnapshotProvider", FakeSnapshots)
    monkeypatch.setattr(runner, "ThetaDataOpenInterestProvider", FakeOI)
    monkeypatch.setattr(runner, "run_signal_smoke", fake_signal_smoke)

    records: list[dict[str, object]] = []
    monkeypatch.setattr(runner, "uuid4", lambda: result_run_id)
    result = await runner.run(0.01, skip_oi_diagnostic=True, stage_callback=records.append)

    stages = [str(record["stage"]) for record in records]
    assert result["status"] == "live_observation_complete"
    assert result["synchronized_input_count"] == 1
    assert result["decisions_persisted"] == 1
    coverage = result["coverage"]
    assert isinstance(coverage, dict)
    assert coverage["counts"]["u_star"] == 1
    assert coverage["counts"]["u_r"] == 1
    assert coverage["counts"]["selected"] == 1
    assert stages.count("websocket_connected") == 1
    assert stages.count("subscriptions_acknowledged") == 1
    assert stages[-3:] == ["observation_completed", "persistence_drained", "report_emitted"]
    assert "configuration_loaded" in stages


def test_duplicate_stage_argument_regression_is_represented_by_record_adapter():
    recorder = []
    from institutional_signal_engine.startup import StageRecorder

    stage_recorder = StageRecorder(lambda record: recorder.append(record))
    stage_recorder.emit_record({"stage": "configuration_loaded", "stage_detail": "fixture"})
    assert recorder[0]["stage"] == "configuration_loaded"
    assert recorder[0]["stage_detail"] == "fixture"
