from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

import pytest
from pydantic import SecretStr

from institutional_signal_engine.config import Settings
from institutional_signal_engine.historical import HistoricalBootstrap
from institutional_signal_engine.indicators import PreviousClose
from institutional_signal_engine.phase4_live_smoke import PILOT_SYMBOLS
from institutional_signal_engine.providers.alpaca_option_snapshots import OptionQuoteEvidence
from institutional_signal_engine.providers.thetadata import ThetaContract
from institutional_signal_engine.schemas import CanonicalEvent, EventKind


@pytest.mark.asyncio
async def test_assembled_phase4_entry_preserves_lineage_through_real_signal_runner(monkeypatch):
    import institutional_signal_engine.live_smoke as signal
    import institutional_signal_engine.phase4_live_smoke as runner

    now = datetime.now(UTC)
    session = now.date().isoformat()
    symbol = "AAPL"
    theta_contract = ThetaContract(symbol, int((now.date()).strftime("%Y%m%d")) + 10, 310000, "C")
    occ = f"AAPL{str(theta_contract.expiration)[2:]}C00310000"
    contract_payload = {
        "id": "trace-contract",
        "symbol": occ,
        "underlying_symbol": symbol,
        "expiration_date": f"{str(theta_contract.expiration)[:4]}-{str(theta_contract.expiration)[4:6]}-{str(theta_contract.expiration)[6:]}",
        "strike_price": "310",
        "type": "call",
        "status": "active",
        "tradable": True,
        "open_interest": 100,
    }
    settings = Settings(
        alpaca_key_id=SecretStr("fixture-key"),
        alpaca_secret_key=SecretStr("fixture-secret"),
        theta_api_key=SecretStr("fixture-theta"),
        database_url=None,
        phase4_pre_enrichment_max_per_symbol=100,
        phase4_max_enrichment_candidates=2000,
    )
    prices = {value: Decimal(310) for value in PILOT_SYMBOLS}

    def bootstrap(symbols: tuple[str, ...], date_value: object) -> HistoricalBootstrap:
        previous = {value: PreviousClose(value, Decimal(309), now, "fixture") for value in symbols}
        profiles = {value: {0: (Decimal(100), Decimal(100))} for value in symbols}
        highs = {
            value: {
                "prior_5_session_high": Decimal(320),
                "prior_20_session_high": Decimal(330),
                "prior_252_session_high": Decimal(340),
            }
            for value in symbols
        }
        return HistoricalBootstrap(session, previous, profiles, highs, "split-adjusted", "fixture")

    class FakeEquities:
        authenticated = True

        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        async def current_prices(self, _symbols: object) -> dict[str, Decimal]:
            return prices

        async def historical_bootstrap(
            self, symbols: object, date_value: object
        ) -> HistoricalBootstrap:
            return bootstrap(tuple(str(value) for value in symbols), date_value)

        async def events(self, symbols: object):
            for value in (symbol, "SPY", "XLK"):
                yield CanonicalEvent(
                    event_id=uuid4(),
                    kind=EventKind.EQUITY,
                    symbol=value,
                    source="alpaca",
                    source_timestamp=now,
                    received_timestamp=now,
                    normalized_timestamp=now,
                    sequence=1,
                    payload={"price": 310, "volume": 100_000, "conditions": ("@",)},
                )

    class FakeCatalog:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        async def discover_active_calls(self, *_args: object, **_kwargs: object):
            from institutional_signal_engine.contract_mapping import AlpacaOptionContract

            yield AlpacaOptionContract.from_payload(contract_payload, now)

    class FakeSnapshots:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        async def snapshots(self, records: object, **_kwargs: object):
            from institutional_signal_engine.contract_mapping import CanonicalOptionIdentity

            identity = CanonicalOptionIdentity(symbol, theta_contract.expiration, 310000, "C")
            return (
                OptionQuoteEvidence(
                    identity,
                    occ,
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

    class FakeTheta:
        connected = True
        authenticated = True
        stream_status = "healthy"
        subscription_acknowledged = True

        def __init__(
            self,
            *_args: object,
            contracts: object = (),
            request_types: object = (),
            stage_callback: object = None,
            **_kwargs: object,
        ) -> None:
            self.contracts = tuple(contracts)
            self.request_types = tuple(request_types)
            self.request_registry = {}
            self.acknowledged_ids = set()
            self.diagnostics = []
            self.rejected_event_diagnostics = []
            self.rejected_event_overflow = 0
            self.rejected_request_types = []
            self.acknowledged_contracts = {theta_contract}
            self.stage_callback = stage_callback

        async def events(self, _symbols: object):
            self.connected = True
            self.acknowledged_ids = set(range(1, len(self.contracts) * len(self.request_types) + 1))
            if self.stage_callback is not None:
                self.stage_callback({"record_type": "phase4_stage", "stage": "websocket_connected"})
                self.stage_callback(
                    {"record_type": "phase4_stage", "stage": "subscriptions_acknowledged"}
                )
            for kind, price in (("quote", 3.10), ("trade", 3.10)):
                yield CanonicalEvent(
                    event_id=uuid4(),
                    kind=EventKind.OPTIONS,
                    symbol=symbol,
                    source="thetadata",
                    source_timestamp=now,
                    received_timestamp=now,
                    normalized_timestamp=now,
                    sequence=1,
                    payload={
                        "provider_event_kind": kind,
                        "price": price,
                        "volume": 1000 if kind == "trade" else 0,
                        "contract": {
                            "root": symbol,
                            "expiration": theta_contract.expiration,
                            "strike": 310000,
                            "right": "C",
                        },
                        "quote_context": {"bid": 3.09, "ask": 3.10},
                        "exchange": "CBOE",
                        "conditions": ("@",),
                    },
                )

    monkeypatch.setattr(runner, "_load_settings", lambda: settings)
    monkeypatch.setattr(
        signal,
        "Settings",
        SimpleNamespace(from_env=lambda: settings, from_env_file=lambda _path: settings),
    )
    monkeypatch.setattr(runner, "AlpacaEquitiesProvider", FakeEquities)
    monkeypatch.setattr(signal, "AlpacaEquitiesProvider", FakeEquities)
    monkeypatch.setattr(runner, "AlpacaOptionsContractProvider", FakeCatalog)
    monkeypatch.setattr(runner, "AlpacaOptionSnapshotProvider", FakeSnapshots)
    monkeypatch.setattr(runner, "ThetaDataOpenInterestProvider", FakeOI)
    monkeypatch.setattr(signal, "ThetaDataOptionsProvider", FakeTheta)
    result = await runner.run(0.01, skip_oi_diagnostic=True)
    assert result["run_id"]
    assert int(result.get("synchronized_input_count", 0)) > 0
    assert int(result.get("decisions_persisted", 0)) > 0
    assert result["orders_constructed"] == 0
    assert result["orders_submitted"] == 0
    finalization = result["universe_finalization"]
    assert isinstance(finalization, dict)
    assert finalization["run_id"] == result["run_id"]
