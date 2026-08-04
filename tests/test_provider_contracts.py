from datetime import UTC

import pytest

from institutional_signal_engine.config import Settings
from institutional_signal_engine.providers.alpaca import AlpacaEquitiesProvider
from institutional_signal_engine.providers.common import ProviderError
from institutional_signal_engine.providers.thetadata import ThetaDataOptionsProvider


def test_alpaca_normalizes_trade_without_secret_output():
    provider = AlpacaEquitiesProvider("wss://example.invalid", "key", "secret")
    event = provider._normalize(
        {"T": "t", "S": "AAPL", "t": "2026-01-01T14:30:00Z", "i": 4, "p": 100, "s": 1000}
    )
    assert event is not None and event.symbol == "AAPL"
    assert event.source_timestamp.tzinfo == UTC
    assert "secret" not in repr(event)


def test_alpaca_feed_name_is_not_interpreted_as_a_websocket_url():
    settings = Settings.from_env({"ALPACA_DATA_FEED": "iex"})
    assert settings.alpaca_data_feed == "iex"
    assert settings.alpaca_data_url == "wss://stream.data.alpaca.markets/v2/iex"

    with pytest.raises(ValueError, match="feed name"):
        Settings.from_env({"ALPACA_DATA_FEED": "wss://malformed.example"})


def test_provider_health_state_starts_unauthenticated():
    alpaca = AlpacaEquitiesProvider("wss://example.invalid", "key", "secret")
    theta = ThetaDataOptionsProvider("ws://127.0.0.1:25520/v1/events", "secret")
    assert not alpaca.authenticated
    assert not theta.connected
    assert not theta.subscription_acknowledged


def test_alpaca_authentication_waits_past_connected_acknowledgement():
    assert not AlpacaEquitiesProvider._authentication_result([{"T": "success", "msg": "connected"}])
    assert AlpacaEquitiesProvider._authentication_result([{"T": "success", "msg": "authenticated"}])
    with pytest.raises(ProviderError):
        AlpacaEquitiesProvider._authentication_result([{"T": "error", "code": 401}])


def test_thetadata_normalizes_official_trade_shape():
    provider = ThetaDataOptionsProvider("ws://127.0.0.1:25520/v1/events", "secret")
    event = provider._normalize(
        {
            "header": {"type": "TRADE", "status": "CONNECTED"},
            "contract": {"root": "AAPL"},
            "trade": {
                "ms_of_day": 3600000,
                "sequence": -7,
                "size": 12,
                "price": 1.25,
                "date": 20260101,
            },
        }
    )
    assert event is not None and event.kind.value == "options"
    assert event.sequence == 4294967289
    assert event.payload["call_premium"] == 1500


def test_thetadata_records_subscription_acknowledgement_without_payload():
    provider = ThetaDataOptionsProvider("ws://127.0.0.1:25520/v1/events", "secret")

    assert provider._observe_control({"header": {"type": "REQ_RESPONSE", "status": "CONNECTED"}})
    assert provider.subscription_acknowledged
    assert provider.stream_status == "connected"
