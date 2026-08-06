from datetime import UTC
from decimal import Decimal

import pytest

from institutional_signal_engine.config import Settings
from institutional_signal_engine.providers.alpaca import AlpacaEquitiesProvider
from institutional_signal_engine.providers.common import ProviderError
from institutional_signal_engine.providers.thetadata import ThetaContract, ThetaDataOptionsProvider


def test_runtime_file_loader_treats_shell_sensitive_values_as_data(tmp_path):
    runtime = tmp_path / "runtime.env"
    runtime.write_text(
        """TRADING_ENABLED=false
ALPACA_API_KEY_ID=key;$(must_not_execute)
ALPACA_API_SECRET_KEY=secret # literal; `text`
THETADATA_API_KEY=theta$token\\nwith-space
OPTIONS_API_BASE_URL=ws://127.0.0.1:25520/v1/events
""",
        encoding="utf-8",
    )
    settings = Settings.from_env_file(runtime)
    assert settings.alpaca_key_id is not None
    assert settings.alpaca_key_id.get_secret_value() == "key;$(must_not_execute)"
    assert settings.alpaca_secret_key is not None
    assert settings.alpaca_secret_key.get_secret_value() == "secret # literal; `text`"
    assert settings.theta_api_key is not None
    assert settings.theta_api_key.get_secret_value() == "theta$token\\nwith-space"
    assert not settings.trading_enabled


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
            "contract": {
                "root": "AAPL",
                "expiration": 20260102,
                "strike": 100000,
                "right": "C",
            },
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
    assert event.payload["trade_size"] == 12
    assert event.payload["trade_price"] == 1.25


def test_thetadata_records_subscription_acknowledgement_without_payload():
    provider = ThetaDataOptionsProvider("ws://127.0.0.1:25520/v1/events", "secret")

    assert provider._observe_control({"header": {"type": "REQ_RESPONSE", "status": "CONNECTED"}})
    assert not provider.subscription_acknowledged
    assert "unmatched_request_response" in provider.diagnostics


def test_thetadata_correlates_acknowledgement_to_outstanding_request():
    contract = ThetaContract("AAPL", 20260807, 310000, "C")
    provider = ThetaDataOptionsProvider(
        "ws://127.0.0.1:25520/v1/events", "secret", contracts=(contract,)
    )
    request = provider.subscription_payloads()[0]
    assert provider._observe_control(
        {
            "header": {
                "type": "REQ_RESPONSE",
                "status": "CONNECTED",
                "response": "SUBSCRIBED",
                "req_id": request["id"],
            }
        }
    )
    assert provider.subscription_acknowledged
    assert request["id"] not in provider.outstanding
    assert provider._observe_control(
        {
            "header": {
                "type": "REQ_RESPONSE",
                "status": "CONNECTED",
                "response": "SUBSCRIBED",
                "req_id": request["id"],
            }
        }
    )
    assert "duplicate_request_response" in provider.diagnostics


def test_thetadata_correlates_official_header_request_id_shape():
    contract = ThetaContract("AAPL", 20260807, 310000, "C")
    provider = ThetaDataOptionsProvider(
        "ws://127.0.0.1:25520/v1/events", "secret", contracts=(contract,)
    )
    request = provider.subscription_payloads()[0]
    assert provider._observe_control(
        {
            "header": {
                "type": "REQ_RESPONSE",
                "status": "CONNECTED",
                "req_id": request["id"],
                "response": "SUBSCRIBED",
            }
        }
    )
    assert provider.subscription_acknowledged
    assert request["id"] not in provider.outstanding


def test_thetadata_status_frames_are_independent_keepalives():
    contract = ThetaContract("AAPL", 20260807, 310000, "C")
    provider = ThetaDataOptionsProvider(
        "ws://127.0.0.1:25520/v1/events", "secret", contracts=(contract,)
    )
    request = provider.subscription_payloads()[0]
    assert provider._observe_control({"header": {"type": "STATUS", "status": "CONNECTED"}})
    assert not provider.subscription_acknowledged
    assert provider._observe_control(
        {
            "header": {
                "type": "REQ_RESPONSE",
                "status": "CONNECTED",
                "response": "SUBSCRIBED",
                "req_id": request["id"],
            }
        }
    )
    assert provider.subscription_acknowledged


@pytest.mark.parametrize("response", ("ERROR", "MAX_STREAMS_REACHED", "INVALID_PERMS"))
def test_thetadata_supported_rejection_responses_are_recorded(response: str):
    contract = ThetaContract("AAPL", 20260807, 310000, "C")
    provider = ThetaDataOptionsProvider(
        "ws://127.0.0.1:25520/v1/events", "secret", contracts=(contract,)
    )
    request = provider.subscription_payloads()[0]
    assert provider._observe_control(
        {
            "header": {
                "type": "REQ_RESPONSE",
                "status": "CONNECTED",
                "response": response,
                "req_id": request["id"],
            }
        }
    )
    assert f"request_rejected:{response.lower()}" in provider.diagnostics
    assert not provider.subscription_acknowledged


def test_thetadata_unknown_id_duplicate_and_unknown_response_fail_closed():
    provider = ThetaDataOptionsProvider("ws://127.0.0.1:25520/v1/events", "secret")
    assert provider._observe_control(
        {
            "header": {
                "type": "REQ_RESPONSE",
                "status": "CONNECTED",
                "response": "SUBSCRIBED",
                "req_id": 99,
            }
        }
    )
    assert "unmatched_request_response" in provider.diagnostics

    contract = ThetaContract("AAPL", 20260807, 310000, "C")
    provider = ThetaDataOptionsProvider(
        "ws://127.0.0.1:25520/v1/events", "secret", contracts=(contract,)
    )
    request = provider.subscription_payloads()[0]
    response = {
        "header": {
            "type": "REQ_RESPONSE",
            "status": "CONNECTED",
            "response": "SUBSCRIBED",
            "req_id": request["id"],
        }
    }
    assert provider._observe_control(response)
    assert provider._observe_control(response)
    assert "duplicate_request_response" in provider.diagnostics
    second = provider.subscription_payloads()[0]
    assert provider._observe_control(
        {
            "header": {
                "type": "REQ_RESPONSE",
                "status": "CONNECTED",
                "response": "NOT_SUPPORTED",
                "req_id": second["id"],
            }
        }
    )
    assert "unknown_request_response" in provider.diagnostics


def test_thetadata_standard_exact_contract_payload_and_strike_conversion():
    contract = ThetaContract.from_dollars("AAPL", 20260807, Decimal("310.00"), "C")
    provider = ThetaDataOptionsProvider(
        "ws://127.0.0.1:25520/v1/events", "secret", contracts=(contract,)
    )
    request = provider.subscription_payloads()[0]
    assert request["msg_type"] == "STREAM"
    assert request["sec_type"] == "OPTION"
    assert request["req_type"] == "TRADE"
    assert request["add"] is True and request["id"] == 1
    assert request["contract"] == {
        "root": "AAPL",
        "expiration": 20260807,
        "strike": 310000,
        "right": "C",
    }
    assert "STREAM_BULK" not in str(request)


def test_thetadata_ids_increment_restore_after_reconnect_and_unsubscribe():
    contract = ThetaContract("AAPL", 20260807, 310000, "C")
    provider = ThetaDataOptionsProvider(
        "ws://127.0.0.1:25520/v1/events", "secret", contracts=(contract,)
    )
    first = provider.subscription_payloads()
    restored = provider.subscription_payloads()
    unsubscribed = provider.unsubscribe_payload(contract)
    assert [request["id"] for request in (*first, *restored, unsubscribed)] == [1, 2, 3]
    assert restored[0]["contract"] == first[0]["contract"]
    assert unsubscribed["add"] is False
