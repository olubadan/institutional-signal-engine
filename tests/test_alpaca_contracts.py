from datetime import UTC, datetime
from decimal import Decimal
from typing import Self
from uuid import uuid4

import httpx
import pytest

from institutional_signal_engine.contract_mapping import (
    AlpacaOptionContract,
    CanonicalOptionIdentity,
    decode_occ_symbol,
    map_alpaca_contract,
    round_trip_validate,
)
from institutional_signal_engine.providers.alpaca_options import AlpacaOptionsContractProvider
from institutional_signal_engine.providers.thetadata import ThetaContract, ThetaDataOptionsProvider
from institutional_signal_engine.universe import AlpacaContractSelector, UniverseManifest

RECEIVED = datetime(2026, 8, 6, 13, 0, tzinfo=UTC)


def payload(**overrides: object) -> dict[str, object]:
    value: dict[str, object] = {
        "id": "contract-1",
        "symbol": "AAPL  260807C00310000",
        "underlying_symbol": "AAPL",
        "root_symbol": "AAPL",
        "expiration_date": "2026-08-07",
        "strike_price": "310.00",
        "type": "call",
        "style": "american",
        "status": "active",
        "tradable": True,
        "open_interest": 5000,
        "open_interest_date": "2026-08-05",
        "close_price": "4.00",
        "close_price_date": "2026-08-05",
        "multiplier": 100,
    }
    value.update(overrides)
    return value


def contract(**overrides: object) -> AlpacaOptionContract:
    return AlpacaOptionContract.from_payload(payload(**overrides), RECEIVED)


def test_occ_decoding_and_exact_decimal_mapping_round_trip():
    decoded = decode_occ_symbol("AAPL  260807C00310000")
    assert decoded == CanonicalOptionIdentity("AAPL", 20260807, 310000, "C")
    assert decode_occ_symbol("AAPL260807C00310000") == decoded
    result = round_trip_validate(map_alpaca_contract(contract()))
    assert result.accepted
    assert result.theta_contract == ThetaContract("AAPL", 20260807, 310000, "C")


def test_mapping_record_preserves_acceptance_and_original_fields():
    result = map_alpaca_contract(contract())
    record = result.record()
    assert record["accepted"] is True
    assert record["contract_id"] == "contract-1"
    assert record["original_fields"]["symbol"] == "AAPL  260807C00310000"
    assert record["mapping_version"] == "alpaca-occ-thetadata-v1"


def test_fractional_strike_uses_decimal_without_float():
    result = map_alpaca_contract(
        contract(
            symbol="AAPL  260807C00310125",
            strike_price="310.125",
        )
    )
    assert result.accepted
    assert result.canonical is not None and result.canonical.strike == 310125


def test_alpaca_selection_uses_dated_oi_liquidity_and_moneyness():
    selected = AlpacaContractSelector().select(
        (
            map_alpaca_contract(
                contract(
                    average_options_volume=2500,
                    bid="3.90",
                    ask="4.10",
                    open_interest_date="2026-07-31",
                )
            ),
            map_alpaca_contract(
                contract(
                    id="contract-2",
                    symbol="AAPL  260801C00310000",
                    expiration_date="2026-08-01",
                    average_options_volume=2500,
                    bid="3.90",
                    ask="4.10",
                    open_interest_date="2026-07-31",
                )
            ),
        ),
        {"AAPL": Decimal(310)},
        RECEIVED.date().replace(month=7, day=31),
    )
    assert selected[0].included
    assert selected[0].expiration == 20260807
    assert selected[0].contracts[0].strike == 310000


def test_alpaca_selection_records_unavailable_average_volume():
    result = map_alpaca_contract(contract(bid="3.90", ask="4.10", open_interest_date="2026-07-31"))
    selected = AlpacaContractSelector().select(
        (result,), {"AAPL": Decimal(310)}, RECEIVED.date().replace(month=7, day=31)
    )
    assert not selected[0].included
    assert "average_options_volume_unavailable" in selected[0].rejection_reasons


def test_alpaca_selection_reports_symbols_with_no_returned_contracts():
    selected = AlpacaContractSelector().select(
        (), {"AAPL": Decimal(310)}, RECEIVED.date(), symbols=("AAPL", "MSFT")
    )
    assert [item.symbol for item in selected] == ["AAPL", "MSFT"]
    assert selected[0].rejection_reasons == ("no_alpaca_contracts_returned",)
    assert selected[1].rejection_reasons == ("no_alpaca_contracts_returned",)


@pytest.mark.parametrize(
    ("field", "value", "failed_field"),
    (
        ("underlying_symbol", "MSFT", "underlying_symbol"),
        ("root_symbol", "MSFT", "root"),
        ("expiration_date", "2026-08-14", "expiration"),
        ("strike_price", "311.00", "strike"),
        ("type", "put", "right"),
    ),
)
def test_field_and_occ_identity_mismatches_fail_closed(
    field: str, value: object, failed_field: str
):
    result = map_alpaca_contract(contract(**{field: value}))
    assert not result.accepted
    assert result.failed_field == failed_field
    assert result.source.original_fields["id"] == "contract-1"


def test_unsupported_deliverable_is_rejected():
    result = map_alpaca_contract(contract(multiplier=10))
    assert not result.accepted
    assert result.failed_field == "multiplier"
    assert result.rejection_reason == "unsupported_deliverable"


def test_incomplete_contract_is_rejected_before_mapping():
    with pytest.raises(ValueError, match="missing_fields"):
        AlpacaOptionContract.from_payload({"id": "missing"}, RECEIVED)


@pytest.mark.asyncio
async def test_alpaca_discovery_follows_all_pages(monkeypatch: pytest.MonkeyPatch):
    pages = [
        {"option_contracts": [payload()], "next_page_token": "page-2"},
        {
            "option_contracts": [
                payload(
                    id="contract-2",
                    symbol="MSFT  260807C00310000",
                    underlying_symbol="MSFT",
                    root_symbol="MSFT",
                )
            ],
            "next_page_token": None,
        },
    ]

    class FakeResponse:
        status_code = 200

        def __init__(self, body: dict[str, object]):
            self.body = body

        def json(self) -> dict[str, object]:
            return self.body

    class FakeClient:
        async def __aenter__(self) -> Self:
            return self

        async def __aexit__(self, *args: object) -> None:
            return None

        async def get(self, path: str, **kwargs: object) -> FakeResponse:
            assert path == "/v2/options/contracts"
            del kwargs
            return FakeResponse(pages.pop(0))

    monkeypatch.setattr(httpx, "AsyncClient", lambda **kwargs: FakeClient())
    provider = AlpacaOptionsContractProvider("https://example.invalid", "key", "secret")
    discovered = [item async for item in provider.discover_active_calls(("AAPL", "MSFT"), limit=1)]
    assert [item.contract_id for item in discovered] == ["contract-1", "contract-2"]
    assert len(pages) == 0


def test_theta_standard_plan_supports_trade_and_quote_without_bulk():
    contract_identity = ThetaContract("AAPL", 20260807, 310000, "C")
    provider = ThetaDataOptionsProvider(
        "ws://127.0.0.1:25520/v1/events",
        "secret",
        contracts=(contract_identity,),
        request_types=("TRADE", "QUOTE"),
    )
    requests = provider.subscription_payloads()
    assert [request["req_type"] for request in requests] == ["TRADE", "QUOTE"]
    assert all(request["msg_type"] == "STREAM" for request in requests)
    assert "STREAM_BULK" not in repr(requests)


def test_theta_event_for_unacknowledged_contract_is_rejected():
    contract_identity = ThetaContract("AAPL", 20260807, 310000, "C")
    provider = ThetaDataOptionsProvider(
        "ws://127.0.0.1:25520/v1/events", "secret", contracts=(contract_identity,)
    )
    provider.connected = True
    event = provider._normalize(
        {
            "header": {"type": "TRADE"},
            "contract": {
                "root": "AAPL",
                "expiration": 20260807,
                "strike": 310000,
                "right": "C",
            },
            "trade": {"date": 20260806, "ms_of_day": 36000000, "size": 1, "price": 1},
        }
    )
    assert event is None
    assert "unacknowledged_contract_event" in provider.diagnostics


def test_manifest_record_is_stable_for_replay():
    identity = ThetaContract("AAPL", 20260807, 310000, "C")
    from institutional_signal_engine.universe import UniverseSelection

    manifest = UniverseManifest(
        uuid4(),
        RECEIVED.date(),
        ("AAPL",),
        (UniverseSelection("AAPL", True, 20260807, (identity,), (), (), ()),),
        (identity,),
        mapping_records=(map_alpaca_contract(contract()).record(),),
    )
    record = manifest.record()
    assert record["subscription_plan"] == [
        {"root": "AAPL", "expiration": 20260807, "strike": 310000, "right": "C"}
    ]
    assert record["mapping_records"][0]["contract_id"] == "contract-1"
