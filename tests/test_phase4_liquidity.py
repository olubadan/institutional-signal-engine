from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Self

import pytest

from institutional_signal_engine.contract_mapping import (
    AlpacaOptionContract,
    CanonicalOptionIdentity,
    map_alpaca_contract,
)
from institutional_signal_engine.liquidity import finalize_liquidity
from institutional_signal_engine.providers.alpaca_option_snapshots import (
    AlpacaOptionSnapshotProvider,
    OptionQuoteEvidence,
)
from institutional_signal_engine.providers.thetadata import ThetaContract
from institutional_signal_engine.providers.thetadata_open_interest import (
    ThetaDataOpenInterestProvider,
    _observed_previous_session,
)
from institutional_signal_engine.universe import UniverseSelection

NOW = datetime(2026, 8, 6, 14, 0, tzinfo=UTC)
CONTRACT = ThetaContract("AAPL", 20260814, 310000, "C")


def alpaca_payload() -> dict[str, object]:
    return {
        "id": "contract-1",
        "symbol": "AAPL260814C00310000",
        "underlying_symbol": "AAPL",
        "root_symbol": "AAPL",
        "expiration_date": "2026-08-14",
        "strike_price": "310",
        "type": "call",
        "status": "active",
        "tradable": True,
        "open_interest": 10,
        "open_interest_date": "2026-08-05",
        "multiplier": 100,
    }


def test_coarse_contract_can_only_be_final_after_both_evidence_sources():
    source = AlpacaOptionContract.from_payload(alpaca_payload(), NOW)
    mapping = map_alpaca_contract(source)
    selection = UniverseSelection("AAPL", True, 20260814, (CONTRACT,), (), (), ())
    result = finalize_liquidity((selection,), (mapping,), {}, {}, NOW)
    assert result.selections[0].included is False
    assert "quote_snapshot_missing" in str(result.records[0]["rejection_reasons"])


def test_quote_snapshot_validity_boundaries():
    quote = OptionQuoteEvidence(
        CanonicalOptionIdentity("AAPL", 20260814, 310000, "C"),
        "AAPL260814C00310000",
        "OPRA",
        NOW,
        Decimal("1.00"),
        1,
        Decimal("1.01"),
        1,
        Decimal("1.00"),
        NOW,
        "fixture",
    )
    assert quote.valid(NOW, 60)
    assert not quote.valid(NOW.replace(minute=14), 60)
    assert quote.spread == Decimal("0.01")


@pytest.mark.asyncio
async def test_alpaca_snapshot_batches_and_preserves_feed(monkeypatch: pytest.MonkeyPatch):
    class Response:
        status_code = 200

        def json(self) -> dict[str, object]:
            return {
                "AAPL260814C00310000": {
                    "latestQuote": {
                        "bp": "1.00",
                        "ap": "1.01",
                        "bs": 2,
                        "as": 3,
                        "t": "2026-08-06T14:00:00Z",
                    },
                    "latestTrade": {"p": "1.00", "t": "2026-08-06T13:59:59Z"},
                }
            }

    class Client:
        async def __aenter__(self) -> Self:
            return self

        async def __aexit__(self, *args: object) -> None:
            del args

        async def get(self, *args: object, **kwargs: object) -> Response:
            del args
            assert kwargs["params"]["feed"] == "opra"
            return Response()

    monkeypatch.setattr(
        "institutional_signal_engine.providers.alpaca_option_snapshots.httpx.AsyncClient",
        lambda **kwargs: Client(),
    )
    identity = CanonicalOptionIdentity("AAPL", 20260814, 310000, "C")
    provider = AlpacaOptionSnapshotProvider("https://example.invalid", "key", "secret")
    values = await provider.snapshots((("AAPL260814C00310000", identity),))
    assert len(values) == 1
    assert values[0].feed == "OPRA"


def test_theta_effective_date_skips_weekend():
    assert _observed_previous_session(date(2026, 8, 10)) == date(2026, 8, 7)
    assert _observed_previous_session(date(2026, 7, 6)) == date(2026, 7, 2)


@pytest.mark.asyncio
async def test_theta_oi_snapshot_identity_mismatch_fails_closed(monkeypatch: pytest.MonkeyPatch):
    class Response:
        status_code = 200

        def json(self) -> list[dict[str, object]]:
            return [
                {
                    "symbol": "MSFT",
                    "expiration": "2026-08-14",
                    "strike": 310.0,
                    "right": "call",
                    "timestamp": "2026-08-06T06:30:00.000",
                    "open_interest": 10,
                }
            ]

    class Client:
        async def __aenter__(self) -> Self:
            return self

        async def __aexit__(self, *args: object) -> None:
            del args

        async def get(self, *args: object, **kwargs: object) -> Response:
            del args, kwargs
            return Response()

    monkeypatch.setattr(
        "institutional_signal_engine.providers.thetadata_open_interest.httpx.AsyncClient",
        lambda **kwargs: Client(),
    )
    with pytest.raises(Exception, match="identity_mismatch"):
        await ThetaDataOpenInterestProvider().snapshot(CONTRACT)
