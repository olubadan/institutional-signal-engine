from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

from institutional_signal_engine.config import Settings
from institutional_signal_engine.contract_mapping import (
    AlpacaOptionContract,
    CanonicalOptionIdentity,
    map_alpaca_contract,
)
from institutional_signal_engine.liquidity import (
    PHASE4_OBSERVATION_MAX_ABSOLUTE_SPREAD,
    PHASE4_OBSERVATION_MAX_PROPORTIONAL_SPREAD,
    PHASE4_OBSERVATION_SPREAD_POLICY_VERSION,
    finalize_liquidity,
)
from institutional_signal_engine.pipeline import SignalPipeline
from institutional_signal_engine.providers.alpaca_option_snapshots import OptionQuoteEvidence
from institutional_signal_engine.providers.thetadata import ThetaContract, ThetaDataOptionsProvider
from institutional_signal_engine.schemas import CanonicalEvent, EventKind
from institutional_signal_engine.universe import UniverseSelection

NOW = datetime(2026, 8, 6, 14, 0, tzinfo=UTC)


def _event(
    kind: EventKind, symbol: str, sequence: int, payload: dict[str, object]
) -> CanonicalEvent:
    return CanonicalEvent(
        event_id=uuid4(),
        kind=kind,
        symbol=symbol,
        source="fixture",
        source_timestamp=NOW,
        received_timestamp=NOW,
        normalized_timestamp=NOW,
        sequence=sequence,
        payload=payload,
    )


def test_selected_symbols_synchronize_independently_without_aapl():
    pipeline = SignalPipeline(Settings(), symbols=("BAC", "NFLX", "NVDA"), now=lambda: NOW)
    for sequence, symbol in enumerate(("SPY", "XLK"), 1):
        kind = EventKind.MARKET_INDEX if symbol == "SPY" else EventKind.SECTOR_INDEX
        pipeline.process(_event(kind, symbol, sequence, {"delta": Decimal("0.01")}))
    for sequence, symbol in enumerate(("BAC", "NFLX", "NVDA"), 1):
        pipeline.process(
            _event(
                EventKind.EQUITY,
                symbol,
                sequence,
                {"price": Decimal(100), "delta": Decimal("0.01"), "volume": 100000},
            )
        )
        pipeline.process(
            _event(
                EventKind.OPTIONS,
                symbol,
                sequence,
                {
                    "option_volume": 10,
                    "open_interest": 100,
                    "call_premium": Decimal(200000),
                    "distance_to_resistance": Decimal("0.1"),
                },
            )
        )
    assert pipeline.incomplete_state_reasons == {"BAC": [], "NFLX": [], "NVDA": []}
    assert pipeline.synchronized_input_count == 3
    assert len(pipeline.decisions) == 3
    assert {
        decision.indicator_provenance["evaluation_symbol"] for decision in pipeline.decisions
    } == {"BAC", "NFLX", "NVDA"}


def test_observation_spread_policy_has_explicit_decimal_boundaries():
    assert PHASE4_OBSERVATION_SPREAD_POLICY_VERSION == "phase4-observation-spread-v1"
    assert PHASE4_OBSERVATION_MAX_ABSOLUTE_SPREAD == Decimal("0.05")
    assert PHASE4_OBSERVATION_MAX_PROPORTIONAL_SPREAD == Decimal("0.20")
    settings = Settings()
    assert settings.phase4_pre_enrichment_max_per_symbol == 100
    assert settings.phase4_max_enrichment_candidates == 2000


def test_observation_spread_diagnostics_preserve_policy_and_rejection_reason():
    contract = ThetaContract("AAPL", 20260814, 310000, "C")
    quote = OptionQuoteEvidence(
        CanonicalOptionIdentity("AAPL", 20260814, 310000, "C"),
        "AAPL260814C00310000",
        "OPRA",
        NOW,
        Decimal("0.03"),
        10,
        Decimal("0.08"),
        10,
        None,
        None,
        "fixture",
    )
    source = AlpacaOptionContract.from_payload(
        {
            "id": "fixture",
            "symbol": "AAPL260814C00310000",
            "underlying_symbol": "AAPL",
            "expiration_date": "2026-08-14",
            "strike_price": "310",
            "type": "call",
            "status": "active",
            "tradable": True,
        },
        NOW,
    )
    result = finalize_liquidity(
        (UniverseSelection("AAPL", True, 20260814, (contract,), (), (), ()),),
        (map_alpaca_contract(source),),
        {contract: quote},
        {},
        NOW,
        require_open_interest=False,
    )
    assert result.selections[0].included is False
    assert result.diagnostics[0]["policy_version"] == PHASE4_OBSERVATION_SPREAD_POLICY_VERSION
    assert result.diagnostics[0]["reason"] == "proportional_spread_threshold_failed"


def test_theta_rejected_event_diagnostics_are_bounded_and_sanitized():
    provider = ThetaDataOptionsProvider(
        "ws://127.0.0.1:25520/v1/events",
        "fixture",
        contracts=(ThetaContract("AAPL", 20260814, 310000, "C"),),
        diagnostic_cardinality=1,
    )
    provider.connected = True
    for strike in (310000, 311000):
        assert (
            provider._normalize(
                {
                    "header": {"type": "TRADE"},
                    "contract": {
                        "root": "AAPL",
                        "expiration": 20260814,
                        "strike": strike,
                        "right": "C",
                    },
                    "trade": {"date": 20260806, "ms_of_day": 36000000, "size": 1, "price": 1},
                }
            )
            is None
        )
    assert sum(int(item["count"]) for item in provider.rejected_event_diagnostics) == 1
    assert provider.rejected_event_overflow == 1
    assert all("trade_price" not in item for item in provider.rejected_event_diagnostics)
