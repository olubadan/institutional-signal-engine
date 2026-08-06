from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import uuid4

import httpx
import pytest

from institutional_signal_engine.config import Settings
from institutional_signal_engine.persistence import InMemoryRepository
from institutional_signal_engine.providers.thetadata import (
    ThetaContract,
    ThetaDataDiscoveryClient,
)
from institutional_signal_engine.schemas import CanonicalEvent, EventKind
from institutional_signal_engine.signals import decide
from institutional_signal_engine.synchronization import Synchronizer
from institutional_signal_engine.universe import (
    ContractEvidence,
    Phase4UniverseSelector,
    SymbolEligibilityEvidence,
    UniverseLimitExceeded,
    UniverseSelection,
    selection_audits,
    subscription_plan,
)


class FakeOptionProvider:
    def __init__(self, expirations: tuple[int, ...], strikes: tuple[int, ...]) -> None:
        self._expirations = expirations
        self._strikes = strikes
        self.calls: list[tuple[str, str, int]] = []

    def expirations(self, symbol: str, as_of: int) -> tuple[int, ...]:
        del as_of
        self.calls.append((symbol, "expirations", 0))
        return self._expirations

    def strikes(self, symbol: str, expiration: int) -> tuple[int, ...]:
        self.calls.append((symbol, "strikes", expiration))
        return self._strikes

    def evidence(
        self, symbol: str, expiration: int, strikes: tuple[int, ...]
    ) -> tuple[ContractEvidence, ...]:
        self.calls.append((symbol, "evidence", expiration))
        return tuple(
            ContractEvidence(
                ThetaContract(symbol, expiration, strike, "C"),
                Decimal(100),
                Decimal(1),
                Decimal("1.01"),
                2_000,
                10_000,
            )
            for strike in strikes
        )


def eligible(symbol: str = "AAPL") -> SymbolEligibilityEvidence:
    return SymbolEligibilityEvidence(
        symbol, Decimal(10000000000), Decimal(50000000), True, Decimal(2000)
    )


def test_each_universe_eligibility_failure_is_explicit():
    fields: tuple[tuple[str, Any], ...] = (
        ("market_cap", None),
        ("average_daily_dollar_volume", None),
        ("listed_options", False),
        ("average_options_volume", None),
    )
    for field, value in fields:
        evidence = eligible().__class__(**{**eligible().__dict__, field: value})
        included, reasons = evidence.evaluate()
        assert not included
        assert reasons


def test_nearest_expiration_and_moneyness_selection_are_deterministic():
    provider = FakeOptionProvider((20260804, 20260807, 20260814), (100000, 120000, 150000))
    selected = Phase4UniverseSelector(provider).select_symbol(eligible(), 20260805)
    assert selected.included
    assert selected.expiration == 20260807
    assert [contract.strike for contract in selected.contracts] == [100000, 120000]
    assert selected.provenance == (
        "eligibility",
        "nearest_future_expiration",
        "liquidity",
        "moneyness",
    )


def test_deduplication_and_stable_subscription_plan():
    contract = ThetaContract("AAPL", 20260807, 310000, "C")
    selection = UniverseSelection("AAPL", True, 20260807, (contract, contract), (), (), ())
    assert subscription_plan((selection, selection)) == (contract,)


def test_exact_limit_is_allowed_and_overflow_fails_closed():
    contracts = tuple(
        ContractEvidence(
            ThetaContract("AAPL", 20260807, 100000 + index, "C"),
            Decimal(100),
            Decimal(1),
            Decimal("1.01"),
            2_000,
            10_000,
        )
        for index in range(15_001)
    )

    class LargeProvider(FakeOptionProvider):
        def __init__(self) -> None:
            super().__init__((20260807,), ())

        def evidence(
            self, symbol: str, expiration: int, strikes: tuple[int, ...]
        ) -> tuple[ContractEvidence, ...]:
            del symbol, expiration, strikes
            return contracts

    provider = LargeProvider()
    with pytest.raises(UniverseLimitExceeded):
        Phase4UniverseSelector(provider).select_pilot((eligible(),), 20260805)


def test_partial_discovery_failure_is_recorded_as_exclusion():
    provider = FakeOptionProvider((), ())
    selection = Phase4UniverseSelector(provider).select_symbol(eligible("MSFT"), 20260805)
    assert not selection.included
    assert selection.rejection_reasons == ("no_liquid_near_term_call_contract",)
    assert "expiration_discovery" in selection.provenance


def test_universe_audit_persists_and_replays():
    repository = InMemoryRepository()
    run_id = uuid4()
    contract = ThetaContract("AAPL", 20260807, 310000, "C")
    selection = UniverseSelection("AAPL", True, 20260807, (contract,), (), ("fixture",), ())
    audit = selection_audits(run_id, (selection,), (contract,))[0]
    repository.record_universe(audit)
    assert tuple(repository.replay_universe(run_id)) == (audit,)


def test_observational_oi_provenance_reaches_synchronized_decision():
    now = datetime(2026, 8, 6, 14, 0, tzinfo=UTC)
    synchronizer = Synchronizer(max_staleness=timedelta(minutes=1))
    payloads = {
        EventKind.EQUITY: {"price": Decimal(310), "volume": 200000, "spread": Decimal("0.01")},
        EventKind.OPTIONS: {
            "option_volume": 100,
            "open_interest": 10,
            "call_premium": Decimal(100000),
            "oi_date_source": "ALPACA_UNDATED",
            "open_interest_verified_as_of": False,
            "evidence_quality": "PHASE4_OBSERVATIONAL",
            "symbol_liquidity_evidence_source": "OWNER_APPROVED_PHASE4_PILOT",
            "symbol_liquidity_verified": False,
            "policy_version": "phase4-observation-liquidity-relaxation-v1",
            "indicator_provenance": {
                "oi_date_source": "ALPACA_UNDATED",
                "open_interest_verified_as_of": "False",
                "evidence_quality": "PHASE4_OBSERVATIONAL",
            },
        },
        EventKind.MARKET_INDEX: {"delta": Decimal("0.01")},
        EventKind.SECTOR_INDEX: {"delta": Decimal("0.01")},
    }
    for sequence, (kind, payload) in enumerate(payloads.items()):
        event = CanonicalEvent(
            event_id=uuid4(),
            kind=kind,
            symbol="AAPL",
            source="fixture",
            source_timestamp=now,
            received_timestamp=now,
            normalized_timestamp=now,
            sequence=sequence,
            payload=payload,
        )
        assert synchronizer.add(event)
    item = synchronizer.snapshot("AAPL", now)
    assert item is not None
    assert item.oi_date_source == "ALPACA_UNDATED"
    assert item.open_interest_verified_as_of is False
    assert item.evidence_quality == "PHASE4_OBSERVATIONAL"
    decision = decide([item], Settings())
    assert decision.indicator_provenance["options:oi_date_source"] == "ALPACA_UNDATED"
    assert decision.indicator_provenance["options:evidence_quality"] == "PHASE4_OBSERVATIONAL"


def test_discovery_http_500_is_sanitized_and_fail_closed(monkeypatch: pytest.MonkeyPatch):
    def fake_get(*args: object, **kwargs: object) -> httpx.Response:
        del args, kwargs
        return httpx.Response(500, json={"error": "server failure", "secret": "redacted"})

    monkeypatch.setattr(httpx, "get", fake_get)
    result = ThetaDataDiscoveryClient("http://127.0.0.1:25503").expirations("AAPL")
    assert result.status_code == 500
    assert result.response_shape == "dict"
    assert result.values == ()
    assert result.diagnostic == "http_500"
    assert "secret" not in repr(result)
