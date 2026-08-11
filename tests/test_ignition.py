import pytest
from pydantic import SecretStr

from institutional_signal_engine.config import Settings
from institutional_signal_engine.ignition import (
    _alpaca_authentication,
    evaluate_ignition,
    validate_subscription_plan,
)
from institutional_signal_engine.persistence import InMemoryRepository
from institutional_signal_engine.persistence_async import AsyncAuditWriter, AuditWrite


def test_ignition_passes_only_when_every_check_passes():
    assert evaluate_ignition({"commit": True, "trading_disabled": True}).passed
    result = evaluate_ignition({"commit": True, "trading_disabled": False})
    assert not result.passed
    assert result.reasons == ("trading_disabled",)


def test_ignition_result_is_sanitized_and_orders_are_zero():
    payload = evaluate_ignition({"secret_present": True}).as_dict()
    assert payload["trading_enabled"] is False
    assert payload["orders_constructed"] == 0
    assert payload["orders_submitted"] == 0


def test_empty_and_over_capacity_plans_fail_closed():
    assert not validate_subscription_plan({"selected_plan": []})
    assert not validate_subscription_plan(
        {
            "selected_plan": [{}] * 10_001,
            "subscription_counts": {"trade_submitted": 10_001, "quote_submitted": 10_001},
        }
    )


def test_valid_plan_is_paired_and_within_provider_limits():
    assert validate_subscription_plan(
        {
            "selected_plan": [{"root": "AAPL"}],
            "subscription_counts": {"trade_submitted": 1, "quote_submitted": 1},
        }
    )


def test_ignition_uses_typed_alpaca_provider_authentication(monkeypatch):
    calls: list[tuple[str, ...]] = []

    class FakeProvider:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        async def current_prices(self, symbols: tuple[str, ...]) -> dict[str, int]:
            calls.append(symbols)
            return {"SPY": 1}

    from institutional_signal_engine import ignition

    monkeypatch.setattr(ignition, "AlpacaEquitiesProvider", FakeProvider)
    settings = Settings(
        alpaca_key_id=SecretStr("fixture-key"),
        alpaca_secret_key=SecretStr("fixture-secret"),
    )
    assert _alpaca_authentication(settings)
    assert calls == [("SPY",)]


@pytest.mark.asyncio
async def test_persistence_writer_probe_drains_without_accepted_event_leak():
    repository = InMemoryRepository()
    writer = AsyncAuditWriter(repository, soft_limit=2, hard_limit=4, batch_size=1)
    writer.start()
    assert writer.enqueue(AuditWrite(probe_id="fixture-probe"))
    await writer.drain()
    assert repository.probes == ["fixture-probe"]
    assert writer.queue.empty()
