"""Alpaca-catalogued, ThetaData-streamed Phase 4 pilot smoke runner."""

import argparse
import asyncio
import json
import os
from collections import Counter
from datetime import datetime, timedelta
from uuid import uuid4
from zoneinfo import ZoneInfo

from .config import Settings
from .contract_mapping import map_alpaca_contract, round_trip_validate
from .live_smoke import _secret
from .live_smoke import run as run_signal_smoke
from .persistence import InMemoryRepository, PostgresRepository
from .providers.alpaca import AlpacaEquitiesProvider
from .providers.alpaca_options import AlpacaOptionsContractProvider
from .providers.common import ProviderError
from .universe import (
    PILOT_SYMBOLS,
    AlpacaContractSelector,
    UniverseManifest,
    subscription_plan,
)


def _load_settings() -> Settings:
    runtime_file = os.environ.get(
        "RUNTIME_ENV_FILE", "/etc/institutional-signal-engine/runtime.env"
    )
    settings = (
        Settings.from_env_file(runtime_file)
        if os.path.exists(runtime_file)
        else Settings.from_env()
    )
    if settings.trading_enabled:
        raise RuntimeError("phase4_live_smoke refuses trading-enabled configuration")
    return settings


async def run(seconds: float) -> dict[str, object]:
    settings = _load_settings()
    alpaca = AlpacaEquitiesProvider(
        settings.alpaca_data_url,
        _secret(settings.alpaca_key_id),
        _secret(settings.alpaca_secret_key),
    )
    catalog = AlpacaOptionsContractProvider(
        "https://paper-api.alpaca.markets",
        _secret(settings.alpaca_key_id),
        _secret(settings.alpaca_secret_key),
    )
    run_id = uuid4()
    as_of = datetime.now(ZoneInfo("America/New_York")).date()
    try:
        prices = await alpaca.current_prices(PILOT_SYMBOLS)
        discovered = [
            contract
            async for contract in catalog.discover_active_calls(
                PILOT_SYMBOLS,
                limit=100,
                expiration_date_gte=as_of + timedelta(days=7),
                expiration_date_lte=as_of + timedelta(days=45),
            )
        ]
    except ProviderError as exc:
        return {
            "run_id": str(run_id),
            "trading_enabled": False,
            "status": "blocked_provider",
            "provider": "alpaca",
            "reason": exc.category,
            "orders_constructed": 0,
            "orders_submitted": 0,
        }
    mapping_results = tuple(
        round_trip_validate(map_alpaca_contract(contract)) for contract in discovered
    )
    selections = AlpacaContractSelector().select(
        mapping_results, prices, as_of, symbols=PILOT_SYMBOLS
    )
    plan = subscription_plan(selections)
    manifest = UniverseManifest(
        run_id,
        as_of,
        tuple(PILOT_SYMBOLS),
        selections,
        plan,
        mapping_records=tuple(result.record() for result in mapping_results),
    )
    repository = (
        PostgresRepository(settings.database_url) if settings.database_url else InMemoryRepository()
    )
    if isinstance(repository, PostgresRepository):
        repository.initialize()
    repository.record_universe(manifest.record())
    if isinstance(repository, PostgresRepository):
        repository.flush()
    report: dict[str, object] = {
        "run_id": str(run_id),
        "trading_enabled": False,
        "symbols_evaluated": list(PILOT_SYMBOLS),
        "symbols_included": [selection.symbol for selection in selections if selection.included],
        "symbols_excluded": {
            selection.symbol: list(selection.rejection_reasons)
            for selection in selections
            if not selection.included
        },
        "alpaca_contracts_received": len(discovered),
        "contracts_accepted": sum(1 for result in mapping_results if result.accepted),
        "contracts_rejected": sum(1 for result in mapping_results if not result.accepted),
        "mapping_failures": [
            {"field": field, "reason": reason, "count": count}
            for (field, reason), count in sorted(
                Counter(
                    (result.failed_field or "unknown", result.rejection_reason or "unknown")
                    for result in mapping_results
                    if not result.accepted
                ).items()
            )
        ],
        "selected_contracts": [
            {
                "symbol": selection.symbol,
                "expiration": selection.expiration,
                "contracts": [
                    {
                        "root": contract.root,
                        "expiration": contract.expiration,
                        "strike": contract.strike,
                        "right": contract.right,
                    }
                    for contract in selection.contracts
                ],
            }
            for selection in selections
            if selection.included
        ],
        "subscription_plan_count": len(plan),
        "manifest_persisted": True,
        "orders_constructed": 0,
        "orders_submitted": 0,
    }
    if not plan:
        report["status"] = "blocked_no_contracts_selected"
        report["reason"] = "mandatory_liquidity_evidence_unavailable_or_mapping_failure"
        return report
    signal_report = await run_signal_smoke(
        seconds,
        symbols=tuple(selection.symbol for selection in selections if selection.included),
        contracts=plan,
        request_types=("TRADE", "QUOTE"),
    )
    report.update(signal_report)
    report["status"] = "live_observation_complete"
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seconds", type=float, default=60.0)
    arguments = parser.parse_args()
    try:
        result = asyncio.run(run(arguments.seconds))
    except (ProviderError, RuntimeError, ValueError) as exc:
        result = {
            "status": "blocked",
            "reason": type(exc).__name__,
            "trading_enabled": False,
            "orders_constructed": 0,
            "orders_submitted": 0,
        }
    print(json.dumps(result, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
