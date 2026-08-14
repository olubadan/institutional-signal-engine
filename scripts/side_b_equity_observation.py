"""Signal-only Alpaca Side-B evidence runner; never contacts order endpoints."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
from typing import Any
from uuid import uuid4

from institutional_signal_engine.config import Settings
from institutional_signal_engine.live_session import PILOT_SYMBOLS
from institutional_signal_engine.providers.alpaca import AlpacaEquitiesProvider
from institutional_signal_engine.providers.common import ProviderError
from institutional_signal_engine.side_b import ForwardOutcomeTracker, SideBJournal


def _secret(value: Any) -> str:
    if value is None:
        raise RuntimeError("provider credential is not configured")
    return value.get_secret_value()


async def run(seconds: float, output: Path) -> dict[str, object]:
    settings = Settings.from_env_file(
        os.environ.get("RUNTIME_ENV_FILE", "/etc/institutional-signal-engine/runtime.env")
    )
    if settings.trading_enabled:
        raise RuntimeError("Side-B runner refuses trading-enabled configuration")
    journal = SideBJournal(output / "SIDE_B.jsonl")
    tracker = ForwardOutcomeTracker(journal)
    run_id = str(uuid4())
    symbols = tuple(sorted({str(symbol).upper() for symbol in PILOT_SYMBOLS}))
    provider = AlpacaEquitiesProvider(
        settings.alpaca_data_url,
        _secret(settings.alpaca_key_id),
        _secret(settings.alpaca_secret_key),
        timeout=30.0,
    )
    journal.append(
        "session.start",
        {
            "run_id": run_id,
            "git_commit": os.environ.get("SIDE_B_GIT_COMMIT", "UNSET"),
            "candidate_symbol_count": len(symbols),
            "symbols_sha256": __import__("hashlib").sha256(",".join(symbols).encode()).hexdigest(),
            "feed": settings.alpaca_data_feed,
            "data_url": settings.alpaca_data_url,
            "trading_enabled": False,
            "orders_constructed": 0,
            "orders_submitted": 0,
        },
    )
    events: list[object] = []

    async def collect() -> str:
        try:
            async for event in provider.events(symbols):
                events.append(event)
                tracker.observe(event)
        except ProviderError as exc:
            journal.append("provider.error", {"provider": "alpaca", "category": exc.category})
            return f"failed:{exc.category}"
        except asyncio.CancelledError:
            raise
        return "healthy"

    task = asyncio.create_task(collect())
    try:
        try:
            status = await asyncio.wait_for(asyncio.shield(task), timeout=seconds)
        except TimeoutError:
            status = "timeout"
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    health = await provider.health()
    journal.append(
        "session.final",
        {
            "status": status,
            "provider_health": health,
            "event_count": len(events),
            "trading_enabled": False,
            "orders_constructed": 0,
            "orders_submitted": 0,
        },
    )
    receipt = tracker.close()
    summary = {
        "run_id": run_id,
        "candidate_symbol_count": len(symbols),
        "requested_symbol_count": len(provider.subscription_requested_symbols),
        "accepted_symbol_count": len(provider.subscription_accepted_symbols),
        "rejected_symbol_count": len(provider.subscription_rejected_symbols),
        "subscription_acknowledged": provider.subscription_acknowledged,
        "authenticated": provider.authenticated,
        "event_count": len(events),
        "provider_status": status,
        "journal_receipt": receipt,
        "trading_enabled": False,
        "orders_constructed": 0,
        "orders_submitted": 0,
    }
    (output / "SUMMARY.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seconds", type=float, default=30.0)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(mode=0o700, parents=True, exist_ok=True)
    print(json.dumps(asyncio.run(run(args.seconds, args.output)), sort_keys=True))


if __name__ == "__main__":
    main()
