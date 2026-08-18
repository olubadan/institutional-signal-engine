"""Standalone historical bootstrap runner -- incremental, resumable.

Replaces the previous run_standalone_bootstrap.py, which made one
monolithic call and wrote only a single final status file (so a failure
at symbol 400 left zero evidence). This runner:

- writes a durable per-symbol checkpoint the moment each symbol
  completes (via stol_bootstrap_incremental.historical_bootstrap_to_disk);
- appends an fsync'd receipt line per symbol (progress is observable
  from minute one);
- supports --resume (skips symbols with valid checkpoints);
- supports --symbols for the test ladder (1 symbol, then 5, then full);
- computes SUCCESS from candidate coverage over VALID checkpoints, not
  from an in-memory payload.

Exit codes: 0 = SUCCESS coverage, 2 = anything else (same as before).

Test ladder (run in order; do not start the full run until 1 and 2 pass):
  1) --symbols AAPL                    -> expect symbols/AAPL.json within
                                          minutes; RSS bounded (<~1 GB)
  2) --symbols AAPL,MSFT,NVDA,SPY,XLK  -> expect 5 checkpoints, flat RSS
  3) (no --symbols)                    -> full 501-symbol run

INTEGRATION NOTE FOR CODEX: verify the AlpacaEquitiesProvider
constructor mapping below matches what _historical_bootstrap_payload
passed (url=stream/feed url whose last path segment is the feed name;
historical_url=https://data.alpaca.markets).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import UTC, date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from stol_bootstrap_incremental import (  # noqa: E402
    completed_symbols,
    historical_bootstrap_to_disk,
)

from institutional_signal_engine.live_session import (  # noqa: E402
    _load_settings,
    _secret,
)
from institutional_signal_engine.providers.alpaca import (  # noqa: E402
    AlpacaEquitiesProvider,
)
from institutional_signal_engine.universe import PILOT_SYMBOLS  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--market-date", required=True)
    parser.add_argument(
        "--symbols",
        default=None,
        help="Comma-separated subset for the test ladder; omit for full universe.",
    )
    parser.add_argument("--concurrency", type=int, default=3)
    parser.add_argument("--per-symbol-deadline", type=float, default=900.0)
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Allow an existing output dir and skip valid checkpoints.",
    )
    args = parser.parse_args()

    if args.resume:
        args.output.mkdir(mode=0o700, parents=True, exist_ok=True)
    else:
        args.output.mkdir(mode=0o700, parents=True, exist_ok=False)

    started = datetime.now(UTC)
    session = date.fromisoformat(args.market_date)
    settings = _load_settings()

    if args.symbols:
        universe = tuple(
            symbol.strip().upper()
            for symbol in args.symbols.split(",")
            if symbol.strip()
        )
        candidate = set(universe)
    else:
        universe = (*PILOT_SYMBOLS, "SPY", "XLK")
        candidate = {str(symbol).upper() for symbol in PILOT_SYMBOLS}

    provider = AlpacaEquitiesProvider(
        url=settings.alpaca_data_url,
        key_id=_secret(settings.alpaca_key_id),
        secret_key=_secret(settings.alpaca_secret_key),
        historical_url="https://data.alpaca.markets",
    )

    try:
        summary = asyncio.run(
            historical_bootstrap_to_disk(
                provider,
                universe,
                session,
                args.output,
                concurrency=args.concurrency,
                per_symbol_deadline=args.per_symbol_deadline,
            )
        )
        completed = completed_symbols(args.output)
        coverage_complete = candidate.issubset(completed)
        status = {
            "status": "SUCCESS"
            if coverage_complete
            else "INCOMPLETE_BASELINE_COVERAGE",
            "started_at": started.isoformat(),
            "finished_at": datetime.now(UTC).isoformat(),
            "candidate_symbol_count": len(candidate),
            "baseline_symbol_count": len(completed),
            "coverage_complete": coverage_complete,
            "source": "alpaca:stocks/bars:completed-regular-sessions",
            "missing_symbols": sorted(candidate - completed)[:25],
            "summary": summary,
        }
        result = 0 if coverage_complete else 2
    except Exception as exc:  # sanitized status only
        status = {
            "status": "FAILED",
            "started_at": started.isoformat(),
            "finished_at": datetime.now(UTC).isoformat(),
            "candidate_symbol_count": len(candidate),
            "error_type": type(exc).__name__,
        }
        result = 2

    (args.output / "HISTORICAL_BOOTSTRAP_STATUS.json").write_text(
        json.dumps(status, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(status, sort_keys=True))
    return result


if __name__ == "__main__":
    raise SystemExit(main())
