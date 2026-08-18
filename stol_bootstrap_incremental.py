"""Incremental, bounded-memory Alpaca historical bootstrap.

Fixes versus the deployed 48af43b path:

1. CONTAMINATION FIX: pagination accumulates rows PER SYMBOL. The old
   fetch_batch_once kept one flat `rows` list per batch and returned
   `{symbol: rows for symbol in batch}` -- every symbol in a daily batch
   (batch_size=20) received ALL 20 symbols' bars mixed together,
   silently corrupting previous closes, completed highs, and
   completed-day sets.

2. MEMORY FIX: symbols are fetched and reduced ONE AT A TIME (small
   worker pool, default 3). Raw minute bars are released immediately
   after each symbol's reduction. Nothing accumulates across the
   501-symbol universe.

3. DURABLE WRITES: each symbol's compact result is written to disk the
   moment it is computed (atomic tmp+rename, sha256 recorded), and an
   append-only fsync'd receipt line is emitted per symbol. Progress is
   observable from the first symbol, not after symbol 501.

4. RESUME: symbols whose checkpoint file exists with a valid hash are
   skipped, so a restart never refetches completed work.

Scientific semantics are UNCHANGED: same 400-day lookback, same
split adjustment, same 09:30-16:00 ET regular-session filter, same
minute-index math, same reducers (calculate_five_minute_baselines),
same provenance strings.

Usage: place this file next to the runner (e.g. /tmp) and run with the
repo venv python so `institutional_signal_engine` imports resolve.

INTEGRATION NOTES FOR CODEX (verify before full run):
- Import paths below assume the installed package layout; adjust if the
  repo uses different module paths.
- ProviderError is constructed as ProviderError("alpaca", reason,
  retryable) to match existing usage; verify signature.
- calculate_five_minute_baselines(symbol, rows_tuple, session,
  "split", provenance) call matches the existing call site exactly.
- A loader that reconstructs HistoricalBootstrap from these checkpoints
  for the live observer is a follow-up task; this module's job is to
  prove bounded, correct, durable completion.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx

from institutional_signal_engine.impact import calculate_five_minute_baselines
from institutional_signal_engine.indicators import ET
from institutional_signal_engine.providers.common import ProviderError

SCHEMA = "stol-bootstrap-symbol-v1"
PROVENANCE = "alpaca:stocks/bars:completed-regular-sessions"
RTH_START = datetime.min.time().replace(hour=9, minute=30)
RTH_END = datetime.min.time().replace(hour=16)


def _rss_kb() -> int:
    """Current process RSS in kB (Linux); -1 if unavailable."""
    try:
        with open("/proc/self/status", encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1])
    except OSError:
        pass
    return -1


def _jsonable(value: Any) -> Any:
    """Best-effort JSON-safe conversion for receipt payloads."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {
            ("|".join(map(str, key)) if isinstance(key, tuple) else str(key)): _jsonable(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_jsonable(item) for item in value]
    if hasattr(value, "__dict__"):
        return {k: _jsonable(v) for k, v in vars(value).items()}
    return str(value)


def _write_checkpoint(path: Path, body: dict[str, Any]) -> str:
    """Atomically write a checkpoint with an embedded sha256; return the hash."""
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    payload = dict(body)
    payload["sha256"] = digest
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, path)
    return digest


def checkpoint_valid(path: Path) -> bool:
    """True if the checkpoint exists and its recorded sha256 verifies."""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return False
    recorded = payload.pop("sha256", None)
    if not recorded:
        return False
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest() == recorded


def completed_symbols(checkpoint_dir: Path) -> set[str]:
    """Symbols with a valid checkpoint under checkpoint_dir/symbols."""
    symbols_dir = Path(checkpoint_dir) / "symbols"
    if not symbols_dir.is_dir():
        return set()
    return {
        entry.stem
        for entry in symbols_dir.glob("*.json")
        if checkpoint_valid(entry)
    }


async def _fetch_symbol_rows(
    client: httpx.AsyncClient,
    headers: dict[str, str],
    feed: str,
    symbol: str,
    timeframe: str,
    start: date,
    end: date,
) -> tuple[list[dict[str, Any]], int]:
    """Fetch all pages of bars for ONE symbol. Returns (rows, page_count).

    Per-symbol accumulation only -- structurally immune to the batch
    cross-contamination defect.
    """
    params: dict[str, str | int] = {
        "symbols": symbol,
        "timeframe": timeframe,
        "start": f"{start.isoformat()}T00:00:00Z",
        "end": f"{end.isoformat()}T23:59:59Z",
        "adjustment": "split",
        "feed": feed,
        "limit": 10000,
    }
    rows: list[dict[str, Any]] = []
    pages = 0
    token: str | None = None
    while True:
        if token is not None:
            params["page_token"] = token
        response: httpx.Response | None = None
        for attempt in range(3):
            try:
                response = await client.get("/v2/stocks/bars", headers=headers, params=params)
            except httpx.TimeoutException as exc:
                if attempt == 2:
                    raise ProviderError("alpaca", "historical_timeout", True) from exc
                await asyncio.sleep(0.5 * (attempt + 1))
                continue
            if response.status_code == 200:
                break
            if response.status_code == 429 or response.status_code >= 500:
                if attempt == 2:
                    raise ProviderError(
                        "alpaca", f"historical_http_{response.status_code}", True
                    )
                retry_after = response.headers.get("retry-after")
                try:
                    delay = min(10.0, max(0.5, float(retry_after or 0)))
                except ValueError:
                    delay = 0.5 * (attempt + 1)
                await asyncio.sleep(delay)
                continue
            raise ProviderError(
                "alpaca", f"historical_http_{response.status_code}", False
            )
        assert response is not None
        if response.status_code != 200:
            raise ProviderError(
                "alpaca", f"historical_http_{response.status_code}", True
            )
        body = await asyncio.to_thread(response.json)
        if not isinstance(body, dict):
            raise ProviderError("alpaca", "historical_malformed", False)
        bars = body.get("bars", {})
        if not isinstance(bars, dict):
            raise ProviderError("alpaca", "historical_malformed_bars", False)
        symbol_rows = bars.get(symbol, [])
        if isinstance(symbol_rows, list):
            rows.extend(row for row in symbol_rows if isinstance(row, dict))
        pages += 1
        token_value = body.get("next_page_token")
        token = str(token_value) if token_value else None
        if token is None:
            return rows, pages


async def _process_one_symbol(
    client: httpx.AsyncClient,
    headers: dict[str, str],
    feed: str,
    symbol: str,
    session: date,
    start: date,
    end: date,
    symbols_dir: Path,
) -> dict[str, Any]:
    """Fetch, reduce, and durably write ONE symbol. Returns a receipt dict.

    Raw rows are released before returning; retained memory per symbol
    is only the compact checkpoint payload while it is being written.
    """
    started_at = datetime.now(UTC).isoformat()

    # --- Daily bars: previous close, completed highs, completed days ---
    daily_rows, daily_pages = await _fetch_symbol_rows(
        client, headers, feed, symbol, "1Day", start, end
    )
    daily_rows.sort(key=lambda row: str(row.get("t", "")))
    completed: list[tuple[str, Decimal, Decimal]] = []
    for row in daily_rows:
        timestamp = datetime.fromisoformat(str(row["t"]))
        local_day = timestamp.astimezone(ET).date()
        if local_day >= session:
            continue
        close = Decimal(str(row["c"]))
        high = Decimal(str(row.get("h", row["c"])))
        completed.append((local_day.isoformat(), close, high))
    daily_rows.clear()
    if not completed:
        raise ProviderError("alpaca", "historical_baseline_missing", False)
    last_day, last_close, _ = completed[-1]
    completed_days = {day for day, _, _ in completed}
    highs = {day: str(high) for day, _, high in completed[-252:]}

    # --- Minute bars: cumulative volume profile + 5-minute baselines ---
    minute_rows, minute_pages = await _fetch_symbol_rows(
        client, headers, feed, symbol, "1Min", start, end
    )
    minute_bar_count = len(minute_rows)
    by_day: dict[str, dict[int, int]] = {}
    compact_rows: list[dict[str, Any]] = []
    for row in minute_rows:
        compact_rows.append(
            {"timestamp": row["t"], "volume": row.get("v", 0), "close": row.get("c")}
        )
        timestamp = datetime.fromisoformat(str(row["t"]))
        local = timestamp.astimezone(ET)
        if local.date().isoformat() not in completed_days or not (
            RTH_START <= local.time() < RTH_END
        ):
            continue
        minute_index = (local.hour * 60 + local.minute) - 570
        by_day.setdefault(local.date().isoformat(), {})[minute_index] = int(
            row.get("v", 0)
        )
    minute_rows.clear()  # release raw bars before the baseline reducer runs

    baselines = calculate_five_minute_baselines(
        symbol,
        tuple(compact_rows),
        session,
        "split",
        PROVENANCE,
    )
    compact_rows.clear()

    day_order = sorted(by_day)
    per_day_cumulative: list[list[int]] = []
    for day in day_order:
        volumes = by_day[day]
        running = 0
        cumulative: list[int] = []
        for minute_index in range(390):
            running += volumes.get(minute_index, 0)
            cumulative.append(running)
        per_day_cumulative.append(cumulative)
    profile = {
        str(minute_index): [day[minute_index] for day in per_day_cumulative]
        for minute_index in range(390)
    }
    by_day.clear()
    per_day_cumulative.clear()

    finished_at = datetime.now(UTC).isoformat()
    body: dict[str, Any] = {
        "schema": SCHEMA,
        "symbol": symbol,
        "session": session.isoformat(),
        "adjustment": "split",
        "source_provenance": PROVENANCE,
        "previous_close": {
            "close": str(last_close),
            "day": last_day,
            "provenance": "alpaca:stocks/bars:adjustment=split",
        },
        "completed_highs": highs,
        "completed_day_count": len(completed_days),
        "profile_day_order": day_order,
        "cumulative_profile": profile,
        "impact_baselines": _jsonable(baselines),
        "daily_pages": daily_pages,
        "minute_pages": minute_pages,
        "minute_bar_count": minute_bar_count,
        "started_at": started_at,
        "finished_at": finished_at,
    }
    digest = _write_checkpoint(symbols_dir / f"{symbol}.json", body)
    return {
        "symbol": symbol,
        "status": "COMPLETE",
        "sha256": digest,
        "minute_bar_count": minute_bar_count,
        "daily_pages": daily_pages,
        "minute_pages": minute_pages,
        "started_at": started_at,
        "finished_at": finished_at,
    }


async def historical_bootstrap_to_disk(
    provider: Any,
    symbols: Any,
    session: date,
    checkpoint_dir: Path | str,
    *,
    concurrency: int = 3,
    per_symbol_deadline: float = 900.0,
) -> dict[str, Any]:
    """Run the full bootstrap with per-symbol durable writes and resume.

    provider: an AlpacaEquitiesProvider instance (uses its key_id,
    secret_key, url, historical_url, timeout attributes).
    """
    requested = tuple(sorted({str(symbol).upper() for symbol in symbols}))
    start = session - timedelta(days=400)
    end = session - timedelta(days=1)
    headers = {
        "APCA-API-KEY-ID": provider.key_id,
        "APCA-API-SECRET-KEY": provider.secret_key,
    }
    feed = provider.url.rsplit("/", 1)[-1]
    checkpoint_dir = Path(checkpoint_dir)
    symbols_dir = checkpoint_dir / "symbols"
    symbols_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    receipts_path = checkpoint_dir / "SYMBOL_RECEIPTS.jsonl"
    receipts_lock = asyncio.Lock()
    sequence = 0

    async def emit_receipt(record: dict[str, Any]) -> None:
        nonlocal sequence
        async with receipts_lock:
            sequence += 1
            line = {
                "seq": sequence,
                "ts": datetime.now(UTC).isoformat(),
                "rss_kb": _rss_kb(),
                **record,
            }
            with receipts_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(line, sort_keys=True) + "\n")
                handle.flush()
                os.fsync(handle.fileno())

    pending = [
        symbol
        for symbol in requested
        if not checkpoint_valid(symbols_dir / f"{symbol}.json")
    ]
    resumed_skip_count = len(requested) - len(pending)
    completed: set[str] = set(requested) - set(pending)
    failed: dict[str, str] = {}
    peak_rss = _rss_kb()

    queue: asyncio.Queue[str] = asyncio.Queue()
    for symbol in pending:
        queue.put_nowait(symbol)

    async with httpx.AsyncClient(
        base_url=provider.historical_url, timeout=provider.timeout
    ) as client:

        async def worker() -> None:
            nonlocal peak_rss
            while True:
                try:
                    symbol = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                try:
                    async with asyncio.timeout(per_symbol_deadline):
                        receipt = await _process_one_symbol(
                            client, headers, feed, symbol, session,
                            start, end, symbols_dir,
                        )
                    completed.add(symbol)
                    await emit_receipt(receipt)
                except ProviderError as exc:
                    reason = (
                        str(exc.args[1])
                        if len(exc.args) > 1
                        else type(exc).__name__
                    )
                    failed[symbol] = reason
                    await emit_receipt(
                        {"symbol": symbol, "status": "FAILED", "error_type": reason}
                    )
                except TimeoutError:
                    failed[symbol] = "symbol_deadline_exceeded"
                    await emit_receipt(
                        {
                            "symbol": symbol,
                            "status": "FAILED",
                            "error_type": "symbol_deadline_exceeded",
                        }
                    )
                current = _rss_kb()
                if current > peak_rss:
                    peak_rss = current

        workers = [
            asyncio.create_task(worker())
            for _ in range(max(1, int(concurrency)))
        ]
        await asyncio.gather(*workers)

    coverage_complete = set(requested).issubset(completed)
    return {
        "status": "SUCCESS" if coverage_complete else "INCOMPLETE_BASELINE_COVERAGE",
        "requested_symbol_count": len(requested),
        "completed_symbol_count": len(completed),
        "resumed_skip_count": resumed_skip_count,
        "failed_symbol_count": len(failed),
        "failed_symbols": dict(sorted(failed.items())[:25]),
        "checkpoint_dir": str(checkpoint_dir),
        "receipts_path": str(receipts_path),
        "peak_rss_kb": peak_rss,
    }
