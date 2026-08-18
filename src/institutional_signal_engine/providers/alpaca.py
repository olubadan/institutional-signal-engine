"""Alpaca live equities market-data WebSocket adapter.

Authentication follows Alpaca's official Trading/Broker WebSocket contract:
the key pair is sent in an auth message within ten seconds of connection.
"""

import asyncio
import json
from collections.abc import AsyncIterator, Iterable
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from time import monotonic_ns
from typing import Any
from uuid import NAMESPACE_URL, uuid5

import httpx
import websockets

from ..historical import HistoricalBootstrap
from ..impact import calculate_five_minute_baselines
from ..indicators import ET, PreviousClose
from ..schemas import CanonicalEvent, EventKind
from .common import ProviderError, reconnecting_stream


class AlpacaEquitiesProvider:
    def __init__(
        self,
        url: str,
        key_id: str,
        secret_key: str,
        timeout: float = 10.0,
        historical_url: str = "https://data.alpaca.markets",
    ) -> None:
        self.url, self.key_id, self.secret_key, self.timeout = url, key_id, secret_key, timeout
        self.historical_url = historical_url.rstrip("/")
        self.authenticated = False
        self.connection_generation = 0
        self.subscription_acknowledged = False
        self.subscription_requested_symbols: tuple[str, ...] = ()
        self.subscription_accepted_symbols: tuple[str, ...] = ()
        self.subscription_rejected_symbols: tuple[str, ...] = ()

    @staticmethod
    def _authentication_result(messages: list[dict[str, Any]]) -> bool:
        if any(message.get("T") == "error" for message in messages):
            raise ProviderError("alpaca", "authentication_failed", False)
        return any(
            message.get("T") == "success" and message.get("msg") == "authenticated"
            for message in messages
        )

    async def _connection(self, symbols: Iterable[str]) -> AsyncIterator[CanonicalEvent]:
        try:
            requested = tuple(sorted({str(symbol).upper() for symbol in symbols}))
            self.subscription_requested_symbols = requested
            self.subscription_acknowledged = False
            async with websockets.connect(
                self.url, open_timeout=self.timeout, ping_interval=20, ping_timeout=10
            ) as ws:
                self.connection_generation += 1
                await ws.send(
                    json.dumps({"action": "auth", "key": self.key_id, "secret": self.secret_key})
                )
                async with asyncio.timeout(self.timeout):
                    while not self.authenticated:
                        auth = json.loads(await ws.recv())
                        self.authenticated = self._authentication_result(auth)
                self.authenticated = True
                await ws.send(
                    json.dumps(
                        {
                            "action": "subscribe",
                            "trades": list(requested),
                            "quotes": list(requested),
                        }
                    )
                )
                async for raw in ws:
                    ingress_wall_timestamp = datetime.now(UTC)
                    ingress_monotonic_ns = monotonic_ns()
                    for message in json.loads(raw):
                        if message.get("T") == "subscription":
                            self.subscription_acknowledged = True
                            accepted: set[str] = set()
                            for key in ("trades", "quotes"):
                                values = message.get(key, [])
                                if isinstance(values, list):
                                    accepted.update(str(value).upper() for value in values)
                            self.subscription_accepted_symbols = tuple(sorted(accepted))
                            self.subscription_rejected_symbols = tuple(
                                sorted(set(requested) - accepted)
                            )
                            continue
                        if message.get("T") == "error":
                            raise ProviderError("alpaca", "stream_rejected", False)
                        event = self._normalize(
                            message,
                            ingress_wall_timestamp=ingress_wall_timestamp,
                            ingress_monotonic_ns=ingress_monotonic_ns,
                        )
                        if event is not None:
                            yield event
        except ProviderError:
            raise
        except websockets.ConnectionClosed as exc:
            raise ProviderError("alpaca", "connection_closed", True) from exc
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
            raise ProviderError("alpaca", "malformed_message", True) from exc

    def _normalize(
        self,
        message: dict[str, Any],
        *,
        ingress_wall_timestamp: datetime | None = None,
        ingress_monotonic_ns: int | None = None,
    ) -> CanonicalEvent | None:
        kind = message.get("T")
        if kind not in {"t", "q"}:
            return None
        timestamp = datetime.fromisoformat(message["t"]).astimezone(UTC)
        symbol = str(message["S"]).upper()
        sequence = int(message.get("i", timestamp.timestamp() * 1_000_000_000))
        price = message.get("p", message.get("ap"))
        if price is None:
            return None
        ask = message.get("ap")
        bid = message.get("bp")
        quote_validity = (
            "VALID"
            if kind == "q"
            and bid is not None
            and ask is not None
            and float(bid) > 0
            and float(ask) >= float(bid)
            else "INVALID"
            if kind == "q"
            else None
        )
        payload = {
            "provider_event_kind": "trade" if kind == "t" else "quote",
            "timestamp_conversion": {
                "precision_converted": False,
                "timezone_converted": not str(message["t"]).endswith("Z"),
            },
            "price": price,
            "volume": int(message.get("s", 0)) if kind == "t" else 0,
            "spread": max(0, float(ask or price) - float(bid or price)),
            "conditions": tuple(message.get("c", [])),
            "exchange": message.get("x"),
            "quote_context": {"bid": bid, "ask": ask},
            "quote_validity": quote_validity,
            "feature_reasons": ("requires_historical_baseline", "requires_stateful_calculation"),
            "_received_monotonic_ns": ingress_monotonic_ns,
            "_normalized_monotonic_ns": monotonic_ns(),
        }
        return CanonicalEvent(
            event_id=uuid5(NAMESPACE_URL, f"alpaca:{symbol}:{timestamp.isoformat()}:{sequence}"),
            kind=EventKind.EQUITY,
            symbol=symbol,
            source="alpaca",
            source_timestamp=timestamp,
            received_timestamp=ingress_wall_timestamp or datetime.now(UTC),
            normalized_timestamp=timestamp,
            sequence=sequence,
            payload=payload,
        )

    async def events(self, symbols: Iterable[str]) -> AsyncIterator[CanonicalEvent]:
        async for event in reconnecting_stream("alpaca", lambda: self._connection(symbols)):
            yield event

    async def historical_bootstrap(
        self, symbols: Iterable[str], session: date
    ) -> HistoricalBootstrap:
        """Load split-adjusted completed-session baselines from Alpaca bars."""
        requested = tuple(sorted({symbol.upper() for symbol in symbols}))
        start = session - timedelta(days=400)
        end = session - timedelta(days=1)
        headers = {
            "APCA-API-KEY-ID": self.key_id,
            "APCA-API-SECRET-KEY": self.secret_key,
        }

        async def fetch(
            timeframe: str,
            on_batch: Any | None = None,
        ) -> dict[str, list[dict[str, Any]]]:
            async with httpx.AsyncClient(
                base_url=self.historical_url, timeout=self.timeout
            ) as client:
                # Alpaca accepts a comma-separated symbol set for bars. Batch
                # requests keep the 501-symbol candidate universe intact while
                # avoiding one synchronous request per symbol.
                # Keep the full candidate universe, but bound response size and
                # concurrent pressure.  A transient timeout on a large batch
                # is retried as smaller batches rather than aborting the whole
                # bootstrap.
                # Minute history is materially larger than daily history.  A
                # multi-symbol minute batch retains every decoded bar until
                # the batch completes, which can exhaust the host before the
                # compact baseline reducer runs.  Keep the full universe but
                # bound the in-memory raw-bar working set to one symbol for
                # minute history.  Daily history remains batched because its
                # response is small.
                batch_size = 1 if timeframe == "1Min" else 20
                semaphore = asyncio.Semaphore(1 if timeframe == "1Min" else 3)

                async def fetch_batch_once(
                    batch: tuple[str, ...],
                ) -> dict[str, list[dict[str, Any]]]:
                    params: dict[str, str | int] = {
                        "symbols": ",".join(batch),
                        "timeframe": timeframe,
                        "start": f"{start.isoformat()}T00:00:00Z",
                        "end": f"{end.isoformat()}T23:59:59Z",
                        "adjustment": "split",
                        "feed": self.url.rsplit("/", 1)[-1],
                        "limit": 10000,
                    }
                    rows: list[dict[str, Any]] = []
                    token: str | None = None
                    while True:
                        if token is not None:
                            params["page_token"] = token
                        response: httpx.Response | None = None
                        for attempt in range(3):
                            try:
                                response = await client.get(
                                    "/v2/stocks/bars", headers=headers, params=params
                                )
                            except httpx.TimeoutException as exc:
                                if attempt == 2:
                                    raise ProviderError(
                                        "alpaca", "historical_timeout", True
                                    ) from exc
                                await asyncio.sleep(0.5 * (attempt + 1))
                                continue
                            if response.status_code == 200:
                                break
                            if response.status_code == 429 or response.status_code >= 500:
                                if attempt == 2:
                                    raise ProviderError(
                                        "alpaca",
                                        f"historical_http_{response.status_code}",
                                        True,
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
                        # Large 50-symbol historical pages can contain a
                        # substantial number of minute bars. JSON
                        # decoding is synchronous CPU work; keep it off
                        # the event-loop thread so the live observer can
                        # reach provider activation while bootstrap
                        # continues in the background.
                        body = await asyncio.to_thread(response.json)
                        if not isinstance(body, dict):
                            raise ProviderError("alpaca", "historical_malformed", False)
                        bars = body.get("bars", {})
                        if not isinstance(bars, dict):
                            raise ProviderError("alpaca", "historical_malformed_bars", False)
                        for symbol in batch:
                            symbol_rows = bars.get(symbol, [])
                            if isinstance(symbol_rows, list):
                                rows.extend(row for row in symbol_rows if isinstance(row, dict))
                        token_value = body.get("next_page_token")
                        token = str(token_value) if token_value else None
                        if token is None:
                            return {symbol: rows for symbol in batch}

                async def fetch_batch(
                    batch: tuple[str, ...],
                ) -> dict[str, list[dict[str, Any]]]:
                    try:
                        async with semaphore:
                            return await fetch_batch_once(batch)
                    except ProviderError as exc:
                        if not exc.retryable or len(batch) <= 1:
                            raise
                        midpoint = max(1, len(batch) // 2)
                        left, right = await asyncio.gather(
                            fetch_batch(batch[:midpoint]),
                            fetch_batch(batch[midpoint:]),
                        )
                        merged: dict[str, list[dict[str, Any]]] = {}
                        for symbol in batch:
                            merged[symbol] = [*left.get(symbol, []), *right.get(symbol, [])]
                        return merged

                batches = tuple(
                    requested[offset : offset + batch_size]
                    for offset in range(0, len(requested), batch_size)
                )
                merged: dict[str, list[dict[str, Any]]] = {symbol: [] for symbol in requested}
                batch_tasks = [asyncio.create_task(fetch_batch(batch)) for batch in batches]
                for completed_task in asyncio.as_completed(batch_tasks):
                    batch_rows = await completed_task
                    if on_batch is not None:
                        await on_batch(batch_rows)
                        continue
                    for symbol, rows in batch_rows.items():
                        merged[symbol].extend(rows)
                return merged

        daily = await fetch("1Day")
        previous: dict[str, PreviousClose] = {}
        highs: dict[str, dict[str, Decimal]] = {}
        profiles: dict[str, dict[int, tuple[Decimal, ...]]] = {}
        impact_baselines = {}
        completed_days_by_symbol: dict[str, set[str]] = {}
        for symbol in requested:
            await asyncio.sleep(0)
            daily_rows = sorted(daily[symbol], key=lambda row: str(row.get("t", "")))
            completed: list[tuple[str, Decimal, Decimal]] = []
            for row in daily_rows:
                timestamp = datetime.fromisoformat(str(row["t"]))
                local_day = timestamp.astimezone(ET).date()
                if local_day >= session:
                    continue
                close = Decimal(str(row["c"]))
                high = Decimal(str(row.get("h", row["c"])))
                completed.append((local_day.isoformat(), close, high))
            if not completed:
                raise ProviderError("alpaca", "historical_baseline_missing", False)
            last_day, last_close, _ = completed[-1]
            previous[symbol] = PreviousClose(
                symbol,
                last_close,
                datetime.fromisoformat(f"{last_day}T00:00:00+00:00"),
                "alpaca:stocks/bars:adjustment=split",
            )
            highs[symbol] = {day: high for day, _, high in completed[-252:]}
            completed_days_by_symbol[symbol] = {day for day, _, _ in completed}

        async def process_minute_batch(
            batch_rows: dict[str, list[dict[str, Any]]],
        ) -> None:
            for symbol, minute_rows in batch_rows.items():
                await asyncio.sleep(0)
                completed_days = completed_days_by_symbol[symbol]
                by_day: dict[str, dict[int, int]] = {}
                for row in minute_rows:
                    timestamp = datetime.fromisoformat(str(row["t"]))
                    local = timestamp.astimezone(ET)
                    if local.date().isoformat() not in completed_days or not (
                        datetime.min.time().replace(hour=9, minute=30)
                        <= local.time()
                        < datetime.min.time().replace(hour=16)
                    ):
                        continue
                    minute_index = (local.hour * 60 + local.minute) - 570
                    by_day.setdefault(local.date().isoformat(), {})[minute_index] = int(
                        row.get("v", 0)
                    )
                daily_profiles: list[tuple[Decimal, ...]] = []
                for day_volumes in by_day.values():
                    running = 0
                    cumulative: list[Decimal] = []
                    for minute_index in range(390):
                        running += day_volumes.get(minute_index, 0)
                        cumulative.append(Decimal(running))
                    daily_profiles.append(tuple(cumulative))
                profiles[symbol] = {
                    minute_index: tuple(day[minute_index] for day in daily_profiles)
                    for minute_index in range(390)
                }
                impact_baselines.update(
                    calculate_five_minute_baselines(
                        symbol,
                        tuple(
                            {
                                "timestamp": row["t"],
                                "volume": row.get("v", 0),
                                "close": row.get("c"),
                            }
                            for row in minute_rows
                        ),
                        session,
                        "split",
                        "alpaca:stocks/bars:completed-regular-sessions",
                    )
                )

        await fetch("1Min", on_batch=process_minute_batch)
        for symbol in requested:
            profiles.setdefault(symbol, {minute_index: () for minute_index in range(390)})
        return HistoricalBootstrap(
            session=session.isoformat(),
            previous_closes=previous,
            cumulative_profiles=profiles,
            completed_highs=highs,
            adjustment="split",
            source_provenance="alpaca:stocks/bars:completed-regular-sessions",
            impact_baselines=impact_baselines,
        )

    async def current_prices(self, symbols: Iterable[str]) -> dict[str, Decimal]:
        """Read current last-trade prices for moneyness selection."""
        requested = tuple(sorted({symbol.upper() for symbol in symbols}))
        headers = {
            "APCA-API-KEY-ID": self.key_id,
            "APCA-API-SECRET-KEY": self.secret_key,
        }
        async with httpx.AsyncClient(base_url=self.historical_url, timeout=self.timeout) as client:
            try:
                response = await client.get(
                    "/v2/stocks/snapshots",
                    headers=headers,
                    params={"symbols": ",".join(requested), "feed": self.url.rsplit("/", 1)[-1]},
                )
            except httpx.TimeoutException as exc:
                raise ProviderError("alpaca", "snapshot_timeout", True) from exc
        if response.status_code != 200:
            raise ProviderError("alpaca", f"snapshot_http_{response.status_code}", False)
        body: object = response.json()
        if not isinstance(body, dict):
            raise ProviderError("alpaca", "snapshot_malformed", False)
        prices: dict[str, Decimal] = {}
        for symbol, snapshot in body.items():
            if not isinstance(snapshot, dict):
                continue
            trade = snapshot.get("latestTrade") or snapshot.get("latest_trade")
            if isinstance(trade, dict) and trade.get("p") is not None:
                prices[str(symbol).upper()] = Decimal(str(trade["p"]))
        return prices

    async def health(self) -> dict[str, object]:
        return {
            "provider": "alpaca",
            "configured": bool(self.key_id and self.secret_key),
            "url": self.url,
            "subscription_acknowledged": self.subscription_acknowledged,
            "requested_symbol_count": len(self.subscription_requested_symbols),
            "accepted_symbol_count": len(self.subscription_accepted_symbols),
            "rejected_symbol_count": len(self.subscription_rejected_symbols),
        }
